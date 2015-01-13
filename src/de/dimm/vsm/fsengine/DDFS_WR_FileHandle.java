/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsengine.hashcache.HashCache;
import de.dimm.vsm.CS_Constants;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.StatCounter;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.backup.IBackupHelper;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Administrator
 */

/*
 *  Wenn Datei neu ist, dass ist handleList leer
 *  Wenn Datei existiert, dass ist handleList mit bestehendem Inhalt vorgeladen
 *
 *
 *
 *
 */

class DDHandleManager
{
    public static final int MAX_DIRTY_BLOCKS_FOR_FLUSH = 20;

    int blockSize;
    DDHandle lastBlock;
    DDFS_WR_FileHandle fh;
    
      
    final List<DDHandle> openHandles;
    final Map<Long,DDHandle> openPosMap;
    Map<Long,DDHandle> existingPosMap;
    

    DDHandleManager( DDFS_WR_FileHandle fh)
    {
        //handleList = new ArrayList<>();
        //posMap = new HashMap<>();
        this.fh = fh;
        this.blockSize = fh.blockSize;
        lastBlock = null;       
        openHandles = new ArrayList<>();
        openPosMap = new HashMap<>();
        existingPosMap = null;
    }

    DDHandle createHandle( long offset, int len, byte[] data)
    {
        DDHandle handle = new DDHandle(offset, len, data);        
        return handle;
    }

    DDHandle getBlockForOffset(long offset, String name)
    {
        long blockOffset = (offset / blockSize) * blockSize;
        return existingPosMap.get(blockOffset);
    }
    void readExistingPosMap()  throws IOException, SQLException
    {
        existingPosMap = new  HashMap<>();
        List<DDHandle> handleList;
        if (!fh.isCreate())
        {
            if (!fh.stream)
            {
                handleList = fh.buildHashBlockList( fh.attrs);
            }
            else
            {
                handleList = fh.buildXABlockList( fh.attrs);
            }
        }
        else
        {
            // CREATE WITH NEW
            handleList = new ArrayList<>();
        }
        
        
        for (Iterator<DDHandle> it = handleList.iterator(); it.hasNext();)
        {
            DDHandle dDHandle = it.next();
            existingPosMap.put(dDHandle.pos, dDHandle);
        }
        fh.origlen = (fh.stream) ? fh.attrs.getStreamSize() : fh.attrs.getFsize();        
    }
    
    void readExistingBlocks() throws IOException
    {
        if (existingPosMap == null)
        {
            try
            {
                readExistingPosMap();
            }

            catch (SQLException sQLException)
            {
                throw new IOException("cannot read pos map");
            }
        }        
    }

    DDHandle rereadBlock(DDHandle handle ) throws IOException
    {
        readExistingBlocks();
        
        DDHandle readhandle = existingPosMap.get(handle.pos);
        if (readhandle != null)
        {
            if (readhandle.len != handle.len)
                throw new IOException("Invalid first Block len "  + readhandle.len + "/" +  handle.len);
            fh.checkAndReadBlock( readhandle);
            handle = readhandle;
        }
        return handle;
    }

    /**
     * Adds all Blocks needed for a data area
     * @param offset
     * @param len
     */
    void addBlocks( long offset, long len, long maxLen) throws UnsupportedEncodingException, IOException
    {
        readExistingBlocks();
 
        long blockOffset = (offset / blockSize) * blockSize;
        
        // ADJUST SIZE IF NECESSARY
        long fileLen = getUnflushedLength();
        if (fileLen < (offset + len))
        {
            fileLen = offset + len;
            //fh.setNodeSize(fileLen);            
        }
       
        while (blockOffset < offset + len)
        {
            int newBlockSize = blockSize;

            // Are we at the end of a file
            if (blockOffset + newBlockSize > fileLen)
            {
                // ONLY CREATE PARTIAL BLOCK FOR NEW DATA
                newBlockSize = (int)(fileLen - blockOffset);                
            }

            DDHandle existHandle = existingPosMap.get(blockOffset);
            if (existHandle != null)
            {
                // Fall A: wir schreiben nur einen teil des Blocks neu: Block komplett lesen
                if (newBlockSize < existHandle.len)
                {
                    Log.debug("Old Block", "Read full Block for partial write: " + existHandle);
                    existHandle = rereadBlock( existHandle);
                }

                // Fall B: Wir nicht von Blockanfang: Block Lesen
                if (offset > blockOffset)
                {
                    Log.debug("Old Block", "Read full Block for offset write: " + existHandle);
                    existHandle = rereadBlock( existHandle);
                }

                // Fall C: Block hat neue Größe
                if (newBlockSize > existHandle.len)
                {
                    existHandle.resize(newBlockSize);
                }
                existingPosMap.put(blockOffset, existHandle);
                blockOffset += newBlockSize;
                lastBlock = existHandle;
                continue;
            }


            DDHandle newHandle = createHandle(blockOffset, newBlockSize, null);
            newHandle.setDirty();
            // WE DO NOT NEED TO READ; AS THIS BLOCK IS A NEW BLOCK
            newHandle.unread = false;

            // READ EXISTING DATA IF NECESSARY
            if (blockOffset < offset)
            {
                Log.debug("New Block", "Read first block partial: " + newHandle);
                newHandle = rereadBlock( newHandle);
            }
            Log.debug("New Block", "Block: " + newHandle);
            existingPosMap.put(blockOffset, newHandle);

            blockOffset += newBlockSize;
            lastBlock = newHandle;
        }
                
    }
    
    List<DDHandle> getHandles( long pos, int len )
    {
        if (existingPosMap.isEmpty())
        {
            return null;
        }
        List<DDHandle> ret = new ArrayList<>();
        long startOffset = (pos / blockSize) * blockSize;   
        while (startOffset < pos + len)
        {
            DDHandle hb = existingPosMap.get(Long.valueOf(startOffset));
            ret.add(hb);
            startOffset += blockSize;
        }
        return ret;
    }    
    
   

    public DDHandle getLastBlock()
    {
        return lastBlock;
    }

    boolean needsFlush()
    {
        boolean doFlush = false;
        Collection<DDHandle> handles = existingPosMap.values();

        int dirtyCnt = 0;
        for (Iterator<DDHandle> it = handles.iterator(); it.hasNext();)
        {
            DDHandle dDHandle = it.next();
             if (dDHandle.isDirty() && dDHandle.isWrittenComplete())
            {
                dirtyCnt++;
                if (dirtyCnt > MAX_DIRTY_BLOCKS_FOR_FLUSH)
                {
                    doFlush = true;
                    break;
                }
            }
        }
        return doFlush;
    }
    List<DDHandle> getHandles()
    {
        Collection<DDHandle> ret =  existingPosMap.values();
        List<DDHandle> list = new ArrayList<>(ret);

        Collections.sort(list, new Comparator<DDHandle>() {

            @Override
            public int compare( DDHandle o1, DDHandle o2 )
            {
                if (o2.pos < o1.pos)
                    return 1;
                if (o2.pos == o1.pos)
                    return 0;
                return -1;
            }
        });
        return list;
    }  
    long getUnflushedLength()
    {
        long ret = fh.length();
        if (ret < fh.maxUnFlushedWritePos)
            ret = fh.maxUnFlushedWritePos;
        
        return ret;
    }
    
    protected void ensure_open( long pos, int len, String rafMode ) throws IOException
    {
        readExistingBlocks();
         
        if (fh.isDirectory())
        {
            throw new IOException("ensure_open Node " + fh.fh.getName() + " -> " + fh.fh.getAbsolutePath() + " fails, is a directory");
        }

        // CLOSE UNNECESSARY HANDLES
        for (int i = 0; i < openHandles.size(); i++)
        {
            DDHandle dDHandle = openHandles.get(i);
            if (dDHandle.isDirty())
                continue;
            if (pos + len <= dDHandle.pos || pos >= dDHandle.pos + dDHandle.len)
            {
                // CLEAR DATA BUT DO NOT REMOVE
                dDHandle.close();
//                openHandles.remove(dDHandle);
//                openPosMap.remove(Long.valueOf(dDHandle.pos));
//                i--;
            }
        }
        
       
        if (!existingPosMap.isEmpty())
        {           
            long startOffset = (pos / blockSize) * blockSize;                        
            while (startOffset < pos + len && startOffset < getUnflushedLength())
            {
                Long posKey = Long.valueOf(startOffset);
                if (!openPosMap.containsKey(posKey))
                {                    
                    DDHandle dDHandle = existingPosMap.get(posKey);
                    // IF WE HAVE TO READ EXISTING DATA, WE DO SO
                    if (dDHandle != null)
                    {
                        if (dDHandle.isUnread())
                        {
                            AbstractStorageNode snode = fh.getStorageNodeForBlock( dDHandle);
                            if (snode == null) {
                                throw new IOException("Speichernode für " + fh.fh.getName() + " nicht gefunden oder nicht valide"); 
                            }
                            dDHandle.open(snode, rafMode);
                        }
                        openHandles.add(dDHandle);           
                        openPosMap.put(Long.valueOf(dDHandle.pos), dDHandle);
                    }
                }
                startOffset += blockSize;
            }
        }

        // OPEN LANDING ZONE FILE IF EXISTENT
        if (fh.fh != null && fh.fh.length() > 0)
        {
            if (fh.raf == null)
            {
                fh.raf = new RandomAccessFile(fh.fh, "r");
            }
        }
    }    
    void resizeExistingBlocks( long size ) 
    {
        Collection<DDHandle> collection = existingPosMap.values();
        for (Iterator<DDHandle> it = collection.iterator(); it.hasNext();)
        {
            DDHandle dDHandle = it.next();
            
            // Block too far out
            if (dDHandle.pos >= size)
            {
                it.remove();
            }
            // Block ist last block and too big ?
            else if (dDHandle.pos + dDHandle.len > size)
            {
                dDHandle.resize((int)(size - dDHandle.pos));
            }
            // Check every other block if it is big enough
            else if (dDHandle.pos + dDHandle.len < size)
            {
                long newBlockSize = size - dDHandle.pos;
                if (newBlockSize > blockSize)
                    newBlockSize = blockSize;
                if (dDHandle.len != newBlockSize)
                {
                    dDHandle.resize((int)newBlockSize);
                }
            }
        }
    }    
}


public final class DDFS_WR_FileHandle implements FileHandle, IBackupHelper
{
    
    protected File fh;
    protected FileSystemElemNode node;
    protected FileSystemElemAttributes attrs;
    protected StoragePoolHandler spHandler;
    protected AbstractStorageNode fsNode;
    protected boolean directory;
    protected boolean create;
    protected boolean stream;
    protected boolean verbose;
    protected RandomAccessFile raf;    
    

    
    fr.cryptohash.Digest digest = null;
    StatCounter stat;
    HashCache hashCache;
    protected FSEIndexer indexer;
    int streamInfo;
    
    int blockSize;

    DDHandleManager hm;
    long origlen;
    long maxUnFlushedWritePos;
    long biggestWrittenBlockPos = 0;

    
   // OPEN ALL NECESSARY BLOCKS FOR READ OPERATION, THIS CAN SPAN MORE THAN ONE DEDUP BLOCK (IF POS AND LEN CROSS BLOCK BOUNDARY OR IF LEN > BLOCKLEN OR BOTH)
    
    protected void ensure_open( long pos, int len, String rafMode ) throws IOException
    {        
        hm.ensure_open(pos, len, rafMode);
    }
    
    public AbstractStorageNode getFsNode()
    {
        return fsNode;
    }

    public StoragePoolHandler getSpHandler()
    {
        return spHandler;
    }

    public boolean isCreate()
    {
        return create;
    }

    public boolean isDirectory()
    {
        return directory;
    }

    public boolean isStream()
    {
        return stream;
    }

    public FileSystemElemNode getNode()
    {
        return node;
    }

    
   
    protected DDFS_WR_FileHandle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node, boolean create, boolean isStream ) throws IOException
    {
        this(fs_node, sp_handler, node, node.getAttributes(), create, isStream);
    }
    protected DDFS_WR_FileHandle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node, FileSystemElemAttributes attrs, boolean create, boolean isStream ) throws IOException
    {
        this.node = node;
        this.attrs = attrs;
        this.fsNode = fs_node;
        this.spHandler = sp_handler;
        this.directory = node.isDirectory();
        this.create = create;
        this.stream = isStream;        
        blockSize =  Main.get_int_prop(GeneralPreferences.FILE_HASH_BLOCKSIZE, CS_Constants.FILE_HASH_BLOCKSIZE);
        hm = new DDHandleManager(this);
        
        // Wenn wir als create gestartet worden -> Truncate to 0
       /* if (create)
        {            
            setNodeSize(0);
        }*/
        // TODO: Optimize
        hm.readExistingBlocks();        
}
    

    public void setStreamInfo( int streamInfo )
    {
        this.streamInfo = streamInfo;
    }

    @Override
    public void truncateFile( long size ) throws IOException, PoolReadOnlyException
    {
        if (spHandler.isReadOnly(node))
            throw new PoolReadOnlyException("Cannot truncateFile to dedup FS");

        hm.resizeExistingBlocks( size );
               
        setNodeSize(size);
        origlen = size;
        
        try
        {
            setAttributes(size);
        }
        catch (SQLException sQLException)
        {
            throw new IOException("Cannot update size");
        }                         
        origlen = size;
    }
    
   
    // Prüfen, ob der Block noch gelesen werden muss
    void checkAndReadBlock(DDHandle actBlock) throws UnsupportedEncodingException, IOException
    {
        if (actBlock.isUnread())
        {
            try
            {
                Log.debug("checkAndReadBlock", "Block: " + actBlock);
                FileHandle fHandle = getSpHandler().open_dedupblock_handle(actBlock.dhb, /*create*/ false);
                actBlock.openRead(fHandle);
                fHandle.close();
            }
            catch (PathResolveException ex)
            {
                throw new IOException("Fehler bei checkAndReadBlock", ex);
            }
        }  
        if (!actBlock.isUnread() && actBlock.getData() == null)
            ;
    }
    
    void initStaticData()
    {
        if (digest == null)
        {           
            digest = new fr.cryptohash.SHA1();
            
            hashCache = LogicControl.getStorageNubHandler().getHashCache(getSpHandler().getPool());
            indexer = LogicControl.getStorageNubHandler().getIndexer(getSpHandler().getPool());
            if (!indexer.isOpen())
            {
                indexer.open();
            }
            stat = new StatCounter("DDFS Write");
        }        
    }

    @Override
    public synchronized void writeBlock( String hashValue, byte[] data, int length, long offset ) throws IOException, PathResolveException, PoolReadOnlyException, UnsupportedEncodingException, SQLException
    {
        initStaticData();
        getSpHandler().check_open_transaction();
        
        // Zum Eintragen hier den aktuellen Zeitpunkt verwenden, wir wissen nicht, 
        // wenn die Attribute geschrieben werden, aber auf jeden Fall später als jetzt
        // Falls nicht, haben wir zumindestens den TS vom letzten gültigen Attribut
        long ts = node.getAttributes().getTs();
        
        DedupHashBlock dhb = check_for_existing_block(hashValue);
        if (dhb != null)
        {
            HashBlock hb = getSpHandler().create_hashentry(node, hashValue, dhb, offset, length, /*reorganize*/ false, ts);
            node.getHashBlocks().addIfRealized(hb);

            // UPDATE BOOTSTRAP
            getSpHandler().getWriteRunner().addBootstrap(getSpHandler(), dhb, hb);
            stat.addDedupBlock(dhb);
            Log.debug("Found DHB " + dhb.toString());
        }
        else
        {
            if (data == null)
                throw new IOException("Data fehlt bei write block, hashvalue " + hashValue + " ist unbekannt" );
            
            dhb = Backup.createDedupHashBlock(this, node, hashValue, streamInfo, offset, length, isStream(), ts);
            if (dhb != null)
            {
                FileHandle fHandle = getSpHandler().open_dedupblock_handle(dhb, /*create*/ true);
                getSpHandler().getWriteRunner().addAndCloseElem(fHandle, data, length, 0);
//                fHandle.writeFile( data, length, /*offset*/ 0);
//                fHandle.close();

                updateHashBlock( isStream(), hashValue, dhb, offset, length, ts );
                stat.addTransferBlock();
               
                Log.debug("Added DHB " + dhb.toString());
            }
            else
            {
                // Cannot happen, Exception is thrown before
                throw new SQLException("Could not create DHB");
            }
        }  
        if (offset + length > biggestWrittenBlockPos)
        {
            biggestWrittenBlockPos = offset + length;        
        }
    }
    
    

    @Override
    public synchronized void writeFile( byte[] b,  int length, long offset ) throws IOException, PoolReadOnlyException, UnsupportedEncodingException
    {
        Log.debug("writeFile", "Writing " + length + "  byte at offset " + offset );
        // Eventuell offene Blöcke schreiben
        checkForFlush( false );

        // Neue fehlende Blöcke der Lücke Schreiben
        hm.addBlocks( offset, length, node.getAttributes().getFsize());

        // Ersten betroffener Block ermitteln
        DDHandle actBlock = hm.getBlockForOffset(offset, this.node.getName());
        if (actBlock == null)
                throw new IOException("Block kann nicht angelegt werden");

        Log.debug("writeFile", "ActBlock " + actBlock );

        // Zu schreibende Länge ermitteln:
        int writeDataLen = length;

        // Offset innerhalb des  ersten betroffenen Block;
        int offsetInBlock = (int)(offset - actBlock.pos);
        
        // Ist Länge mit Offset im Block zu groß für diesen Block ?
        if (writeDataLen + offsetInBlock > actBlock.len)
        {
            writeDataLen = actBlock.len - offsetInBlock;
        }

        // OKAY;
        // offsetInBlock ist gesetzt
        // writeDataLen ist gesetzt
        // wir haben einen Block zum Schreiben, jetzt gehts los
        long writePos = offset;
        int writtenData = 0;
        while (writtenData < length)
        {
            actBlock.arraycopy(b, writtenData, offsetInBlock, writeDataLen);

            writtenData += writeDataLen;
            writePos += writeDataLen;
            
            if (maxUnFlushedWritePos < writePos)
                maxUnFlushedWritePos = writePos;

            // Ist noch mehr für einen nächsten Block?
            if (writtenData < length)
            {
                actBlock = hm.getBlockForOffset(writePos, this.node.getName());

                // Restlänge für nächsten Block
                int rest = length - writtenData;
                if (rest > blockSize)
                {
                    rest = blockSize;
                }

                offsetInBlock = 0;
                writeDataLen = rest;
            }
        }
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        if (spHandler.isReadOnly(node))
            throw new PoolReadOnlyException("Cannot delete in dedup FS");
        return false;
    }

    private void checkForFlush(boolean onClose) throws IOException
    {                
        boolean doFlush = false;
        boolean lenChanged = false;
        if (!onClose)
        {
            doFlush = hm.needsFlush();
        }
        else
        {    
            long newLen = 0;
            Collection<DDHandle> handles = hm.getHandles();
            for (Iterator<DDHandle> it = handles.iterator(); it.hasNext();)
            {
                DDHandle handle = it.next();
                long maxPos = handle.pos + handle.len;
                if (maxPos > newLen)
                {
                    lenChanged = true;
                }

                // Ist beschrieben worden ?
                if (handle.isDirty())
                {
                    doFlush = true;
                }
                if (lenChanged || doFlush)
                    break;
            }
        }

        // Kein Flush und keine Attributsänderung notwendig?
        if (!doFlush && !lenChanged && biggestWrittenBlockPos == 0)
            return;                

        getSpHandler().check_open_transaction();
        FileSystemElemAttributes newAttr = getNode().getAttributes();

        initStaticData();

        try
        {                        
            long newLen = biggestWrittenBlockPos;
            Collection<DDHandle> handles = hm.getHandles();
            for (Iterator<DDHandle> it = handles.iterator(); it.hasNext();)
            {
                DDHandle handle = it.next();
                
                long maxPos = handle.pos + handle.len;
                if (maxPos > newLen)
                    newLen = maxPos;

                // Ist beschrieben worden ?
                if (!handle.isDirty())
                    continue;

                Log.debug("checkForFlush", "Flushing " + handle);
                
                byte[] hash = digest.digest(handle.getData());
                String hashValue = CryptTools.encodeUrlsafe(hash);
                
                long ts = newAttr.getTs();

                DedupHashBlock dhb = check_for_existing_block(hashValue);
                if (dhb != null)
                {
                    HashBlock hb = getSpHandler().create_hashentry(node, hashValue, dhb, handle.pos, handle.len, /*reorganize*/ false, ts);
                    node.getHashBlocks().addIfRealized(hb);

                    // UPDATE BOOTSTRAP
                    getSpHandler().getWriteRunner().addBootstrap(getSpHandler(), dhb, hb);
                    stat.addDedupBlock(dhb);
                    Log.debug("Found DHB " + dhb.toString());
                }
                else
                {
                    dhb = Backup.createDedupHashBlock(this, node, hashValue, streamInfo, handle.pos, handle.len, isStream(), ts);
                    if (dhb != null)
                    {
                        FileHandle fHandle = getSpHandler().open_dedupblock_handle(dhb, /*create*/ true);
                        fHandle.writeFile(handle.getData(), handle.len, /*offset*/ 0);
                        fHandle.close();

                        updateHashBlock( isStream(), hashValue, dhb, handle.pos, handle.len, ts );
                        stat.addTransferBlock();
                        stat.addTransferLen( handle.len );
                        Log.debug("Added DHB " + dhb.toString());
                    }
                    else
                    {
                        // Cannot happen, Exception is thrown before
                        throw new SQLException("Could not create DHB");
                    }
                }  
                // We have written data, discard it and mark as unread
                // TODO: speed up
                handle.dhb = dhb;
                if (handle.getData().length == blockSize)
                {
                    handle.setUnread();
                }
            }

            // New MaxSize            
            if (onClose && newLen > origlen)
            {
                setAttributes( newLen );   
                origlen = newLen;
            }
            maxUnFlushedWritePos = 0;
            //getSpHandler().commit_transaction();
        }
        catch (IOException | PathResolveException | PoolReadOnlyException | SQLException exc)
        {
            Log.err("Fehler bei flush in DDFS_WR", ": " +  exc.getMessage(), exc);
            throw new IOException( exc.getMessage(), exc);
        }
    }
    
    void setAttributes( long newLen) throws SQLException
    {
        initStaticData();        
        
        boolean createNewAttribute = false;
        FileSystemElemAttributes newAttr = getNode().getAttributes();
        long actTs = System.currentTimeMillis();
        // Nur bei Änderungen, die länger als MIN_FILECHANGE_THRESHOLD_S existieren, wird ein neues Attribut vergeben
        if (getSpHandler().isFileChangePersitent(node))
        {
            newAttr = new FileSystemElemAttributes(newAttr);
            createNewAttribute = true;            
        }

        // M-Time setzen, nicht gelöscht
        newAttr.setDeleted(false);
        newAttr.setModificationDateMs(actTs); 
        if (isStream())
            newAttr.setStreamSize(newLen);
        else
            newAttr.setFsize(newLen);
        
        Log.debug("Updating attribute: " + newAttr);
        getSpHandler().mergeOrPersistAttribute(node, newAttr, createNewAttribute, actTs);
        
        
        //getSpHandler().write_bootstrap_data(newAttr);
        indexer.addToIndexAsync(node.getAttributes(), /*ar-Job*/null);                
    }
//    
//    void updateFseAttribute(FileSystemElemNode fsenode, FileSystemElemAttributes newAttributes, long len) throws SQLException
//    {
////        todo: Beim Write und anschließendem Lesen sind nicht alle Hashblöcke in hbList from Node
//        if (isStream())
//            fsenode.getAttributes().setStreamSize(len);
//        else
//            fsenode.getAttributes().setFsize(len);
//
//        Log.debug("updateFseAttribute len " +  len + " Node:" + fsenode.getIdx() + " Attr:" + fsenode.getAttributes().getIdx() + " " +  fsenode.getAttributes() );
//        getSpHandler().check_open_transaction();
//        getSpHandler().em_merge(fsenode.getAttributes());
//        getSpHandler().check_commit_transaction();
//    }
//
//    void createNewFseAttribute(FileSystemElemNode fsenode, FileSystemElemAttributes newAttributes, long len) throws SQLException
//    {
//        long ts = newAttributes.getTs();
//        newAttributes.setAccessDateMs( ts);
//
//        newAttributes.setModificationDateMs( ts);
//        if (!isStream())
//            newAttributes.setFsize(len);
//        else
//            newAttributes.setStreamSize( len);
//
//
//        Log.debug("createNewFseAttribute len " +  len + " Node:" + fsenode.getIdx() + " Attr:" + fsenode.getAttributes().getIdx() + " " +  fsenode.getAttributes() );
//
//        getSpHandler().check_open_transaction();
//
//        newAttributes.setFile(node);
//        getSpHandler().em_persist(newAttributes);
//        fsenode.setAttributes(newAttributes);
//        getSpHandler().em_merge(fsenode);
//        getSpHandler().check_commit_transaction();
//    }

    void updateHashBlock( boolean isXa, String remote_hash, DedupHashBlock dhb, long offset, int read_len, long ts) throws PoolReadOnlyException, SQLException, IOException, PathResolveException
    {
        if (isXa)
        {
            // ADD HASHENTRY TO DB
            XANode xa = getSpHandler().create_xa_hashentry(node, remote_hash, streamInfo, dhb, offset, read_len, /*reorganize*/ true,  ts);

            node.getXaNodes().addIfRealized(xa);

            // UPDATE BOOTSTRAP
            getSpHandler().getWriteRunner().addBootstrap( getSpHandler(), dhb, xa);
        }
        else
        {
            // REGISTER HASHBLOCK TO DB
            HashBlock hb = getSpHandler().create_hashentry(node, remote_hash, dhb, offset, read_len, /*reorganize*/ true, ts);

            node.getHashBlocks().addIfRealized(hb);

            // UPDATE BOOTSTRAP
            getSpHandler().getWriteRunner().addBootstrap( getSpHandler(), dhb, hb);
        }

        // WRITE TO CACHE
        if (hashCache.isInited())
        {
            hashCache.addDhb(remote_hash, dhb.getIdx());
            stat.setDhbCacheSize( hashCache.size() );
        }
    }
    
    DedupHashBlock check_for_existing_block(String hash) throws PathResolveException, UnsupportedEncodingException, IOException
    {
        DedupHashBlock dhb = Backup.check_for_existing_block( getSpHandler(), stat, hashCache, hash );
        if (dhb != null)
        {
            FileHandle file = getSpHandler().check_exist_dedupblock_handle(dhb);
            if (file == null || !file.exists())
            {
                Log.err("Filesystemblock nicht gefunden für Hash", ": " + dhb.getHashvalue());
                return null;
            }
        }
        return dhb;
    }


    @Override
    public StoragePoolHandler getPoolHandler()
    {
        return getSpHandler();
    }

    @Override
    public StatCounter getStat()
    {
        return stat;
    }

    @Override
    public HashCache getHashCache()
    {
        return hashCache;
    }

    @Override
    public void flushWriteRunner() throws IOException, InterruptedException
    {
        // Do nothing
    }


     @Override
    public boolean exists()
    {
        try
        {
            hm.ensure_open(0, 0, DDHandle.RAF_RD_ONLY);
            close();
            return true;
        }
        catch (Exception iOException)
        {
        }
        return false;
    }


    protected List<DDHandle> buildHashBlockList( FileSystemElemAttributes attrs ) throws IOException
    {
        List<HashBlock> hbList = node.getHashBlocks().getList(spHandler.getEm());

        // IN CASE WE HAVE WRITTEN THIS PRESENTLY, WE HAVE TO REREAD LIST
        if (hbList instanceof LazyList)
        {
            LazyList lhb = (LazyList) hbList;
            lhb.unRealize();
        }

        if (verbose)
        {
            System.out.println("HBLIST 1: " + hbList.size());
        }

        if (!hbList.isEmpty())
        {
            // BUILD SORTED LIST FOR THIS FILE BASED ON poolQry
            hbList = Restore.filter_hashblocks(hbList, attrs);
        }

        if (verbose)
        {
            System.out.println("HBLIST 2: " + hbList.size());
        }

        List<DDHandle> ret = new ArrayList<>();

        for (int i = 0; i < hbList.size(); i++)
        {
            HashBlock hb = hbList.get(i);
            if (hb.getDedupBlock() != null)
            {
                ret.add(new DDHandle(hb));
            }
        }

        if (verbose)
        {
            System.out.println("HBLIST 3: " + ret.size());
        }

        // SANITY CHECK
        long offset = 0;
        for (int i = 0; i < ret.size(); i++)
        {
            DDHandle hb = ret.get(i);
            if (offset != hb.pos)
            {
                System.out.println("HashBlocks");
                for (int l = 0; l < hbList.size(); l++)
                {
                    HashBlock _hb = hbList.get(l);
                    System.out.println(_hb.toString().toString() + " dhb:" + _hb.getDedupBlock());
                }

                System.out.println("DDHandles");
                for (int l = 0; l < ret.size(); l++)
                {
                    DDHandle _ddhb = ret.get(l);
                    System.out.println("Pos: " + _ddhb.pos + " Len: " + _ddhb.len + " DHB: " + _ddhb.dhb.toString());
                }

                throw new IOException("Invalid Block order");
            }
            offset += hb.len;
        }

        if (verbose)
        {
            System.out.println("HBLIST 4: " + ret.size());
        }
        return ret;
    }

    List<DDHandle> buildXABlockList( FileSystemElemAttributes attrs ) throws IOException, SQLException
    {
        List<XANode> hbList = spHandler.createQuery("select T1 from XANode T1 where T1.fileNode_idx=" + node.getIdx(), XANode.class);

        if (verbose)
        {
            System.out.println("XALIST 1: " + hbList.size());
        }

        // IN CASE WE HAVE WRITTEN THIS PRESENTLY, WE HAVE TO REREAD LIST
        if (hbList instanceof LazyList)
        {
            LazyList lhb = (LazyList) hbList;
            lhb.unRealize();
        }

        if (!hbList.isEmpty())
        {
            // BUILD LIST FOR THIS FILE
            hbList = Restore.filter_xanodes(hbList, attrs);
        }
        if (verbose)
        {
            System.out.println("XALIST 2: " + hbList.size());
        }

        List<DDHandle> ret = new ArrayList<>();

        for (int i = 0; i < hbList.size(); i++)
        {
            XANode hb = hbList.get(i);
            if (hb.getDedupBlock() != null)
            {
                ret.add(new DDHandle(hb));
            }
        }
        // SANITY CHECK
        long offset = 0;
        for (int i = 0; i < ret.size(); i++)
        {
            DDHandle hb = ret.get(i);
            if (offset != hb.pos)
            {
                throw new IOException("Invalid Block order");
            }
            offset += hb.len;
        }
        if (verbose)
        {
            System.out.println("XALIST 3: " + ret.size());
        }


        return ret;
    }
    
    @Override
    public long length()
    {        
        return getNodelLen();
    } 
    
    void setNodeSize(long size) throws IOException
    {        
        Log.debug("setNodeSize " + size);
        if (isStream())
            node.getAttributes().setStreamSize(size);
        else
            node.getAttributes().setFsize(size);        
                             
    }
    
    @Override
    public void close() throws IOException
    {
        checkForFlush(true);

        for (DDHandle dDHandle:  hm.openHandles)
        {
            dDHandle.close();
        }
        hm.openHandles.clear();
        hm.openPosMap.clear();

        if (hm.existingPosMap != null)
        {
            for (DDHandle dDHandle: hm.existingPosMap.values())
            {                
                dDHandle.close();
            }
            hm.existingPosMap.clear();
        }

        if (raf != null)
        {
            raf.close();
            raf = null;
        }
        // Am Ende muss ein Commit aufgerufen werden wg. SetTime / FileSize
        try
        {
            getSpHandler().commit_transaction();
        }
        catch (SQLException sQLException)
        {
            Log.err("Fehler beim Committen in Close von " + node.getName(), sQLException);      
        }
        try {
            getSpHandler().getWriteRunner().flush();
            if (getSpHandler().getWriteRunner().isWriteError())
                throw new IOException("Fehler beim Schreiben in WriteRunner");
        }
        catch (InterruptedException ex) {
            Logger.getLogger(DDFS_WR_FileHandle.class.getName()).log(Level.SEVERE, null, ex);
        }
    }    

    AbstractStorageNode getStorageNodeForBlock(DDHandle ddh)
    {
        // LOOK FOR FIRST EXISTING NODE
        List<AbstractStorageNode> s_nodes = spHandler.get_primary_storage_nodes(/*forWrite*/ false);
        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);
            if (s_node.isFS())
            {
                StorageNodeHandler snHandler = spHandler.get_handler_for_node(s_node);
                try
                {
                    FileHandle lfh = snHandler.create_file_handle(ddh.dhb, false);
                    if (lfh.exists())
                    {
                        return s_node;
                    }
                }
                catch (PathResolveException | UnsupportedEncodingException exc)
                {
                    Log.err("getStorageNodeForBlock", exc);  
                }
            }
        }
        return null;
    }
    
    @Override
    public void force( boolean b ) throws IOException
    {
        // Mac calls this for fun
        //throw new IOException("DD Filesystem is readonly");
    }
          @Override
    public void create() throws IOException, PoolReadOnlyException
    {
        Log.err("Unused create in DDFS_WR_FIlehandle");        
    }

    @Override
    public synchronized byte[] read( int length, long offset ) throws IOException
    {
        byte[] b = new byte[length];

        int rlen = read(b, length, offset);
        if (rlen == -1)
        {
            throw new IOException("Read error (-1)");
        }

        if (rlen != b.length)
        {
            byte[] bb = new byte[rlen];
            System.arraycopy(b, 0, bb, 0, rlen);
            b = bb;
        }
        return b;

    }
 @Override
    public synchronized int read( byte[] b, int length, long offset ) throws IOException
    {
        Log.debug("readFile len " +  length + " offs " + offset + " Node:" + node.getIdx() );
                
        if (verbose)
        {
            System.out.println("DD Read: " + offset + " " + length);
        }
        // SKIP EMPTY READS
        if (length == 0)
            return 0;


        // SKIP READ EOF
        if (offset >= hm.getUnflushedLength())
        {
            return 0;
        }
        

        hm.ensure_open(offset, length, DDHandle.RAF_RD_ONLY);

        List<DDHandle> handles = hm.getHandles(offset, length);

        if ((handles == null || handles.isEmpty()) && length > 0)
        {
            throw new IOException("Read error (-1)");
        }
        

        int byteRead = 0;
        int arrayOffset = 0;

        long actReadOffset = offset;
        for (DDHandle dDHandle: handles)
        {
            // CALC OFFSET INTO BLOCK
            if (dDHandle == null)
            {
                throw new IOException("no datat at offset " + actReadOffset);
            }
            
            // IS THIS BLOCK CORRECT FOR THIS DATA AREA?
            if (dDHandle.pos <= actReadOffset && dDHandle.pos + dDHandle.len > actReadOffset)
            {
                checkAndReadBlock( dDHandle );

                int fileOffset = (int) (actReadOffset - dDHandle.pos);

                // CALCULATE LENGTH WE CAN READ FROM THIS BLOCK
                int realLen = length - arrayOffset;
                if (realLen > dDHandle.len - fileOffset)
                {
                    realLen = dDHandle.len - fileOffset;
                }
                

                System.arraycopy(dDHandle.getData(), fileOffset, b, arrayOffset, realLen);

                // ADD THE LENGTH TO OFFSETS
                actReadOffset += realLen;
                arrayOffset += realLen;
                byteRead += realLen;
            }
        }
        // TODO READ RAF IF BLOCKS NOT AVAILABLE
        // MAYBE WE HAVE TO CREATE dDHandle WITH raf OF LANDINGZONE FILE

        return byteRead;
    }

    
    public static FileHandle create_versioned_fs_handle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node,FileSystemElemAttributes attrs) throws PathResolveException, IOException, SQLException
    {
        DDFS_WR_FileHandle fs = new DDFS_WR_FileHandle(fs_node, sp_handler, node, attrs, false, false);
        return fs;
    }
    
    public static FileHandle create_fs_handle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node, boolean create) throws PathResolveException, IOException, SQLException
    {
        DDFS_WR_FileHandle fs = new DDFS_WR_FileHandle(fs_node, sp_handler, node, create, false);
        return fs;
    }
    public static FileHandle create_fs_stream_handle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node, int streamInfo, boolean create ) throws PathResolveException, IOException, SQLException
    {
        DDFS_WR_FileHandle fs = new DDFS_WR_FileHandle(fs_node, sp_handler, node, create, true);
        fs.setStreamInfo(streamInfo);
        return fs;
    } 

    private long getNodelLen()
    {
        return isStream() ? node.getAttributes().getStreamSize() : node.getAttributes().getFsize();
    }
}
