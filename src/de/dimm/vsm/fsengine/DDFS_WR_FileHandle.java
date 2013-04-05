/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

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
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;


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

public class DDFS_WR_FileHandle extends DDFS_FileHandle implements IBackupHelper
{
    public static final int MAX_DIRTY_BLOCKS_FOR_FLUSH = 20;
    public static final int MIN_FILECHANGE_THRESHOLD_S = 120;

    FileSystemElemAttributes newAttr = null;
    fr.cryptohash.Digest digest = null;
    StatCounter stat;
    HashCache hashCache;
    protected FSEIndexer indexer;
    int streamInfo;
    boolean createNewAttribute;
    int blockSize;


   
    protected DDFS_WR_FileHandle( AbstractStorageNode fs_node, StoragePoolHandler sp_handler, boolean isDirectory, boolean create, boolean isStream )
    {
        super( fs_node, sp_handler, isDirectory, create, isStream);
        blockSize =  Main.get_int_prop(GeneralPreferences.FILE_HASH_BLOCKSIZE, CS_Constants.FILE_HASH_BLOCKSIZE);
        
    }

    public void setStreamInfo( int streamInfo )
    {
        this.streamInfo = streamInfo;
    }

   

    @Override
    public void truncateFile( long size ) throws IOException, PoolReadOnlyException
    {
        if (spHandler.isReadOnly())
            throw new PoolReadOnlyException("Cannot truncateFile to dedup FS");

        for (int i = 0; i < handleList.size(); i++)
        {
            DDHandle dDHandle = handleList.get(i);
            if (dDHandle.pos + dDHandle.len > size)
                continue;

            // Haben wir einen angefangenen Block?
            if (dDHandle.pos < size)
            {
                // Restgröße ermitteln
                int newBlockSize = (int)(size - dDHandle.pos);

                // Und Datenblock neu schreiben
                byte[] data = dDHandle.data;
                dDHandle.data = new byte[newBlockSize];
                System.arraycopy(data, 0, dDHandle.data, 0, newBlockSize);
                dDHandle.len = newBlockSize;
                dDHandle.setDirty(true);
            }
            dDHandle.close();
        }
        checkForFlush( true );
    }

    long getLastValidBlockadress()
    {
        long lastValidBlockadress = 0;
        DDHandle lh = getLastHandle();
        if (lh != null)
        {
            lastValidBlockadress = lh.pos;
            if (lh.len == blockSize)
            {
                lastValidBlockadress = lh.pos + lh.len;
            }
        }
        return lastValidBlockadress;
    }
    DDHandle getLastValidBlock()
    {
        DDHandle lh = getLastHandle();
        if (lh != null)
        {
            if (lh.len == blockSize && !lh.isDirty())
            {
                // WE DONT NEED TO TOUCH THIS ONE
                lh = null;
            }
        }
        return lh;
    }

    // Block, der von diesem Offset betroffen ist
    DDHandle getBlockForOffset(long offset)
    {
        if (handleList.isEmpty())
            return null;
        
        long blockedSize = handleList.size() * blockSize;
        if (offset > blockedSize)
            return null;

        long idx = offset / blockSize;
        if (idx > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Datei " + getFsNode().getName() + " ist zu lang: " + offset);

        // Auf Blockgrenze?
        if (idx == handleList.size())
            return null;

        return handleList.get((int)idx);
    }


    @Override
    public synchronized void writeFile( byte[] b,  int length, long offset ) throws IOException, PoolReadOnlyException
    {
        Log.debug("writeFile", "Writing " + length + "  byte at offset " + offset );
        // Eventuell offene Blöcke schreiben
        checkForFlush( false );

        // Erster betroffener Block        
        DDHandle actBlock = getBlockForOffset(offset);

        int writeDataLen = length;

        // Offset in ersten betroffenen Block;
        int offsetInBlock = 0;
        if (actBlock != null)
        {
            offsetInBlock = (int)(offset - actBlock.pos);
        }
        
        // Haben wir einen Block gefunden, in dem weitergeschrieben werden soll ?
        if (actBlock != null)
        {
            // Ist Block groß genug ?
            if (writeDataLen + offsetInBlock  > actBlock.len)
            {
                // IF NECESSARY, UPDATE BLOCK
                if (actBlock.len != blockSize)
                {
                    int newBlockSize = offsetInBlock + length;
                    if (newBlockSize > blockSize)
                        newBlockSize = blockSize;

                    byte[] data = actBlock.data;

                    // Falls das kein kompletter Block ist, dann Block auf volle Länge setzen
                    if (data.length != blockSize)
                    {
                        actBlock.data = new byte[blockSize];
                        System.arraycopy(data, 0, actBlock.data, 0, data.length);
                    }
                    actBlock.len = newBlockSize;
                }
            }

            // Zu schreibende länge errechnen
            if (writeDataLen + offsetInBlock > actBlock.len)
            {
                writeDataLen = actBlock.len - offsetInBlock;
            }
        }
        else
        {
            if (writeDataLen > blockSize)
            {
                writeDataLen = blockSize;
            }
            DDHandle lastBlock = getLastValidBlock();
            long lastValidPos = 0;

            // Prüfen, ob letzter Block nicht komplett ist
            if (lastBlock != null)
            {
                if (lastBlock.len != blockSize )
                {
                    byte[] data = actBlock.data;

                    // Falls das kein kompletter Block ist, dann Block auf volle Länge setzen
                    if (lastBlock.data.length != blockSize)
                    {
                        lastBlock.data = new byte[blockSize];
                        System.arraycopy(data, 0, lastBlock.data, 0, data.length);
                    }
                    lastBlock.len = blockSize;
                    lastBlock.setDirty(true);
                }
                lastValidPos = lastBlock.pos + lastBlock.len;
            }

            // Neue fehlende Blöcke der Lücke Schreiben
            while( lastValidPos + blockSize < offset)
            {
                DDHandle newHandle = new DDHandle(lastValidPos, blockSize, null);
                newHandle.setDirty(true);
                handleList.add(newHandle);
                lastValidPos += blockSize;
            }

            // Offset in letzen Block errechnen
            if (lastValidPos < offset)
            {
                offsetInBlock = (int)(offset - lastValidPos);
                
                // Eventuell länge korrigieren
                if (writeDataLen + offsetInBlock > blockSize)
                {
                    writeDataLen = blockSize - offsetInBlock;
                }                                
            }
            // Neuen Datenblock anlegen
            byte data[] = new byte[offsetInBlock + writeDataLen];
            actBlock = new DDHandle(lastValidPos, offsetInBlock + writeDataLen, data);
            handleList.add(actBlock);
        }

        // OKAY;
        // offsetInBlock ist gesetzt
        // writeDataLen ist gesetzt
        // wir haben einen Block zum Schreiben, jetzt gehts los
        int writtenData = 0;
        while (writtenData < length)
        {
            // Aktuellen Block beschrieben
            System.arraycopy(b, writtenData, actBlock.data, offsetInBlock, writeDataLen);
            actBlock.setDirty(true);

            writtenData += writeDataLen;

            // Ist noch mehr für einen nächsten Block?
            if (writtenData < length)
            {
                // Restlänge für nächsten Block
                int rest = length - writtenData;
                if (rest > blockSize)
                {
                    rest = blockSize;
                }

                // Neuen Block anlegen
                byte data[] = new byte[rest];
                actBlock = new DDHandle(actBlock.pos + actBlock.len, rest, data);
                handleList.add(actBlock);
                offsetInBlock = 0;
                writeDataLen = rest;
            }
        }
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        if (spHandler.isReadOnly())
            throw new PoolReadOnlyException("Cannot delete in dedup FS");

        return false;
    }

    private void checkForFlush(boolean immediate) throws IOException
    {
        int dirtyCnt = 0;
        if (!immediate)
        {
            for (int i = 0; i < handleList.size(); i++)
            {
                // Ist beschrieben worden ?
                if (handleList.get(i).isDirty())
                {
                    dirtyCnt++;
                    if (dirtyCnt > MAX_DIRTY_BLOCKS_FOR_FLUSH)
                    {
                        immediate = true;
                        break;
                    }
                }
            }
        }
        if (!immediate)
            return;

        getSpHandler().check_open_transaction();

        // Erster schreibender Zugriff, dann alles notwendige anlegen
        if (newAttr == null)
        {
            Log.debug("checkForFlush", "Creating new attribute for " + getNode());

            digest = new fr.cryptohash.SHA1();
            newAttr = getNode().getAttributes();
            newAttr.setTs(System.currentTimeMillis());
            // Neuer TS
            long diffSinceLastUpdate = System.currentTimeMillis() - newAttr.getTs();
            if (diffSinceLastUpdate/1000 > MIN_FILECHANGE_THRESHOLD_S)
            {
                newAttr = new FileSystemElemAttributes(newAttr);
                createNewAttribute = true;
            }
            hashCache = LogicControl.getStorageNubHandler().getHashCache(getSpHandler().getPool());
            indexer = LogicControl.getStorageNubHandler().getIndexer(getSpHandler().getPool());
            if (!indexer.isOpen())
            {
                indexer.open();
            }
            stat = new StatCounter("DDFS Write");
        }

        try
        {            
            long newLen = 0;
            for (int i = 0; i < handleList.size(); i++)
            {
                DDHandle handle = handleList.get(i);
                newLen += handle.len;

                // Ist beschrieben worden ?
                if (!handleList.get(i).isDirty())
                    continue;

                if (handle.data == null)
                {
                    throw new IOException("Missing data in Block " + handle);
                }

                Log.debug("checkForFlush", "Flushing " + handle);

                
                byte[] hash = digest.digest(handle.data);
                String hashValue = CryptTools.encodeUrlsafe(hash);

                DedupHashBlock dhb = check_for_existing_block(hashValue);
                if (dhb != null)
                {
                    HashBlock hb = getSpHandler().create_hashentry(node, hashValue, dhb, handle.pos, handle.len, /*reorganize*/ false, newAttr.getTs());
                    node.getHashBlocks().addIfRealized(hb);

                    // UPDATE BOOTSTRAP
                    getSpHandler().write_bootstrap_data(dhb, hb);
                    stat.addDedupBlock(dhb);
                }
                else
                {
                    dhb = Backup.createDedupHashBlock(this, node, hashValue, streamInfo, handle.pos, handle.len, isStream(), newAttr.getTs());
                    if (dhb != null)
                    {
                        FileHandle fHandle = getSpHandler().open_dedupblock_handle(dhb, /*create*/ true);
                        fHandle.writeFile(handle.data, handle.len, /*offset*/ 0);

                        updateHashBlock( isStream(), hashValue, dhb, handle.pos, handle.len, actAttribute.getTs() );
                        stat.addTransferBlock();
                        stat.addTransferLen( handle.len );
                    }
                    else
                    {
                        // Cannot happen, Exception is thrown before
                        throw new SQLException("Could not create DHB");
                    }
                }                
            }

            if (createNewAttribute)
            {
                createNewFseAttribute( node, newAttr,  newLen );
            }
            else
            {
                updateFseAttribute( node, newAttr,  newLen );
            }
            getSpHandler().write_bootstrap_data(newAttr);
            indexer.addToIndexAsync(node.getAttributes(), /*ar-Job*/null);
            getSpHandler().commit_transaction();
        }
        catch (Exception exc)
        {
            Log.err("Fehler bei flush in DDFS_WR", ": " +  exc.getMessage(), exc);
            throw new IOException( exc.getMessage(), exc);
        }
    }
    void updateFseAttribute(FileSystemElemNode fsenode, FileSystemElemAttributes newAttributes, long len) throws SQLException
    {
        
        if (isStream())
            fsenode.getAttributes().setStreamSize(len);
        else
            fsenode.getAttributes().setFsize(len);

        getSpHandler().check_open_transaction();
        getSpHandler().em_merge(fsenode.getAttributes());
        getSpHandler().check_commit_transaction();

    }

    void createNewFseAttribute(FileSystemElemNode fsenode, FileSystemElemAttributes newAttributes, long len) throws SQLException
    {
        long ts = newAttributes.getTs();
        newAttributes.setAccessDateMs( ts);

        newAttributes.setModificationDateMs( ts);
        if (!isStream())
            newAttributes.setFsize(len);
        else
            newAttributes.setStreamSize( len);

        fsenode.setAttributes(newAttributes);

        getSpHandler().check_open_transaction();

        getSpHandler().em_persist(newAttributes);
        getSpHandler().em_merge(fsenode);
        getSpHandler().check_commit_transaction();
    }

    void updateHashBlock( boolean isXa, String remote_hash, DedupHashBlock dhb, long offset, int read_len, long ts) throws PoolReadOnlyException, SQLException, IOException, PathResolveException
    {
        if (isXa)
        {
            // ADD HASHENTRY TO DB
            XANode xa = getSpHandler().create_xa_hashentry(node, remote_hash, streamInfo, dhb, offset, read_len, /*reorganize*/ true,  ts);

            node.getXaNodes().addIfRealized(xa);

            // UPDATE BOOTSTRAP
            getSpHandler().write_bootstrap_data( dhb, xa);
        }
        else
        {
            // REGISTER HASHBLOCK TO DB
            HashBlock hb = getSpHandler().create_hashentry(node, remote_hash, dhb, offset, read_len, /*reorganize*/ true, ts);

            node.getHashBlocks().addIfRealized(hb);

            // UPDATE BOOTSTRAP
            getSpHandler().write_bootstrap_data( dhb, hb);
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
    public void close() throws IOException
    {
        checkForFlush(true);

        super.close();

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


 



}
