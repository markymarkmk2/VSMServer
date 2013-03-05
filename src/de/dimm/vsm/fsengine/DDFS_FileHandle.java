/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


class DDHandle
{
    public static final String RAF_RD_ONLY = "r";
    public static final String RAF_RDWR = "rw";

    DedupHashBlock dhb;
    long pos;
    int len;
    byte[] data;

    public DDHandle( HashBlock hb,  RandomAccessFile raf )
    {
        pos = hb.getBlockOffset();
        len = hb.getBlockLen();
        this.dhb = hb.getDedupBlock();
        data = null;
    }
    public DDHandle( XANode hb, RandomAccessFile raf )
    {
        pos = hb.getBlockOffset();
        len = hb.getBlockLen();
        this.dhb = hb.getDedupBlock();
        data = null;
    }
    public DDHandle()
    {
        pos = 0;
        len = 0;
        this.dhb = null;
        data = null;
    }
    void close() throws IOException
    {
        data = null;
    }
    void open(AbstractStorageNode fs_node, String rafMode) throws  FileNotFoundException, IOException
    {
        if (rafMode.equals(RAF_RDWR))
            openWrite(  fs_node );
        else
            openRead(fs_node);
    }
    void openRead(AbstractStorageNode fs_node) throws  FileNotFoundException, IOException
    {
        StringBuilder sb = new StringBuilder();
        try
        {
            StorageNodeHandler.build_node_path(dhb, sb);
        }
        catch (PathResolveException pathResolveException)
        {
            throw new IOException("Cannot open DDFS", pathResolveException);
        }
        catch (UnsupportedEncodingException unsupportedEncodingException)
        {
            throw new IOException("Cannot open DDFS", unsupportedEncodingException);
        }       

        File fh = new File( fs_node.getMountPoint() + sb.toString() );
        RandomAccessFile raf = new RandomAccessFile(fh, "r");

        data = new byte[len];
        int rlen = raf.read(data);
        raf.close();

        if (rlen != len)
        {
            throw new IOException("Short read in open DDFS_FileHandle (" + rlen + "/" + len + ")");
        }
    }

    void openWrite(AbstractStorageNode fs_node) throws  FileNotFoundException, IOException
    {
        dhb = new DedupHashBlock();
        dhb.setStorageNode(fs_node);
    }


}
/**
 *
 * @author Administrator
 */
public class DDFS_FileHandle implements FileHandle
{
    File fh;
    FileSystemElemNode node;
    List<DDHandle> handleList;
    StoragePoolHandler sp_handler;
    AbstractStorageNode fs_node;
    boolean isDirectory;
    boolean create;
    boolean isStream;
    RandomAccessFile raf;
    FileSystemElemAttributes actAttribute;



    List<DDHandle> lastHandles;

    static private boolean  verbose = false;

    private DDFS_FileHandle(AbstractStorageNode fs_node, StoragePoolHandler sp_handler, boolean isDirectory, boolean create, boolean isStream)
    {
        this.fs_node = fs_node;
        this.sp_handler = sp_handler;
        this.isDirectory = isDirectory;
        this.create = create;
        this.isStream = isStream;
        
        lastHandles = new ArrayList<DDHandle>();
    }
    
    List<DDHandle> buildHashBlockList(FileSystemElemAttributes attrs) throws IOException
    {
        List<HashBlock> hbList = node.getHashBlocks().getList(sp_handler.getEm());
 
        // IN CASE WE HAVE WRITTEN THIS PRESENTLY, WE HAVE TO REREAD LIST
        if (hbList instanceof LazyList)
        {
            LazyList lhb = (LazyList)hbList;
            lhb.unRealize();
        }

        if (verbose)
            System.out.println("HBLIST 1: " + hbList.size());

        if (!hbList.isEmpty())
        {
            // BUILD SORTED LIST FOR THIS FILE BASED ON poolQry
            hbList = Restore.filter_hashblocks(hbList, attrs);
        }

        if (verbose)
            System.out.println("HBLIST 2: " + hbList.size());

        List<DDHandle> ret = new ArrayList<DDHandle>();

        for (int i = 0; i < hbList.size(); i++)
        {
            HashBlock hb = hbList.get(i);
            if (hb.getDedupBlock() != null)
                ret.add(new DDHandle(hb, null));
        }

        if (verbose)
            System.out.println("HBLIST 3: " + ret.size());

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
                    System.out.println( _hb.toString().toString() + " dhb:" + _hb.getDedupBlock());
                }

                System.out.println("DDHandles");
                for (int l = 0; l < ret.size(); l++)
                {
                    DDHandle _ddhb = ret.get(l);
                    System.out.println("Pos: " + _ddhb.pos + " Len: " + _ddhb.len + " DHB: " + _ddhb.dhb.toString());
                }

                throw new IOException( "Invalid Block order" );
            }
            offset += hb.len;
        }

        if (verbose)
            System.out.println("HBLIST 4: " + ret.size());
        return ret;
    }

    List<DDHandle> buildXABlockList(FileSystemElemAttributes attrs) throws IOException, SQLException
    {
        List<XANode> hbList = sp_handler.createQuery("select T1 from XANode T1 where T1.fileNode_idx=" + node.getIdx(), XANode.class);

        if (verbose)
            System.out.println("XALIST 1: " + hbList.size());

        // IN CASE WE HAVE WRITTEN THIS PRESENTLY, WE HAVE TO REREAD LIST
        if (hbList instanceof LazyList)
        {
            LazyList lhb = (LazyList)hbList;
            lhb.unRealize();
        }

        if (!hbList.isEmpty())
        {
            // BUILD LIST FOR THIS FILE
            hbList = Restore.filter_xanodes(hbList, attrs);
        }
        if (verbose)
            System.out.println("XALIST 2: " + hbList.size());

        List<DDHandle> ret = new ArrayList<DDHandle>();

        for (int i = 0; i < hbList.size(); i++)
        {
            XANode hb = hbList.get(i);
            if (hb.getDedupBlock() != null)
                ret.add(new DDHandle(hb, null));
        }
        // SANITY CHECK
        long offset = 0;
        for (int i = 0; i < ret.size(); i++)
        {
            DDHandle hb = ret.get(i);
            if (offset != hb.pos)
                throw new IOException( "Invalid Block order" );
            offset += hb.len;
        }
        if (verbose)
            System.out.println("XALIST 3: " + ret.size());


        return ret;
    }

    List<DDHandle> getHandles( long pos, int len )
    {
        if (handleList.isEmpty())
            return null;

        List<DDHandle> ret = new ArrayList<DDHandle>();

        int bs = handleList.get(0).len;
        int blockNr = (int)(pos / bs);
       
        for (int i = blockNr; i < handleList.size(); i++)
        {
            DDHandle hb = handleList.get(i);
            
            if (hb.pos >= pos + len)
                break;

            ret.add( hb );
        }
        if (verbose)
            System.out.println("DDLIST 1: " + ret.size());

        return ret;
    }

    @Override
    public long length()
    {
        if (handleList == null)
            return 0;

        long l = 0;
        for (int i = 0; i < handleList.size(); i++)
        {
            DDHandle dDHandle = handleList.get(i);
            l+= dDHandle.len;
        }
        return l;
    }

   
    private FileSystemElemAttributes getActAttribute(long ts)
    {
       List<FileSystemElemAttributes> attrList = node.getHistory().getList(sp_handler.getEm());
       if (attrList.size() == 1) {
           return attrList.get(0);
       }
       
        // DETECT CORRECT ATTRIBUTE
                // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(attrList, new Comparator<FileSystemElemAttributes>() {

            @Override
            public int compare( FileSystemElemAttributes o1, FileSystemElemAttributes o2 )
            {
                if (o1.getTs() != o2.getTs()) 
                    return (o1.getTs() - o2.getTs() > 0) ? -1 : 1;
                
                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });
        int lastValidIdx = -1;
        
        for (int i = 0; i < attrList.size(); i++) {
            FileSystemElemAttributes fileSystemElemAttributes = attrList.get(i);
             
            if (fileSystemElemAttributes.getTs() > ts)
                break;
            lastValidIdx = i;            
        }
        if (lastValidIdx >= 0)
            return attrList.get(lastValidIdx);
          
        // OMG we are lost, no Attribute found to ts, we must recover
        // so we log and give back actual attribute
        LogManager.err_db("Kein Attribut zu TS " + ts + " für node " + node.toString() + " gefunden");
        return node.getAttributes();
    }

    private void create_fs_file( FileSystemElemNode node ) throws PathResolveException, IOException, SQLException
    {
        this.node = node;
        long ts = sp_handler.poolQry.getSnapShotTs();
        this.actAttribute = getActAttribute(ts);
       
        if (!isStream)
            handleList = buildHashBlockList(actAttribute);
        else
            handleList = buildXABlockList(actAttribute);
        


        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_node_path(node, sb);


        fh = new File( fs_node.getMountPoint() + sb.toString() );
    }

  
    public static FileHandle create_fs_handle(AbstractStorageNode fs_node, StoragePoolHandler sp_handler, FileSystemElemNode node, boolean create, boolean isStream) throws PathResolveException, IOException, SQLException
    {
        DDFS_FileHandle fs =  new DDFS_FileHandle( fs_node, sp_handler, node.isDirectory(),  create, isStream);
        fs.create_fs_file(node);
        return fs;
    }

    @Override
    public String toString()
    {
        return node.toString();
    }



    public File get_fh()
    {
        return fh;
    }

    @Override
    public void create() throws IOException, PoolReadOnlyException
    {        
        throw new PoolReadOnlyException("DD Filesystem is readonly");
    }


    // OPEN ALL NECESSARY BLOCKS FOR READ OPERATION, THIS CAN SPAN MORE THAN ONE DEDUP BLOCK (IF POS AND LEN CROSS BLOCK BOUNDARY OR IF LEN > BLOCKLEN OR BOTH)
    private void ensure_open(long pos, int len) throws IOException
    {
        ensure_open(pos, len, DDHandle.RAF_RD_ONLY);
    }
    private void ensure_open_write(long pos, int len) throws IOException
    {
        ensure_open(pos, len, "rw");
    }

    // OPEN ALL NECESSARY BLOCKS FOR READ OPERATION, THIS CAN SPAN MORE THAN ONE DEDUP BLOCK (IF POS AND LEN CROSS BLOCK BOUNDARY OR IF LEN > BLOCKLEN OR BOTH)
    private void ensure_open(long pos, int len, String rafMode) throws IOException
    {
        if (create)
            throw new IOException("DD Filesystem is readonly");

        if (isDirectory)
            throw new IOException( "ensure_open Node " + fh.getName() + " -> " + fh.getAbsolutePath() + " fails, is a directory");

        // CLOSE UNNECESSARY HANDLES
        for (int i = 0; i < lastHandles.size(); i++)
        {
            DDHandle dDHandle = lastHandles.get(i);
            if (pos + len <= dDHandle.pos ||pos >= dDHandle.pos + dDHandle.len)
            {
                lastHandles.remove(dDHandle);
                dDHandle.close();
            }
        }
        
        // CHECK IF WE HAVE ALL NECESSARY BLOCKS
        if (!lastHandles.isEmpty())
        {
            long start = lastHandles.get(0).pos;
            long end = start + lastHandles.get(0).len;


            int nextIdx = 1;
            while (end < pos + len && nextIdx < lastHandles.size())
            {
                if (end != lastHandles.get(nextIdx).pos)
                    break;

                end += lastHandles.get(nextIdx).len;
            }

            // OKAY ALL BLOCKS ARE IN CONSECUTIVE ORDER
            if (end >= pos + len)
                return;
        }

        // CLOSE ALL HANDLES
        for (int i = 0; i < lastHandles.size(); i++)
        {
            DDHandle dDHandle = lastHandles.get(i);
            if (pos + len <= dDHandle.pos || dDHandle.pos + dDHandle.len <= pos)
            {
                lastHandles.remove(dDHandle);
                dDHandle.close();
            }
        }

        int startOffset = 0;

        if (!handleList.isEmpty())
        {
            // SPEED UP INDEXING OF BLOCK BY CALCULATING INDEX, THIS SHOULD WORK WITH CONSTANT BLOCKSIZE (WHICH IS GUARANTEED INSIDE ONE FILE)
            DDHandle dd = handleList.get(0);
            if (pos > dd.len)
            {
                int idx = (int)(pos / dd.len);
                if (idx < handleList.size())
                {
                    if (handleList.get(idx).pos <= pos && handleList.get(idx).pos + handleList.get(idx).len > pos)
                    {
                        startOffset = idx;
                    }
                }
            }

            for (int i = startOffset; i < handleList.size(); i++)
            {
                DDHandle dDHandle = handleList.get(i);

                if (pos + len > dDHandle.pos && dDHandle.pos + dDHandle.len > pos)
                {
                    dDHandle.open(fs_node, rafMode);
                    lastHandles.add(dDHandle);
                }

                // BLOCKS TOO LARGE, BREAK
                if (pos + len < dDHandle.pos)
                    break;
            }
        }

        // OPEN LANDING ZONE FILE IF EXISTENT
        if (fh.length() > 0)
        {
            if (raf == null)
            {
                 raf = new RandomAccessFile(fh, "r");
            }
        }
    }

    @Override
    public void force( boolean b ) throws IOException
    {
        // Mac calls this for fun
        //throw new IOException("DD Filesystem is readonly");
    }

    @Override
    public synchronized int read( byte[] b,  int length, long offset ) throws IOException
    {
        if (verbose)
            System.out.println("DD Read: " + offset + " " + length);

        ensure_open(offset, length);

        List<DDHandle> handles = getHandles( offset, length );

        if (handles == null)
            return 0;

        int byteRead = 0;
        int arrayOffset = 0;

        long actReadOffset = offset;
        for (int i = 0; i < handles.size(); i++)
        {
            DDHandle dDHandle = handles.get(i);

            // IS THIS BLOCK CORRECT FOR THIS DATA AREA?
            if (dDHandle.pos <= actReadOffset && dDHandle.pos + dDHandle.len > actReadOffset)
            {
                // CALC OFFSET INTO BLOCK
                if (dDHandle == null)
                    throw new RuntimeException("Isser doch null??");
                
                int fileOffset = (int)(actReadOffset - dDHandle.pos);

                // CALCULATE LENGTH WE CAN READ FROM THIS BLOCK
                int realLen = length - arrayOffset;
                if (realLen > dDHandle.len - fileOffset)
                {
                    realLen = dDHandle.len - fileOffset;
                }

               // System.out.println("DDHandle: " + dDHandle.pos + " " + dDHandle.len + " Offset " + fileOffset + " Len " + realLen);
                // LOCATE AND READ
//                dDHandle.raf.seek(fileOffset);
//                dDHandle.raf.read(b, arrayOffset, realLen);

                System.arraycopy(dDHandle.data, fileOffset, b, arrayOffset, realLen);

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

    @Override
    public synchronized  byte[] read(  int length, long offset ) throws IOException
    {
       byte[] b = new byte[length];

        int rlen = read(b, length, offset);
        if (rlen == -1)
            throw new IOException("Read error (-1)");

        if (rlen != b.length)
        {
            byte[] bb = new byte[rlen];
            System.arraycopy(b, 0, bb, 0, length);
            b = bb;
        }
        return b;

    }

    @Override
    public void close() throws IOException
    {
        for (int i = 0; i < lastHandles.size(); i++)
        {
            DDHandle dDHandle = lastHandles.get(i);
            dDHandle.close();
        }
        lastHandles.clear();

        if (handleList != null)
        {
            for (int i = 0; i < handleList.size(); i++)
            {
                DDHandle dDHandle = handleList.get(i);
                dDHandle.close();
            }
            handleList.clear();
        }

        if (raf != null)
        {
            raf.close();
            raf = null;
        }

    }
    //TODO:

    @Override
    public void truncateFile( long size ) throws IOException, PoolReadOnlyException
    {
        if (sp_handler.isReadOnly())
            throw new PoolReadOnlyException("Cannot truncateFile to dedup FS");
    }

    @Override
    public synchronized void writeFile( byte[] b,  int length, long offset ) throws IOException, PoolReadOnlyException
    {
        if (sp_handler.isReadOnly())
            throw new PoolReadOnlyException("Cannot write to dedup FS");

    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        if (sp_handler.isReadOnly())
            throw new PoolReadOnlyException("Cannot delete in dedup FS");

        return false;
    }

    @Override
    public boolean exists()
    {
        try
        {
            ensure_open(0, 0);
            close();
            return true;
        }
        catch (Exception iOException)
        {
        }
        return false;
    }




}
