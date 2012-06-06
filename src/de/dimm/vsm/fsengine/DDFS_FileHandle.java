/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
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
import java.util.List;


class DDHandle
{
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
    void close() throws IOException
    {
        data = null;
    }

    void open(StorageNodeHandler sn_handler) throws  FileNotFoundException, IOException
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
        AbstractStorageNode fs_node = sn_handler.storageNode;

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
    StorageNodeHandler sn_handler;
    boolean isDirectory;
    boolean create;
    boolean isStream;
    RandomAccessFile raf;
    

    List<DDHandle> lastHandles;

    static private boolean  verbose = false;

    private DDFS_FileHandle(StorageNodeHandler sn_handler, boolean isDirectory, boolean create, boolean isStream)
    {
        this.sn_handler = sn_handler;
        this.isDirectory = isDirectory;
        this.create = create;
        this.isStream = isStream;
        
        lastHandles = new ArrayList<DDHandle>();
    }
    
    List<DDHandle> buildHashBlockList() throws IOException
    {
        List<HashBlock> hbList = node.getHashBlocks().getList(sn_handler.storage_pool_handler.getEm());

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
            hbList = Restore.filter_hashblocks(hbList, sn_handler.storage_pool_handler.poolQry);
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

    List<DDHandle> buildXABlockList() throws IOException, SQLException
    {
        List<XANode> hbList = sn_handler.storage_pool_handler.createQuery("select T1 from XANode T1 where T1.fileNode_idx=" + node.getIdx(), XANode.class);

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
            hbList = Restore.filter_xanodes(hbList, sn_handler.storage_pool_handler.poolQry);
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

   

    private void create_fs_file( FileSystemElemNode node ) throws PathResolveException, IOException, SQLException
    {
        this.node = node;
        if (!isStream)
            handleList = buildHashBlockList();
        else
            handleList = buildXABlockList();



        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_node_path(node, sb);


        AbstractStorageNode fs_node = this.sn_handler.storageNode;

        fh = new File( fs_node.getMountPoint() + sb.toString() );
    }

    private void open_handles(  ) throws PathResolveException, IOException, SQLException
    {
        handleList = buildHashBlockList();

    }
  
    public static FileHandle create_fs_handle(StorageNodeHandler sn_handler, FileSystemElemNode node, boolean create, boolean isStream) throws PathResolveException, IOException, SQLException
    {
        DDFS_FileHandle fs =  new DDFS_FileHandle( sn_handler,  node.isDirectory(),  create, isStream);
        fs.create_fs_file(node);
        return fs;
    }

    public static FileHandle create_dedup_handle(StorageNodeHandler sn_handler, DedupHashBlock dedup_block, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        return FS_FileHandle.create_dedup_handle(sn_handler, dedup_block, create);
    }


    @Override
    public String toString()
    {
        return fh.getAbsolutePath();
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
                    dDHandle.open(sn_handler);
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

    @Override
    public void truncateFile( long size ) throws IOException, PoolReadOnlyException
    {
        throw new PoolReadOnlyException("Cannot truncateFile in dedup FS");
    }

    @Override
    public synchronized void writeFile( byte[] b,  int length, long offset ) throws IOException, PoolReadOnlyException
    {
        throw new PoolReadOnlyException("Cannot write to dedup FS");
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        throw new PoolReadOnlyException("Cannot delete in dedup FS");
    }

}
