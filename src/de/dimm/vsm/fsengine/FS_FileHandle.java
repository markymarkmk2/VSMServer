/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author Administrator
 */
public class FS_FileHandle implements FileHandle
{
    File fh;
    RandomAccessFile raf;
    StorageNodeHandler sn_handler;
    boolean isDirectory;
    boolean create;

    private FS_FileHandle(StorageNodeHandler sn_handler, boolean isDirectory, boolean create)
    {
        this.sn_handler = sn_handler;
        this.isDirectory = isDirectory;
        this.create = create;

    }
    
   
    private void create_fs_file( FileSystemElemNode node ) throws PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_node_path(node, sb);


        AbstractStorageNode fs_node = this.sn_handler.storageNode;

        fh = new File( fs_node.getMountPoint() + sb.toString() );
    }

    private void create_xa_file( FileSystemElemNode node ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_xa_node_path(node, sb);

        AbstractStorageNode fs_node = this.sn_handler.storageNode;

        fh = new File( fs_node.getMountPoint() + sb.toString() );
    }

    private void create_dedup_file( DedupHashBlock dedup_block ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_node_path(dedup_block, sb);


        AbstractStorageNode fs_node = this.sn_handler.storageNode;

        fh = new File( fs_node.getMountPoint() + sb.toString() );


    }
    
    public static FileHandle create_fs_handle(StorageNodeHandler sn_handler, FileSystemElemNode node, boolean create) throws PathResolveException
    {
        FS_FileHandle fs =  new FS_FileHandle( sn_handler,  node.isDirectory(),  create);
        fs.create_fs_file(node);
        return fs;
    }

    public static FileHandle create_dedup_handle(StorageNodeHandler sn_handler, DedupHashBlock dedup_block, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        FS_FileHandle fs =  new FS_FileHandle( sn_handler,  false,  create);
        fs.create_dedup_file(dedup_block);
        return fs;
    }
    public static FileHandle create_xa_handle(StorageNodeHandler sn_handler, FileSystemElemNode node, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        FS_FileHandle fs =  new FS_FileHandle( sn_handler,  false,  create);
        fs.create_xa_file(node);
        
        // AS THIS IS A SUBDIR TO FSNODE WE CHECK IF WE HAVE TO CREATE DIR ON THE FLY
        if (create)
        {
            if (!fs.fh.getParentFile().exists())
            {
                fs.fh.getParentFile().mkdir();
            }
        }
        return fs;
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
        if (sn_handler.isReadOnly())
            throw new PoolReadOnlyException("");


        if (isDirectory)
        {
            // DIRECTORY IS ALLOWED TO EXIST PREVIOUSLY, MEBY WE ABORTED EARLIER
            if (fh.exists())
                return;
            
            boolean ret = fh.mkdir();
            if (!ret)
            {
                Log.warn("Kann Verzeichnis nicht erzeugen", ": " + fh.getAbsolutePath() );

                ret = fh.mkdirs();
                if (!ret)
                    throw new IOException( Main.Txt("Cannot recursivly create DirNode") + ": " + fh.getAbsolutePath());
            }
        }
        else
        {
            if (fh.exists() && fh.length() > 0)
                throw new IOException(  Main.Txt("Knoten existiert bereits") +  ": " + fh.getAbsolutePath());

            ensure_open();
            close();
        }
    }

    private void ensure_open() throws IOException
    {
        if (isDirectory)
            throw new IOException( "ensure_open Node " + fh.getName() + " -> " + fh.getAbsolutePath() + " fails, is a directory");
        if (raf == null)
        {
            if (create)
            {
                if (!fh.getParentFile().exists())
                    fh.getParentFile().mkdirs();

                raf = new RandomAccessFile(fh, "rw");
            }
            else
            {
                raf = new RandomAccessFile(fh, "r");
            }
        }
    }

    @Override
    public void force( boolean b ) throws IOException
    {
        // CREATE IF NOT EXISTS
        if (!fh.exists())
        {
            FileOutputStream fos = new FileOutputStream(fh, true);
            fos.close();
        }
        // IF OPEN THEN FLUSH
        if (raf != null)
        {
            raf.getChannel().force(b);
        }

    }

    @Override
    public synchronized int read( byte[] b,  int length, long offset ) throws IOException
    {
        ensure_open();

        raf.seek(offset);

        return raf.read(b, 0, length);
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
        if (raf != null)
        {
            raf.close();
            raf = null;
        }
    }

    @Override
    public void truncateFile( long size ) throws IOException, PoolReadOnlyException
    {
        ensure_open();

        raf.setLength(size);      
    }

    @Override
    public synchronized void writeFile( byte[] b,  int length, long offset ) throws IOException, PoolReadOnlyException
    {
        ensure_open();

        raf.seek(offset);
        
        raf.write(b, 0, length);
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        boolean ret = true;
        if (fh.exists())
        {
            // REMOVE AUTOMATIC SUBDIRS
            if (fh.isDirectory())
            {
                File b = new File( fh, StorageNodeHandler.BOOTSRAP_PATH);
                if (b.exists())
                    b.delete();
                File x = new File( fh, StorageNodeHandler.XA_PATH);
                if (x.exists())
                    x.delete();
            }
            ret = fh.delete();
        }

        return ret;
    }

    @Override
    public long length()
    {
        if (fh.exists())
        {
            return fh.length();
        }
        return -1;
    }

}
