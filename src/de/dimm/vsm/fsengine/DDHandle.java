/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author Administrator
 */
public class DDHandle
{

    public static final String RAF_RD_ONLY = "r";
    public static final String RAF_RDWR = "rw";
    DedupHashBlock dhb;
    long pos;
    int len;
    byte[] data;
    File fh;
    boolean dirty;
    boolean unread;

    @Override
    public String toString()
    {
        return "DHB: " + dhb + " Pos: " + pos + " Len: " + len + " dirty: " + Boolean.toString(dirty);
    }

    public DDHandle( HashBlock hb )
    {
        pos = hb.getBlockOffset();
        len = hb.getBlockLen();
        this.dhb = hb.getDedupBlock();
        data = null;
        unread = true;
    }

    public DDHandle( XANode hb )
    {
        pos = hb.getBlockOffset();
        len = hb.getBlockLen();
        this.dhb = hb.getDedupBlock();
        data = null;
        unread = true;
    }

    public DDHandle( long pos, int len, byte[] data )
    {
        this.pos = pos;
        this.len = len;
        this.data = data;
    }

    public void setDirty( boolean dirty )
    {
        this.dirty = dirty;
    }

    public boolean isDirty()
    {
        return dirty;
    }

    public boolean isUnread()
    {
        return unread;
    }

    public boolean isWrittenComplete()
    {
        if (data != null)
        {
            return len == data.length;
        }
        return false;
    }

    public void checkIsRead( AbstractStorageNode fs_node ) throws FileNotFoundException, IOException
    {
        if (isUnread())
        {
            openRead(fs_node);
            unread = false;
        }
    }

    void close() throws IOException
    {
        data = null;
    }

    void open( AbstractStorageNode fs_node, String rafMode ) throws FileNotFoundException, IOException
    {
        if (rafMode.equals(RAF_RDWR))
        {
            System.err.println("Open write " + this.toString());
            openWrite(fs_node);
        }
        else
        {
            System.err.println("Open read " + this.toString());
            openRead(fs_node);
        }
    }

    void openRead( AbstractStorageNode fs_node ) throws FileNotFoundException, IOException
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

        fh = new File(fs_node.getMountPoint() + sb.toString());
        RandomAccessFile raf = new RandomAccessFile(fh, "r");

        data = new byte[len];
        int rlen = raf.read(data);
        raf.close();

        if (rlen != len)
        {
            throw new IOException("Short read in open DDFS_FileHandle (" + rlen + "/" + len + ")");
        }
    }

    void openWrite( AbstractStorageNode fs_node ) throws FileNotFoundException, IOException
    {
        dhb = new DedupHashBlock();
        dhb.setStorageNode(fs_node);
    }

    void openRead( FileHandle fHandle ) throws FileNotFoundException, IOException
    {
        data = new byte[len];
        int rlen = fHandle.read(data, len, 0);

        if (rlen != len)
        {
            throw new IOException("Short read in open DDFS_FileHandle pos " + pos + "(is " + rlen + " / wants " + len + " )");
        }
        unread = false;
    }
}
