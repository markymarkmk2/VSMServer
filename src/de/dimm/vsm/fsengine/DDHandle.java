/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.log.Log;
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
    private byte[] data;
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

    public void setDirty()
    {
        this.dirty = true;
        this.dhb = null;
    }

    public boolean isDirty()
    {
        return dirty;
    }

    public boolean isUnread()
    {
        return unread;
    }
    public void arraycopy(byte [] src, int srcOffset, int trgOffset, int len)
    {
        if (data == null)
        {
            if (this.len < len)
            {
                this.len = len;
            }
            createEmpty(this.len);
        }
        System.arraycopy(src, srcOffset, data, trgOffset, len);
        setDirty();
    }
    public void resize(int newLen)
    {
        Log.debug("Resizing Block " + this.toString() + " to len " + newLen);
        
        if (data != null)
        {
            byte[] tmp = data;
            data = new byte[newLen];
            int copyLen = newLen;
            if (copyLen > tmp.length)
                copyLen = tmp.length;

            System.arraycopy(tmp, 0, data, 0, copyLen);
        }
        len = newLen;
        
        setDirty();        
    }
    public void createEmpty(int len)
    {
        data = new byte[len];
        this.len = len;
        setDirty();
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

    public byte[] getData() throws IOException
    {
        if (data == null)
        {
            throw new IOException("Missing data in Block " + this);
        }
        
        return data;
    }
    

    void close() throws IOException
    {
        data = null;
        unread = true;
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
        catch (PathResolveException | UnsupportedEncodingException pathResolveException)
        {
            throw new IOException("Cannot open DDFS", pathResolveException);
        }

        fh = new File(fs_node.getMountPoint() + sb.toString());
        int rlen;
        try (RandomAccessFile raf = new RandomAccessFile(fh, "r"))
        {
            data = new byte[len];
            rlen = raf.read(data);
        }

        if (rlen != len)
        {
            throw new IOException("Short read in open DDFS_FileHandle (" + rlen + "/" + len + ")");
        }
        unread = false;
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

    void setUnread()
    {
        data = null;
        unread = true;
    }
}
