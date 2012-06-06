/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.BootstrapHandle;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class MultiBootstrapHandle implements BootstrapHandle
{
    List<BootstrapHandle> fh_list;

    public MultiBootstrapHandle()
    {
        fh_list = new ArrayList<BootstrapHandle>();
    }
    public void add( BootstrapHandle fh )
    {
        fh_list.add(fh);
    }


    @Override
    public boolean delete()
    {
        boolean ret = true;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            if (!handle.delete())
                ret = false;
        }
        return ret;
    }

    @Override
    public void write_bootstrap( FileSystemElemNode node ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            handle.write_bootstrap(node);
        }
    }

    @Override
    public void read_bootstrap( FileSystemElemNode node ) throws IOException
    {
        throw new IOException("Cannot read on multiple bootstrap handle.");
    }

    @Override
    public void write_bootstrap( HashBlock hb ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            handle.write_bootstrap(hb);
        }
    }

    @Override
    public void write_bootstrap( XANode xa ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            handle.write_bootstrap(xa);
        }
    }

    @Override
    public void read_bootstrap( HashBlock hb ) throws IOException
    {
        throw new IOException("Cannot read on multiple bootstrap handle.");
    }
    
    @Override
    public void read_bootstrap( XANode hb ) throws IOException
    {
        throw new IOException("Cannot read on multiple bootstrap handle.");
    }


    @Override
    public <T> void write_object( T object ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            handle.write_object(object);
        }
    }

    @Override
    public <T> T read_object( T object ) throws IOException
    {
        throw new IOException("Cannot read on multiple bootstrap handle.");
    }

}
