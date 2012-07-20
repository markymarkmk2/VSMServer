/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.BootstrapHandle;
import de.dimm.vsm.records.FileSystemElemAttributes;
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
    public void write_bootstrap( FileSystemElemAttributes attr ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            handle.write_bootstrap(attr);
        }
    }

    @Override
    public void read_bootstrap( FileSystemElemAttributes attr ) throws IOException
    {
        IOException lastException = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            try
            {
                handle.read_bootstrap(attr);
                return;
            }
            catch (IOException iOException)
            {
                lastException = iOException;
            }
        }
        throw lastException;
    }

    @Override
    public void read_bootstrap( FileSystemElemNode node ) throws IOException
    {
        IOException lastException = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            try
            {
                handle.read_bootstrap(node);
                return;
            }
            catch (IOException iOException)
            {
                lastException = iOException;
            }
        }
        throw lastException;
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
        IOException lastException = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            try
            {
                handle.read_bootstrap(hb);
                return;
            }
            catch (IOException iOException)
            {
                lastException = iOException;
            }
        }
        throw lastException;
    }
    
    @Override
    public void read_bootstrap( XANode hb ) throws IOException
    {
        IOException lastException = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            try
            {
                handle.read_bootstrap(hb);
                return;
            }
            catch (IOException iOException)
            {
                lastException = iOException;
            }
        }
        throw lastException;
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
        IOException lastException = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            BootstrapHandle handle = fh_list.get(i);
            try
            {
                handle.read_object(object);
                return object;
            }
            catch (IOException iOException)
            {
                lastException = iOException;
            }
        }
        throw lastException;
    }

}
