/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class MultiFileHandle implements FileHandle
{
    List<FileHandle> fh_list;

    public MultiFileHandle()
    {
        fh_list = new ArrayList<FileHandle>();
    }
    public void add( FileHandle fh )
    {
        fh_list.add(fh);
    }


    @Override
    public void force( boolean b ) throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            fileHandle.force(b);
        }
    }

    @Override
    public int read( byte[] b, int length, long offset ) throws IOException
    {
        IOException lastExc = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            try
            {
                return fileHandle.read(b, length, offset);
            }
            catch (Exception exc)
            {
                lastExc = new IOException( exc.getMessage(), exc );
            }
        }
        throw lastExc;
    }
    @Override
    public byte[] read( int length, long offset ) throws IOException
    {
        IOException lastExc = null;
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            try
            {
                return fileHandle.read( length, offset);
            }
            catch (Exception exc)
            {
                lastExc = new IOException( exc.getMessage(), exc );
            }
        }
        throw lastExc;
    }

    @Override
    public void close() throws IOException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            fileHandle.close();
        }
    }

    @Override
    public void create() throws IOException, PoolReadOnlyException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            fileHandle.create();
        }
    }

    @Override
    public void truncateFile( long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            fileHandle.truncateFile(size);
        }
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        for (int f = 0; f < fh_list.size(); f++)
        {
            FileHandle fileHandle = fh_list.get(f);
            fileHandle.writeFile(b, length, offset);
        }
    }

    @Override
    public boolean delete() throws PoolReadOnlyException
    {
        boolean ret = true;
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            if (!fileHandle.delete())
                ret = false;
        }
        return ret;
    }

    @Override
    public long length()
    {
        for (int i = 0; i < fh_list.size(); i++)
        {
            FileHandle fileHandle = fh_list.get(i);
            try
            {
                return fileHandle.length();
            }
            catch (Exception iOException)
            {

            }
        }
       
        return 0;
    }


}
