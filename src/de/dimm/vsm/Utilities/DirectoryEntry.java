/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;



/**
 *
 * @author mw
 */
class DirectoryEntryIterator<File> implements Iterator
{
    ArrayList<DirectoryEntry> list;
    int act_idx;

    private DirectoryEntryIterator()
    {
        list = new ArrayList<DirectoryEntry>();
        act_idx = 0;
        
    }
    static DirectoryEntryIterator create_all_iterator( DirectoryEntry e )
    {
        DirectoryEntryIterator it = new DirectoryEntryIterator( );
        it.add_all_recursive(e);
        return it;
    }
    static DirectoryEntryIterator create_file_iterator( DirectoryEntry e )
    {
        DirectoryEntryIterator it = new DirectoryEntryIterator( );
        it.add_file_recursive(e);
        return it;
    }
    static DirectoryEntryIterator create_dir_iterator( DirectoryEntry e )
    {
        DirectoryEntryIterator it = new DirectoryEntryIterator( );
        it.add_dir_recursive(e);
        return it;
    }

    void add_all_recursive( DirectoryEntry e )
    {
        list.add(e);

        for (int i = 0; i < e.children.size(); i++)
        {
            DirectoryEntry directoryEntry = e.children.get(i);

            add_all_recursive( directoryEntry );
        }
    }
    void add_file_recursive( DirectoryEntry e )
    {
        if (e.file.isFile())
            list.add(e);

        for (int i = 0; i < e.children.size(); i++)
        {
            DirectoryEntry directoryEntry = e.children.get(i);

            add_file_recursive( directoryEntry );
        }
    }
    void add_dir_recursive( DirectoryEntry e )
    {
        if (e.file.isDirectory())
            list.add(e);

        for (int i = 0; i < e.children.size(); i++)
        {
            DirectoryEntry directoryEntry = e.children.get(i);

            add_dir_recursive( directoryEntry );
        }
    }


    @Override
    public boolean hasNext()
    {
        if (act_idx < list.size())
            return true;
        
        return false;
    }

    @Override
    public Object next()
    {
        if (hasNext())
            return list.get(act_idx++).file;

        return null;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
public class DirectoryEntry
{
    File file;
    DirectoryEntry parent;
    ArrayList<DirectoryEntry> children;

    public Iterator<File> get_iterator()
    {
        return (Iterator<File>)DirectoryEntryIterator.create_all_iterator( this );
    }
    public Iterator<File> get_file_iterator()
    {
        return (Iterator<File>)DirectoryEntryIterator.create_file_iterator( this );
    }
    public Iterator<File> get_dir_iterator()
    {
        return (Iterator<File>)DirectoryEntryIterator.create_dir_iterator( this );
    }
    public DirectoryEntry( File f )
    {
        this( null, f );
    }

    public DirectoryEntry( DirectoryEntry _parent, File f )
    {
        parent = _parent;
        file = f;
        children = new ArrayList<DirectoryEntry>();

        if (file.isDirectory())
        {
            File[] list = file.listFiles();

            for (int i = 0; i < list.length; i++)
            {
                children.add( new DirectoryEntry( this, list[i] ) );
            }
        }
    }

    public void delete_recursive()
    {
        for (int i = 0; i < children.size(); i++)
        {
            DirectoryEntry directoryEntry = children.get(i);
            if (directoryEntry.getFile().isDirectory())
            {
                directoryEntry.delete_recursive();

            }
            else
            {
                directoryEntry.getFile().delete();
            }
        }
        file.delete();
    }
    public boolean is_unchanged(DirectoryEntry de)
    {
        // COMPARE NAMES
        if (file.getAbsolutePath().compareTo( de.getFile().getAbsolutePath()) != 0)
            return false;

        // COMPARE STAMP
        if (file.lastModified() != de.getFile().lastModified())
            return false;

        // COMPARE SIZE
        if (file.length() != de.getFile().length())
            return false;

        // TRY TO OPEN REG FILES
        if (file.isFile())
        {
            RandomAccessFile raf = null;
            try
            {
                raf = new RandomAccessFile(file, "r");
                if (file.length() > 0)
                {
                    raf.seek( file.length() - 1);
                    raf.read();
                }
                raf.close();
            }
            catch (IOException ex)
            {
                // NOT COMPLETE, WE FAIL
                return false;
            }
            finally
            {
                if (raf != null)
                {
                    try
                    {
                        raf.close();
                    }
                    catch (IOException ex)
                    {
                    }
                }
            }
        }

        if (children != null && de.getChildren() == null)
            return false;
        if (children == null && de.getChildren() != null)
            return false;

        // COMPARE CHILDREN COUNT
        if (children != null)
        {
            if ( children.size() != de.getChildren().size())
            return false;

            // RECURSIVELY CHECK CHILDREN
            for (int i = 0; i < children.size(); i++)
            {
                DirectoryEntry directoryEntry = children.get(i);
                if (!directoryEntry.is_unchanged( de.getChildren().get(i) ))
                    return false;
            }
        }
        return true;
    }

    /**
     * @return the file
     */
    public File getFile()
    {
        return file;
    }

    /**
     * @return the children
     */
    public ArrayList<DirectoryEntry> getChildren()
    {
        return children;
    }
}
