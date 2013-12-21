/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;


/**
 *
 * @author Administrator
 */

class FSEMapEntry
{
    FileSystemElemNode node;
    FileHandle fh;

    public FSEMapEntry( FileSystemElemNode node, FileHandle fh )
    {
        this.node = node;
        this.fh = fh;
    }

    @Override
    public String toString()
    {
        return node.toString() + ": " + fh.toString();
    }
    

}

public class FSEMapEntryHandler
{   
    StoragePoolHandler poolHandler;

    // MAYBE THIS HAS TO BE PUT TO SOMEWHERE CENTRAL, INCLUDING LOCKING ETC
    final HashMap<Long,FSEMapEntry> openFileHandleMap = new HashMap<>();

    public FSEMapEntryHandler( StoragePoolHandler poolHandler )
    {
        this.poolHandler = poolHandler;
    }

    // THIS IS THE MAIN FH INDEX
    long newFilehandleIdx = 1;
    public long open_fh( FileSystemElemNode node, boolean create ) throws IOException, PathResolveException, SQLException
    {
        long newFileNo = newFilehandleIdx++;
        synchronized(openFileHandleMap)
        {
            FileHandle fh = poolHandler.open_file_handle(node, create);
            if (fh != null)
            {
                openFileHandleMap.put(newFileNo, new FSEMapEntry(node, fh) );
                //Log.debug( "open_fh: opening " + node.getName() + " -> " + newFileNo );
                return newFileNo;
            }
        }
        Log.err( "open_fh: open failed " + node.getName() );
        return -1;
    }
    public long open_versioned_fh( FileSystemElemNode node, FileSystemElemAttributes attrs ) throws IOException, PathResolveException, SQLException
    {
        long newFileNo = newFilehandleIdx++;
        synchronized(openFileHandleMap)
        {
            FileHandle fh = poolHandler.open_versioned_file_handle(node, attrs);
            if (fh != null)
            {
                openFileHandleMap.put(newFileNo, new FSEMapEntry(node, fh) );
                //Log.debug( "open_fh: opening " + node.getName() + " -> " + newFileNo );
                return newFileNo;
            }
        }
        Log.err( "open_fh: open failed " + node.getName() );
        return -1;
    }
   
    public long open_stream( FileSystemElemNode node, int streamInfo, boolean create ) throws IOException, PathResolveException, UnsupportedEncodingException, SQLException
    {
        long newFileNo = newFilehandleIdx++;
        synchronized(openFileHandleMap)
        {
            FileHandle fh = poolHandler.open_xa_handle(node, streamInfo, create);
            openFileHandleMap.put(newFileNo, new FSEMapEntry(node, fh) );
        }
        //Log.debug( "open_fh: opening " + node.getName() + " -> " + newFileNo );
        return newFileNo;
    }

    public void close_fh( long fileNo ) throws IOException
    {
        FileHandle fh = getFhByFileNo( fileNo );
        FileSystemElemNode node = getNodeByFileNo( fileNo );


        if (fh != null)
        {
            //Log.debug( "close_fh: " + fileNo );

            fh.close();
            removeByFileNo( fileNo );
            // If we clear then we loose our stored Object in FileResolver
//            if (!Main.get_bool_prop(GeneralPreferences.CACHE_ON_WRITE_FS, false) && node != null && node.getParent() != null)
//            {               
//                node.getParent().getChildren().unRealize();    
//                 //node.getParent().getChildren().getList(poolHandler.getEm());
//            }
        }
        else
            Log.err( "close_fh: Cannot resolve fh " + fileNo );

    }

    public FileHandle getFhByFileNo( long idx )
    {
        if (idx == -1)
            return null;

        FSEMapEntry entry = openFileHandleMap.get(idx);
        if (entry != null)
        {
            return entry.fh;
        }
        Log.err( "getFhByFileIdx: Cannot resolve fh " + idx );
        return null;
    }
    public FileSystemElemNode getNodeByFileNo( long idx )
    {
        if (idx == -1)
            return null;
        
        FSEMapEntry entry = openFileHandleMap.get(idx);
        if (entry != null)
            return entry.node;

        Log.err( "getNodeByFileIdx: Cannot resolve fh " + idx );
        return null;
    }
    public void removeByFileNo( long idx )
    {
        FSEMapEntry entry = openFileHandleMap.remove(idx);

        if (entry == null)
            Log.err( "removeByFileIdx: Cannot resolve fh " + idx );

        //Log.debug("handleMap entries: " + openFileHandleMap.size());
    }

}
