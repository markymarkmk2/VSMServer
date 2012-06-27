/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.interfaces.FileHandle;
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

}

public class FSEMapEntryHandler
{
    StoragePoolHandler poolHandler;

    // MAYBE THIS HAS TO BE PUT TO SOMEWHERE CENTRAL, INCLUDING LOCKING ETC
    final HashMap<Long,FSEMapEntry> openFileHandleMap = new HashMap<Long, FSEMapEntry>();

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
    public long open_stream( FileSystemElemNode node, boolean create ) throws IOException, PathResolveException, UnsupportedEncodingException, SQLException
    {
        long newFileNo = newFilehandleIdx++;
        synchronized(openFileHandleMap)
        {
            FileHandle fh = poolHandler.open_xa_handle(node, create);
            openFileHandleMap.put(newFileNo, new FSEMapEntry(node, fh) );
        }
        //Log.debug( "open_fh: opening " + node.getName() + " -> " + newFileNo );
        return newFileNo;
    }

    public void close_fh( long fileNo ) throws IOException
    {
        FileHandle fh = getFhByFileNo( fileNo );


        if (fh != null)
        {
            //Log.debug( "close_fh: " + fileNo );

            fh.close();
            removeByFileNo( fileNo );
        }
        else
            Log.debug( "close_fh: Cannot resolve fh " + fileNo );

    }

    public FileHandle getFhByFileNo( long idx )
    {
        FSEMapEntry entry = openFileHandleMap.get(idx);
        if (entry != null)
        {
            return entry.fh;
        }
        Log.debug( "getFhByFileIdx: Cannot resolve fh " + idx );
        return null;
    }
    public FileSystemElemNode getNodeByFileNo( long idx )
    {
        FSEMapEntry entry = openFileHandleMap.get(idx);
        if (entry != null)
            return entry.node;

        Log.debug( "getNodeByFileIdx: Cannot resolve fh " + idx );
        return null;
    }
    public void removeByFileNo( long idx )
    {
        FSEMapEntry entry = openFileHandleMap.remove(idx);

        if (entry == null)
            Log.debug( "removeByFileIdx: Cannot resolve fh " + idx );

        //Log.debug("handleMap entries: " + openFileHandleMap.size());
    }

}
