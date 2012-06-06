/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.DBConnException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.records.*;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author mw
 */

public class FileSystemElemNodeHandler
{
   
    private FileSystemElemNode node;
    StoragePoolHandler storage_pool_handler;

    public FileSystemElemNodeHandler( FileSystemElemNode node, StoragePoolHandler storage_pool_handler )
    {
        this.node = node;
        this.storage_pool_handler = storage_pool_handler;
    }


    private static void todo(String s )
    {
        System.err.println("Todo: " + s);
    }

//    public void set_mode( int mode ) throws IOException, SQLException, DBConnException
//    {
//        node.getAttributes().setPosixMode(mode);
//        PersistenceManager.merge_atomic( node );
//    }
//
//    public void set_owner_id( int uid ) throws IOException, SQLException, DBConnException
//    {
//        node.getAttributes().setUid(uid);
//        PersistenceManager.merge_atomic( node );
//    }
//
//    public void set_group_id( int gid ) throws IOException, SQLException, DBConnException
//    {
//        node.getAttributes().setGid(gid);
//        PersistenceManager.merge_atomic( node );
//    }

    public int get_owner_id()
    {
        
        return node.getAttributes().getUid();
    }

    public int get_group_id()
    {
        return node.getAttributes().getGid();
    }

    public int get_mode()
    {
        return node.getAttributes().getPosixMode();
    }

    public long get_timestamp()
    {

        return node.getAttributes().getTs();
    }

    public long get_unix_access_date()
    {        
        return node.getAttributes().getAccessDateMs()/1000;
    }
    public long get_unix_modification_date()
    {
        return node.getAttributes().getModificationDateMs()/1000;
    }
    public long get_unix_creation_date()
    {
        return node.getAttributes().getCreationDateMs()/1000;
    }

    public long get_size()
    {
        if (isDirectory())
            return 0;

        return node.getAttributes().getFsize();
    }


    public Object get_GUID()
    {        
        return node.getIdx();
    }

//    public void set_creation_date( long l ) throws SQLException, DBConnException
//    {
//        node.getAttributes().setCreationDateMs(l);
//        PersistenceManager.merge_atomic( node );
//    }
//
//    public void set_last_accessed( long l ) throws SQLException, DBConnException
//    {
//        node.getAttributes().setAccessDateMs(l);
//        PersistenceManager.merge_atomic( node );
//    }
//
//    public void set_last_modified( long l ) throws SQLException, DBConnException
//    {
//        node.getAttributes().setModificationDateMs(l);
//        PersistenceManager.merge_atomic( node );
//    }
    public long get_creation_date(  )
    {
        return node.getAttributes().getCreationDateMs();
    }

    public long get_last_accessed(  )
    {
        return node.getAttributes().getAccessDateMs();
    }

    public long get_last_modified(  )
    {
        return node.getAttributes().getModificationDateMs();
    }


    public String getAclInfo( String name )
    {
        return node.getAttributes().getAclInfoData();
    }

//    public void add_xattribute( String name, String valStr )
//    {
//        node.getAttributes().setXattribute( node.getAttributes().getAclInfo() + "," + name + "=" + valStr);
//    }

    public boolean isFile()
    {
        return node.isFile();
    }
    public boolean isDirectory()
    {
        return node.isDirectory();
    }
    public boolean isSymbolicLink()
    {
        return node.isSymbolicLink();
    }
    public boolean isHardLink()
    {
        return node.isHardLink();
    }

    public String get_name()
    {
        return node.getName();
    }
    
    public void create_symlink( String to ) throws IOException
    {
         todo("create_symlink");
    }

//    public void truncate( long size ) throws IOException, SQLException, DBConnException
//    {
//        FileHandle handle = storage_pool_handler.open_file_handle(node, /*creraste*/ true);
//        handle.truncateFile(size);
//        handle.close();
//
//        node.getAttributes().setFsize(size);
//
//        PersistenceManager.merge_atomic( node );
//    }

    public void set_attribute( String string, Integer valueOf )
    {
        todo("setAttribute");

    }

    public String read_symlink() throws IOException
    {
        todo("readSymbolicLink");
        return "";
    }


    public void rename_To(  String string ) throws IOException, SQLException, DBConnException
    {       
        node.getAttributes().rename_To( string );

        node = storage_pool_handler.em_merge( node );
    }

    public FileHandle open_file_handle(boolean create) throws IOException, SQLException, DBConnException, PathResolveException
    {
        FileHandle handle = storage_pool_handler.open_file_handle(node, create);
        return handle;
    }

    public long getId()
    {
        return node.getIdx();
    }

    public FileSystemElemNode getNode()
    {
        return node;
    }

//    public void set_ms_times( long c, long a, long m ) throws SQLException, DBConnException
//    {
//        if (c > 0)
//            node.getAttributes().setCreationDateMs( c );
//        if (a > 0)
//            node.getAttributes().setAccessDateMs(a);
//        if (m > 0)
//            node.getAttributes().setModificationDateMs(m);
//
//        PersistenceManager.merge_atomic( node );
//    }

    public ArrayList<String> list_xattributes()
    {
        return node.listXattributes();
    }


}

