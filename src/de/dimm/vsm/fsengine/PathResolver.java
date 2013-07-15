/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class PathResolver
{
    StoragePoolHandler handler;

    public PathResolver( StoragePoolHandler handler )
    {
        this.handler = handler;
    }

    // THIS IS THE ENTRY FROM THE MOUNTED DRIVE BEFORE get_child_nodes, AFTER resolve_elem_by_path
    public FileSystemElemNode resolve_node_by_remote_elem(  RemoteFSElem node ) throws SQLException
    {
       // TODO: CACHE
        FileSystemElemNode fseNode = null;
        if ( node.getFileHandle() >= 0)
        {
            fseNode = handler.getNodeByFileNo( node.getFileHandle() );
        }
        if (fseNode == null)
        {
            if ( node.getIdx() > 0)
            {
                fseNode = handler.resolve_fse_node_from_db( node.getIdx());
            }
            else
            {
                fseNode = handler.resolve_elem_by_path(node.getPath());
            }
        }
        return fseNode;
    }

    // THIS IS THE ENTRY FROM THE MOUNTED DRIVE
    public FileSystemElemNode resolve_elem_by_path( String dir_path) throws SQLException
    {
        String[] path_arr = dir_path.split("/");

        FileSystemElemNode act_dir = handler.getRootDir();
//        FileSystemElemNode act_dir = handler.resolve_fse_node_from_db(handler.getPool().getRootDir().getIdx());
        if (path_arr.length == 0)
            return act_dir;


        for (int i = 0; i < path_arr.length; i++)
        {
            if (path_arr[i].length() == 0)
                continue;

            String string = path_arr[i];

            act_dir = handler.resolve_child_node( act_dir, string);

            if (act_dir == null)
                break;
        }
        return act_dir;
    }

    // ONLY NEEDED ON CREATE NODE
    public FileSystemElemNode resolve_parent_dir_node(  String dir_path )
    {
        FileSystemElemNode root_dir = handler.getRootDir();

        String[] path_arr = dir_path.split("/");
        FileSystemElemNode act_dir = root_dir;

        if (path_arr.length <= 1)
            return act_dir;


        for (int i = 0; i < path_arr.length - 1; i++)
        {
            String string = path_arr[i];
            if (string.equals(""))
            {
                act_dir = root_dir;
            }
            else
            {
                act_dir = handler.resolve_child_node( act_dir, string );
            }
            if (act_dir == null)
                break;
        }
        if (act_dir != null && act_dir.isDirectory())
            return act_dir;

        return null;
    }

    // ONLY NEEDED ON CREATE NODE
    // CREATES A DIRECTORY-NODE INCLUDING ALL NECESSARY PARENTDIRECTORIES FOR A PATH
    public FileSystemElemNode create_parent_dir_node( String dir_path ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        String[] path_arr = dir_path.split("/");
        FileSystemElemNode act_dir = handler.getRootDir();

        if (path_arr.length <= 1)
            return act_dir;


        for (int i = 0; i < path_arr.length - 1; i++)
        {
            String string = path_arr[i];
            if (string.equals(""))
            {
                act_dir = handler.getRootDir();
            }
            else
            {
                act_dir = handler.resolve_child_node( act_dir, string );
            }
            if (act_dir == null)
            {
                StringBuilder sb = new StringBuilder();
                for (int s = 0; s <= i; s++)
                {
                    if (path_arr[s].equals(""))
                        continue;

                    sb.append("/");
                    sb.append(path_arr[s]);
                }
                act_dir = handler.create_fse_node_complete(sb.toString(), FileSystemElemNode.FT_DIR);
            }
        }
        if (act_dir != null && act_dir.isDirectory())
            return act_dir;

        return null;
    }

    public FileSystemElemNode getRootDir()
    {
        return handler.getPool().getRootDir();
    }

}