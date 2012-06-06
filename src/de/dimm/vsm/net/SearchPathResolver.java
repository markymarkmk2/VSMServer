/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.fsengine.ArrayLazyList;
import de.dimm.vsm.fsengine.PathResolver;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class SearchPathResolver extends PathResolver
{
    SearchContext context;
    FileSystemElemNode rootDir;

    public SearchPathResolver(SearchContext context, StoragePoolHandler h)
    {
        super(h);
        this.context = context;
        rootDir = FileSystemElemNode.createDirNode();
        rootDir.getAttributes().setName("/");
        rootDir.getAttributes().setCreationDateMs(System.currentTimeMillis());
        rootDir.getAttributes().setModificationDateMs(System.currentTimeMillis());
        rootDir.setChildren(new ArrayLazyList<FileSystemElemNode>(context.resultList));
    }

    @Override
    public FileSystemElemNode resolve_elem_by_path( String dir_path ) throws SQLException
    {

        if (dir_path.equals("/"))
        {
            return rootDir;
        }
        return super.resolve_elem_by_path(dir_path);
    }

    @Override
    public FileSystemElemNode resolve_node_by_remote_elem( RemoteFSElem node ) throws SQLException
    {
        if (node.getPath().equals("/"))
        {
            return rootDir;
        }
        return super.resolve_node_by_remote_elem(node);
    }

    // ONLY NEEDED ON CREATE NODE
    @Override
    public FileSystemElemNode resolve_parent_dir_node(  String dir_path )
    {
        return super.resolve_parent_dir_node( dir_path);
    }

    // ONLY NEEDED ON CREATE NODE
    @Override
    public FileSystemElemNode create_parent_dir_node( String dir_path ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        return super.create_parent_dir_node(dir_path);
    }
    @Override
    public FileSystemElemNode getRootDir()
    {
        return rootDir;
    }
}
