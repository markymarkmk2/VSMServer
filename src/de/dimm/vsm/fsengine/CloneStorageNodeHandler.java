/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.BootstrapHandle;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mw
 *
 *
 * From <Path/content/> on we have the database-IDS for the directories / files as Folder / Filenames
 */
public class CloneStorageNodeHandler extends StorageNodeHandler
{

    List<AbstractStorageNode> cloneStorageNodes;    

    public CloneStorageNodeHandler( AbstractStorageNode storageNode, StoragePoolHandler fsh_handler )
    {
        super(storageNode, fsh_handler);

        cloneStorageNodes = new ArrayList<AbstractStorageNode>();

        AbstractStorageNode rnode = storageNode;

        while (rnode.getCloneNode() != null)
        {
            cloneStorageNodes.add(rnode.getCloneNode());
            rnode = rnode.getCloneNode();
        }
        
    }

    @Override
    public boolean initNode()
    {
        boolean ret = super.initNode();

        for (int i = 0; i < cloneStorageNodes.size(); i++)
        {            
            initNode( cloneStorageNodes.get(i) );
        }

        return ret;
    }



    @Override
    public FileHandle create_file_handle(FileSystemElemNode node, boolean create) throws PathResolveException
    {
        if (storageNode.isFS())
        {
            FileHandle ret1 = FS_FileHandle.create_fs_handle(storageNode, node, create );
            MultiFileHandle mfh = new MultiFileHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                FileHandle ret2 = FS_FileHandle.create_fs_handle(cloneStorageNodes.get(i), node, create );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public FileHandle create_xa_node_handle(FileSystemElemNode node, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            FileHandle ret1 = FS_FileHandle.create_xa_handle(storageNode, node, create );
            MultiFileHandle mfh = new MultiFileHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                FileHandle ret2 = FS_FileHandle.create_xa_handle(cloneStorageNodes.get(i), node, create );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public FileHandle create_file_handle(DedupHashBlock block, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            FileHandle ret1 = FS_FileHandle.create_dedup_handle(storageNode, block, create );
            MultiFileHandle mfh = new MultiFileHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                FileHandle ret2 = FS_FileHandle.create_dedup_handle(cloneStorageNodes.get(i), block, create );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public BootstrapHandle create_bootstrap_handle(FileSystemElemNode node) throws PathResolveException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret1 = new FS_BootstrapHandle(storageNode, node );
            MultiBootstrapHandle mfh = new MultiBootstrapHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                BootstrapHandle ret2 = new FS_BootstrapHandle(cloneStorageNodes.get(i), node );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public BootstrapHandle create_bootstrap_handle(DedupHashBlock block) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret1 = new FS_BootstrapHandle(storageNode, block );
            MultiBootstrapHandle mfh = new MultiBootstrapHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                BootstrapHandle ret2 = new FS_BootstrapHandle(cloneStorageNodes.get(i), block );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public BootstrapHandle create_bootstrap_handle(AbstractStorageNode node) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret1 = new FS_BootstrapHandle(storageNode, node );
            MultiBootstrapHandle mfh = new MultiBootstrapHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                BootstrapHandle ret2 = new FS_BootstrapHandle(cloneStorageNodes.get(i), node );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    @Override
    public FileHandle create_DDFS_handle( StoragePoolHandler aThis, FileSystemElemNode node, boolean create, boolean b ) throws PathResolveException, IOException, SQLException
    {
        if (storageNode.isFS())
        {
            FileHandle ret1 = DDFS_FileHandle.create_fs_handle(storageNode, aThis, node, create, /*isStream*/ b );
            MultiFileHandle mfh = new MultiFileHandle();
            mfh.add(ret1);
            for (int i = 0; i < cloneStorageNodes.size(); i++)
            {
                FileHandle ret2 = DDFS_FileHandle.create_fs_handle(cloneStorageNodes.get(i),aThis, node, create, b );
                mfh.add(ret2);
            }
            return mfh;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }


}
