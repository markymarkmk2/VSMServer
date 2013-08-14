/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.fixes;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.FS_BootstrapHandle;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.JDBCStoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Administrator
 */
public class FixBootstrapEntries implements IFix{
    JDBCEntityManager em;
    StoragePool pool;
    long cnt = 0;
    long actCnt = 0;
    int lastPercent = 0;
    int level = 0;
    boolean abort;
    
    StoragePoolHandler spHandler;

    public FixBootstrapEntries( StoragePool pool, JDBCEntityManager em )
    {
        this.em = em;
        this.pool = pool; 
    }

    @Override
    public void setAbort( boolean abort )
    {
        this.abort = abort;
    }
    
    

    @Override
    public void runFix() throws SQLException
    {
        Log.info("Fixing bootstrop entries started for pool " + pool.toString());
        spHandler = new JDBCStoragePoolHandler(em, User.createSystemInternal(), pool, false);
        spHandler.add_storage_node_handlers();
        
        boolean foundSNode = false;
        for (AbstractStorageNode snode : pool.getStorageNodes(em))
        {
            if (!snode.isFS())
            {
                Log.info("Skipping non FS node " + snode.toString());
                continue;        
            }
            
            
            File fsnode = new File(snode.getMountPoint());
            if (!fsnode.exists())
            {
                Log.warn("Skipping non existant node " + snode.toString());
                continue;        
            }
            
            foundSNode = true;
            break;
        }
        if (!foundSNode)
        {
            Log.err("No existing Storage Nodes found");
            return;
        }
        try (Statement st = em.getConnection().createStatement(); ResultSet rs = st.executeQuery("select count(idx) from Filesystemelemnode"))
        {
            if (rs.next())
            {
                cnt = rs.getLong(1);
            }
        }
        Log.info("Nodes to check: " + cnt);

        em.check_open_transaction();

        FileSystemElemNode root = em.em_find(FileSystemElemNode.class, pool.getRootDir().getIdx());

        checkBootstrapEntries( root, /*recurse*/true );

        em.commit_transaction();
    }

    private void checkBootstrapEntries( FileSystemElemNode node, boolean recurse ) throws SQLException
    {
        actCnt++;
        if (cnt > 0)
        {
            int percent = (int)(actCnt * 100 / cnt);
            if (percent != lastPercent)
            {
                lastPercent = percent;
                Log.info( percent + " % done...");
                em.commit_transaction();
            }
        }
                
        try
        {
            checkBootstrapEntry(node);            
        }
        catch (Exception exception)
        {
            Log.err("Das Schreiben der Bootstrapdaten schlug fehl für " + node.toString(), exception);
        }
        if (!node.isDirectory())
            return;
        
        level++;

        List<FileSystemElemNode> children = new ArrayList<>();
        children.addAll(node.getChildren(em));
        
        for (int i = 0; i < children.size(); i++)
        {
            if (abort)
                break;
            FileSystemElemNode actNode = children.get(i);
                                    
            if (recurse)
            {
                checkBootstrapEntries(actNode, /*recurse*/true);
            }           
        }
       
        node.getChildren().unRealize();
        level--;
    }

    private void checkBootstrapEntry( FileSystemElemNode keepNode ) throws SQLException
    {
        Set<AbstractStorageNode> snodes = new HashSet<>();
        List<PoolNodeFileLink> linkList = spHandler.get_pool_node_file_links(keepNode);
        if (linkList != null && !linkList.isEmpty())
        {
            for (PoolNodeFileLink poolNodeFileLink : linkList)
            {
                snodes.add(poolNodeFileLink.getStorageNode());
            }            
        }
        else
        {
            snodes.addAll( spHandler.get_primary_storage_nodes(false));
        }
        
        for (AbstractStorageNode snode : snodes)
        {
            if (!snode.isFS())
                continue;
            
            File fsnode = new File(snode.getMountPoint());
            if (!fsnode.exists())
                continue;
            
            try
            {
               FS_BootstrapHandle bh = new FS_BootstrapHandle(snode, keepNode);
               if (!bh.exists())
                   bh.write_bootstrap(keepNode);
                
               
                for (FileSystemElemAttributes attr : keepNode.getHistory(em))
                {
                    // Skip first Attr (is in Node Bootstrap)
                    if (attr.getIdx() == keepNode.getAttributes().getIdx())
                        continue;
                    bh = new FS_BootstrapHandle(snode, attr);
                    if (!bh.exists())
                       bh.write_bootstrap(attr);
                }
                for (HashBlock block : keepNode.getHashBlocks(em))
                {
                    bh = new FS_BootstrapHandle(snode, block.getDedupBlock(), block);
                    if (!bh.exists())
                       bh.write_bootstrap(block);                                        
                }                
                for (XANode block : keepNode.getXaNodes(em))
                {
                    bh = new FS_BootstrapHandle(snode, block.getDedupBlock(), block);
                    if (!bh.exists())
                       bh.write_bootstrap(block);                                        
                }                
                for (PoolNodeFileLink link : keepNode.getLinks(em))
                {
                    bh = new FS_BootstrapHandle(snode, link);
                    if (!bh.exists())
                       bh.write_bootstrap(link);                                        
                }                
            }
            catch (PathResolveException | IOException exc)
            {
                Log.err("Abbruch bei node " + keepNode.toString(), exc);
            }
            
        }
            
        
        
        
    }
}
