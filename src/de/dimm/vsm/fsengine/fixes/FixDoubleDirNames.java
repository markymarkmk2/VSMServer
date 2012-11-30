/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.fixes;

import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.XANode;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Administrator
 */
public class FixDoubleDirNames {
    JDBCEntityManager em;
    StoragePool pool;
    long cnt = 0;
    long actCnt = 0;
    int lastPercent = 0;
    int level = 0;

    public FixDoubleDirNames( StoragePool pool, JDBCEntityManager em )
    {
        this.em = em;
        this.pool = pool;
    }

    public void runFix() throws SQLException
    {
        Log.info("Fixing duplicate names started");
        //cnt = 4935881;
        Statement st = em.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select count(idx) from Filesystemelemnode");
        if (rs.next())
        {
            cnt = rs.getLong(1);
        }
        rs.close();
        st.close();
        Log.info("Nodes to check: " + cnt);

        em.check_open_transaction();

        FileSystemElemNode root = em.em_find(FileSystemElemNode.class, pool.getRootDir().getIdx());

        checkDuplicates( root, /*recurse*/true );

        em.commit_transaction();
    }

    private void checkDuplicates( FileSystemElemNode node, boolean recurse ) throws SQLException
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
        if (!node.isDirectory())
        {
            return;
        }
        
        level++;

        //Log.debug("Checking " + level + ": " + node.getName() );

        // SORT IN INCREMENTING INDEX, OLD NODES ARE FIRST
        List<FileSystemElemNode> children = new ArrayList<FileSystemElemNode>();
        children.addAll(node.getChildren(em));
        Collections.sort(children , new Comparator<FileSystemElemNode>() {

            @Override
            public int compare( FileSystemElemNode o1, FileSystemElemNode o2 )
            {
                if (o2.getIdx() == o1.getIdx())
                    return 0;
                return (o1.getIdx() > o2.getIdx()) ? 1 : -1;
            }
        });


        // NOW FIND NODES WITH DUPLICATE NAMES AND MERGE SECOND FOUND NODE INTO FIRST FOUND NODE
        Map<String,FileSystemElemNode> map = new HashMap<String, FileSystemElemNode>();
        boolean merged = false;

        for (int i = 0; i < children.size(); i++)
        {
            FileSystemElemNode actNode = children.get(i);
            
            //  FIRST RECURSE DOWN, THEN WE CAN DELETE UNNEEDED DIRS ON OUR WAY BACK
            if (recurse)
            {
                checkDuplicates(actNode, /*recurse*/true);
            }

            if (map.containsKey(actNode.getName()))
            {

                FileSystemElemNode keepNode = map.get(actNode.getName());
                mergeNodes( keepNode, actNode );
                merged = true;


                if(!(actNode.getChildren(em).isEmpty() && actNode.getHashBlocks(em).isEmpty() && actNode.getHistory(em).isEmpty() && actNode.getXaNodes(em).isEmpty()))
                {
                    throw new SQLException("Objekt ist nicht leer");
                }

                em.em_merge(keepNode);
                actNode.setParent(null);
                
                em.em_merge(actNode);
                try
                {
                    em.em_remove(actNode);
                }
                catch (SQLException sQLException)
                {
                    Log.err("Mist");
                    throw sQLException;
                }
                // OKAY, DUPLICATE NODE WAS MERGED, FINALLY UPDATE CHILDLIST
                children.remove(actNode);
                if (node.getChildren().isRealized())
                {
                    node.getChildren().remove(actNode);
                }

                i--;

                em.check_commit_transaction();

                // CLEAN UP NEW MERGED CHILDLIST
                checkDuplicates(keepNode, /*recurse*/ false);
            }
            else
            {
                map.put(actNode.getName(), actNode);
            }
        }
       

        node.getChildren().unRealize();
        level--;
    }

    private void mergeNodes( FileSystemElemNode keepNode, FileSystemElemNode delNode ) throws SQLException
    {

        Log.debug("Merging " + keepNode.getName() + " ID:" + delNode.getIdx() + " to ID:" + keepNode.getIdx());
        List<FileSystemElemAttributes> delAttrList = delNode.getHistory(em);



        // UPDATE ATTRIBUTES
        while( delAttrList.size() > 0 )
        {
            FileSystemElemAttributes attr = delAttrList.remove(0);
            keepNode.getHistory().addIfRealized(attr);

            attr.setFile(keepNode);

            em.em_merge(attr);

            // DO WE HAVE A NEWER ATTRIBUTE?
            if (keepNode.getAttributes().getTs() < attr.getTs())
            {
                // THEN UPDATE IT
                keepNode.setAttributes(attr);
            }
        }

        // POOLNODEFILELINKS
        List<PoolNodeFileLink> delPnflList = delNode.getLinks(em);
        List<PoolNodeFileLink> keepPnflList = keepNode.getLinks(em);
        // UPDATE POOLNODE FILE LINKS
        while ( delPnflList.size() > 0)
        {
            PoolNodeFileLink pfnl = delPnflList.remove(0);
            AbstractStorageNode sNode = pfnl.getStorageNode();
            boolean found = false;
            for (int j = 0; j < keepPnflList.size(); j++)
            {
                PoolNodeFileLink poolNodeFileLink = keepPnflList.get(j);
                if (poolNodeFileLink.getStorageNode().getIdx() == sNode.getIdx())
                {
                    found = true;
                    break;
                }
            }
            // REMOVE DUPLICATES
            if (found)
            {
                pfnl.setFileNode(null);
                pfnl.setStorageNode(null);
                em.em_remove(pfnl);
                continue;
            }

            pfnl.setFileNode(keepNode);
            em.em_merge(pfnl);

            keepPnflList.add(pfnl);
        }

        // HASHBLOCKS AND XABLOCKS MUST BE UPDATED AND MERGED INTO OLD NODE
        List<HashBlock> delHashList = delNode.getHashBlocks(em);
        while ( delHashList.size() > 0)
        {
            HashBlock hb = delHashList.remove(0);
            keepNode.getHashBlocks().addIfRealized(hb);
            hb.setFileNode(keepNode);
            em.em_merge(hb);
        }

        List<XANode> dekXaList = delNode.getXaNodes(em);
        while ( dekXaList.size() > 0)
        {
            XANode xn = dekXaList.remove(0);
            keepNode.getXaNodes().addIfRealized(xn);
            xn.setFileNode(keepNode);
            em.em_merge(xn);
        }

        // JETZT ALLE KINDER UMHAENGEN
        List<FileSystemElemNode> children  = delNode.getChildren(em);
        while ( children.size() > 0)
        {
            FileSystemElemNode xn = children.remove(0);
            keepNode.getChildren().addIfRealized(xn);
            xn.setParent(keepNode);
            em.em_merge(xn);
        }
    }
}
