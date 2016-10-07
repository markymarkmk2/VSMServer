/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.fixes;

import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteFSElem;
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
public class FixDoubleDirNames implements IFix {
    private final GenericEntityManager em;
    private final StoragePool pool;
    private long cnt = 0;
    private long actCnt = 0;
    private long correctedCnt = 0;
    private int lastPercent = 0;
    private int level = 0;
    private boolean abort;

    public FixDoubleDirNames( StoragePool pool, GenericEntityManager em )    {
        this.em = em;
        this.pool = pool;
    }



    @Override
    public boolean runFix() throws SQLException
    {
        Log.info("Fixing duplicate names started");
        if (em instanceof JDBCEntityManager) {
            try (Statement st = ((JDBCEntityManager) em).getConnection().createStatement(); ResultSet rs = st.executeQuery("select count(idx) from Filesystemelemnode")) {
                if (rs.next()) {
                    cnt = rs.getLong(1);
                }
            }
        }
        Log.info("Nodes to check: " + cnt);

        em.check_open_transaction();

        FileSystemElemNode root = em.em_find(FileSystemElemNode.class, pool.getRootDir().getIdx());

        checkDuplicates( root, /*recurse*/true );

        em.commit_transaction();
        
        return true;
    }

    public boolean runDirectoryFix( StoragePoolHandler sp, RemoteFSElem path ) throws SQLException, IOException {

        FileSystemElemNode fseNode = null;
        try {
            fseNode = sp.resolve_node_by_remote_elem(path);
            if (fseNode == null) {
                throw new IOException("Kein Eintrag in VSM-DB für: " + path.getName());

            }
        }
        catch (SQLException sQLException) {
            throw new IOException("Abbruch beim Lesen von Eintrag in VSM-DB für: " + path.getName() + ": " + sQLException.getMessage());

        }

        em.check_open_transaction();

        FileSystemElemNode root = em.em_find(FileSystemElemNode.class, pool.getRootDir().getIdx());

        checkDuplicates(fseNode, /**
                 * recurse
                 */
                false);

        em.commit_transaction();

        return correctedCnt > 0;
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
        List<FileSystemElemNode> children = new ArrayList<>();
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
        Map<String,FileSystemElemNode> map = new HashMap<>();
        
        for (int i = 0; i < children.size(); i++)
        {
            if (abort)
                break;
            
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
                

                if(!(actNode.getChildren(em).isEmpty() && actNode.getHashBlocks(em).isEmpty() && actNode.getHistory(em).isEmpty() && actNode.getXaNodes(em).isEmpty()))
                {
                    throw new SQLException("Objekt ist nicht leer");
                }

                try {
                    em.em_merge(keepNode);
                    em.commit_transaction();
                    correctedCnt++;
                }
                catch (SQLException sQLException) {
                    Log.err("Mergen des Original Nodes fehlgeschlagen: " + keepNode.getName() + " Parent:" + node.getName(), sQLException);
                }

                // Parent entfernen
                actNode.setParent(null);
                
                try {
                    em.em_merge(actNode);
                    em.commit_transaction();
                }
                catch (SQLException sQLException) {
                    Log.err("Mergen des duplizierten Nodes fehlgeschlagen: " + actNode.getName() + " Parent:" + node.getName(), sQLException);
                }
                try {
                    em.em_remove(actNode);
                    em.commit_transaction();
                }
                catch (SQLException sQLException) {
                    Log.err("Löschen des duplizierten Nodes fehlgeschlagen: "
                            + "Node :" + actNode.getName() + " " + actNode.getIdx()
                            + " Parent:" + node.getName() + " " + node.getIdx(),
                            sQLException);
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
                if (recurse) {
                    checkDuplicates(keepNode, /*
                             * recurse
                             */ false);
                }
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

    @Override
    public String getStatusStr()
    {
        return "";
    }

    @Override
    public String getStatisticStr()
    {
        return "";
    }

    @Override
    public Object getResultData()
    {
        return "";
    }

    @Override
    public String getProcessPercent()
    {
        return Integer.toString(lastPercent);
    }

    @Override
    public String getProcessPercentDimension()
    {
        return "%";
    }

    @Override
    public void abortJob()
    {
        abort = true;
    }

    @Override
    public boolean isAborted()
    {
        return abort;
    }
    
    

    @Override
    public void close()
    {
        try {
            em.close_transaction();
        }
        catch (SQLException sQLException) {
            Log.err("Closing EM", sQLException);
        }
        em.close_entitymanager();
    }
}
