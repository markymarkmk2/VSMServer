/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 *
 * @author mw
 */
public class JDBCStoragePoolHandler extends StoragePoolHandler
{
    JDBCEntityManager em;

    

    HandlePersistRunner persistRunner;

    public JDBCStoragePoolHandler( JDBCEntityManager em, StoragePool pool, StoragePoolQry qry ) throws SQLException
    {
        super(pool, qry);
        this.em = em;

        // MAP ALL LAZY LIST HANDLERS TO NEW EM
        //updateLazyLists();

        add_storage_node_handlers();
    }

//    final void updateLazyLists()
//    {
//        // MAP ALL LAZY LIST HANDLERS TO NEW EM
//        updateLazyListsHandler( pool.getStorageNodes() );
//        updateLazyListsHandler( pool.getRootDir());
//    }

    private JDBCStoragePoolHandler( JDBCEntityManager em, User user, StoragePool pool, boolean readOnly, long snapShotTs ) throws SQLException
    {
        this(em, pool, new StoragePoolQry( user, readOnly, snapShotTs, false));
    }

    public JDBCStoragePoolHandler( JDBCEntityManager em, User user, StoragePool pool, boolean readOnly ) throws SQLException
    {
        // ACTUAL FILESYSTEM, CANNOT BE A SNAPSHOTFILESYSTEM
        this(em, user, pool, readOnly, -1);
    }

    public JDBCStoragePoolHandler( JDBCEntityManager em, User user, StoragePool pool, long snapShotTs ) throws SQLException
    {
        // SNAPSHOT FILESYSTEM, CAN ONLY BE RDONLY
        this(em, user, pool, /*rdonly*/ true, snapShotTs);
    }

    public void setPersistRunner( HandlePersistRunner persistRunner )
    {
        this.persistRunner = persistRunner;
    }


//    @Override
//    public void updateLazyListsHandler(  List l )
//    {
//        if (l != null && l instanceof LazyList)
//        {
//            LazyList ll = (LazyList)l;
//            ll.setHandler(em);
//        }
//    }
    
//    @Override
//    public void updateLazyListsHandler(  FileSystemElemNode node )
//    {
//        updateLazyListsHandler( node.getChildren());
//        updateLazyListsHandler( node.getHashBlocks());
//        updateLazyListsHandler( node.getLinks());
//        updateLazyListsHandler( node.getHistory());
//        updateLazyListsHandler( node.getXaNodes());
//    }

    @Override
    public void addDedupBlock2Cache(DedupHashBlock blk )
    {
         Cache c = em.getDedupBlockCache();

         Element elem = new Element(blk.getHashvalue(), blk);
         c.putIfAbsent(elem);
    }

    @Override
    public DedupHashBlock getDedupBlockFromCache( String remote_hash )
    {
        Cache c = em.getDedupBlockCache();
        Element elem = c.get(remote_hash);
        if (elem != null)
            return (DedupHashBlock)elem.getObjectValue();

        return null;
    }

    long fsMax = 0;
    long fsMin = Long.MAX_VALUE;
    long cnt = 0;

    @Override
    public DedupHashBlock findHashBlock( String remote_hash )
    {
        DedupHashBlock blk = getDedupBlockFromCache( remote_hash );
        if (blk != null)
            return blk;

        try
        {
            long fs = System.nanoTime();

            blk = createSingleResultQuery("SELECT * FROM DedupHashBlock T1 WHERE T1.hashvalue='" + remote_hash + "'", DedupHashBlock.class);

            long fe = System.nanoTime();

            long diff = fe - fs;

            cnt++;
            if (cnt > 10)
            {
                // SHOW us
                diff /=1000;
                if (cnt%10 == 0)
                    System.out.println("Act HBtime " + diff + " us");

                if (diff > fsMax)
                {
                    fsMax = diff;
                    System.out.println("Max HBtime " + fsMax + " us");
                }
                if (diff < fsMin)
                {
                    fsMin = diff;
                    System.out.println("Min HBtime " + fsMin + " us");
                }
            }

            if (blk != null)
            {
                addDedupBlock2Cache( blk );
            }
            return blk;
        }
        catch (SQLException sQLException)
        {
            Log.err("Cannot findHashBlocks", sQLException);
        }
        return null;
    }

   


    @Override
    public void em_persist( Object o ) throws SQLException
    {
        if (persistRunner != null)
        {
            persistRunner.em_persist(o, em);
        }
        else
        {
            em.em_persist(o);
        }
    }

    @Override
    public void em_remove( Object o ) throws SQLException
    {
        em.em_remove(o);
    }

    @Override
    public void em_detach( Object o ) throws SQLException
    {
        em.em_detach(o);
    }

    @Override
    public <T> T em_merge( T t ) throws SQLException
    {
        if (persistRunner != null)
        {
            return persistRunner.em_merge(t, em);
        }

        return em.em_merge(t);
    }

    @Override
    public <T> T em_find( Class<T> t, long idx ) throws SQLException
    {
        T ret =  em.em_find(t, idx);
//        if (ret != null && ret.getClass() == FileSystemElemNode.class)
//        {
//            FileSystemElemNode fse = (FileSystemElemNode)ret;
//            FileSystemElemAttributes attr = em.em_find(FileSystemElemAttributes.class, fse.getAttributesIdx());
//            if (attr == null)
//                throw new SQLException("Missing attribute " + fse.getAttributesIdx() + " for FseNode " + fse.getIdx());
//            fse.setAttributes(attr);
//        }
        return ret;
    }

    @Override
    public void tx_commit()
    {
        em.tx_commit();
    }

    @Override
    public void check_open_transaction()
    {
        em.check_open_transaction();
    }

    @Override
    public void commit_transaction() throws SQLException
    {
        if (persistRunner != null)
        {
            persistRunner.commit_transaction(em);
            try
            {
                persistRunner.waitForFinish();
            }            
            catch (InterruptedException interruptedException)
            {
            }
        }
        else
        {
            em.commit_transaction();
        }
    }

    @Override
    public boolean is_transaction_active()
    {
        return em.is_transaction_active();
    }

    @Override
    public void check_commit_transaction() throws SQLException
    {
        if (persistRunner != null)
        {
            persistRunner.check_commit_transaction(em);
        }
        else
        {
            em.check_commit_transaction();
        }
    }

    @Override
    public void rollback_transaction()
    {

        em.rollback_transaction();
    }

    @Override
    public void close_transaction() throws SQLException
    {
        if (persistRunner != null)
        {
            persistRunner.close_transaction(em);
        }
        else
        {
            em.close_transaction();
        }
    }

    @Override
    public void close_entitymanager()
    {
        if (persistRunner != null)
        {
            persistRunner.close();
        }
        Log.debug("Schließe EntityManager");
        em.close_entitymanager();
  
    }

    @Override
    public <T> List<T> createQuery( String string, Class<T> aClass ) throws SQLException
    {
        return em.createQuery(string, aClass);
    }
    @Override
    public <T> List<T> createQuery( String string, Class<T> aClass, int maxResults ) throws SQLException
    {
        return em.createQuery(string, aClass, maxResults);
    }
    @Override
    public <T> List<T> createQuery( String string, Class<T> aClass, int maxResults, boolean distinct ) throws SQLException
    {
        return em.createQuery(string, aClass, maxResults, distinct);
    }

    @Override
    public List<Object[]> createNativeQuery( String string, int maxResults ) throws SQLException
    {
        return em.createNativeQuery(string, maxResults);
    }

    @Override
    public int nativeUpdate( String string )
    {
        return em.nativeUpdate(string);
    }

    @Override
    public boolean nativeCall( String string )
    {
        return em.nativeCall(string);
    }


    @Override
    public <T> T createSingleResultQuery( String string, Class<T> aClass ) throws SQLException
    {
        return em.createSingleResultQuery(string, aClass);
    }

    @Override
    public void em_refresh( Object fseNode )
    {
        em.em_refresh(fseNode);
    }


    @Override
    public GenericEntityManager getEm()
    {
        return em;
    }
    public JDBCEntityManager getJDBCEm()
    {
        return em;
    }

    int reinitCounter = 0;
    @Override
    protected void reinitConnection() throws IOException
    {
        // RETRY UP TO N TIMES
        reinitCounter++;
        if (reinitCounter > 50)
        {
            Log.err("Zu viele Verbindungsabbrüche");
            throw new IOException("Cannot reinit Connection");
        }

        try
        {
            if (em.getConnection().isValid(1))
            {
                return;
            }

            em.reopenConnection();
        }
        catch (Exception sQLException)
        {
            Log.err("Verbindung kann nicht wieder aufgebaut werden", sQLException);
        }

    }
    public int getPersistQueueLen()
    {
        if (persistRunner != null)
            return persistRunner.getQueueLen();
        return 0;
    }
    

}
