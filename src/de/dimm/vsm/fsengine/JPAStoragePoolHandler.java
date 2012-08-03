/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.sql.SQLException;
import java.util.List;
import javax.persistence.EntityManager;

/**
 *
 * @author mw
 */
public class JPAStoragePoolHandler extends StoragePoolHandler /*implements RemoteFSApi*/
{
    

    JPAEntityManager em;

    public JPAStoragePoolHandler( EntityManager em, StoragePool pool, StoragePoolQry qry )
    {
        super( pool, qry );
        this.em = new JPAEntityManager(em);
    }
    
    private JPAStoragePoolHandler( EntityManager em,  User user, StoragePool pool, boolean readOnly, long snapShotTs )
    {
        this( em, pool, new StoragePoolQry(user, readOnly, snapShotTs, false) );
    }

    public JPAStoragePoolHandler( EntityManager em,  User user, StoragePool pool, boolean readOnly )
    {
        // ACTUAL FILESYSTEM, CANNOT BE A SNAPSHOTFILESYSTEM
        this( em, user, pool, readOnly, -1 );
    }


    public JPAStoragePoolHandler( EntityManager em,  User user, StoragePool pool, long snapShotTs )
    {
        // SNAPSHOT FILESYSTEM, CAN ONLY BE RDONLY
        this( em, user, pool, /*rdonly*/ true, snapShotTs );
    }
    private static void todo(String s )
    {
        System.err.println("Todo: " + s);
    }
    @Override
    public DedupHashBlock findHashBlock( String remote_hash )
    {
        List<DedupHashBlock> list = em.createQuery( "SELECT T1 FROM DedupHashBlock T1 WHERE T1.hashvalue='" + remote_hash + "'", DedupHashBlock.class );
        return list.get(0);
    }

   



    @Override
    public void em_persist( Object o ) throws SQLException
    {
        em.em_persist(o);
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
        return em.em_merge(t);
    }

    @Override
    public <T> T em_find( Class<T> t, long idx ) throws SQLException
    {
        return em.em_find(t, idx);
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
    public void commit_transaction()
    {
        em.commit_transaction();
    }

    @Override
    public boolean is_transaction_active()
    {
        return em.is_transaction_active();
    }

    @Override
    public void check_commit_transaction()
    {
        em.check_commit_transaction();
    }

    @Override
    public void rollback_transaction()
    {
        em.rollback_transaction();
    }

    @Override
    public void close_transaction()
    {
        em.close_transaction();
    }

    @Override
    public void close_entitymanager()
    {
        em.close_entitymanager();
    }

    @Override
    public <T> List<T> createQuery( String string, Class<T> aClass )
    {
        return em.createQuery(string, aClass);
    }
    @Override
    public <T> List<T> createQuery( String string, Class<T> aClass, int maxResults )
    {
        return em.createQuery(string, aClass, maxResults);
    }
    @Override
    public List<Object[]> createNativeQuery( String string, int maxResults )
    {
        return em.createNativeQuery(string, maxResults);
    }

    @Override
    public <T> T createSingleResultQuery( String string, Class<T> aClass )
    {
        return em.createSingleResultQuery(string, aClass);
    }

    @Override
    public void em_refresh( Object fseNode )
    {
        em.em_refresh(fseNode);
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
    public <T> List<T> createQuery( String string, Class<T> aClass, int maxResults, boolean distinct ) throws SQLException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

//    @Override
//    public void updateLazyListsHandler( List l )
//    {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }
//
//    @Override
//    public void updateLazyListsHandler( FileSystemElemNode node )
//    {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }

    @Override
    public GenericEntityManager getEm()
    {
        return em;
    }

}
