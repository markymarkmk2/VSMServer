/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsengine.hashcache.HashCache;
import de.dimm.vsm.DBChecker;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import javax.persistence.EntityManagerFactory;

/**
 *
 * @author Administrator
 */
public abstract class AbstractStoragePoolNubHandler  implements IStoragePoolNubHandler {

    protected final ArrayList<PoolMapper> mapperList;
    protected EntityManagerFactory baseEmf;

    public AbstractStoragePoolNubHandler()
    {
        baseEmf = LogicControl.get_base_util_emf();
        mapperList = new ArrayList<>();        
    }
    
    
    @Override
    public void check_db_changes() throws SQLException {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                DBChecker.check_pool_db_changes(poolMapper.em.getConnection());
            }
        }
    }    

    @Override
    public int getActiveConnections( StoragePool pool ) {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx()) {
                    return poolMapper.connectionPoolManager.getActiveConnections();
                }
            }
        }
        return -1;
    }

    @Override
    public JDBCConnectionFactory getConnectionFactory( StoragePool pool ) throws SQLException {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx()) {
                    return poolMapper.connectionPoolManager;
                }
            }
        }
        throw new SQLException("No entry found for Pool " + pool.toString());
    }

    @Override
    public HashCache getHashCache( StoragePool pool ) {
        PoolMapper pm = getPoolMapper(pool);
        if (pm != null) {
            return pm.hashCache;
        }
        return null;
    }

    @Override
    public FSEIndexer getIndexer( StoragePool pool ) {
        PoolMapper pm = getPoolMapper(pool);
        if (pm != null) {
            return pm.indexer;
        }
        return null;
    }

    @Override
    public PoolMapper getPoolMapper( StoragePool pool ) {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (poolMapper.pool.getIdx() == pool.getIdx()) {
                    return poolMapper;
                }
            }
        }
        return null;
    }

    @Override
    public StoragePool getStoragePool( long idx ) throws SQLException {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (idx == poolMapper.pool.getIdx()) {
                    return poolMapper.pool;
                }
            }
        }
        throw new SQLException("No entry found for Pool " + idx);
    }

    @Override
    public JDBCEntityManager getUtilEm( StoragePool pool ) throws SQLException {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx()) {
                    return poolMapper.em;
                }
            }
        }
        throw new SQLException("No Mapper found vor pool " + pool.toString());
    }

    @Override
    public void infoStats() {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                poolMapper.calcStats();
                poolMapper.logStats();
            }
        }
    }

    @Override
    public ArrayList<StoragePool> listStoragePools() {
        ArrayList<StoragePool> ret = new ArrayList<>();
        for (int i = 0; i < mapperList.size(); i++) {
            PoolMapper m = mapperList.get(i);
            ret.add(m.pool);
        }
        return ret;
    }

    protected void rebuildIndex( PoolMapper poolMapEntry ) {
        FSEIndexer indexer = poolMapEntry.indexer;
        Connection conn = null;
        try {
            conn = poolMapEntry.connectionPoolManager.createConnection();
            indexer.rebuildIndex(conn);
        }
        catch (Exception sQLException) {
            Log.err("Fehler beim Neuaufbau des Indexes", sQLException);
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException sQLException) {
                }
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                try {
                    poolMapper.indexer.close();
                    poolMapper.em.close_transaction();
                    poolMapper.em.close_entitymanager();
                    poolMapper.poolEmf.close();
                    poolMapper.connectionPoolManager.dispose();
                    poolMapper.hashCache.shutdown();
                }
                catch (Exception e) {
                    Log.err("Abbruch beim Schließen des EntityManagers", poolMapper.pool.toString(), e);
                }
            }
        }
    }

  
    @Override
    public boolean isCacheLoading()
    {
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                PoolMapper poolMapper = mapperList.get(i);
                if (poolMapper.isCacheLoading())
                    return true;
            }
        }
        return false;
    }    
    
    
    protected StoragePool createNewStoragePool( GenericEntityManager gem ) throws Exception {
        StoragePool p = new StoragePool();
        p.setCreation(new Date());
        p.setName(Main.Txt("Neuer StoragePool"));
        p.setLandingZone(false);
        try {
            // CREATE ROOT DIR
            FileSystemElemNode root_node = FileSystemElemNode.createDirNode();
            root_node.getAttributes().setName("/");
            long now = System.currentTimeMillis();
            root_node.getAttributes().setCreationDateMs(now);
            root_node.getAttributes().setModificationDateMs(now);
            root_node.getAttributes().setAccessDateMs(now);
            gem.check_open_transaction();
            root_node.getAttributes().setFile(null);
            gem.em_persist(root_node.getAttributes(), true);
            gem.em_persist(root_node, true);
            root_node.getAttributes().setFile(root_node);
            gem.em_merge(root_node.getAttributes());
            // Reload from DB
            root_node = gem.em_find(FileSystemElemNode.class, root_node.getIdx());
            p.setRootDir(root_node);
            gem.em_persist(p);
            //gem.em_merge(p);
            root_node.setPool(p);
            gem.em_merge(root_node);
            gem.commit_transaction();
            return p;
        }
        catch (Exception e) {
            Log.err("Abbruch bei createNewStoragePool", e);            
            gem.rollback_transaction();
            throw e;
        }
    }
    
}
