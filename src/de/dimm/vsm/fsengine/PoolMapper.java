/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import javax.persistence.EntityManagerFactory;

/**
 *
 * @author Administrator
 */
public class PoolMapper
{
    StoragePool pool;
    StoragePoolNub nub;
    MiniConnectionPoolManager connectionPoolManager;
    JDBCEntityManager em;
    EntityManagerFactory poolEmf;
    FSEIndexer indexer;
    HashCache hashCache;

    public PoolMapper( StoragePool pool, StoragePoolNub nub, MiniConnectionPoolManager poolManager, JDBCEntityManager em, EntityManagerFactory poolEmf, FSEIndexer indexer )
    {
        this.pool = pool;
        this.nub = nub;
        this.connectionPoolManager = poolManager;
        this.em = em;
        this.poolEmf = poolEmf;
        this.indexer = indexer;
        hashCache = new HashCache(pool);
    }


}