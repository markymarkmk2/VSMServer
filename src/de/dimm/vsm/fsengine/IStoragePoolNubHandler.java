/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsengine.hashcache.HashCache;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author Administrator
 */
public interface IStoragePoolNubHandler {

    void check_db_changes() throws SQLException;

    StoragePool createEmptyPoolDatabase( StoragePoolNub nub ) throws PathResolveException, IOException, SQLException;

    StoragePool createEmptyPoolDatabase( StoragePoolNub nub, String dbPath ) throws PathResolveException, IOException, SQLException;

    int getActiveConnections( StoragePool pool );

    JDBCConnectionFactory getConnectionFactory( StoragePool pool ) throws SQLException;

    HashCache getHashCache( StoragePool pool );

    FSEIndexer getIndexer( StoragePool pool );
    
    String getIndexPath( StoragePoolNub nub );    

    PoolMapper getPoolMapper( StoragePool pool );

    StoragePool getStoragePool( long idx ) throws SQLException;

    JDBCEntityManager getUtilEm( StoragePool pool ) throws SQLException;

    void infoStats();

    void initMapperList();

    ArrayList<StoragePool> listStoragePools();

    StoragePool mountPoolDatabase( StoragePoolNub nub, String dbPath, boolean rebuild ) throws PathResolveException, IOException, SQLException;

    void removePoolDatabase( StoragePool pool, boolean physically ) throws SQLException;

    void shutdown();

    boolean isCacheLoading();
    
}
