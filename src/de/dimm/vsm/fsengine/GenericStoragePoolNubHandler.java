/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.DBChecker;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.DirectoryEntry;
import de.dimm.vsm.fsengine.fixes.FixBootstrapEntries;
import de.dimm.vsm.fsengine.fixes.FixDoubleDirNames;
import de.dimm.vsm.fsengine.fixes.IFix;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

/**
 *
 * @author Administrator
 */



public class GenericStoragePoolNubHandler
{

    final ArrayList<PoolMapper> mapperList;
    EntityManagerFactory baseEmf;

    
    public static final String DB_PARAM_NAME = "VSMParams";
    
    public static boolean isDiskDB( String jdbcString)
    {
        return jdbcString.startsWith( "jdbc:derby");
    }
    public static boolean isRemotekDB( String jdbcString)
    {
        return !isDiskDB( jdbcString );
    }

    public GenericStoragePoolNubHandler()
    {
        
        baseEmf = LogicControl.get_base_util_emf();
        
        mapperList = new ArrayList<PoolMapper>();
        
    }
    
    public void removePoolDatabase( StoragePool pool, boolean physically) throws SQLException
    {
        PoolMapper poolMapper = null;
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {                
                if (mapperList.get(i).pool.getIdx() == pool.getIdx())
                {
                    poolMapper = mapperList.remove(i);
                    break;
                }
            }
        }


        if (poolMapper == null)
            throw new SQLException("No Mapper found vor pool " + pool.toString() );

        try
        {
            poolMapper.indexer.close();
            poolMapper.em.close_transaction();
            poolMapper.em.close_entitymanager();
            poolMapper.poolEmf.close();
            poolMapper.connectionPoolManager.dispose();

            poolMapper.hashCache.shutdown();

            // REMOVE NUB
            LogicControl.get_base_util_em().check_open_transaction();
            LogicControl.get_base_util_em().em_remove(poolMapper.nub);
            LogicControl.get_base_util_em().commit_transaction();
        }
        catch (Exception e)
        {

            Log.err("Abbruch beim Schließen des EntityManagers", pool.toString(), e);
        }

        
        if (physically && hasPhysicalDBPath(poolMapper.nub))
        {
            try
            {
                closeDerbyDB(poolMapper);
            }
            catch (SQLException e)
            {
                Log.err("Abbruch beim Schließen der Datenbank", pool.toString(), e);
            }
            String path = getDatabasePath( poolMapper.nub );
            if (path != null)
            {
                File dbDir = new File(path);
                if (dbDir.exists())
                {
                    DirectoryEntry de = new DirectoryEntry(dbDir);
                    de.delete_recursive();
                }
            }
            path = getDbRootPath( poolMapper.nub);
            if (path != null)
            {
                File dbDir = new File(path);
                if (dbDir.exists())
                {
                    dbDir.delete();
                }
            }
        }
    }
    
    boolean hasPhysicalDBPath( StoragePoolNub nub )
    {
        return isDiskDB(nub.getJdbcConnectString());
    }

    private static String getDatabasePath(StoragePoolNub nub)
    {
        // TODO: DIFFERENT EMBEDDED DATABASES
        if (!isDiskDB(nub.getJdbcConnectString()))
            return null;

//        String jdbcConnectString = "jdbc:derby:" + dbPath + "/VSMParams;create=true";
        String url = nub.getJdbcConnectString();
        String start = "jdbc:derby:";
        try
        {
            String[] urlParts = url.substring(start.length()).split(";");
            String db = urlParts[0];
            return db;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    protected String getDbPath( StoragePoolNub nub )
    {
        String s = Main.get_prop(GeneralPreferences.DB_PATH, Main.DATABASEPATH );
        s = s.replace('\\', '/');
        if (!s.endsWith("/"))
            s += "/";

        String dbPath = s + nub.getDbName() + "/" + DB_PARAM_NAME;
        return dbPath;
    }
    protected String getDbRootPath( StoragePoolNub nub )
    {
        String s = Main.get_prop(GeneralPreferences.DB_PATH, Main.DATABASEPATH );
        s = s.replace('\\', '/');
        if (!s.endsWith("/"))
            s += "/";

        String dbPath = s + nub.getDbName();
        return dbPath;
    }
    protected String getIndexPath( StoragePoolNub nub )
    {
        String s = Main.get_prop(GeneralPreferences.DB_PATH, Main.DATABASEPATH );
        s = s.replace('\\', '/');
        if (!s.endsWith("/"))
            s += "/";

        String dbPath = s + nub.getDbName() + "/Index";
        return dbPath;
    }

    public StoragePool createEmptyPoolDatabase( StoragePoolNub nub, boolean discDb ) throws PathResolveException, IOException, SQLException
    {
        // TODO: DIFFERENT EMBEDDED DATABASES
        if (discDb)
        {
            String dbPath = Main.DATABASEPATH + nub.getDbName();
            File dir = new File( dbPath);


            if ( dir.exists())
            {

                return mountPoolDatabase(nub, dbPath, /*rebuild*/true);
            }

            dir.getParentFile().mkdirs();
        }       
        else
        {
            throw new RuntimeException("createEmptyPoolDatabase mit unbekanntem DB-Typ");
//            createRemoteDB(nub);
        }

        return createEmptyPoolDatabase(nub);
    }
    
    String createJdbcConnectString( StoragePoolNub nub )
    {
        String dbName = nub.getDbName();
        String jdbcConnectString;
        String jdbcConnect = getDBConnectString(nub);
        if (jdbcConnect.startsWith("jdbc:derby:"))
        {            
            jdbcConnectString = jdbcConnect + Main.DATABASEPATH + dbName;            
        }
        else
        {
            jdbcConnectString = jdbcConnect + "DB_" + nub.getIdx() + "_" + dbName;
        }
        return jdbcConnectString;        
    }
    
    public StoragePool createEmptyPoolDatabase( StoragePoolNub nub) throws PathResolveException, IOException, SQLException
    {

        String jdbcConnectString = createJdbcConnectString(nub) + ";create=true";

        HashMap<String,String> map = new HashMap<String, String>();
        map.put("javax.persistence.jdbc.url", jdbcConnectString);

        EntityManagerFactory poolEmf = Persistence.createEntityManagerFactory("VSM", map);
        MiniConnectionPoolManager poolManager = JDBCEntityManager.initializeJDBCPool(poolEmf, jdbcConnectString);        

        JDBCEntityManager em = new JDBCEntityManager( 0, poolManager);

        em.check_open_transaction();
        
        List<StoragePool> list = em.createQuery("select T1 from StoragePool T1", StoragePool.class);

        em.close_transaction();

        StoragePool pool = null;

        if (list.size() > 1)
        {
            throw new IOException( "Too many Pools in PoolDB " + jdbcConnectString );
        }
        if (list.isEmpty())
        {
            try
            {
                pool = createNewStoragePool(em);
            }
            catch (Exception ex)
            {
                throw new IOException( "Cannot create Pool in PoolDB " + jdbcConnectString );
            }
        }
        else
        {
            throw new IOException( "PoolDB is not empty " + jdbcConnectString );
        }

        // REGISTER NEW SETTINGS
        nub.setJdbcConnectString(createJdbcConnectString(nub));
        nub.setPoolIdx(pool.getIdx());

        // CREATE LUCENE INDEX
        FSEIndexer indexer = new FSEIndexer(getIndexPath(nub));

        // ADD TO LIST
        PoolMapper poolMapEntry = new PoolMapper(pool, nub, poolManager, em, poolEmf, indexer);
        poolMapEntry.hashCache.init(em.getConnection());
        synchronized(mapperList)
        {
            mapperList.add(poolMapEntry);
        }

        return pool;
    }

    public StoragePool mountPoolDatabase( StoragePoolNub nub, String dbName, boolean rebuild) throws PathResolveException, IOException, SQLException
    {
        String jdbcConnectString = createJdbcConnectString(nub);
        if (rebuild)
        {
            jdbcConnectString += ";create=true";
        }
        if (Main.isPerformanceDiagnostic())
        {
            jdbcConnectString += ";traceLevel=2";
        }

        HashMap<String,String> map = new HashMap<String, String>();

        if (rebuild)
            map.put("eclipselink.ddl-generation" ,"create-tables");
        else
            map.put("eclipselink.ddl-generation" ,"");
        
        map.put("javax.persistence.jdbc.url", jdbcConnectString);

        EntityManagerFactory poolEmf = Persistence.createEntityManagerFactory("VSM", map);        
        MiniConnectionPoolManager poolManager = JDBCEntityManager.initializeJDBCPool(poolEmf, jdbcConnectString);
        try (Connection conn = poolManager.createConnection())
        {
            DBChecker.check_pool_db_changes(conn);
        }
        
        JDBCEntityManager em = new JDBCEntityManager( 0, poolManager );
        List<StoragePool> list = em.createQuery("select T1 from StoragePool T1", StoragePool.class);
        StoragePool pool = null;

        // MUST CONTAIN EXACTLY ONE POOL
        if (list.size() > 1)
        {
            throw new IOException( Main.Txt("Zu viele Storagepools in PoolDB") + " " + jdbcConnectString );
        }
        if (list.isEmpty())
        {
            throw new IOException( Main.Txt("Keine Storagepools in PoolDB") + " " + jdbcConnectString );
        }
        else
        {
            pool = list.get(0);
        }
        nub.setPoolIdx(pool.getIdx());
        nub.setJdbcConnectString(createJdbcConnectString(nub));

        // CREATE LUCENE INDEX
        FSEIndexer indexer = new FSEIndexer(getIndexPath(nub));

        PoolMapper poolMapEntry = new PoolMapper(pool, nub, poolManager, em, poolEmf, indexer);
        poolMapEntry.hashCache.init(em.getConnection());

        synchronized(mapperList)
        {
            mapperList.add(poolMapEntry);
        }

             
        return pool;
    }


    public ArrayList<StoragePool> listStoragePools()
    {
        ArrayList<StoragePool> ret = new ArrayList<StoragePool>();

        for (int i = 0; i < mapperList.size(); i++)
        {
            PoolMapper m = mapperList.get(i);
            ret.add(m.pool);
        }
        return ret;
    }

    public final void initMapperList()
    {
        ArrayList<PoolMapper> mp = new ArrayList<PoolMapper>();

        EntityManager em = null;

        try
        {
            em = baseEmf.createEntityManager();
            TypedQuery<StoragePoolNub> qry = em.createQuery("select s from StoragePoolNub s", StoragePoolNub.class);
            List<StoragePoolNub> list = qry.getResultList();

            for (int i = 0; i < list.size(); i++)
            {
                StoragePoolNub storagePoolNub = list.get(i);
                if (storagePoolNub.isDisabled())
                {
                    Log.info(Main.Txt("Überspringe deaktivierten StoragePoolNub"), ": " + storagePoolNub.getIdx() + "/" + storagePoolNub.getPoolIdx());
                    continue;
                }

                String path = getDbPath(storagePoolNub);
                File dbPath = new File(path);dbPath.getAbsolutePath();
                if (!dbPath.exists())
                {
                    Log.info(Main.Txt("Überspringe fehlenden StoragePool"), ": " + storagePoolNub.getIdx() + "/" + storagePoolNub.getPoolIdx());
                    continue;
                }

                PoolMapper map = loadPoolDB(storagePoolNub);

                if (map != null)
                {
                    Log.info(Main.Txt("StoragePool Zuordnung"), ": " + map.pool.getName() + " -> " + map.nub.getIdx() + " (" + map.pool.getIdx() + ")" );
                    mp.add(map);
                }
            }

            synchronized (mapperList)
            {
                mapperList.clear();
                mapperList.addAll(mp);
            }
        }
        catch (Exception e)
        {
            Log.err(Main.Txt("Abbruch beim Initialisieren der StoragePoolMapperListe"), e);

            if (em != null)
                em.close();
        }
    }

    private PoolMapper loadPoolDB( StoragePoolNub storagePoolNub )
    {
        String jdbcConnect = storagePoolNub.getJdbcConnectString();
        if (jdbcConnect == null || jdbcConnect.isEmpty())
            return null;

        if (Main.getRebuildDB() && !jdbcConnect.contains("create=true"))
        {
            jdbcConnect += ";create=true";
        }
        if (Main.isPerformanceDiagnostic())
        {
            jdbcConnect += ";traceLevel=2";
        }

        
        try
        {
            HashMap<String,String> map = new HashMap<String, String>();
            map.put("javax.persistence.jdbc.url", jdbcConnect);

            if (Main.getRebuildDB())
                map.put("eclipselink.ddl-generation" ,"create-tables");
            else
                map.put("eclipselink.ddl-generation" ,"");

            EntityManagerFactory poolEmf = Persistence.createEntityManagerFactory("VSM", map);            
            MiniConnectionPoolManager poolManager = JDBCEntityManager.initializeJDBCPool(poolEmf, jdbcConnect);
            if (poolManager == null)
                throw new IOException( Main.Txt("Pool-Datenbank kann nicht geöffnet werden") +  " " + jdbcConnect );

            
            JDBCEntityManager em = new JDBCEntityManager( 0, poolManager);

            em.check_open_transaction();
            try (Connection c = poolManager.createConnection())
            {
                DBChecker.check_pool_db_changes(c);
            }

            // MUST CONTAIN EXACTLY ONE POOL
            List<StoragePool> list = em.createQuery("select T1 from StoragePool T1", StoragePool.class);

            if (list == null)
            {
                throw new IOException( Main.Txt("Ungültige Datenbank") + " " + jdbcConnect );
            }
            em.close_transaction();

            if (list.size() > 1)
            {
                throw new IOException( Main.Txt("Zu viele Pools in Datenbank") + " " + jdbcConnect );
            }
            if (list.isEmpty())
            {
                throw new IOException( Main.Txt("Keine Pools in Datenbank") + " " + jdbcConnect );
            }
            StoragePool pool = list.get(0);

            // SET IDX FOR OBJECT CACHE
            em.setPoolIdx(pool.getIdx());

            // NOW RELOAD FROM CORRECT OBJECT CACHE
            pool = em.em_find(StoragePool.class, pool.getIdx());

            checkFixes(pool, em, storagePoolNub);

            // CREATE LUCENE INDEX

            String idxPath = getIndexPath(storagePoolNub);
            FSEIndexer indexer = new FSEIndexer(getIndexPath(storagePoolNub));

            PoolMapper poolMapEntry = new PoolMapper(pool, storagePoolNub, poolManager, em, poolEmf, indexer);

            try
            {
                poolMapEntry.hashCache.init(em.getConnection());
            }
            catch (IOException iOException)
            {
                Log.warn("HashCache kann nicht geladen werden, Fallback to JavaCache", iOException.getMessage());
                poolMapEntry.hashCache = new JavaHashCache(pool);
                poolMapEntry.hashCache.init(em.getConnection());
            }
            if (!new File(idxPath).exists())
            {
                rebuildIndex( poolMapEntry );
            }
            return poolMapEntry;
        }
        catch (IOException iOException)
        {
            Log.warn("StoragePool kann nicht geladen werden", iOException.getMessage());
        }
        catch (Exception iOException)
        {
            Log.warn("StoragePool kann nicht geladen werden", iOException);
        }
        return null;
    }

    public JDBCConnectionFactory getConnectionFactory( StoragePool pool ) throws SQLException
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx())
                    return poolMapper.connectionPoolManager;
            }
        }
        throw new SQLException("No entry found for Pool " + pool.toString());
    }
    
    public StoragePool getStoragePool( long idx ) throws SQLException
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                if (idx == poolMapper.pool.getIdx())
                    return poolMapper.pool;
            }
        }
        throw new SQLException("No entry found for Pool " + idx);
    }

    public int getActiveConnections( StoragePool pool )
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx())
                    return poolMapper.connectionPoolManager.getActiveConnections();
            }
        }
        return -1;
    }




    private StoragePool createNewStoragePool(GenericEntityManager gem) throws Exception
    {
        StoragePool p =  new StoragePool();
        p.setCreation( new Date() );
        p.setName(Main.Txt("Neuer StoragePool"));
        p.setLandingZone(false);

        try
        {
            // CREATE ROOT DIR
            FileSystemElemNode root_node = FileSystemElemNode.createDirNode();
            root_node.getAttributes().setName("/");
            
            long now = System.currentTimeMillis();
            root_node.getAttributes().setCreationDateMs( now );
            root_node.getAttributes().setModificationDateMs( now );
            root_node.getAttributes().setAccessDateMs( now );

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
        catch (Exception e)
        {
            e.printStackTrace();
            gem.rollback_transaction();

            throw e;
        }        
    }

    public JDBCEntityManager getUtilEm( StoragePool pool ) throws SQLException
    {

        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                if (pool.getIdx() == poolMapper.pool.getIdx())
                    return poolMapper.em;
            }
        }
        throw new SQLException("No Mapper found vor pool " + pool.toString() );
    }

    public void check_db_changes() throws SQLException
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                DBChecker.check_pool_db_changes(poolMapper.em.getConnection());
            }
        }
    }

    private void closeDerbyDB(PoolMapper mp) throws SQLException
    {
        // DriverManager.getConnection("jdbc:derby:MyDbTest;shutdown=true");
        String cs = mp.nub.getJdbcConnectString();
        int idx = cs.indexOf(';');
        if (idx > 0)
        {
            cs = cs.substring(0, idx);
        }
        cs += ";shutdown=true";

        try
        {
            DriverManager.getConnection(cs);
        }
        catch (SQLNonTransientConnectionException exc )
        {
            // THIS IS A REGULAR EXCEPTION WHILE CLOSING DERBY
        }
    }

    public void shutdown()
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);

                try
                {
                    poolMapper.indexer.close();
                    poolMapper.em.close_transaction();
                    poolMapper.em.close_entitymanager();
                    poolMapper.poolEmf.close();
                    poolMapper.connectionPoolManager.dispose();

                    poolMapper.hashCache.shutdown();
                }
                catch (Exception e)
                {
                    Log.err("Abbruch beim Schließen des EntityManagers", poolMapper.pool.toString(), e);
                }
            }
        }
    }

    public PoolMapper getPoolMapper( StoragePool pool )
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                if (poolMapper.pool.getIdx() == pool.getIdx())
                    return poolMapper;
            }
        }
        return null;
    }

    public FSEIndexer getIndexer( StoragePool pool )
    {
        PoolMapper pm = getPoolMapper( pool );
        if (pm != null)
            return pm.indexer;

        return null;
    }
    public HashCache getHashCache( StoragePool pool )
    {
        PoolMapper pm = getPoolMapper( pool );
        if (pm != null)
            return pm.hashCache;

        return null;
    }


    private void rebuildIndex( PoolMapper poolMapEntry )
    {
        FSEIndexer indexer = poolMapEntry.indexer;
        Connection conn = null;
        try
        {
            conn = poolMapEntry.connectionPoolManager.createConnection();
            indexer.rebuildIndex(conn);
            
        }
        catch (Exception sQLException)
        {
            Log.err("Fehler beim Neuaufbau des Indexes", sQLException);
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    conn.close();
                }
                catch (SQLException sQLException)
                {
                }
            }
        }
    }

    public void infoStats()
    {
        synchronized(mapperList)
        {
            for (int i = 0; i < mapperList.size(); i++)
            {
                PoolMapper poolMapper = mapperList.get(i);
                poolMapper.calcStats();
                poolMapper.logStats();
            }
        }
    }

    private void checkFixes( StoragePool pool, JDBCEntityManager em, StoragePoolNub storagePoolNub )
    {
        if (Main.get_bool_prop(GeneralPreferences.FIX_DUPL_NAMES, false))
        {
            IFix fix = new FixDoubleDirNames(pool, em);
            try
            {
                Log.info("Fixing duplicate names started");
                fix.runFix();
                Log.info("Fixing duplicate names done");
            }
            catch( Exception exc )
            {
                Log.err("Fixing duplicate names failed:", exc);
            }

        }
        if (Main.get_bool_prop(GeneralPreferences.FIX_BOOTSTRAP, false))
        {
            IFix fix = new FixBootstrapEntries(pool, em);
            try
            {
                Log.info("Fixing bootstrap started");
                fix.runFix();
                Log.info("Fixing bootstrap done");
            }
            catch( Exception exc )
            {
                Log.err("Fixing duplicate names failed:", exc);
            }

        }
    }

    private String getDBConnectString( StoragePoolNub nub )
    {
        return Main.get_prop(GeneralPreferences.JDBC_URL, "jdbc:derby:");
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}