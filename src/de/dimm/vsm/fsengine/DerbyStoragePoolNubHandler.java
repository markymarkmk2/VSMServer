/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsengine.hashcache.JavaHashCache;
import de.dimm.vsm.DBChecker;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.DirectoryEntry;
import de.dimm.vsm.fsengine.derbyserver.DerbyDBServer;
import de.dimm.vsm.fsengine.derbyserver.StandaloneDerbyCmdDBServer;
import de.dimm.vsm.fsengine.derbyserver.StandaloneDerbyDBServer;
import de.dimm.vsm.fsengine.fixes.FixBootstrapEntries;
import de.dimm.vsm.fsengine.fixes.FixDoubleDirNames;
import de.dimm.vsm.fsengine.fixes.IFix;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.apache.commons.lang.StringUtils;





/**
 *
 * @author Administrator
 */






public class DerbyStoragePoolNubHandler extends AbstractStoragePoolNubHandler
{
    public static final String RELPARAMPATH = "/VSMParams";       
    public static final String JDBC_DERBY_PREFIX = "jdbc:derby:";

    private static boolean isAbsPath( String dbName ) {        
        File f = new File(dbName);
        return f.isAbsolute();
    }

    
    Map<String,DerbyDBServer> serverMap;

    public DerbyStoragePoolNubHandler() {
        serverMap = new ConcurrentHashMap<>();
    }
    
    private boolean existsDbServer( StoragePoolNub nub, String _url )
    {
        if (serverMap.containsKey(_url))
            return true;
        
        String server = DerbyStoragePoolNubHandler.getServerByConnectString(nub);
        int port = DerbyStoragePoolNubHandler.getPortByConnectString(nub);
        
        for (DerbyDBServer entry: serverMap.values())
        {
            if (entry.isSame( server, port))
                return true;
        }
        return false;
    }
    
    
    
    @Override
    public void initMapperList() {
        ArrayList<PoolMapper> mp = new ArrayList<>();
        EntityManager em = null;
        try {
            em = baseEmf.createEntityManager();
            TypedQuery<StoragePoolNub> qry = em.createQuery("select s from StoragePoolNub s", StoragePoolNub.class);
            List<StoragePoolNub> list = qry.getResultList();
            for (int i = 0; i < list.size(); i++) {
                StoragePoolNub storagePoolNub = list.get(i);
                if (storagePoolNub.isDisabled()) {
                    Log.info(Main.Txt("Überspringe deaktivierten StoragePoolNub"), ": " + storagePoolNub.getIdx() + "/" + storagePoolNub.getPoolIdx());
                    continue;
                }
                String path = getDbPath(storagePoolNub);
                File dbPath = new File(path);
                if (!dbPath.exists()) {
                    Log.info(Main.Txt("Überspringe fehlenden StoragePool"), ": " + storagePoolNub.getIdx() + "/" + storagePoolNub.getPoolIdx() + ": " + dbPath);
                    continue;
                }
                PoolMapper map = loadPoolDB(storagePoolNub);
                if (map != null) {
                    Log.info(Main.Txt("StoragePool Zuordnung"), ": " + map.pool.getName() + " -> " + map.nub.getIdx() + " (" + map.pool.getIdx() + ")");
                    mp.add(map);
                }
            }
            synchronized (mapperList) {
                mapperList.clear();
                mapperList.addAll(mp);
            }
        }
        catch (Exception e) {
            Log.err(Main.Txt("Abbruch beim Initialisieren der StoragePoolMapperListe"), e);
            if (em != null) {
                em.close();
            }
        }
    }

    private static String getDbPath( StoragePoolNub nub )
    {
        String dbPath = getDbRootPath(nub) + RELPARAMPATH;        
        return dbPath;
    }
    private static String getDbRootPath( StoragePoolNub nub )
    {
        String dbPath = LogicControl.getDbPath() + "db_" + nub.getIdx();
        dbPath = Main.get_prop( GeneralPreferences.DATABASE_PATH + "_" + nub.getIdx(), dbPath);                
        return dbPath;
    }
    
    @Override
    public  String getIndexPath( StoragePoolNub nub )
    {
        String dbPath = getDbRootPath(nub) + "/Index";
        return dbPath;
    }
   
    @Override
    public StoragePool createEmptyPoolDatabase( StoragePoolNub nub ) throws PathResolveException, IOException, SQLException {
        // TODO: DIFFERENT EMBEDDED DATABASES
        String dbPath = getDbRootPath(nub);
        File dir = new File(dbPath);
        if (dir.exists()) {
            return mountPoolDatabase(nub, dbPath, true);
        }
        dir.getParentFile().mkdirs();
        return createEmptyPoolDatabase(nub, dbPath);
    }    
    
    @Override
    public StoragePool createEmptyPoolDatabase( StoragePoolNub nub, String dbPath) throws PathResolveException, IOException, SQLException
    {

        String jdbcConnectString = JDBC_DERBY_PREFIX + dbPath + RELPARAMPATH + ";create=true";

        HashMap<String,String> map = new HashMap<>();
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
        String relDbPath = dbPath;
        if (relDbPath.contains( "//"))
        {
            relDbPath = relDbPath.substring( relDbPath.lastIndexOf( "//") + 1 );
        }
        jdbcConnectString = JDBC_DERBY_PREFIX + relDbPath + RELPARAMPATH;
        nub.setJdbcConnectString(jdbcConnectString);
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

    @Override
    public StoragePool mountPoolDatabase( StoragePoolNub nub, String dbPath, boolean rebuild) throws PathResolveException, IOException, SQLException
    {
        String jdbcConnectString = JDBC_DERBY_PREFIX + dbPath + RELPARAMPATH;
        if (rebuild)
        {
            jdbcConnectString += ";create=true";
        }
        if (Main.isPerformanceDiagnostic())
        {
            jdbcConnectString += ";traceLevel=2";
        }

        HashMap<String,String> map = new HashMap<>();

        if (rebuild)
            map.put("eclipselink.ddl-generation" ,"create-tables");
        else
            map.put("eclipselink.ddl-generation" ,"");
        
        map.put("javax.persistence.jdbc.url", jdbcConnectString);

        EntityManagerFactory poolEmf = Persistence.createEntityManagerFactory("VSM", map);        
        MiniConnectionPoolManager poolManager = JDBCEntityManager.initializeJDBCPool(poolEmf, jdbcConnectString);
        try (Connection conn = poolManager.createConnection()) {
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
        nub.setJdbcConnectString(JDBC_DERBY_PREFIX + dbPath + RELPARAMPATH);

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
    
    public static String getServerByConnectString( StoragePoolNub nub ) {
        String server =  Main.get_prop( GeneralPreferences.DATABASE_SERVER + "_" + nub.getIdx());
        if (server == null) {
            server = Main.get_prop( GeneralPreferences.DATABASE_SERVER);
        }
        return server;
    }

    public static int getPortByConnectString( StoragePoolNub nub ) {
         int port = Main.get_int_prop( GeneralPreferences.DATABASE_PORT + "_" + nub.getIdx(), Main.get_int_prop( GeneralPreferences.DATABASE_PORT, 1527)  );
         return port;
    }
    
    private static String getEffectiveJdbcConnectString(StoragePoolNub storagePoolNub)
    {
        if (StringUtils.isEmpty(storagePoolNub.getJdbcConnectString()))
            return null;
        
        String jdbcConnect = JDBC_DERBY_PREFIX + getDbPath(storagePoolNub);
        
        //String jdbcConnect = storagePoolNub.getJdbcConnectString();
        if (jdbcConnect == null || jdbcConnect.isEmpty())
            return null;
        
       /* String defaultDbPath = ":" + Main.DATABASEPATH;
        if (jdbcConnect.contains(defaultDbPath))
        {
            // DB-Pathmapping
            jdbcConnect = jdbcConnect.replaceFirst(defaultDbPath, ":" + LogicControl.getDbPath());                   
        }*/
        
        // NetworkMapping, Do we have active Server for this Nub?
        String server =  getServerByConnectString(storagePoolNub);
        
        // Create Network connect string
        if (server != null)
        {
            // Get Port for Nub 
            int port = getPortByConnectString(storagePoolNub);

            String dbName = jdbcConnect.substring( JDBC_DERBY_PREFIX.length() );

            jdbcConnect =  JDBC_DERBY_PREFIX + "//" +  server + ":" +  port + "/";

            if (!isAbsPath(dbName)) {
                File wrkDir = new File(dbName).getAbsoluteFile();
                dbName = wrkDir.getAbsolutePath();
            }
            jdbcConnect += dbName;
        }        
        return jdbcConnect;
    }
    private static boolean is_win()
    {
        return (System.getProperty("os.name").startsWith("Win"));
    }

    private PoolMapper loadPoolDB( StoragePoolNub storagePoolNub )
    {
        String jdbcConnect = getEffectiveJdbcConnectString( storagePoolNub );
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
        
        if (isNetworkServer(jdbcConnect))
        {
            if (existsDbServer(storagePoolNub, jdbcConnect))
            {
                Log.debug("Es existiert bereits ein gleicher DB-Server, DB-Server wird verwendet", jdbcConnect );
            }
            else
            {
                DerbyDBServer server;
                try {
                    if (is_win())                        
                        server = new StandaloneDerbyDBServer(storagePoolNub, jdbcConnect);
                    else
                        server = new StandaloneDerbyCmdDBServer(storagePoolNub, jdbcConnect);
                }
                catch (Exception exception) {
                    Log.err("Kann DB-Server nicht starten", jdbcConnect, exception );
                    return null;
                }
                serverMap.put(jdbcConnect, server);
                server.start();
            }
        }
        
        Log.debug("Lade Pool DB", jdbcConnect);
        
        try
        {
            HashMap<String,String> map = new HashMap<>();
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
            try (Connection c = poolManager.createConnection()) {
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
            Log.err("StoragePool kann nicht geladen werden", iOException);
        }
        catch (Exception iOException)
        {
            Log.err("StoragePool kann nicht geladen werden", iOException);
        }
        return null;
    }

    @Override
    public void shutdown() {
        super.shutdown(); //To change body of generated methods, choose Tools | Templates.
        
        for (DerbyDBServer server: serverMap.values())
        {
            server.stop();
        }
    }
    
    

    private void closeDerbyDB(PoolMapper mp) throws SQLException
    {
        // DriverManager.getConnection("jdbc:derby:MyDbTest;shutdown=true");
        String cs = getEffectiveJdbcConnectString( mp.nub );
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

    @Override
    public void removePoolDatabase( StoragePool pool, boolean physically ) throws SQLException {
        PoolMapper poolMapper = null;
        synchronized (mapperList) {
            for (int i = 0; i < mapperList.size(); i++) {
                if (mapperList.get(i).pool.getIdx() == pool.getIdx()) {
                    poolMapper = mapperList.remove(i);
                    break;
                }
            }
        }
        if (poolMapper == null) {
            throw new SQLException("No Mapper found vor pool " + pool.toString());
        }
        try {
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
        catch (Exception e) {
            Log.err("Abbruch beim Schlie\u00dfen des EntityManagers", pool.toString(), e);
        }
        if (physically) {
            try {
                closeDerbyDB(poolMapper);
            }
            catch (SQLException e) {
                Log.err("Abbruch beim Schliessen der Datenbank", pool.toString(), e);
            }
            String path = getDbPath(poolMapper.nub);
            if (path != null) {
                File dbDir = new File(path);
                if (dbDir.exists()) {
                    DirectoryEntry de = new DirectoryEntry(dbDir);
                    de.delete_recursive();
                }
            }
            path = getDbRootPath(poolMapper.nub);
            if (path != null) {
                File dbDir = new File(path);
                if (dbDir.exists()) {
                    dbDir.delete();
                }
            }
        }
    }

    private boolean isNetworkServer( String jdbcConnect ) {
        return jdbcConnect.startsWith(JDBC_DERBY_PREFIX + "//");
    }
}
