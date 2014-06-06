/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm;

import de.dimm.vsm.Utilities.DefaultSSLServerSocketFactory;
import de.dimm.vsm.Utilities.DefaultSSLSocketFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Utilities.LicenseChecker;
import de.dimm.vsm.Utilities.ThreadPoolWatcher;
import de.dimm.vsm.auth.UserManager;
import de.dimm.vsm.net.servlets.AgentApiDispatcher;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.backup.BackupManager;
import de.dimm.vsm.backup.hotfolder.HotFolderManager;
import de.dimm.vsm.fsengine.JDBCConnectionFactory;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.PoolMapper;
import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import de.dimm.vsm.fsengine.checks.CheckManager;
import de.dimm.vsm.fsengine.checks.ICheck;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.lifecycle.RetentionManager;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.log.VSMLogger;
import de.dimm.vsm.mail.NotificationEntry;
import de.dimm.vsm.mail.NotificationServer;
import de.dimm.vsm.mail.SmtpNotificationServer;
import de.dimm.vsm.net.AgentIdleManager;
import de.dimm.vsm.net.AutoMountManager;
import de.dimm.vsm.net.CDPManager;
import de.dimm.vsm.net.FixHashUrlBug;
import de.dimm.vsm.net.GuiWrapper;
import de.dimm.vsm.net.LogQuery;
import de.dimm.vsm.net.LoginManager;
import de.dimm.vsm.net.NetServer;
import de.dimm.vsm.net.PoolStatusResult;
import de.dimm.vsm.net.ScheduleStatusEntry;
import de.dimm.vsm.net.SearchContextManager;
import de.dimm.vsm.net.StoragePoolHandlerContextManager;
import de.dimm.vsm.net.StoragePoolHandlerServlet;
import de.dimm.vsm.net.interfaces.GuiLoginApi;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.servlets.ServerApiServlet;
import de.dimm.vsm.net.servlets.UiApiServlet;
import de.dimm.vsm.net.servlets.UiLoginServlet;
import de.dimm.vsm.records.MessageLog;
import de.dimm.vsm.records.Role;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import de.dimm.vsm.tasks.TaskEntry;
import de.dimm.vsm.tasks.TaskInterface.TASKSTATE;
import de.dimm.vsm.vaadingui.VaadinWysiwyg;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.eclipse.jetty.servlet.ServletHolder;


/**
 *
 * @author Administrator
 */
public class LogicControl
{
    public static final String PRODUCT_BASE = "VSM";

    private static final String keyStore = "server.jks";
    private static final String keyPwd = "1234fuenf";
    

    public static final int LTM_FTP = 0x0001;
    public static final int LTM_S3 = 0x0002;
    static EntityManagerFactory emf = null;
    static EntityManager em = null;
    static AgentApiDispatcher api_disp = null;
    static LicenseChecker licChecker = null;
    public static final boolean use_ssl = false;


    NetServer netServer = null;
    NetServer sslNetServer = null;
    StoragePoolHandlerServlet poolHandlerServlet = null;

    ServerApiServlet apiServlet = null;
    UiLoginServlet loginServlet = null;
    UiApiServlet uiApiServlet = null;
    HotFolderManager hfManager = null;
    LoginManager loginManager = null;
    Main main;

    List<Backup> schedulerList;

    JobManager jobManager;
    BackupManager backupmanager;
    RetentionManager retentionManager;
    StoragePoolHandlerContextManager poolContextManager;
    SearchContextManager searchContextManager;
    UserManager userManager;
    CDPManager cdpManager;
    NotificationServer notificationServer;
    CheckManager checkManager;
    AutoMountManager autoMountManager;
    AgentIdleManager agentIdleManager;
    

    ArrayList<WorkerParent> workerList;
    TaskPreferences taskPrefs;

    private static DerbyStoragePoolNubHandler storagePoolNubHandler;
    ThreadPoolWatcher threadPoolWatcher;

    public ThreadPoolWatcher getThreadPoolWatcher()
    {
        return threadPoolWatcher;
    }

    public static String getKeyPwd()
    {
        return Main.get_prefs().get_prop(GeneralPreferences.KEYSTORE_PWD, keyPwd);        
    }

    public static String getKeyStore()
    {
        return Main.get_prefs().get_prop(GeneralPreferences.KEYSTORE, keyStore);
        
    }
    public static String getDbPath()
    {
        String path = Main.get_prop(GeneralPreferences.DATABASE_PATH, Main.DATABASEPATH );
        path = path.replace('\\', '/');
        if (!path.endsWith("/"))
            path = path + "/";
        
        return path;
    }

    public static void sleep( int cycle )
    {
        try
        {
            Thread.sleep(cycle);
        }
        catch (InterruptedException interruptedException)
        {
        }
    }

    public TaskEntry[] getTaskArray()
    {
        ArrayList<TaskEntry> list = new ArrayList<TaskEntry>();

        for (int i = 0; i < workerList.size(); i++)
        {
            WorkerParent wp = workerList.get(i);
            if (wp.isVisible())
                list.add( new TaskEntry(wp));

        }
        return list.toArray( new TaskEntry[0]);
    }

    public long getTaskIdx( WorkerParent aThis )
    {
        for (int i = 0; i < workerList.size(); i++)
        {
            WorkerParent wp = workerList.get(i);

            if (wp == aThis)
                return i;
        }
        return -1;
    }

    void writeTaskPreferences()
    {
        if (taskPrefs != null)
            taskPrefs.writeTaskState(workerList);
        else
            throw new RuntimeException("Initialisation error!");
    }

    public static DerbyStoragePoolNubHandler getStorageNubHandler()
    {
        return storagePoolNubHandler;
    }

    // USED FOR TEST
    public static void setStorageNubHandler( DerbyStoragePoolNubHandler nubHandler )
    {
        storagePoolNubHandler = nubHandler;
    }


    public MessageLog[] listLogs( int cnt, long offsetIdx, LogQuery lq )
    {
        return Log.listLogs(cnt, offsetIdx, lq);
    }
    public MessageLog[] listLogsSinceIdx(long idx, LogQuery lq )
    {
        return Log.listLogsSinceIdx(idx, lq);
    }

    public long getLogCounter()
    {
        return Log.getLogCounter();
    }


    public void shutdown()
    {
        netServer.stop_server();
        sslNetServer.stop_server();
                
        jobManager.setShutdown(true);
           
        int maxWait = 1000;
        while (jobManager.isStarted() && !jobManager.isFinished() && maxWait > 0)
        {
            sleep(100);
            maxWait--;
        }
        if (jobManager.isFinished())
            jobManager.close();

        if (maxWait <= 0)
        {
            Log.err(Main.Txt( "Der Dienst kann nicht gestoppt werden"), jobManager.getName());
        }
        
        for (int i = workerList.size() - 1; i >= 0; i--)
        {
            WorkerParent wp = workerList.get(i);
            wp.setShutdown(true);
        }
        for (int i = workerList.size() - 1; i >= 0; i--)
        {
            WorkerParent wp = workerList.get(i);
            maxWait = 1000;
            while (wp.isStarted() && !wp.isFinished() && maxWait > 0)
            {
                sleep(100);
                maxWait--;
            }
            if (wp.isFinished())
                wp.close();
            
            if (maxWait <= 0)
            {
                Log.err(Main.Txt( "Der Dienst kann nicht gestoppt werden"), wp.getName());
            }
        }
        
        // DATABASES
        storagePoolNubHandler.shutdown();


        // WE NEED THIS TILL THE END
        try
        {
            get_log_em().commit_transaction();
        }
        catch (SQLException sQLException)
        {
        }
//        get_log_em().close_entitymanager();
//        get_txt_em().commit_transaction();
//        get_txt_em().close_entitymanager();

    }


    public BackupManager getBackupManager()
    {
        return backupmanager;
    }

    public HotFolderManager getHfManager() {
        return hfManager;
    }

    public CDPManager getCdpManager() {
        return cdpManager;
    }

    public AutoMountManager getAutoMountManager()
    {
        return autoMountManager;
    }
    
    

    
    public AgentIdleManager getAgentIdleManager() {
        return agentIdleManager;
    }
    
    
    public static class LicenseHandler
    {

        public static boolean isLicensed()
        {
            return licChecker.is_licensed(PRODUCT_BASE);

        }

        public static boolean isFTPStorageLicensed()
        {
            return licChecker.is_licensed(PRODUCT_BASE, LTM_FTP);
        }

        public static boolean isS3StorageLicensed()
        {
            return licChecker.is_licensed(PRODUCT_BASE, LTM_S3);
        }

        public static int get_licensed_tb()
        {
            return licChecker.get_max_units(PRODUCT_BASE);
        }

        public LicenseHandler()
        {
        }

        public ArrayList<String> get_modules_text()
        {
            ArrayList<String> ret = new ArrayList<String>();
            if (licChecker.is_licensed(PRODUCT_BASE, LTM_FTP))
            {
                ret.add("FTP");
            }
            if (licChecker.is_licensed(PRODUCT_BASE, LTM_S3))
            {
                ret.add("S3");
            }
            return ret;
        }
    }

    public LogicControl( Main _main )
    {
        main = _main;
       // initSecurity();
        VSMLogger.setup();
    }
    
    final void initSecurity()
    {
        Security.setProperty( "ssl.ServerSocketFactory.provider", DefaultSSLServerSocketFactory.class.getCanonicalName());
        DefaultSSLSocketFactory.setKeyStore("server.jks");
        DefaultSSLSocketFactory.setPassword("1234fuenf".toCharArray());
//        System.setProperty("javax.net.ssl.trustStore", getKeyStore());
//        System.setProperty("javax.net.ssl.keyStore", getKeyStore());
//        System.setProperty("javax.net.ssl.keyStorePassword", getKeyPwd());        
   
    }    
    void init( boolean agent_tcp) throws SQLException
    {
        notificationServer = SmtpNotificationServer.createSmtpNotificationServer();

        if (storagePoolNubHandler == null)
        {
            storagePoolNubHandler = new DerbyStoragePoolNubHandler();
            // LOAD POOL-DATABASES FROM NUB-DB
            storagePoolNubHandler.initMapperList();
        }

        if (Main.getRebuildDB())
        {
            check_db_changes();
        }

        checkFixes();
        
        schedulerList = new ArrayList<Backup>();

        createApiEntry();

        if (licChecker == null)
        {
            licChecker = new LicenseChecker(PRODUCT_BASE, /*TB*/ 10, /*mods*/ LTM_FTP | LTM_S3);
            licChecker.read_licenses();
        }

        workerList = new ArrayList<WorkerParent>();


        poolContextManager = new StoragePoolHandlerContextManager();
        workerList.add(poolContextManager);
        searchContextManager = new SearchContextManager();
        workerList.add(searchContextManager);

        initNetServer(  searchContextManager, poolContextManager, agent_tcp );


        hfManager = new HotFolderManager(storagePoolNubHandler);
        workerList.add(hfManager);


        jobManager = new JobManager();
        workerList.add( jobManager );

        loginManager = new LoginManager();
        workerList.add( loginManager );
        
        backupmanager = new BackupManager();
        workerList.add( backupmanager );

        retentionManager = new RetentionManager(storagePoolNubHandler);
        workerList.add( retentionManager );

        cdpManager = new CDPManager();
        workerList.add(cdpManager);

        taskPrefs = new TaskPreferences(workerList);
        taskPrefs.readTaskState(workerList);
        
        checkManager = new CheckManager();
        workerList.add(checkManager);
        
        autoMountManager = new AutoMountManager();
        workerList.add(autoMountManager);
        
        agentIdleManager = new AgentIdleManager(storagePoolNubHandler);
        workerList.add(agentIdleManager);


        List<Role> roles = null;
        try
        {
            roles = get_base_util_em().createQuery("select * from Role T1", Role.class);
        }
        catch (Exception sQLException)
        {
            Log.err(Main.Txt( "Rollen können nicht geladen werden"), sQLException);
        }
        userManager = new UserManager(roles);

        try
        {
            notificationServer.loadNotifications(get_base_util_em());
        }
        catch (IOException iOException)
        {
            Log.err(Main.Txt( "Benachrichtigungen können nicht geladen werden"), iOException);
        }
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < workerList.size(); i++)
        {
            WorkerParent wp = workerList.get(i);

            if (wp.initialize() && wp.check_requirements(sb))
            {
                wp.start_run_loop();
                if (!wp.isPersistentState())
                {
                    wp.setTaskState(TASKSTATE.RUNNING);
                }
            }
        }
        if (sb.length() > 0)
        {
            Log.info(Main.Txt( "TaskManager lieferte"), ": " + sb.toString());
        }

        threadPoolWatcher = new ThreadPoolWatcher("MainPoolWatcher");


    }

    public NotificationServer getNotificationServer()
    {
        return notificationServer;
    }
    

    public void addNotification(NotificationEntry e)
    {
        if (notificationServer == null)
            return;
        notificationServer.addNotificationEntry( e );
    }
    
    public List<NotificationEntry> listNotificationEntries()
    {
        return notificationServer.listNotificationEntries();
    }

    public void addScheduler( Backup sch )
    {
        for (int i = 0; i < schedulerList.size(); i++)
        {
            Backup scheduler = schedulerList.get(i);

            if (scheduler.getSched().getIdx() == sch.getSched().getIdx())
            {
                schedulerList.remove(scheduler);
                i--;
            }
        }
        schedulerList.add( sch );
    }
    
    public boolean abortScheduler( Schedule sched )
    {
        for (int i = 0; i < schedulerList.size(); i++)
        {
            Backup scheduler = schedulerList.get(i);
            if (scheduler.getSched().getIdx() == sched.getIdx())
            {
                return scheduler.abort();
            }
        }
        return false;
    }

    public List<ScheduleStatusEntry> listSchedulerStats()
    {
        List<ScheduleStatusEntry> ret = new ArrayList<ScheduleStatusEntry>();

        for (int i = 0; i < schedulerList.size(); i++)
        {
            Backup scheduler = schedulerList.get(i);

            ScheduleStatusEntry entry = scheduler.getStatusEntry();

            ret.add(entry);
        }
        return ret;
    }


    ServletHolder guiServlet;
    ServletHolder searchServlet;
    final void initNetServer( SearchContextManager scm, StoragePoolHandlerContextManager spcm, boolean agent_tcp)
    {
        netServer = new NetServer();
        sslNetServer = new NetServer();

        try
        {

            apiServlet =  new ServerApiServlet();
            netServer.addServlet("net", apiServlet);

            uiApiServlet =  new UiApiServlet();
            sslNetServer.addServlet("ui", uiApiServlet);

            loginServlet =  new UiLoginServlet();
            sslNetServer.addServlet("login", loginServlet);
            
            poolHandlerServlet = new StoragePoolHandlerServlet(scm, spcm);
            netServer.addServlet("fs", poolHandlerServlet);

            guiServlet = VaadinWysiwyg.createGuiServlet("de.dimm.vsm.vaadin.VSMClientApplication");            
            netServer.addServletHolder("client/*", guiServlet);
            netServer.addServletHolder("VAADIN/*", guiServlet);

            searchServlet = VaadinWysiwyg.createGuiServlet("de.dimm.vsm.vaadin.VSMSearchApplication");            
            netServer.addServletHolder("search/*", searchServlet);

            netServer.start_server(Main.getServerPort(), false, getKeyStore(), getKeyPwd());
            sslNetServer.start_server(Main.getSslServerPort(), true, getKeyStore(), getKeyPwd());
        }
        catch (Exception exception)
        {
            Log.err(Main.Txt( "Starten des Webservers schlug fehl"), exception);
        }
    }
    public void restartGui()
    {
        try
        {
            guiServlet.doStop();
            searchServlet.doStop();

            guiServlet.setServlet(guiServlet.getServlet());
            searchServlet.setServlet(searchServlet.getServlet());

        }
        catch (Exception ex)
        {
            Log.err(Main.Txt( "Stoppen der Gui schlug fehl"), ex);
        }
        try
        {
            guiServlet = VaadinWysiwyg.createGuiServlet("de.dimm.vsm.vaadin.VSMClientApplication");

            netServer.addServletHolder("client/*", guiServlet);
            netServer.addServletHolder("VAADIN/*", guiServlet);


            searchServlet = VaadinWysiwyg.createGuiServlet("de.dimm.vsm.vaadin.VSMSearchApplication");

            netServer.addServletHolder("search/*", searchServlet);

            guiServlet.doStart();
            searchServlet.doStart();
        }
        catch (Exception ex)
        {
            Log.err(Main.Txt( "Neustart der Gui schlug fehl"), ex);
        }
    }

    public static String get_db_home()
    {
        if (System.getProperty("DBHOME") == null)
        {
            File f = new File(".");
            f = new File(f.getAbsolutePath());
            f = f.getParentFile();
            f = new File(f, "VSMParams");

            System.setProperty("DBHOME", f.getAbsolutePath());
        }

        return System.getProperty("DBHOME");
    }


    public List<StoragePool> getStoragePoolList()
    {
        return storagePoolNubHandler.listStoragePools();
    }
    public StoragePool getStoragePool( long poolIdx ) throws SQLException
    {
        return storagePoolNubHandler.getStoragePool(poolIdx);
    }


    public JDBCEntityManager get_util_em(StoragePool pool)
    {
        try
        {
            return storagePoolNubHandler.getUtilEm(pool);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return null;
        }
    }
    static JDBCEntityManager baseEm = null;
    static EntityManagerFactory baseEmf = null;

    static public EntityManagerFactory get_base_util_emf()
    {
        // CREATE STATIC OBJECTS
        get_base_util_em();
        return baseEmf;
    }



    public static JDBCEntityManager get_base_util_em()
    {
        if (baseEm != null)
            return baseEm;

        String jdbcConnect = "jdbc:derby:" + getDbPath() + "VSMBase";
        if (Main.getRebuildDB())
        {
            jdbcConnect += ";create=true";
        }


        try
        {
            HashMap<String,String> map = new HashMap<String, String>();
            map.put("javax.persistence.jdbc.url", jdbcConnect);

            if (Main.getRebuildDB())
                map.put("eclipselink.ddl-generation" ,"create-tables");
            else
                map.put("eclipselink.ddl-generation" ,"");

            baseEmf = Persistence.createEntityManagerFactory("VSMBase", map);
            JDBCConnectionFactory conn = JDBCEntityManager.initializeJDBCPool(baseEmf, jdbcConnect);
            baseEm = new JDBCEntityManager( /*poolIdx*/0, conn);
            return baseEm;
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return null;
        }
    }

    static JDBCEntityManager logEm = null;
    static EntityManagerFactory logEmf = null;

    public static JDBCEntityManager get_log_em()
    {
        if (logEm != null)
            return logEm;

        String jdbcConnect = "jdbc:derby:" + getDbPath() + "VSMLog";
        if (Main.getRebuildDB())
        {
            jdbcConnect += ";create=true";
        }

        try
        {
            HashMap<String,String> map = new HashMap<String, String>();
            map.put("javax.persistence.jdbc.url", jdbcConnect);

            if (Main.getRebuildDB())
                map.put("eclipselink.ddl-generation" ,"create-tables");
            else
                map.put("eclipselink.ddl-generation" ,"");

            logEmf = Persistence.createEntityManagerFactory("VSMLog", map);
            JDBCConnectionFactory conn = JDBCEntityManager.initializeJDBCPool(logEmf, jdbcConnect);
            logEm = new JDBCEntityManager( /*poolIdx*/0, conn);
            return logEm;
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return null;
        }
    }

    static JDBCEntityManager textEm = null;
    static EntityManagerFactory textEmf = null;

    public static JDBCEntityManager get_txt_em()
    {
        if (textEm != null)
            return textEm;

        String jdbcConnect = "jdbc:derby:" + getDbPath() + "VSMText";
        if (Main.getRebuildDB())
        {
            jdbcConnect += ";create=true";
        }

        try
        {
            HashMap<String,String> map = new HashMap<String, String>();
            map.put("javax.persistence.jdbc.url", jdbcConnect);

            if (Main.getRebuildDB())
                map.put("eclipselink.ddl-generation" ,"create-tables");
            else
                map.put("eclipselink.ddl-generation" ,"");

            textEmf = Persistence.createEntityManagerFactory("VSMText", map);
            JDBCConnectionFactory conn = JDBCEntityManager.initializeJDBCPool(textEmf, jdbcConnect);
            textEm = new JDBCEntityManager( /*poolIdx*/0, conn);
            return textEm;
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return null;
        }
    }

    public StoragePool createPool() throws Exception
    {
        StoragePoolNub nub = new StoragePoolNub();

        try
        {
            get_base_util_em().check_open_transaction();

            get_base_util_em().em_persist(nub);

            StoragePool pool = storagePoolNubHandler.createEmptyPoolDatabase(nub);

            if (pool != null)
            {
                // UPDATE CONNECTSTRING
                get_base_util_em().em_merge(nub);
                get_base_util_em().commit_transaction();

                return pool;
            }
        }
        catch (Exception sQLException)
        {
            get_base_util_em().rollback_transaction();

            Log.err(Main.Txt( "Storagepool kann nicht erzeugt werden"),  sQLException);
            throw sQLException;
        }
        return null;


    }

    public void deletePool( StoragePool node ) throws SQLException
    {
        storagePoolNubHandler.removePoolDatabase( node , /*physically*/false);
    }
    public void deletePoolPhysically( StoragePool node ) throws SQLException
    {
        storagePoolNubHandler.removePoolDatabase( node , /*physically*/true);
    }


    public static GuiLoginApi getGuiLoginApi()
    {
        return Main.get_control().getLoginManager();
    }
    public static GuiServerApi getGuiServerApi(GuiWrapper o )
    {
        GuiWrapper wr = (GuiWrapper)o;
        return Main.get_control().getLoginManager().getApi(wr.getLoginIdx());
    }
    static void createApiEntry()
    {
        api_disp = new AgentApiDispatcher(use_ssl, "vsmserver.jks", "123456", /*tcp*/ true);
    }
    public static AgentApiEntry getApiEntry( InetAddress addr, int port, boolean withMsg )
    {
        if (api_disp == null)
        {
            createApiEntry();
        }
        return api_disp.get_api(addr, port, withMsg);
    }
    public static AgentApiEntry getApiEntry( InetAddress addr, int port )
    {
        if (api_disp == null)
        {
            createApiEntry();
        }
        return api_disp.get_api(addr, port, /*withMsg*/ true);
    }

    public static AgentApiEntry getApiEntry( ClientInfo info ) throws UnknownHostException
    {
        if (api_disp == null)
        {
            createApiEntry();
        }
        InetAddress addr = InetAddress.getByName(info.getIp());
        AgentApiEntry api = api_disp.get_api(addr, info.getPort(),/*withMsg*/ false);

        if (!api.isOnline())
        {
            Log.warn(Main.Txt("Verbindung kann nicht aufgebaut werden"), info.toString());
            return null;            
        }
        return api;
    }

    public static AgentApiEntry getApiEntry( String ip, int port, boolean withMsg ) throws UnknownHostException
    {
        InetAddress addr = InetAddress.getByName(ip);
        return getApiEntry(addr, port, withMsg);
    }
    public static AgentApiEntry getApiEntry( String ip, int port ) throws UnknownHostException
    {
        InetAddress addr = InetAddress.getByName(ip);
        return getApiEntry(addr, port, /*withMsg*/true);
    }

    public static boolean isLicensed()
    {
        return licChecker.is_licensed(PRODUCT_BASE);

    }

    public static boolean isFTPStorageLicensed()
    {
        return licChecker.is_licensed(PRODUCT_BASE, LTM_FTP);
    }

    public static boolean isS3StorageLicensed()
    {
        return licChecker.is_licensed(PRODUCT_BASE, LTM_S3);
    }

    public static int get_licensed_tb()
    {
        return licChecker.get_max_units(PRODUCT_BASE);
    }

    private void check_db_changes() throws SQLException
    {

        JDBCEntityManager _em = get_base_util_em();

        DBChecker.check_db_changes(_em.getConnection());

        storagePoolNubHandler.check_db_changes();
  
    }

    public NetServer getNetServer()
    {
        return netServer;
    }
    public NetServer getSslNetServer()
    {
        return sslNetServer;
    }

    public StoragePoolHandlerServlet getPoolHandlerServlet()
    {
        return poolHandlerServlet;
    }

    public JobManager getJobManager()
    {
        return jobManager;
    }

    public LoginManager getLoginManager()
    {
        return loginManager;
    }
    
    public UserManager getUsermanager()


    {
        return userManager;
    }

    public CheckManager getCheckManager() {
        return checkManager;
    }
    


    public void initSchedules()
    {
        backupmanager.updateStartList();
    }

    public Date getNextStart( Schedule sched )
    {
        return backupmanager.getNextStart(sched);
    }
    public String getVersion()
    {
        return Main.get_version_str();
    }
    public String getGuiVersion()
    {
        try
        {
            Class cl = Class.forName("de.dimm.vsm.vaadin.VSMCMain");
            Method m = cl.getMethod("getVersion", (Class[]) null);
            return m.invoke(null, (Object[]) null).toString();
        }
        catch (Exception exception)
        {
        }
        return "?";
    }
    public String getChangelog()
    {
        return getChangelog(false);
    }
    public String getChangelog(Boolean isHtml)
    {
        String s = readTextFromJar( this.getClass().getCanonicalName(), "ChangeLog.txt");
        StringBuilder sb = new StringBuilder();
        if (isHtml.booleanValue())
        {
            sb.append("<h0>Changelog für VSM Server <b>").append(getVersion()).append("</b></h0><br>");
            sb.append("________________________________________________<p><p>");
            s = s.replaceAll("\n\n", "<p>");
            s = s.replaceAll("\n", "<br>");
        }
        else
        {
            sb.append("Changelog für VSM Server ").append(getVersion()).append("\n");
            sb.append("-----------------------------------------------\n\n");
        }
        sb.append(s);
        return sb.toString();
    }
    public String getGuiChangelog(Boolean isHtml)
    {
        String s = readTextFromJar("de.dimm.vsm.vaadin.GenericMain", "ChangeLog.txt");
        StringBuilder sb = new StringBuilder();
        if (isHtml.booleanValue())
        {
            sb.append("<h0>Changelog für VSM GUI <b>").append(getGuiVersion()).append("</b></h0><br>");
            sb.append("________________________________________________<p><p>");
            s = s.replaceAll("\n\n", "<p>");
            s = s.replaceAll("\n", "<br>");
        }
        else
        {
            sb.append("Changelog für VSM Server ").append(getGuiVersion()).append("\n");
            sb.append("-----------------------------------------------\n\n");
        }
        sb.append(s);
        return sb.toString();
    }
    
    static String readTextFromJar( String classname, String s )
    {
        InputStream is = null;
        BufferedReader br = null;
        String line;

        StringBuilder sb = new StringBuilder();
        try
        {
            Class clazz = Class.forName(classname);
            is = clazz.getResourceAsStream(s);
            br = new BufferedReader(new InputStreamReader(is));
            while (null != (line = br.readLine()))
            {
                if (sb.length() > 0)
                {
                    sb.append('\n');
                }
                sb.append(line);
            }
        }
        catch (Exception e)
        {
            Log.err(Main.Txt("Fehler beim Lesen von Changelog"), e);
        }
        finally
        {
            try
            {
                if (br != null)
                {
                    br.close();
                }
                if (is != null)
                {
                    is.close();
                }
            }
            catch (IOException e)
            {
            }
        }
        return sb.toString();
    }

    private void checkFixes()
    {
        if (Main.get_bool_prop(GeneralPreferences.HASH_URL_FORMAT_FIX, false))
        {
            FixHashUrlBug.fix(this);
        }
    }

    public PoolStatusResult getPoolStatusResult( StoragePool pool )
    {
        PoolMapper mapper = storagePoolNubHandler.getPoolMapper(pool);
        if (mapper != null)
            return mapper.getLastStatus();
        return null;
    }
    public PoolStatusResult calcPoolStatusResult( StoragePool pool )
    {
        PoolMapper mapper = storagePoolNubHandler.getPoolMapper(pool);
        if (mapper != null)
        {
            mapper.calcStats();
            return mapper.getLastStatus();
        }
        return null;
    }
    public PoolStatusResult abortCalcPoolStatusResult( StoragePool pool )
    {
        PoolMapper mapper = storagePoolNubHandler.getPoolMapper(pool);
        if (mapper != null)
        {
            mapper.abortCalcStats();
            return mapper.getLastStatus();
        }
        return null;
    }
    public long deleteFreeBlocks( StoragePool pool ) throws Exception
    {
         return retentionManager.handleDeleteFreeBlocks(pool);
    }
    public void abortDeleteFreeBlocks(  )
    {
        retentionManager.abortDeleteFreeBlocks();
    }

    public void reloadNotificationSettings(  ) throws IOException
    {
        notificationServer.loadNotifications(get_base_util_em());
    }

    static public ICheck getCheck(String className)
    {
        String checkClassName = "de.dimm.vsm.fsengine.checks." + className;
        try
        {
            Class clazz = Class.forName(checkClassName);
            return (ICheck) clazz.newInstance();
        }
        catch (Exception exception)
        {
            LogManager.err_db("Unknown CheckClass " + checkClassName + ": " + exception.getMessage());
            
        }
        return null;
    }
    
    public void setTrace( Boolean b)
    {
        Log.setTraceEnabled(b.booleanValue());
    }
    public Boolean getTrace()
    {
        return Log.isTraceEnabled();
    }

}
