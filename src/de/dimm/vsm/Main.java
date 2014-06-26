/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import static de.dimm.vsm.LogicControl.getDbPath;
import de.dimm.vsm.Utilities.DefaultTextProvider;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.Utilities.VariableResolver;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.mail.NotificationEntry;
import de.dimm.vsm.txtscan.TxtScan;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Statistics;


/**
 *
 * @author Administrator
 */
public class Main
{

    static String source_str = "trunk";
    static String version_str = "1.7.1";
        
    public static int writeThreads = 1;
    public static int maxOpenFiles = 1024;

    static LogicControl control = null;

    public static final String PREFS_PATH = "preferences/";
    public static final String TEMP_PATH = "temp/";
    public static final String LOG_PATH = "logs/";
    public static final String SCRIPT_PATH = "scripts/";
    public static final String DATABASEPATH = "db/";
    public static final String LICENSE_PATH = "license/";

    private static String work_dir;

    private static GeneralPreferences general_prefs;

    private static TextBaseManager textManager;
    
    private static Main me;
    public final static String APPNAME = "VSM";
    private static boolean rebuildDB = false;

    private static void checkAdditionArg( String[] args, int i, String errMsg )
    {
        if (i >= args.length || args[i] == null || args[i].isEmpty())
        {
            System.out.println(errMsg);
            System.exit(-1);
        }
    }

    boolean agent_tcp = true;
    static boolean performanceDiagnostic = false;

    ShutdownHook hook;
    public static final long MAX_DERBY_PAGECACHE_SIZE_BYTE = 200*1024*1024; // == real Mem
    public static final long MIN_DERBY_PAGECACHE_SIZE_BYTE = 8092*1024; // == real Mem


    static boolean useIpV6()
    {
        return false;
    }

    
    public  Main()
    {

    }

    public static boolean isPerformanceDiagnostic()
    {
        return performanceDiagnostic;
    }

    public static void setPerformanceDiagnostic( boolean performanceDiagnostic )
    {
        Main.performanceDiagnostic = performanceDiagnostic;
    }
    


    public static LogicControl get_control()
    {
        return control;
    }
    public static void addNotification(NotificationEntry e)
    {
        if (control != null)
            control.addNotification(e);
    }
    public static void fire( String key, String extraText, VariableResolver vr )
    {
        if (control != null)
            control.getNotificationServer().fire(key, extraText, vr);
    }
    public static void release( String key )
    {
        if (control != null)
            control.getNotificationServer().release(key);
    }

    static void print_system_property( String key )
    {
        Log.info( "Property",  key + ": " + System.getProperty(key) );
    }

    static boolean isJava7orBetter()
    {
        String javaVer = System.getProperty("java.version");
        try
        {

            String[] a = javaVer.split("\\.");
            int maj = Integer.parseInt(a[0]);
            int min = Integer.parseInt(a[1]);
            if (maj == 1 && min < 7)
            {
                return false;

            }
        }
        catch (Exception exc)
        {
            System.out.println("Fehler beim Ermitten der Javaversion: " + javaVer + ": " + exc.getMessage());
        }
        return true;
    }    
    public void init() throws SQLException
    {
        
        if (!isJava7orBetter())
        {
            System.err.println("Java Version must be at least 1.7, aborting");
            System.exit(1);
        }

        textManager = new TextBaseManager("DE");
        DefaultTextProvider.setProvider(textManager);

        Log.info( "VSM Server Version",  get_version_str() );

        work_dir = new File(".").getAbsolutePath();
        if (work_dir.endsWith("."))
            work_dir = work_dir.substring(0, work_dir.length() - 2);

        rebuildDB = checkRebuildDB();

        print_system_property( "java.version" );
        print_system_property( "java.vendor" );
        print_system_property( "java.home");
        print_system_property( "java.class.path");
        print_system_property( "os.name");
        print_system_property( "os.arch");
        print_system_property( "os.version");
        print_system_property( "user.dir");

        create_prefs();
        
        textManager.setLang(get_prop(GeneralPreferences.LANGUAGE, "DE"));

        
        me = this;


        initShutdown();

        try
        {
            File f = new File( LOG_PATH );
            if (!f.exists())
                f.mkdirs();

            f = new File( PREFS_PATH );
            if (!f.exists())
                f.mkdirs();


            f = new File( LICENSE_PATH );
            if (!f.exists())
                f.mkdirs();

            f = new File( getDbPath() );
            if (!f.exists())
            {
                f.mkdirs();
            }

            f = new File( TEMP_PATH );
            if (!f.exists())
                f.mkdirs();
        }
        catch ( Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR, "Cannot create local dirs: " + exc.getMessage() );
        }

        setDerbyProperties();

        JDBCEntityManager.MAX_CONNECTIONS = get_int_prop(GeneralPreferences.MAX_CONNECTIONS, JDBCEntityManager.MAX_CONNECTIONS);

        control = new LogicControl(this);

        control.init(agent_tcp);

        hook = new ShutdownHook();

        Runtime.getRuntime().addShutdownHook(hook);
    }

    public static String get_version_str()
    {
        return version_str + " " + source_str;
    }

    public static GeneralPreferences get_prefs()
    {
        return general_prefs;
    }
    public static void create_prefs()
    {
        general_prefs = new GeneralPreferences();
        general_prefs.read_props();
    }

    public static List<InetAddress> getInetAddresses( boolean ipv6)
    {
        List<InetAddress> list = new ArrayList<InetAddress>();

        try
        {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();

            while(en.hasMoreElements())
            {
                NetworkInterface ni = en.nextElement();

                if (ni.isUp())
                {
                    Enumeration<InetAddress> ai = ni.getInetAddresses();

                    while (ai.hasMoreElements())
                    {
                        InetAddress ad = ai.nextElement();
                        if (ad.isLoopbackAddress())
                            continue;

                        if (ipv6)
                        {
                            if (ad instanceof Inet6Address)
                                list.add(ad);
                        }
                        else
                        {
                            if (!(ad instanceof Inet6Address))
                                list.add(ad);
                        }
                    }
                }
            }
        }
        catch (Exception exception)
        {
        }
        return list;
    }

    public static InetAddress getServerAddress()
    {
        InetAddress serverIp = null;

        try
        {
            serverIp = InetAddress.getLocalHost();
            if (serverIp.isLoopbackAddress())
            {
                List<InetAddress> list = getInetAddresses( Main.useIpV6() );

                if (list.size() > 0)
                    serverIp = list.get(0);
            }
            //Log.info("ServerIP", serverIp.toString());
        }
        catch ( Exception exc )
        {
            Log.err("ServerIP kann nicht ermittelt werden", exc);
        }
        return serverIp;
    }
    
    public static int getServerPort()
    {
        return get_int_prop(GeneralPreferences.PORT, 8080);
    }
    public static int getSslServerPort()
    {
        return get_int_prop(GeneralPreferences.SSL_PORT, 8443);
    }

    public static void checkJavaVersion()
    {
        Properties sProp = java.lang.System.getProperties();
        String sVersion = sProp.getProperty("java.version");
        sVersion = sVersion.substring(0, 3);
        Float f = Float.valueOf(sVersion);
        if (f.floatValue() < (float) 1.6)
        {
            System.out.println("Java version must be 1.6 or newer");
            System.exit(-1);
        }
    }

    public static void main( String[] args )
    {
        boolean changeLog = false;
        boolean doStat = false;
        
        for (int i = 0; i < args.length; i++)
        {
            String string = args[i];
            if (string.equals("-fake-write-backup"))
                Backup.speed_test_no_write = true;
            if (string.equals("-fake-db-backup"))
                Backup.speed_test_no_db = true;
            if (string.equals("-v"))
            {
                performanceDiagnostic = true;
                Log.setTraceEnabled(true);
            }
            if (string.equals("-stats"))
            {
                doStat = true;
            }
            if (string.equals("-version"))
            {
                System.out.println(version_str);
                System.exit(0);
            }
            if (string.equals("-changelog"))
            {
                LogicControl log = new LogicControl(null);
                System.out.println(log.getChangelog());
                System.exit(0);
            }
            if (string.equals("-import-text-db") )
            {
                checkAdditionArg(args, i +2, "Importdatei und oder code (utf8 / cp1252) fehlen");
                String filename = args[i + 1];
                String code = args[i + 2];
               
                try
                {
                    TextBaseManager.importTextCsv(filename, code);
                }
                catch (Exception exception)
                {
                    System.out.println(exception.getMessage());
                    System.exit(-1);
                }
                System.exit(0);
            }
            if (string.equals("-export-text-db") )
            {
                checkAdditionArg(args, i +2, "Ixportdatei und oder code (utf8 / cp1252) fehlen");
                
                String filename = args[i + 1];
                String code = args[i + 2];
                try
                {
                    TextBaseManager.exportTextCsv(filename, code);
                }
                catch (Exception exception)
                {
                    System.out.println(exception.getMessage());
                    System.exit(-1);
                }
                System.exit(0);
            }
            if (string.equals("-clean-txt-scan") )
            {
                TxtScan.main( null);
                
                try
                {
                    TextBaseManager.filterTxtScan();
                }
                catch (Exception exception)
                {
                    System.out.println(exception.getMessage());
                    System.exit(-1);
                }
                System.exit(0);
            }           
        }
        
        // SETUP PATH FOR GUI JAR
        Main.initGuiJar();
        
        // SETUP LOG
        Log.setDebugEnabled(true);
        LogManager.setDbLogger( new Log());

        Main main = new Main();
        try
        {
            main.init();

            if (doStat)
                LogicControl.getStorageNubHandler().infoStats();
        }
        catch (SQLException exception)
        {
            Log.err("Error in while initializing", exception);
        }
        checkJavaVersion();
        
        
        TextBaseManager.setOpenTxtGuiException( get_bool_prop(GeneralPreferences.TXTBASE_EDIT));

        main.run();
        try
        {
            get_control().getNetServer().join_server();
            get_control().getSslNetServer().join_server();
        }
        catch (Exception exception)
        {
            Log.err("Error in main", exception);
        }
    }

    public static String getWorkDir()
    {
        return work_dir;
    }

    public static String getLang()
    {
        return Main.get_prop( GeneralPreferences.LANGUAGE, "DE") ;
    }


    public static String Txt( String key)
    {
        if (textManager != null)
            return textManager.Txt(key);
        
        return key;
    }
    public static String GuiTxt( String key)
    {
        if (textManager != null)
            return textManager.GuiTxt(key);
        
        return key;
    }


    static public String get_prop( String pref_name )
    {
        if (general_prefs != null)
        {
            return Main.general_prefs.get_prop(pref_name);
        }
        return null;
    }
    static public String get_prop( String pref_name, int channel )
    {
        if (general_prefs != null)
        {
            return Main.general_prefs.get_prop(pref_name + "_" + channel);
        }
        return null;
    }

    static public long get_long_prop( String pref_name, long def )
    {
        if (general_prefs != null)
        {
            String ret = Main.general_prefs.get_prop(pref_name);
            if (ret != null)
            {
                try
                {
                    return Long.parseLong( ret );
                }
                catch (Exception exc)
                {
                    LogManager.msg_system( LogManager.LVL_ERR,  "Long preference " + pref_name + " has wrong format");
                }
            }
        }
        return def;
    }

    static public long get_long_prop( String pref_name )
    {
        return get_long_prop( pref_name, 0);
    }

   static public int get_int_prop( String pref_name, int def )
    {
        if (general_prefs != null)
        {
            String ret = Main.general_prefs.get_prop(pref_name);
            if (ret != null)
            {
                try
                {
                    return Integer.parseInt( ret );
                }
                catch (Exception exc)
                {
                    LogManager.msg_system( LogManager.LVL_ERR,  "Int preference " + pref_name + " has wrong format");
                }
            }
        }
        return def;
    }

    static public int get_int_prop( String pref_name )
    {
        return get_int_prop( pref_name, 0);
    }

    static public String get_prop( String pref_name, String def )
    {
        String ret = get_prop(pref_name);
        if (ret == null)
            ret = def;

        return ret;
    }
    static public void set_prop( String pref_name, String v )
    {
        if (general_prefs != null)
        {
            Main.general_prefs.set_prop(pref_name, v);
        }
    }
    static public void set_prop( String pref_name, String v, int channel )
    {
        if (general_prefs != null)
        {
            Main.general_prefs.set_prop(pref_name + "_" + channel, v);
        }
    }
    static public void set_long_prop( String pref_name, long v )
    {
        if (general_prefs != null)
        {
            Main.general_prefs.set_prop(pref_name, Long.toString(v));
        }
    }
    static public boolean get_bool_prop( String pref_name, boolean def )
    {
        String bool_true = "tTjJyY1";
        String bool_false = "fFnN0";

        if (general_prefs != null)
        {
            String ret = Main.general_prefs.get_prop(pref_name);
            if (ret != null)
            {
                if (bool_true.indexOf(ret.charAt(0)) >= 0)
                    return true;
                if (bool_false.indexOf(ret.charAt(0)) >= 0)
                    return false;

                LogManager.msg_system( LogManager.LVL_ERR,  "Boolean preference " + pref_name + " has wrong format");
            }
        }
        return def;
    }

    static public boolean get_bool_prop( String pref_name )
    {
        return get_bool_prop( pref_name, false);
    }
    static public void set_bool_prop( String pref_name, boolean  v )
    {
        if (general_prefs != null)
        {
            Main.general_prefs.set_prop(pref_name, v ? "1" : "0");
        }
    }


    public static String getActDateString()
    {
        return getDateString(System.currentTimeMillis());
    }
    public static String getDateString(long t)
    {
        return getDateString( new Date(t));
    }
    static SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH.mm");
    public static String getDateString(Date d)
    {
        return sdf.format(d);
    }

    public static File getGuiJar()
    {
        return guiJar;
    }

    static void initGuiJar()
    {
        guiJar = new File(System.getProperty("user.dir") + "/lib/VSMGui.jar");
        if (!guiJar.exists())
            guiJar = new File(System.getProperty("user.dir") + "/dist/lib/VSMGui.jar");
    }
    static File guiJar;
    private void run()
    {
        int cnt = 0;

        File prefs = new File(Main.PREFS_PATH, general_prefs.getFilename() );

        long guiMod = guiJar.lastModified();
        long prefMod = prefs.lastModified();

        while (true)
        {
            LogicControl.sleep(1000);
            cnt++;


            // EVERY MINUTE
            if ((cnt %60) == 0)
            {
                long fm = Runtime.getRuntime().freeMemory();
                CacheManager.create();
                if (CacheManager.getInstance().cacheExists(JDBCEntityManager.OBJECT_CACHE))
                {
                    Cache ch = CacheManager.getInstance().getCache(JDBCEntityManager.OBJECT_CACHE);
                    ch.evictExpiredElements();
                    if (performanceDiagnostic)
                    {
                        Statistics st = ch.getStatistics();
                        System.out.println("Size: " + ch.getSize() + ": " + st.toString() );
                        System.out.println("Open Commits: " + JDBCEntityManager.getOpenCommits() );
                    }
                }
                System.out.println("Free Mem " + SizeStr.format(fm));                
                
            }


            long actGuiMod = guiJar.lastModified();
            if (actGuiMod != guiMod)
            {
                LogicControl.sleep(5000);
                actGuiMod = guiJar.lastModified();
                guiMod = actGuiMod;
                get_control().restartGui();
            }
            long actPrefMod = prefs.lastModified();
            if (actPrefMod != prefMod)
            {
                LogicControl.sleep(5000);
                actPrefMod = prefs.lastModified();
                prefMod = actPrefMod;
                general_prefs.read_props();
            }
            
            File f = new File("shutdown.txt");
            if (!f.exists())
                continue;

            if (handleShutdown( f ))
            {
                System.exit(0);
                break;
            }

        }
    }

    void setSystemPropPref( String name, String val )
    {
        System.getProperties().setProperty(name, general_prefs.get_prop(name, val));
        Log.debug("Setze Systempref", name + ": " + val);
    }

    private void setDerbyProperties()
    {
        long memSize = Runtime.getRuntime().maxMemory();
        System.out.println("MaxMem: " + Long.toString(memSize));
        if (memSize != Long.MAX_VALUE)
        {
            // NOT MORE THAN 1/20 OF MAX MEM
            memSize /= 20;
            if (memSize > MAX_DERBY_PAGECACHE_SIZE_BYTE)
            {
                memSize = MAX_DERBY_PAGECACHE_SIZE_BYTE;
            }
            if (memSize < MIN_DERBY_PAGECACHE_SIZE_BYTE)
            {
                memSize = MIN_DERBY_PAGECACHE_SIZE_BYTE;
            }
            System.out.println("DBPageMem: " + Long.toString(memSize));
            setSystemPropPref( "derby.storage.pageCacheSize", Long.toString( memSize / 4096) );
        }


        setSystemPropPref( "derby.storage.pageSize", "4096" );
//        setSystemPropPref( "derby.locks.deadlockTrace","true");
        setSystemPropPref( "derby.language.disableIndexStatsUpdate","true");
        //setSystemPropPref( "derby.storage.indexStats.auto","false");

    }

    public static void set_service_shutdown()
    {
        ShutdownHook hook = Main.me.hook;

        hook.start();
        try
        {
            hook.join();
        }
        catch (InterruptedException interruptedException)
        {
        }
        System.exit(0);
    }

    private boolean handleShutdown( File f )
    {
        Log.info("Shutdown gestartet");
        int waitSeconds = 0;
        if (f.length() > 0)
        {
            try
            {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String data = br.readLine();
                br.close();
                waitSeconds = Integer.parseInt(data);
            }
            catch (Exception exception)
            {
            }
        }
        f.delete();

        File shutdownInProcess = new File("shutdown_in_process.txt");
        try
        {
            shutdownInProcess.createNewFile();
        }
        catch (IOException iOException)
        {
            Log.err("Kann shutdown_in_process datei nicht erzeugen", iOException);
        }

        Log.info("Shutdown in", waitSeconds + "s");

        for (int i = 0; i < waitSeconds; i++)
        {
            LogicControl.sleep(1000);
            File shutdownAbort = new File("shutdown_abort.txt");
            if (shutdownAbort.exists())
            {
                Log.info("Shutdown abgebrochen");
                shutdownInProcess.delete();
                return false;
            }
        }

        hook.start();
        try
        {
            hook.join();
        }
        catch (InterruptedException interruptedException)
        {
        }
        
        shutdownInProcess.delete();

        File shutdown = new File("shutdown_ok.txt");
        try
        {
            shutdown.createNewFile();
        }
        catch (IOException iOException)
        {
            Log.err("Kann shutdown datei nicht erzeugen", iOException);
        }
        Log.info("Shutdown abgeschlossen");
        return true;
        
    }

    private void initShutdown()
    {
        File shutdown = new File("shutdown_ok.txt");
        if (shutdown.exists())
            shutdown.delete();
        shutdown = new File("shutdown_in_process.txt");
        if (shutdown.exists())
            shutdown.delete();
    }

    static public boolean getRebuildDB()
    {
        return rebuildDB;
    }

    long readDBRelease( File dbrelease )
    {
        long ret = -1;
        if (dbrelease.exists())
        {
            String v = null;
            BufferedReader br = null;
            try
            {
                br = new BufferedReader(new FileReader(dbrelease));
                v = br.readLine();
                br.close();
                ret = Long.parseLong(v);
            }
            catch (Exception iOException)
            {
                if (br != null)
                {
                    try
                    {
                        br.close();
                    }
                    catch (IOException iOException1)
                    {
                    }
                }
            }
        }
        return ret;
    }

    void writeDBRelease( File dbrelease )
    {        
        BufferedWriter bw = null;
        try
        {
            bw = new BufferedWriter(new FileWriter(dbrelease));
            bw.write( Long.toString(DBChecker.getActDBRelease()) );
            bw.close();
        }
        catch (Exception iOException)
        {
            if (bw != null)
            {
                try
                {
                    bw.close();
                }
                catch (IOException iOException1)
                {
                }
            }
        }
    }

    private boolean checkRebuildDB()
    {
        boolean rebuild = false;
        File dbrelease = new File(getDbPath() + "dbVer.txt");

        long r = readDBRelease(dbrelease);

        if (r != DBChecker.getActDBRelease())
        {
            rebuild = true;
            writeDBRelease( dbrelease );
        }
        

        return rebuild;
    }



}
