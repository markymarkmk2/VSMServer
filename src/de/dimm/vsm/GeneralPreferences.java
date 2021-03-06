/*
 * Preferences.java
 *
 * Created on 5. Oktober 2007, 18:58
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.Utilities.Preferences;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;





/**
 *
 * @author Administrator
 */
public class GeneralPreferences extends Preferences
{
    
    public static final String SIUNITS ="SIUnits";
    public static final String NAME ="Name";
    public static final String STATION_ID ="StationID";
    public static final String DEBUG = "Debug";
    public static final String PORT = "Port";
    public static final String SSL_PORT = "SslPort";
    public static final String WEB_DAV_PORT = "WebDavPort";
    public static final String WEB_DAV_SEARCH_PORT = "WebDavSearchPort";
    public static final String SERVER = "Server";
    public static final String IP = "IP";
    public static final String GW = "Gateway";
    public static final String DNS = "NameServer";
    public static final String DHCP = "DHCP";
    public static final String MASK = "MASK";
    public static final String NETINTERFACE = "NetworkInterface";
    public static final String VPN_SERVER = "VPNServer";
    public static final String VPN_PORT = "VPNPort";
    public static final String PXENABLE = "ProxyEnable";
    public static final String PXSERVER = "ProxyServer";
    public static final String PXPORT = "ProxyPort";
    public static final String PXSOCKSPORT = "ProxySocksPort";
    public static final String RDATE_COMMAND = "RDateCommand";
    public static final String TEMPFILEDIR ="TempFileDir";
    
    public static final String ALLOW_CONTINUE_ON_ERROR = "AllowContinueOnError";
    public static final String DB_USER  = "DBUser";
    public static final String DB_PWD = "DBPWD";
    public static final String SERVER_SSL = "ServerSSL";
    public static final String COUNTRYCODE = "CountryCode";
    public static final String AUTO_UPDATE = "AutoUpdate";
    public static final String UPDATESERVER = "UpdateServer";
    public static final String HTTPUSER = "HttpUser";
    public static final String HTTPPWD = "HttpPwd";
    public static final String SYNCSRV_PORT = "SyncServerPort";
    public static final String FQDN = "FQDN";
    public static final String TRUSTSTORE = "TrustStore";
    public static final String AUDIT_DB_CONNECT = "AuditDBConnect";
    public static final String AUTO_SET_IP = "AutoSetIP";
    public static final String HTTPD_PORT = "HttpdPort";
    public static final String LANGUAGE = "Language";
    public static final String PERFORMANCE_DIAGNOSTIC = "PerformaceDiagnostic";
    public static final String HASH_URL_FORMAT_FIX = "HashUrlFormatFix";
    public static final String HASH_URL_FORMAT_EMPTY_LINKS = "HashUrlFormatAllowEmptyHashLinks";



    public static final String FILE_HASH_BLOCKSIZE = "MinFileTreshold";

    public static final String BA_START_WINDOW_S = "BackupStartWindowSecs";
    public static final String DB_PATH = "DatabasePath";
    public static final String IDX_PATH = "IndexPath";
    public static final String SYSADMIN_NAME = "SysAdminName";
    public static final String SYSADMIN_PWD = "SysAdminPwd";

    public static final String IGNORE_ACL = "IgnoreACL";
    public static final String SMTP_SERVER = "SMTPServer";
    public static final String SMTP_PORT = "SMTPPort";
    public static final String SMTP_USER = "SMTPUser";
    public static final String SMTP_USER_PWD = "SMTPUserPwd";
    public static final String SMTP_FROM = "SMTPFrom";
    public static final String SMTP_TLS = "SMTPTLS";
    public static final String WITH_BOOTSTRAP = "WithBootstrap";
    public static final String USE_H2_CACHE = "UseH2Cache";
    public static final String USE_NO_CACHE = "UseNoCache";
    public static final String USE_LAZY_CACHE = "UseLazyCache";
    public static final String MIN_FREE_NODE_SPACE_GB = "MinFreeNodeSpaceGB";
    public static final String FIX_DUPL_NAMES = "FixDuplNames";
    public static final String BLOCK_DUPL_DIRS = "BlockDuplDir";
    public static final String CACHE_ON_WRITE_FS = "NoCacheOnWriteFS";

    public static final String KEYSTORE = "KeystoreFile";
    public static final String KEYSTORE_PWD = "KeystorePwd";


    public static final String JDBC_URL = "JdbcUrl";
    public static final String FIX_BOOTSTRAP = "FixBootstrap";
    public static final String BOOTSTRAPFIX_OVERWRITE = "BootstrapFixOverwrite";
    public static final String TXTBASE_EDIT = "TextBaseEdit";
    public static final String MAX_OPEN_POOLHANDLERS = "MaxOpenPoolHandlers";
    public static final String AGENT_CONN_TIMEOUT = "AgentConnTimeout";
    public static final String AGENT_TX_TIMEOUT = "AgentTxTimeout";
    public static final String MAX_CONNECTIONS = "MaxConnections";
    
    public static final String DATABASE_SERVER = "DatabaseServer";
    public static final String DATABASE_PORT = "DatabasePort";
    public static final String DATABASE_PATH = "DatabasePath";
    public static final String HASH_LAZY_LOAD_TRESHOLD_MB= "HashLazyLoadGrenzeMB";
    public static final String USE_REDIS_CACHE = "UseRedisCache";
    public static final String NO_IP_RESOLVE = "NoIpResolve";  // Achtung, die Pref wird aus GUI abgefragt, dort ist Kostante unbekannt, dajer dort im Klartext 
    public static final String STOP_CDP_ON_SHUTDOWN = "StopCdpOnShutdown";
    public static final String MAX_CACHE_LOAD_S = "MaxCacheLoadS";
    public static final String NODE_ATTR_QRY_CNT = "NodeAttrQryCnt";
    public static final String BACKUP_BREAKS_RETENTION = "BackupBreaksRetention";
    public static final String RETENTION_BREAKS_BACKUP = "RetentionBreaksBackup";
    public static final String MAX_COMMIT_TIMEOUT = "MaxCommitTimeout";
    public static final String KICK_JOB_TIMEOUT_S = "KickJobTimeoutS";
    public static final String PREVIEW_ROOT = "PreviewRoot";
    public static final String PREVIEW_GMPATH = "PreviewGmPath";
    public static final String PREVIEW_IMPATH = "PreviewImPath";
    public static final String PREVIEW_WIDTH = "PreviewWidth";
    public static final String PREVIEW_HEIGHT = "PreviewHeight";
    public static final String IM_CMD_OPTS = "ImCmdOptions";
    public static final String GM_CMD_OPTS = "GmCmdOptions";
    public static final String IM_SUFFIXES = "ImSuffixes";
    public static final String GM_SUFFIXES = "GmSuffixes";
    
    
    /** Creates a new instance of Preferences */
    public GeneralPreferences()
    {
        this( Main.PREFS_PATH );
    }

    public GeneralPreferences(String _path)
    {
        super(_path);

        prop_names.add( SIUNITS );
        prop_names.add( NAME );
        prop_names.add( STATION_ID );
        prop_names.add( DEBUG );
        prop_names.add( PORT );
        prop_names.add( SSL_PORT );
        prop_names.add( SERVER );
        prop_names.add( IP );
        prop_names.add( GW );
        prop_names.add( DNS );
        prop_names.add( DHCP );
        prop_names.add( MASK );
        prop_names.add( NETINTERFACE );
        prop_names.add( PXENABLE );
        prop_names.add( PXSERVER );
        prop_names.add( PXPORT );
        prop_names.add( PXSOCKSPORT );
        prop_names.add( VPN_SERVER );
        prop_names.add( VPN_PORT );
        prop_names.add( RDATE_COMMAND );
        prop_names.add( TEMPFILEDIR );
        prop_names.add( DB_USER );
        prop_names.add( DB_PWD );
        prop_names.add( ALLOW_CONTINUE_ON_ERROR );
        prop_names.add( SERVER_SSL );
        prop_names.add( COUNTRYCODE );
        prop_names.add( AUTO_UPDATE );
        prop_names.add( UPDATESERVER );
        prop_names.add( HTTPUSER );
        prop_names.add( HTTPPWD );
        prop_names.add( SYSADMIN_NAME );
        prop_names.add( SYSADMIN_PWD );
        prop_names.add( SYNCSRV_PORT );
        prop_names.add( FQDN );
        prop_names.add( TRUSTSTORE );
        prop_names.add( AUDIT_DB_CONNECT );
        prop_names.add( AUTO_SET_IP );
        prop_names.add( HTTPD_PORT );
        prop_names.add( FILE_HASH_BLOCKSIZE );
        prop_names.add( BA_START_WINDOW_S );
        prop_names.add( LANGUAGE );
        prop_names.add( PERFORMANCE_DIAGNOSTIC );
        prop_names.add( DB_PATH );
        prop_names.add( IDX_PATH );
        prop_names.add( IGNORE_ACL );
        prop_names.add( HASH_URL_FORMAT_FIX );
        prop_names.add( WITH_BOOTSTRAP );
        prop_names.add( HASH_URL_FORMAT_EMPTY_LINKS );
        prop_names.add( USE_H2_CACHE );
        prop_names.add( USE_NO_CACHE );
        prop_names.add( MIN_FREE_NODE_SPACE_GB );
        prop_names.add( FIX_DUPL_NAMES );
        prop_names.add( FIX_BOOTSTRAP );
        prop_names.add( BLOCK_DUPL_DIRS );
        prop_names.add( CACHE_ON_WRITE_FS );
        prop_names.add( KEYSTORE );
        prop_names.add( KEYSTORE_PWD );
        prop_names.add( JDBC_URL );
        prop_names.add( BOOTSTRAPFIX_OVERWRITE );
        prop_names.add( TXTBASE_EDIT );
        prop_names.add( MAX_OPEN_POOLHANDLERS );
        prop_names.add( AGENT_CONN_TIMEOUT );
        prop_names.add( AGENT_TX_TIMEOUT );
        prop_names.add( MAX_CONNECTIONS );
        prop_names.add( USE_LAZY_CACHE );
        prop_names.add( HASH_LAZY_LOAD_TRESHOLD_MB );
        prop_names.add( USE_REDIS_CACHE );
        prop_names.add( NO_IP_RESOLVE );
        prop_names.add( STOP_CDP_ON_SHUTDOWN );
        prop_names.add( MAX_CACHE_LOAD_S );
        prop_names.add( NODE_ATTR_QRY_CNT );
        prop_names.add( BACKUP_BREAKS_RETENTION );
        prop_names.add( RETENTION_BREAKS_BACKUP );
        prop_names.add( MAX_COMMIT_TIMEOUT );
        prop_names.add( KICK_JOB_TIMEOUT_S );
        prop_names.add( PREVIEW_ROOT );
        prop_names.add( PREVIEW_WIDTH );
        prop_names.add( PREVIEW_HEIGHT );
        prop_names.add( PREVIEW_GMPATH );
        prop_names.add( PREVIEW_IMPATH );
        prop_names.add( WEB_DAV_PORT );
        prop_names.add( WEB_DAV_SEARCH_PORT );
        prop_names.add( IM_CMD_OPTS );
        prop_names.add( GM_CMD_OPTS );
        prop_names.add( IM_SUFFIXES );
        prop_names.add( GM_SUFFIXES );
        
        

        String[] log_types = LogManager.get_log_types();
        for (int i = 0; i < log_types.length; i++)
        {
            String string = log_types[i];
            prop_names.add( "LOG_" + string );
        }

    }

    @Override
    public void read_props()
    {
        super.read_props();

        // SET SI DEFAULT TRUE
        boolean si = get_boolean_prop(GeneralPreferences.SIUNITS, true);
        if (si)
            Log.info("Verwende SI Einheiten (1GB == 10^9 Byte)");
        else
            Log.info("Verwende EDV Einheiten (1GB == 1024*1024*1024 Byte)");
        SizeStr.setSI(si);
        Main.setPerformanceDiagnostic( get_boolean_prop(GeneralPreferences.PERFORMANCE_DIAGNOSTIC,  Main.isPerformanceDiagnostic() ));
        long minFreeNodeSpaceGB = get_long_prop(MIN_FREE_NODE_SPACE_GB, 1);
        if (minFreeNodeSpaceGB <= 0)
        {

            minFreeNodeSpaceGB = 1;
        }
        Log.info(MIN_FREE_NODE_SPACE_GB + ": " + minFreeNodeSpaceGB);
        StoragePoolHandler.setNodeMinFreeSpace( minFreeNodeSpaceGB * 1000 * 1000 * 1000l );
    }

    @Override
    protected boolean check_prop( String s )
    {
        if (s.startsWith("derby."))
            return true;
        if (s.startsWith("SMTP"))
            return true;
        if (s.startsWith( DATABASE_SERVER))
            return true;
        if (s.startsWith( DATABASE_PATH))
            return true;
        if (s.startsWith( DATABASE_PORT))
            return true;
        if (s.startsWith( USE_NO_CACHE))
            return true;
        if (s.startsWith( USE_LAZY_CACHE))
            return true;
        if (s.startsWith( USE_REDIS_CACHE))
            return true;
        if (s.startsWith( USE_H2_CACHE))
            return true;
        
        return super.check_prop(s);
    }
    
}
