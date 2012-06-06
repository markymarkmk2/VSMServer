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



    public static final String FILE_HASH_BLOCKSIZE = "MinFileTreshold";

    public static final String BA_START_WINDOW_S = "BackupStartWindowSecs";
    public static final String DB_PATH = "DatabasePath";
    public static final String IDX_PATH = "IndexPath";
    public static final String SYSADMIN_NAME = "SysAdminName";
    public static final String SYSADMIN_PWD = "SysAdminPwd";

    public static final String IGNORE_ACL = "IgnoreACL";



    
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
    }
    
}
