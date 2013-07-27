/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

import de.dimm.vsm.log.LogManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

/**
 *
 * @author mw
 */
public class KeyToolHelper
{
    public static final String ALIAS = "mailsecurer";



    
    public static String get_keytool_cmd()
    {
        String java_home = System.getProperty("java.home").trim();
        String key_tool_cmd = java_home + "/bin/keytool";
        return key_tool_cmd;
    }
    public static File get_system_keystore()
    {
        String java_home = System.getProperty("java.home").trim();
        return new File(java_home + "/lib/security/cacerts");
    }
    public static String get_system_keystorepass()
    {
        return "changeit";
    }
    public static File get_ms_keystore()
    {
        return new File("mskeystore.jks");
    }
    public static String get_ms_keystorepass()
    {
        return "mailsecurer";
    }

    public static ArrayList<X509Certificate[]> list_certificates( boolean system_keystore) throws KeyStoreException, FileNotFoundException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException
    {
        String keystore_pwd;
        if (system_keystore)
        {
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            keystore_pwd = get_ms_keystorepass();
        }


        ArrayList<X509Certificate[]> cert_list = new ArrayList<X509Certificate[]>();
        KeyStore ks = load_keystore(system_keystore);

        /*
         * Allocate and initialize a KeyManagerFactory.
         */
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, keystore_pwd.toCharArray());
        KeyManager[] managers = kmf.getKeyManagers();
        for (int i = 0; i < managers.length; i++)
        {
            KeyManager keyManager = managers[i];
            if (keyManager instanceof X509KeyManager)
            {
                X509KeyManager xk = (X509KeyManager)keyManager;
                String[] aliases = xk.getServerAliases("RSA", null);
                for (int j = 0; j < aliases.length; j++)
                {
                    String a = aliases[j];
                    X509Certificate[] cert_array = xk.getCertificateChain(a);
                    cert_list.add(cert_array);
                }
            }
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ks);
        TrustManager[] tmanagers = tmf.getTrustManagers();
        for (int i = 0; i < tmanagers.length; i++)
        {
            TrustManager trustManager = tmanagers[i];
            if (trustManager instanceof X509TrustManager)
            {
                X509TrustManager xk = (X509TrustManager)trustManager;
                X509Certificate[] cert_array = xk.getAcceptedIssuers();
                cert_list.add(cert_array);
            }
        }


        return cert_list;
    }

    public static String import_cacert( String alias, File ca_cert_file, boolean system_store, boolean trustcert)
    {
        String ret;
        String keystore;
        String keystore_pwd;

        String key_tool_cmd = get_keytool_cmd();

        if (system_store)
        {
            keystore = get_system_keystore().getAbsolutePath();
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            keystore = get_ms_keystore().getAbsolutePath();
            keystore_pwd = get_ms_keystorepass();
        }
        ArrayList<String> cmd_list = new ArrayList<String>();
        cmd_list.add( key_tool_cmd );
        cmd_list.add( "-import" );
        cmd_list.add( "-noprompt" );
        cmd_list.add( "-alias" );
        cmd_list.add( alias );
        cmd_list.add( "-storepass" );
        cmd_list.add( keystore_pwd );
        cmd_list.add( "-file" );
        cmd_list.add( ca_cert_file.getAbsolutePath() );
        cmd_list.add( "-keystore" );
        cmd_list.add( keystore );
        if (trustcert)
        {
            cmd_list.add( "-trustcacerts" );
        }

        String[] cert_import_cmd = cmd_list.toArray(new String[0]);

        CmdExecutor exec = new CmdExecutor(cert_import_cmd);

        int code = exec.exec();
        if (code == 0)
        {
            ret = "0: ok";
        }
        else
        {
            ret = "4: " + exec.get_out_text() + " " + exec.get_err_text();
        }
        return ret;
    }

    public static String get_alias_from_certificate( Certificate cert, boolean system_keystore )
    {
        try
        {
            KeyStore ks = load_keystore(system_keystore);
            String alias = ks.getCertificateAlias(cert);
            return alias;

        }
        catch (Exception keyStoreException)
        {
            LogManager.msg( LogManager.LVL_ERR, LogManager.TYP_SECURITY, "Cannot get alias from certificate: " + keyStoreException.getMessage());
            return null;
        }
    }


    public static String delete_alias( String alias,  boolean system_store )
    {
        String ret;
        String ca_cert_file;
        String keystore_pwd;

        String key_tool_cmd = get_keytool_cmd();

        if (system_store)
        {
            ca_cert_file = get_system_keystore().getAbsolutePath();
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            ca_cert_file = get_ms_keystore().getAbsolutePath();
            keystore_pwd = get_ms_keystorepass();
        }

        String[] cert_import_cmd = { key_tool_cmd, "-delete", "-noprompt", "-alias", alias, "-keypass", keystore_pwd, "-storepass", keystore_pwd,
                                     "-keystore", ca_cert_file};

        CmdExecutor exec = new CmdExecutor(cert_import_cmd);

        int code = exec.exec();
        if (code == 0)
        {
            ret = "0: ok";
        }
        else
        {
            ret = "5: " + exec.get_out_text() + " " + exec.get_err_text();
        }
        return ret;
    }

    public static boolean create_csr( String alias, boolean system_keystore, StringBuffer sb )
    {        
        String ca_cert_file;
        String keystore_pwd;

        String key_tool_cmd = get_keytool_cmd();

        if (system_keystore)
        {
            ca_cert_file = get_system_keystore().getAbsolutePath();
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            ca_cert_file = get_ms_keystore().getAbsolutePath();
            keystore_pwd = get_ms_keystorepass();
        }



        String[] cert_import_cmd = { key_tool_cmd, "-certreq", "-noprompt", "-keyalg", "RSA", "-alias", alias, "-keypass", keystore_pwd, "-storepass", keystore_pwd,
                                     "-keystore", ca_cert_file};

        CmdExecutor exec = new CmdExecutor(cert_import_cmd);

        int code = exec.exec();
        if (code == 0)
        {
            sb.append( exec.get_out_text() );
            return true;
        }
        else
        {
            sb.append(exec.get_out_text()).append(" ").append( exec.get_err_text());
            return false;
        }
    }
    
    public static KeyStore load_keystore( boolean system_keystore )
    {
        String ca_cert_file;
        String keystore_pwd;


        if (system_keystore)
        {
            ca_cert_file = get_system_keystore().getAbsolutePath();
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            ca_cert_file = get_ms_keystore().getAbsolutePath();
            keystore_pwd = get_ms_keystorepass();
        }

        // CHECK FOR EXISTENCE
        File keystore = new File(ca_cert_file);
        if ( !keystore.exists() )
        {
            LogManager.msg( LogManager.LVL_ERR, LogManager.TYP_SECURITY,"Keystor does not exist: " + ca_cert_file);
            return null;
        }
        

        try
        {
            KeyStore ks = KeyStore.getInstance("JKS");

            FileInputStream fis = new FileInputStream(ca_cert_file);
            ks.load(fis, keystore_pwd.toCharArray());
            fis.close();
            
            return ks;
        }
        catch (Exception keyStoreException)
        {
            LogManager.msg( LogManager.LVL_ERR, LogManager.TYP_SECURITY,"Cannot get load keystore " + ca_cert_file + ": "  + keyStoreException.getMessage());
            return null;
        }
    }

    public static boolean is_keystore_valid(boolean system_keystore) throws IOException
    {


        KeyStore ks = load_keystore( system_keystore);
        if (ks == null)
            return false;
        try
        {
            if (ks.containsAlias(ALIAS))
            {
                return true;
            }
            else
            {
                LogManager.msg( LogManager.LVL_ERR, LogManager.TYP_SECURITY,"Missing alias " + ALIAS + " in key store");
            }
        }
        catch (KeyStoreException ex)
        {
                LogManager.msg( LogManager.LVL_ERR, LogManager.TYP_SECURITY,"Invalid alias " + ALIAS + " in key store: " + ex.getMessage());
        }
        return false;
    }

    public static String create_key(String dn_string, String alias, String keylength, boolean system_keystore)
    {
        String ca_cert_file;
        String keystore_pwd;

        if (system_keystore)
        {
            ca_cert_file = get_system_keystore().getAbsolutePath();
            keystore_pwd = get_system_keystorepass();
        }
        else
        {
            ca_cert_file = get_ms_keystore().getAbsolutePath();
            keystore_pwd = get_ms_keystorepass();
        }

        String key_tool_cmd = get_keytool_cmd();
       
        String[] cert_genkey_cmd = { key_tool_cmd, "-genkey", "-noprompt", "-keyalg", "RSA", "-alias", alias, "-keypass", keystore_pwd, "-storepass", keystore_pwd,
                                     "-dname", dn_string, "-keystore", ca_cert_file, "-validity", "3650", "-keysize", keylength};

        CmdExecutor exec = new CmdExecutor(cert_genkey_cmd);

        int code = exec.exec();
        if (code != 0)
        {
            return "4: " + exec.get_out_text() + " " + exec.get_err_text();
        }

        String[] cert_selcert_cmd = { key_tool_cmd, "-selfcert", "-noprompt", "-alias", alias, "-keypass", keystore_pwd, "-storepass", keystore_pwd,
                                      "-keystore", ca_cert_file};

        exec = new CmdExecutor(cert_selcert_cmd);

        code = exec.exec();
        if (code == 0)
        {
            return "0: ok";
        }
        else
        {
            return "5: " + exec.get_out_text() + " " + exec.get_err_text();
        }
    }

}
