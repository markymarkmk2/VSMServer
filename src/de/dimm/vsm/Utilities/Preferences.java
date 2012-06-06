/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.Main;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author mw
 */
public class Preferences
{
    String prefs_path;
    protected ArrayList<String> prop_names;
    Properties props;
    protected String password;


    public static final String ENC_PASSWORD = "EncryptionPassword";
    public static final String DFLT_PASSWORD = "12345";

    public Preferences( String _path )
    {
        prefs_path = _path;
        if (prefs_path.charAt(prefs_path.length() - 1 ) != '/')
            prefs_path += "/";

        prop_names = new ArrayList<String>();
        prop_names.add( ENC_PASSWORD );
    }

    public Preferences()
    {
        this(Main.PREFS_PATH);
    }
    public String getFilename()
    {
        return "preferences.dat";
    }

    String base_prop_name( String s )
    {
        int idx = s.lastIndexOf("_");
        if (idx >= 0)
        {
            try
            {
                int n = Integer.parseInt(s.substring(idx + 1));
                return s.substring(0, idx);
            }
            catch (Exception exc)
            {
            }
        }
        return s;
    }

    boolean check_prop( String s )
    {
        for (int i = 0; i < prop_names.size(); i++)
        {
            String base_prop = base_prop_name(s);
            if (prop_names.get(i).compareTo(base_prop) == 0)
            {
                return true;
            }
        }
        return false;
    }
    private void warn_unknown( String p)
    {
        LogManager.msg_system(LogManager.LVL_ERR, Main.Txt("Unknown_property") + " <" + p + ">");
    }

    public String get_prop( String p )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
            return null;
        }
        String ret = props.getProperty(p);
        return ret;
    }
    public String get_prop( String p, String def )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
            return null;
        }
        String ret = props.getProperty(p);
        if (ret == null)
            ret = def;
        
        return ret;
    }

    public boolean get_boolean_prop( String p, boolean  def )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
            return false;
        }
        String ret = props.getProperty(p);

        if (ret == null || ret.length() == 0)
            return def;

        // WE ALLOW 1, Yes, Ja, True case insensitive
        try
        {
            if (ret.charAt(0) == '1' || ret.toLowerCase().charAt(0) == 'j' ||ret.toLowerCase().charAt(0) == 'y'  || ret.toLowerCase().charAt(0) == 't' )
                return true;
        }
        catch (Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR, Main.Txt("Cannot_read_boolean_property") + ": ", exc);
        }

        return false;
    }
    public boolean get_boolean_prop( String p)
    {
        return get_boolean_prop(p, false);
    }
    public long get_long_prop( String p, long def )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
            return -1;
        }
        String ret = props.getProperty(p);

        if (ret == null || ret.length() == 0)
            return def;

        try
        {
            return Long.parseLong(ret);
        }
        catch (Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR, Main.Txt("Cannot_read_long_property") + ": ", exc);
        }

        return -1;
    }
    public long get_long_prop( String p )
    {
        return get_long_prop(p, 0);
    }

    public int get_int_prop( String p, int def )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
            return -1;
        }
        String ret = props.getProperty(p);

        if (ret == null || ret.length() == 0)
            return def;

        try
        {
            return Integer.parseInt(ret);
        }
        catch (Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR, Main.Txt("Cannot_read_int_property") + ": ", exc);
        }

        return -1;
    }
    public long get_int_prop( String p )
    {
        return get_int_prop(p, 0);
    }


    boolean noPwd;

    public boolean hasNoPwd()
    {
        return noPwd;
    }
    public ArrayList<String> get_prop_list()
    {
        return prop_names;
    }

    public void read_props()
    {
        noPwd = false;

        File prop_file = new File(prefs_path + getFilename());
        props = new Properties();
        try
        {
            if (prop_file.exists())
            {
                FileInputStream istr = new FileInputStream(prop_file);
                props.load(istr);
                istr.close();
            }
        }
        catch (Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR, Main.Txt("Cannot_read_properties") + " " + prop_file.getAbsolutePath() + ": ", exc);
        }

        password = get_prop(ENC_PASSWORD);

        if (password == null)
        {
            noPwd = true;
            password = "12345";
        }
        else
        {
            String str = CryptTools.crypt_internal(password, LogManager.get_instance(), CryptTools.ENC_MODE.DECRYPT);
            if (str == null)
            {
                LogManager.msg_system( LogManager.LVL_ERR, "Cannot decrypt password from preferences");
            }
            else
            {
                password = str;
            }
        }
    }

    public void set_prop( String p, String v )
    {
        if (!check_prop(p))
        {
            warn_unknown(p);
        }
        props.setProperty(p, v);
    }

    public boolean store_props()
    {
        if (password == null)
        {
            LogManager.msg_system( LogManager.LVL_ERR, "There is no password set");
        }
        else
        {
            String pwd = CryptTools.crypt_internal(password, LogManager.get_instance(), CryptTools.ENC_MODE.ENCRYPT);
            set_prop(ENC_PASSWORD, pwd);
        }

        File prop_file = new File(prefs_path + getFilename());
        try
        {
            FileOutputStream ostr = new FileOutputStream(prop_file);
            props.store(ostr, Main.APPNAME + " Properties, please do not edit");
            ostr.close();
            return true;
        }
        catch (Exception exc)
        {
            LogManager.msg_system( LogManager.LVL_ERR,  Main.Txt("Cannot_write_properties") + " " + prop_file.getAbsolutePath() + ": ", exc);
        }
        return false;
    }




    public String get_password()
    {
        if (password == null)
            return DFLT_PASSWORD;

        return password;
    }
    

    public void set_password( String pwd )
    {
        // TRY TO OBFUSCATE PWD
        if (pwd != null && pwd.length() > 0)
        {
             password = pwd;
        }
    }


}
