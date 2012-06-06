/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */



/**
 *
 * @author Administrator
 */

package de.dimm.vsm.Utilities;

import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.WorkerParent;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import javax.swing.JDialog;
import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;



class ServerEntry
{
    private String host;
    private int port;
    private String user;
    private String pwd;

    ServerEntry( String host, int port, String user, String pwd )
    {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
    }
    

    String getHost()
    {
        return host;
    }

    int getPort()
    {
        return port;
    }

    String getUser()
    {
        return user;
    }

    String getPwd()
    {
        return pwd;
    }
}
/**
 *
 * @author Administrator
 */
public class FileTransferManager extends WorkerParent 
{
            
    int actual_server_idx;
    public static final String NAME = "FileTransferManager";
    String last_errortext = "";
    
    ArrayList<ServerEntry> server_list;
    Header[] response_hlist;
    protected boolean with_busy = false;
    protected JDialog parent = null;
    protected boolean with_abort = false;
    
    String activity_text = "";
    protected boolean is_proxy_enabled = false;
    String px_server;
    int px_port;
    
    
    public FileTransferManager(String server, String user, String pwd)
    {
        this( server, 80, user, pwd );
    }
    public void enable_busy( boolean b, JDialog _parent )
    {
        with_busy = b;
        parent = _parent;
    }
    public void enable_abort( boolean b )
    {
        with_abort = b;
    }
            

    public FileTransferManager(String server, int port, String user, String pwd )
    {
        super(NAME);
        server_list = new ArrayList<ServerEntry>();
        server_list.add( new ServerEntry( server, port, user, pwd ) );        
    }
    
    public void set_proxy_data( String _px_server, int _px_port )
    {
        px_server = _px_server;
        px_port = _px_port;
        is_proxy_enabled = false;
        
        if (px_server != null && px_server.length() > 0)
            is_proxy_enabled = true;
    }
    
    public String get_resonse_header( String key)
    {
        if (response_hlist == null)
            return null;
        
        for (int i = 0; i < response_hlist.length; i++)
        {
            if (response_hlist[i].getName().compareTo(key) == 0)
                return response_hlist[i].getValue();
        }
        return null;
    }

    public String get_last_errortext()
    {
        return last_errortext;
    }
    
    ArrayList<ServerEntry> get_server_list()
    {
        return server_list;
    }
    
    @Override
    public boolean initialize()
    {
        return true;
    }
    
    
   
    // LOOKS ON ALL SERVERS
    public boolean download_file( String server_path, String local_path, boolean obfuscate)
    {
        if (server_list.isEmpty())
            return false;
        
        ServerEntry entry = server_list.get( actual_server_idx );
        
        boolean ok = download_file( entry , server_path, local_path, obfuscate);
        
        if (ok)
            return true;
        
        for (int i = 0; i < server_list.size() && !ok; i++)
        {
            actual_server_idx++;
            actual_server_idx = actual_server_idx % server_list.size();            

            entry = server_list.get( actual_server_idx );
            ok = download_file( entry , server_path, local_path, obfuscate);
        }
        
        return ok;
    }
    
    // LOOKS ON ALL SERVERS
    public GetMethod open_dl_stream( String server_path)
    {
        if (server_list.isEmpty())
            return null;
        
        ServerEntry entry = server_list.get( actual_server_idx );
        
        GetMethod ret = open_dl_stream( entry , server_path);
        
        if (ret != null)
            return ret;
        
        for (int i = 0; i < server_list.size() && ret == null; i++)
        {
            actual_server_idx++;
            actual_server_idx = actual_server_idx % server_list.size();            

            entry = server_list.get( actual_server_idx );
            ret = open_dl_stream( entry , server_path);
        }
        
        return ret;
    }
    
    
    // LOOKS ON ALL SERVERS
    public boolean upload_file( String server_path, String local_path, boolean obfuscate)
    {
        if (server_list.isEmpty())
            return false;
        
        ServerEntry entry = server_list.get( actual_server_idx );
        
        boolean ok = upload_file( entry , server_path, local_path, obfuscate);
        
        if (ok)
            return true;
        
        if (with_abort)
        {
            if (is_busy_aborted())
            {
                return false;
            }
        }
        
        for (int i = 0; i < server_list.size() && !ok; i++)
        {
            actual_server_idx++;
            actual_server_idx = actual_server_idx % server_list.size();            

            entry = server_list.get( actual_server_idx );
            ok = upload_file( entry , server_path, local_path, obfuscate);
            
            if (with_abort)
            {
                if (is_busy_aborted())
                {
                    return false;
                }
            }
        }
        
        return ok;
    }
    
    // LOOKS ON ALL SERVERS
    public boolean exists_file( String server_path)
    {
        if (server_list.isEmpty())
            return false;
        
        ServerEntry entry = server_list.get( actual_server_idx );
        
        boolean ok = exists_file( entry , server_path);
        
        if (ok)
            return true;
        
        for (int i = 0; i < server_list.size() && !ok; i++)
        {
            actual_server_idx++;
            actual_server_idx = actual_server_idx % server_list.size();            

            entry = server_list.get( actual_server_idx );
            ok = exists_file( entry , server_path);
        }
        
        return ok;
    }
    
    void check_open_busy(ServerEntry entry, String path, boolean upload)
    {
        if (!with_busy)
            return;
        
        String file = path;
        if (path.lastIndexOf("/") > 0)
        {
            file = path.substring( path.lastIndexOf("/") + 1);
        }
        
        activity_text = (upload ? "Uploading" : "Downloading") + " " + file + " " + (upload ? "to " : "from ") + entry.getHost();
        
        
        show_busy(parent, activity_text + ", connecting...", with_abort);
    }
    
    protected void check_close_busy()
    {
        hide_busy();        
    }
    
    protected boolean check_busy_aborted()
    {
        if (with_busy && with_abort)
            return is_busy_aborted();
        
        return false;
    }
    protected void show_busy(JDialog parent, String str, boolean abortable )
    {
    }
    protected void hide_busy()
    {
    }
    
    protected void check_update_busy( int percent)
    {
        if (with_busy)
            show_busy(parent, activity_text + " " + percent + "% done", with_abort);
    }
    protected void err_log( String txt )
    {
    }
    protected void debug_msg( int lvl, String txt )
    {
    }
    protected boolean is_busy_aborted()
    {
        return false;
    }
    
    
    boolean download_file( ServerEntry entry, String server_path, String local_path, boolean obfuscate)
    {
        
        check_open_busy(entry, local_path, false);
            
        HostConfiguration host_conf = new HostConfiguration();
        
        String host = entry.getHost();
        int port    = entry.getPort();
        String http_user = entry.getUser();
        String http_pwd = entry.getPwd();
        
        host_conf.setHost( host,  port );
        
                
        GetMethod get = new GetMethod(server_path);
        
        HttpClient http_client = new HttpClient();
        Credentials defaultcreds = new UsernamePasswordCredentials(http_user, http_pwd);
        http_client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), defaultcreds);

        // PROXY SETTINGS
        if (is_proxy_enabled)
        {            
            if (px_server == null || px_server.length() == 0 || px_port == 0)
            {
                err_log("Proxysettings are invalid! I will ignore these and try direct connect");
            }
            else
            {
                host_conf.setProxy( px_server, px_port );            
            }
        }
        
        int data_len = 0;
        try
        {
            debug_msg( 1, "Executing GET from <" + host + ":" + port + server_path + " to " + local_path );
            
            http_client.executeMethod(host_conf, get );
            
            int rcode = get.getStatusCode();
            StatusLine sl = get.getStatusLine();
            
            if (rcode < 200 || rcode > 210)
            {                
                // SERVER ANSWERS WITH ERROR
                last_errortext = get.getStatusText();
                return false;
            }
            response_hlist = get.getResponseHeaders();

            long len = 0;
            
            int last_percent = 0;
            
            try
            {
                String cont_len_str = get_resonse_header("Content-Length");
                len = Long.parseLong(cont_len_str);
            }
            catch ( NumberFormatException numberFormatException )
            {
            }
            
            InputStream istr = get.getResponseBodyAsStream();
                        
            BufferedInputStream bis = new BufferedInputStream( istr );
            FileOutputStream fw = new FileOutputStream( local_path );
            
            int bs = 4096;
            byte[] buffer = new byte[bs];
            
            
            while (true)
            {
                int rlen = bis.read( buffer );
                
                data_len += rlen;
                
                if (rlen == -1)
                    break;
                if (obfuscate)
                {
                    for (int i = 0; i < rlen; i++)
                    {
                        buffer[i] = (byte)~buffer[i];   // KOMPLEMENT: BILLIG UND GUT
                    }
                }
                fw.write( buffer,0, rlen );   
                
                if (with_busy && len > 0)
                {
                    int percent = (int)((100 * data_len) / len);
                    if (percent > last_percent + 1)
                    {
                        last_percent = percent;
                        check_update_busy( percent );
                    }
                    
                    if (check_busy_aborted())
                    {
                        throw new Exception("Aborted by user");
                    }
                }
                
                                            
            }
            fw.close();
            bis.close();
            
        }
        catch ( Exception exc )
        {
            err_log("Download of " + server_path + " from " +  host + " failed: " + exc.getMessage() );      
            last_errortext = exc.getMessage();
            return false;
        }
        finally
        {
            check_close_busy();
        }

        
        
        if (data_len == 0)
            return false;
        
        File f = new File( local_path );
        if (f.exists())           
            return true;
        
        return false;
    }

    GetMethod open_dl_stream( ServerEntry entry, String server_path)
    {
        HostConfiguration host_conf = new HostConfiguration();
        
        String host = entry.getHost();
        int port    = entry.getPort();
        String http_user = entry.getUser();
        String http_pwd = entry.getPwd();
        
        host_conf.setHost( host,  port );
        
                
        GetMethod get = new GetMethod(server_path);
        
        HttpClient http_client = new HttpClient();
        Credentials defaultcreds = new UsernamePasswordCredentials(http_user, http_pwd);
        http_client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), defaultcreds);

        // PROXY SETTINGS
        if (is_proxy_enabled)
        {
            if (px_server == null || px_server.length() == 0 || px_port == 0)
            {
                err_log("Proxysettings are invalid! I will ignore these and try direct connect");
            }
            else
            {
                host_conf.setProxy( px_server, px_port );            
            }
        }
        
        
        try
        {
            debug_msg( 1, "Executing GET from <" + host + ":" + port + server_path + " to local stream" );
            
            http_client.executeMethod(host_conf, get );
            
            int rcode = get.getStatusCode();
            //StatusLine sl = get.getStatusLine();
            
            if (rcode < 200 || rcode > 210)
            {                
                // SERVER ANSWERS WITH ERROR
                last_errortext = get.getStatusText();
                return null;
            }
            response_hlist = get.getResponseHeaders();
            
            
            
            return get;
            
        }
        catch ( Exception exc )
        {
            err_log("Download of " + server_path + " from " +  host + " failed: " + exc.getMessage() );      
            last_errortext = exc.getMessage();
        }
        return null;
    }
    
    
    boolean upload_file( ServerEntry entry, String server_path, String local_path, boolean obfuscate)
    {
        check_open_busy( entry, local_path, true );
        
        final HostConfiguration host_conf = new HostConfiguration();
        
        String host = entry.getHost();
        int port    = entry.getPort();
        String http_user = entry.getUser();
        String http_pwd = entry.getPwd();
        
        host_conf.setHost( host,  port );
        
        final HttpClient http_client = new HttpClient();
        Credentials defaultcreds = new UsernamePasswordCredentials(http_user, http_pwd);
        http_client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), defaultcreds);        

        // PROXY SETTINGS
        if (is_proxy_enabled)
        {
            
            if (px_server == null || px_server.length() == 0 || px_port == 0)
            {
                err_log("Proxysettings are invalid! I will ignore these and try direct connect");
            }
            else
            {
                host_conf.setProxy( px_server, px_port );            
            }
        }
               
        InputStream fis = null;
        try
        {
            debug_msg( 1, "Executing POST " + local_path + " to <" + host + ":" + port + "/" + server_path + ">" );
            if (with_busy)
            {
                show_busy( parent, activity_text + ", transfering...", with_abort);
            }
            

            // SEND WITH HTTP-PUT
            final PutMethod put = new PutMethod(server_path);
            
            // HANDLE ENCRYPTION
            fis = new FileInputStream(local_path);
            if (obfuscate)
            {
                Deobfuscator obf = new Deobfuscator(local_path);
                fis = obf;
            }
            fis = new BufferedInputStream( fis );
                
            
 /*           long len = new File(local_path).length();
            InputStreamRequestEntity isre = new InputStreamRequestEntity( fis, len );
            
            put.setRequestEntity(isre);
            */
            // Deprcated BUT ABOVE DOES NOT WORK
            put.setRequestBody(fis);
            
            put.setDoAuthentication(true); 
                        
            // SEND IN THREAD, THEN WE CAN ABORT USING GUI
            BackgroundWorker sw = new BackgroundWorker(NAME)
            {

                @Override
                public Object construct()
                {
                    try
                    {
                        int stat = http_client.executeMethod(host_conf, put);
                        return new Integer(stat);
                    }
                    catch ( Exception ex )
                    {
                        return new Integer(-1);
                    }
                    
                }
            };
            sw.start();
            
            // DO IN LOOP
            boolean do_abort = false;
            
            while (!sw.finished())
            {
                Thread.sleep(100);
                if (with_abort)
                {
                    if (is_busy_aborted())
                    {
                        put.abort();
                        do_abort = true;                       
                    }
                }                
            }
            sw.get();
                        
            if (do_abort)
            {
                last_errortext = "Abort";
                return false;
            }
            int rcode = put.getStatusCode();                 

            if (rcode < 200 || rcode > 210)
            {                
                // SERVER ANSWERS WITH ERROR   
                last_errortext = put.getStatusText();
                return false;
            }                        
        }
        catch ( Exception exc )
        {
            LogManager.printStackTrace(exc);
            err_log("Upload of " + server_path + " to " +  host + " failed: " + exc.getMessage() ); 
            last_errortext = exc.getMessage();
            return false;
        }
        finally
        {
            // CLEAN UP
            try
            {
                fis.close();
            }
            catch ( Exception e )
            {
            }

            check_close_busy();
        }

        
        
        return true;
    }
    

    boolean exists_file( ServerEntry entry, String server_path )
    {
        HostConfiguration host_conf = new HostConfiguration();
        
        String host = entry.getHost();
        int port    = entry.getPort();
        String http_user = entry.getUser();
        String http_pwd = entry.getPwd();
        
        host_conf.setHost( host,  port );
        
                
        HeadMethod get = new HeadMethod(server_path);
        
        HttpClient http_client = new HttpClient();
        Credentials defaultcreds = new UsernamePasswordCredentials(http_user, http_pwd);
        http_client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), defaultcreds);

        // PROXY SETTINGS
        if (is_proxy_enabled)
        {
            if (px_server == null || px_server.length() == 0 || px_port == 0)
            {
                err_log("Proxysettings are invalid! I will ignore these and try direct connect");
            }
            else
            {
                host_conf.setProxy( px_server, px_port );            
            }
        }
        
       
        try
        {
            debug_msg( 1, "Executing HEADER from <" + host + ":" + port + server_path );
            
            http_client.executeMethod(host_conf, get );
            
            response_hlist = get.getResponseHeaders();
            int rcode = get.getStatusCode();
            
            
            if (rcode < 200 || rcode > 210)
            {                
                // SERVER ANSWERS WITH ERROR
                last_errortext = get.getStatusText();
                return false;
            }
            return true;
        }
        catch ( Exception exc )
        {
            err_log("Existance check of " + server_path + " from " +  host + " failed: " + exc.getMessage() );       
            last_errortext = exc.getMessage();
        }
        
        return false;
    }

    
    boolean delete_file( ServerEntry entry, String server_path )
    {
        HostConfiguration host_conf = new HostConfiguration();
        
        String host = entry.getHost();
        int port    = entry.getPort();
        String http_user = entry.getUser();
        String http_pwd = entry.getPwd();
        
        host_conf.setHost( host,  port );
        
                
        DeleteMethod get = new DeleteMethod(server_path);
        
        HttpClient http_client = new HttpClient();
        Credentials defaultcreds = new UsernamePasswordCredentials(http_user, http_pwd);
        http_client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), defaultcreds);

        // PROXY SETTINGS
        if (is_proxy_enabled)
        {
            
            if (px_server == null || px_server.length() == 0 || px_port == 0)
            {
                err_log("Proxysettings are invalid! I will ignore these and try direct connect");
            }
            else
            {
                host_conf.setProxy( px_server, px_port );            
            }
        }
        
       
        try
        {
            debug_msg( 1, "Executing DELETE from <" + host + ":" + port + server_path );
            
            http_client.executeMethod(host_conf, get );
            
            response_hlist = get.getResponseHeaders();
            int rcode = get.getStatusCode();
            
            
            if (rcode < 200 || rcode > 210)
            {                
                // SERVER ANSWERS WITH ERROR
                last_errortext = get.getStatusText();
                return false;
            }
            return true;
        }
        catch ( Exception exc )
        {
            err_log("Delete of " + server_path + " from " +  host + " failed: " + exc.getMessage() );       
            last_errortext = exc.getMessage();
        }
        
        return false;
    }
    
            
    // LOOKS ON ALL SERVERS!!
    public boolean delete_file( String server_path)
    {
        if (server_list.isEmpty())
            return false;
        
        boolean deleted_one = false;

        // DELETE ON ALL SERVERS!!
        ServerEntry entry = server_list.get( actual_server_idx );
        
        boolean ok = delete_file( entry , server_path);
        if (ok)
        {
            debug_msg(1, "Deleted " + server_path + " on " +  entry.getHost() + " successfully" );      
            deleted_one = true;
        }
        
        for (int i = 0; i < server_list.size() && !ok; i++)
        {
            actual_server_idx++;
            actual_server_idx = actual_server_idx % server_list.size();            

            entry = server_list.get( actual_server_idx );
            ok = delete_file( entry , server_path);
            if (ok)
            {
                debug_msg(1, "Deleted " + server_path + " on " +  entry.getHost() + " successfully" );      
                deleted_one = true;
            }
        }
        
        return deleted_one;
    }
            
    
    
    @Override
    public boolean check_requirements(StringBuffer sb)
    {
        boolean ok = true;
        
        if (server_list.isEmpty())
        {
            sb.append("Server list is empty" );
            ok = false;
        }
        return ok;        
        
    }

    @Override
    public void run()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }



    @Override
    public String get_task_status()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    
}
