/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import static de.dimm.vsm.LogicControl.getKeyPwd;
import static de.dimm.vsm.LogicControl.getKeyStore;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import java.io.IOException;
import java.util.Date;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerContext
{
    StoragePoolHandler handler;
    String drive;
    String agentIp;
    int port;
    Date lastUsage;
    NetServer webDavServer;

    public StoragePoolHandlerContext( StoragePoolHandler handler, String drive, String agentIp, int port )
    {
        this.handler = handler;
        this.drive = drive;
        this.agentIp = agentIp;
        this.port = port;
        lastUsage = new Date();
    }
    public void touch()
    {
        lastUsage = new Date();
    }
    public boolean isExpired()
    {
        return (System.currentTimeMillis() - lastUsage.getTime() > StoragePoolHandlerContextManager.EXPIRE_MS);
    }
    void closePoolHandler()
    {
        handler.close_entitymanager();
        if (webDavServer != null) {
            webDavServer.stop_server();
        }
    }

    long getTs()
    {
        return lastUsage.getTime();
    }

    NetServer createWebDavServer( StoragePoolWrapper wrapper, long loginIdx, long wrapperIdx, int webDavPort ) throws IOException {
        
        // Aus dem User das Logintoken holen -> das ist unser WebDavToken: eindeutig je User 
        User usr = Main.get_control().getLoginManager().getUser(loginIdx);              
        wrapper.setWebDavToken(usr.getLoginToken());
        
        
        webDavServer = new NetServer();
        DefaultServlet servlet = new DefaultServlet();
        ServletHolder htmlSH = new ServletHolder(servlet);
        webDavServer.addServletHolder("*", htmlSH);
        ServletContextHandler context = webDavServer.getContext();
        FilterHolder fh = new FilterHolder(new VSMWebDavFilter(loginIdx, wrapperIdx, /*isSearch*/ false));        
        
        context.addFilter(fh, "/" + usr.getLoginToken() + "/*", null);
                
        if (webDavServer.start_server(webDavPort, false, getKeyStore(), getKeyPwd())) {
            return webDavServer;
        }
        webDavServer.stop_server();
        return null;
    }
}