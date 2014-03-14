/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.GuiLoginApi;
import java.net.InetAddress;
import java.net.MalformedURLException;


/**
 *
 * @author Administrator
 */
public class LoginApiDispatcher extends GenericApiDispatcher
{    
    private static int connTimeout = 5000;
    private static int txTimeout = 60*1000;

    public LoginApiDispatcher(  boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        super(ssl, keystore, keypwd, tcp);
    }

    public static void setConnTimeout( int connTimeout )
    {
        LoginApiDispatcher.connTimeout = connTimeout;
    }

    public static void setTxTimeout( int txTimeout )
    {
        LoginApiDispatcher.txTimeout = txTimeout;
    }
    
    

    private static LoginApiEntry generate_api( InetAddress addr, int port, boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        return generate_api(addr, port, ssl, "ui", keystore, keypwd, tcp);
    }

    private static LoginApiEntry generate_api( InetAddress addr, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        LoginApiEntry uiApiEntry = null;

        try
        {
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, path, ssl, tcp, connTimeout, txTimeout);

            GuiLoginApi api = (GuiLoginApi) factory.create(GuiLoginApi.class);

            uiApiEntry = new LoginApiEntry( api, factory );
        }
        catch (MalformedURLException malformedURLException)
        {
            Log.err("Fehler in Addresse", malformedURLException);
        }

        return uiApiEntry;
    }

    public LoginApiEntry get_api( InetAddress addr, int port, boolean withMsg )
    {        
        LoginApiEntry agentApiEntry = generate_api(addr, port, ssl, keystore, keypwd, tcp);
        agentApiEntry.check_online(withMsg);        
        return agentApiEntry;
    }
}
