/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import java.net.InetAddress;
import java.net.MalformedURLException;


/**
 *
 * @author Administrator
 */
public class GuiApiDispatcher extends GenericApiDispatcher
{    
    private static int connTimeout = 5000;
    private static int txTimeout = 60*1000;

    public static void setConnTimeout( int connTimeout )
    {
        GuiApiDispatcher.connTimeout = connTimeout;
    }

    public static void setTxTimeout( int txTimeout )
    {
        GuiApiDispatcher.txTimeout = txTimeout;
    }
    
    public GuiApiDispatcher(  boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        super(ssl, keystore, keypwd, tcp);
    }

    private static GuiApiEntry generate_api( InetAddress addr, int port, boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        return generate_api(addr, port, ssl, "ui", keystore, keypwd, tcp);
    }

    private static GuiApiEntry generate_api( InetAddress addr, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        GuiApiEntry uiApiEntry = null;

        try
        {
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, path, ssl, tcp, connTimeout, txTimeout);
            GuiServerApi api = (GuiServerApi) factory.create(GuiServerApi.class);
            uiApiEntry = new GuiApiEntry( api, factory );
        }
        catch (MalformedURLException malformedURLException)
        {
            Log.err("Fehler in Addresse", malformedURLException);
        }

        return uiApiEntry;
    }

    public GuiApiEntry get_api( InetAddress addr, int port, boolean withMsg )
    {        
        GuiApiEntry agentApiEntry = generate_api(addr, port, ssl, keystore, keypwd, tcp);
        agentApiEntry.check_online(withMsg);        
        return agentApiEntry;
    }
}
