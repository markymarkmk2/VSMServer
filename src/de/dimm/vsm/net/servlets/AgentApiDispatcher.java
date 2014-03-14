/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.AgentApi;
import java.net.InetAddress;
import java.net.MalformedURLException;


/**
 *
 * @author Administrator
 */
public class AgentApiDispatcher extends GenericApiDispatcher
{
    private static int connTimeout = 5000;
    private static int txTimeout = 60*1000;

    public AgentApiDispatcher(  boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        super(ssl, keystore, keypwd, tcp);
    }

    public static void setConnTimeout( int connTimeout )
    {
        AgentApiDispatcher.connTimeout = connTimeout;
    }

    public static void setTxTimeout( int txTimeout )
    {
        AgentApiDispatcher.txTimeout = txTimeout;
    }

    private static AgentApiEntry generate_api( InetAddress addr, int port, boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        return generate_api(addr, port, ssl, "net", keystore, keypwd, tcp);
    }

    private static AgentApiEntry generate_api( InetAddress addr, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        AgentApiEntry agentApiEntry = null;

        try
        {
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, path, ssl, tcp, connTimeout, txTimeout);
            AgentApi api = (AgentApi) factory.create(AgentApi.class);
            agentApiEntry = new AgentApiEntry( api, factory );
        }
        catch (MalformedURLException malformedURLException)
        {
            Log.err("Fehler in Addresse", malformedURLException);
        }

        return agentApiEntry;
    }

    public AgentApiEntry get_api( InetAddress addr, int port, boolean withMsg )
    {
        AgentApiEntry agentApiEntry = generate_api(addr, port, ssl, keystore, keypwd, tcp);
        agentApiEntry.check_online(withMsg);        
        return agentApiEntry;
    }
}
