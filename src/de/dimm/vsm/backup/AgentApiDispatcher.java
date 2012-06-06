/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.backup;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.AgentApi;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author Administrator
 */
public class AgentApiDispatcher
{

    List<AgentApiEntry> apiList;
    boolean ssl;
    String keystore;
    String keypwd;
    boolean tcp;
    public static final boolean API_CONNECT_CACHE = false;  // EVERY CALL OPENS A NEW CONNECTION; AGENT IS MULTITASKING

    public AgentApiDispatcher(  boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        this.ssl = ssl;
        this.keystore = keystore;
        this.keypwd = keypwd;
        this.tcp = tcp;

        apiList = new ArrayList<AgentApiEntry>();
    }


    public boolean isSsl()
    {
        return ssl;
    }

    private static AgentApiEntry generate_api( InetAddress addr, int port, boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        return generate_api(addr, port, ssl, "net", keystore, keypwd, tcp);
    }

    private static AgentApiEntry generate_api( InetAddress addr, int port, boolean ssl, String path, String keystore, String keypwd, boolean tcp )
    {
        AgentApiEntry agentApiEntry = null;
        System.setProperty("javax.net.ssl.trustStore", keystore);

        try
        {
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, path, ssl, tcp);

            AgentApi api = (AgentApi) factory.create(AgentApi.class);

            agentApiEntry = new AgentApiEntry( api, factory );


//            InetSocketAddress saddr = new InetSocketAddress(addr, port);
//            try
//            {
//                factory.getSock().connect(saddr, 3000);
//                factory.getSock().close();
//            }
//            catch (IOException e)
//            {
//                System.out.println("Connect failed: " + e.getMessage());
//            }
        }
        catch (MalformedURLException malformedURLException)
        {
            Log.err("Fehler in Addresse", malformedURLException);
        }

        return agentApiEntry;
    }

    public AgentApiEntry get_api( InetAddress addr, int port )
    {
        if (API_CONNECT_CACHE)
        {
            for (int i = 0; i < apiList.size(); i++)
            {
                AgentApiEntry agentApiEntry = apiList.get(i);
                if (agentApiEntry.getAddr().equals(addr) && agentApiEntry.getPort() == port)
                {
                    agentApiEntry.check_online();
                    return agentApiEntry;
                }
            }
        }
        AgentApiEntry agentApiEntry = generate_api(addr, port, ssl, keystore, keypwd, tcp);

        agentApiEntry.check_online();
        
        if (API_CONNECT_CACHE)
        {
            apiList.add(agentApiEntry);
        }

        return agentApiEntry;
    }
}
