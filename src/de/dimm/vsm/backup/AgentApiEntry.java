/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.AgentApi;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

/**
 *
 * @author Administrator
 */
public class AgentApiEntry
{
    private AgentApi api;
    private boolean online;
    RemoteCallFactory factory;

    public AgentApiEntry( AgentApi api, RemoteCallFactory factory )
    {
        this.api = api;
        this.factory = factory;
    }
    
    public RemoteCallFactory getFactory()
    {
        return factory;
    }
    

    public AgentApi getApi()
    {
        return api;
    }

    public InetAddress getAddr()
    {
        return factory.getAdress();
    }

    public int getPort()
    {
        return factory.getPort();
    }

    public boolean isOnline()
    {
        return online;
    }
    public void resetSocket() throws IOException
    {
        factory.resetSocket();
    }
    public void close() throws IOException
    {
        factory.close();
    }

    public boolean check_online(boolean mithMsg)
    {
        try
        {
            api.get_properties();
            online = true;
            return true;
        }
        catch (Exception e)
        {
            if (!(e instanceof ConnectException))
            {
                try
                {
                    factory.resetSocket();
                    api.get_properties();
                    online = true;
                    return true;
                }
                catch (Exception e2)
                {
                    if (mithMsg)
                    {
                        if (e2 instanceof ConnectException)
                            Log.warn("Adresse kann nicht kontaktiert werden", "%s:%d", getAddr().toString(), getPort());
                        else if (e2 instanceof SocketTimeoutException)
                            Log.warn("Adresse kann nicht kontaktiert werden", "%s:%d", getAddr().toString(), getPort());
                        else
                            Log.warn("Adresse kann nicht kontaktiert werden", getAddr().toString() + ":" + getPort(), e2);
                    }
                }
            }
        }
        online = false;
        return false;
    }
}