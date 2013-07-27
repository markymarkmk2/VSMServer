/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import de.dimm.vsm.net.RemoteCallFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public abstract class AbstractApiEntry
{
    protected boolean online;
    protected RemoteCallFactory factory;
    protected Properties agentProps;

    public AbstractApiEntry( RemoteCallFactory factory )
    {
        this.factory = factory;
    }
    
        public RemoteCallFactory getFactory()
    {
        return factory;
    }
        
    public int getPort()
    {
        return factory.getPort();
    }

 public InetAddress getAddr()
    {
        return factory.getAdress();
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
        

    public abstract boolean check_online(boolean mithMsg);
    
    public boolean hasBooleanOption( String opt )
    {
        if (agentProps == null)
        {
            if (!check_online(false))
                return false;
        }
        String v = agentProps.getProperty(opt, Boolean.FALSE.toString());
        return v.equals(Boolean.TRUE.toString());
    }
}
