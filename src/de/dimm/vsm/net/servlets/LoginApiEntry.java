/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net.servlets;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteCallFactory;
import de.dimm.vsm.net.interfaces.GuiLoginApi;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class LoginApiEntry extends AbstractApiEntry
{
    private GuiLoginApi api;

    public LoginApiEntry( GuiLoginApi api, RemoteCallFactory factory )
    {
        super(factory);
        this.api = api;
    }
    

    public GuiLoginApi getApi()
    {
        return api;
    }
   
    @Override
    public boolean check_online(boolean mithMsg)
    {
        try
        {
            agentProps = api.getProperties();
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
                    api.getProperties();
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