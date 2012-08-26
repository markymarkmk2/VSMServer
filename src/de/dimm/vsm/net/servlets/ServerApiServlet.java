/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net.servlets;

import com.caucho.hessian.server.HessianServlet;
import de.dimm.vsm.net.CdpEvent;
import de.dimm.vsm.net.CdpTicket;
import de.dimm.vsm.net.ServerApiImpl;
import de.dimm.vsm.net.interfaces.ServerApi;
import java.util.List;
import java.util.Properties;




/**
 *
 * @author Administrator
 */
public class ServerApiServlet extends HessianServlet implements ServerApi
{
    ServerApiImpl api;

    public ServerApiServlet() throws Exception
    {
        api = new ServerApiImpl();
    }


    @Override
    public boolean alert( String reason, String msg )
    {
        return api.alert(reason, msg);
    }

    @Override
    public boolean alert( List<String> reason, String msg )
    {
        return api.alert(reason, msg);
    }

    @Override
    public Properties get_properties()
    {
        return api.get_properties();
    }

    @Override
    public boolean cdp_call( CdpEvent ev, CdpTicket ticket )
    {
        return api.cdp_call(ev, ticket);
    }
    @Override
    public boolean cdp_call( List<CdpEvent> evList, CdpTicket ticket )
    {
        return api.cdp_call(evList, ticket);
    }
  
}
