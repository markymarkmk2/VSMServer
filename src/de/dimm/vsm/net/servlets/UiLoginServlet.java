/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import com.caucho.hessian.server.HessianServlet;
import de.dimm.vsm.Main;
import de.dimm.vsm.net.GuiWrapper;
import de.dimm.vsm.net.interfaces.GuiLoginApi;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class UiLoginServlet extends HessianServlet implements GuiLoginApi
{
    public class UiWrapper
    {
        long id;
    }

    @Override
    public GuiWrapper login( String user, String pwd )
    {
        return Main.get_control().getLoginManager().login(user, pwd);
    }

    @Override
    public GuiWrapper relogin( GuiWrapper wrapper, String user, String pwd )
    {
        return Main.get_control().getLoginManager().relogin(wrapper, user, pwd);
    }

    @Override
    public boolean logout( GuiWrapper wrapper )
    {
        return Main.get_control().getLoginManager().logout(wrapper);
    }

    @Override
    public GuiServerApi getDummyGuiServerApi()
    {
        return Main.get_control().getLoginManager().getDummyGuiServerApi();
    }

    @Override
    public boolean isStillValid( GuiWrapper wrapper )
    {
        return Main.get_control().getLoginManager().isStillValid(wrapper);
    }  

    @Override
    public Properties getProperties()
    {
        return Main.get_control().getLoginManager().getProperties();
    }
    
    
}
