/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.Application;
import com.vaadin.ui.Window;
import com.vaadin.ui.Window.Notification;
import de.dimm.vsm.vaadin.GenericMain;
import de.dimm.vsm.vaadin.VSMCLoginForm;
import de.dimm.vsm.vaadin.VSMCLoginForm.LoginListener;
import de.dimm.vsm.vaadin.VSMCLoginForm.VSMCLoginEvent;
import de.dimm.vsm.vaadin.VSMCMain;
import org.vaadin.jouni.animator.Animator;
import org.vaadin.jouni.animator.client.ui.VAnimatorProxy.AnimType;

/**
 *
 * @author Administrator
 */
public class LoginElem extends GUIElem
{
    VSMCMain main;
    public LoginElem(VSMCMain m)
    {
        super(m);
        main = m;
    }

    @Override
    String getItemText()
    {
        if (main.isLoggedIn())
            return Txt("Logout");
        else
            return Txt("Login");
    }

    @Override
    void action()
    {
        if (main.isLoggedIn())
        {
            main.handleLogin(false);
            updateGui();
            return;
        }

        VSMCLoginForm df = new VSMCLoginForm();
        df.setWidth("100%");
        final Window myw = new Window(Txt("Login"));
        final Application app = parentWin.getRoot().getApplication();
        myw.setWidth("350px");
        df.addListener( new LoginListener()
        {

            @Override
            public void onLogin(VSMCLoginEvent event)
            {
                // TODO Auto-generated method stub
                String name = event.getLoginParameter("username");
                String pwd =  event.getLoginParameter("password");

                boolean success = false;
                if (name.equals(pwd))
                {
                    app.getMainWindow().removeWindow(myw);
                    
                    main.handleLogin(true);
                    updateGui();
                }
                else
                {
                    parentWin.getRoot().getWindow().showNotification(Txt("LoginFailed"),Txt("TryAgain"), Notification.TYPE_WARNING_MESSAGE);
                }
            }
        });
     // TODO Auto-generated method stub

        myw.setModal(true);
        myw.addComponent(df);


        app.getMainWindow().addWindow(myw);

        main.getAnimatorProxy().animate(myw, AnimType.FADE_IN).setDuration(300);


    }

}
