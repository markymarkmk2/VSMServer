/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.auth;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import de.dimm.vsm.vaadin.VSMCMain;

/**
 *
 * @author Administrator
 */
public class AppHeader
{
    VSMCMain main;
    User actUser;
    Label lbUser;
    Label lbLastLogin;
    Label lbUserMode;
    //Label lbEmpty = new Label("");
    VerticalLayout gui;

    public AppHeader( VSMCMain main )
    {
        this.main = main;

        gui = new VerticalLayout();
        gui.setWidth("100%");
        gui.setHeight("100px");
        gui.setMargin(false);

        Button bt = main.getLoginElem().createButton();
        gui.addComponent(bt);
        gui.setComponentAlignment(bt, Alignment.TOP_RIGHT);

        lbUser = new Label("");
        lbUser.setContentMode(Label.CONTENT_XHTML);
        lbUser.setWidth(null);
        lbLastLogin = new Label("");
        lbLastLogin.setContentMode(Label.CONTENT_XHTML);
        lbLastLogin.setWidth(null);
        lbUserMode = new Label("");
        lbUserMode.setContentMode(Label.CONTENT_XHTML);
        lbUserMode.setWidth(null);

        gui.addComponent(lbUser);
        gui.addComponent(lbLastLogin);
        gui.addComponent(lbUserMode);
        gui.setComponentAlignment(lbUser, Alignment.TOP_RIGHT);
        gui.setComponentAlignment(lbLastLogin, Alignment.TOP_RIGHT);
        gui.setComponentAlignment(lbUserMode, Alignment.TOP_RIGHT);

        main.setAppHeader(gui);
    }
    public void setUser( User u )
    {
        actUser = u;
        lbUser.setValue("Willkommen <b>" + actUser.toString() + "</b> ");
        lbLastLogin.setValue("<small>Letzter Login: 08.08.2011 um 13:23</small> " );
        lbUserMode.setValue("<small>Superuser</small> ");

        gui.requestRepaint();
    }
    public void resetUser()
    {
        actUser = null;
        lbUser.setValue("");
        lbLastLogin.setValue("");
        lbUserMode.setValue("");

        gui.requestRepaint();
    }
    public Component getGui()
    {
        return gui;
    }
    

}
