/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin;


import com.vaadin.Application;
import com.vaadin.ui.Component;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.VerticalLayout;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.vaadin.GuiElems.BackupEditor;
import de.dimm.vsm.vaadin.auth.AppHeader;
import de.dimm.vsm.vaadin.GuiElems.ExitElem;
import de.dimm.vsm.vaadin.GuiElems.GuiElemActionCallback;
import de.dimm.vsm.vaadin.GuiElems.LoginElem;
import de.dimm.vsm.vaadin.GuiElems.PoolEditor;
import de.dimm.vsm.vaadin.GuiElems.SidebarButton;
import de.dimm.vsm.vaadin.GuiElems.SidebarButtonCallback;
import de.dimm.vsm.vaadin.GuiElems.StatusWin;
import de.dimm.vsm.vaadin.auth.User;
import javax.persistence.EntityManager;

/**
 *
 * @author Administrator
 */
public class VSMCMain extends GenericMain
{
    MenuItem menuItem;
    Application app;
    AppHeader header;
    LoginElem loginElem;
    boolean loggedIn;

    Component poolEditor;
    Component backupEditor;
    StatusWin statusWin;

    SidebarButton btstatus;




    public VSMCMain(Application _app)
    {
        super("VSMCClient");
        
        app = _app;

        root.setSizeFull();
        root.setStyleName("vsm");
        
        setupComponents();

        createHeader();
    }

    public boolean isLoggedIn()
    {
        return loggedIn;
    }

    public LoginElem getLoginElem()
    {
        return loginElem;
    }

    public final void setupComponents()
    {
        menuItem = getMenuBar().addItem(Txt("Datei"), null);
        
        loginElem = new LoginElem(this);
        GuiElemActionCallback cb = new GuiElemActionCallback()
        {
            @Override
            public void actionFinished( boolean ok )
            {
                handleLogin( ok );
            }
        };
        loginElem.setCallback(cb);
        loginElem.attachTo(menuItem);
        

        menuItem.addSeparator();
        ExitElem exitElem = new ExitElem(this);
        exitElem.attachTo(menuItem);

       

        final VerticalLayout sidebar = new VerticalLayout();
        sidebar.setSpacing(true);
        sidebar.setStyleName("sidebar");

        btstatus = new SidebarButton(Txt("Status"), new SidebarButtonCallback()
        {
            @Override
            public void action()
            {
                showStatus();
            }
        });
        SidebarButton bt1 = new SidebarButton(Txt("StoragePools"), new SidebarButtonCallback()
        {
            @Override
            public void action()
            {
                showStoragePools();
            }
        });
        SidebarButton bt2 = new SidebarButton(Txt("Schedules"), new SidebarButtonCallback()
        {
            @Override
            public void action()
            {
                showBackups();
            }
        });

        SidebarButton bthilfe = new SidebarButton(Txt("Hilfe"), new SidebarButtonCallback()
        {
            @Override
            public void action()
            {

            }
        });

        sidebar.addComponent(btstatus);
        sidebar.addComponent(bt1);
        sidebar.addComponent(bt2);
        sidebar.addComponent(bthilfe);

        setSideComponent(sidebar, 120);
       
    }

    @Override
    public void exitApp()
    {
        header.resetUser();
        app.close();
        
    }
    public void handleLogin( boolean ok )
    {
        loggedIn = ok;
        if (ok)
        {
            header.setUser( new User("Mark Williams" ) );

            btstatus.setSelected();
            btstatus.getCallback().action();
        }
        else
        {
            header.resetUser();
            app.close();
        }
    }

    private void createHeader()
    {
        setAppIcon("appicon.png");
        header = new AppHeader(this);
        setAppHeader(header.getGui());
    }
    

    private void showStoragePools()
    {
        if (poolEditor == null)
            poolEditor = new PoolEditor(this);

        setMainComponent(poolEditor );
    }

    private void showBackups()
    {
        if (backupEditor == null)
            backupEditor = new BackupEditor(this);

        setMainComponent(backupEditor );
    }
    private void showStatus()
    {
        if (statusWin == null)
            statusWin = new StatusWin(this);

        setMainComponent(statusWin);
        

    }
    
    EntityManager em = null;
    public EntityManager get_em()
    {
        if (em == null)
            em = LogicControl.get_emf().createEntityManager();
        return em;
    }
}
