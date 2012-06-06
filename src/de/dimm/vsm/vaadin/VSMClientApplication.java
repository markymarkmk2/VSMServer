package de.dimm.vsm.vaadin;

import com.vaadin.Application;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;

import eu.livotov.tpt.TPTApplication;

public class VSMClientApplication extends TPTApplication implements ClickListener 
{
    private static final long serialVersionUID = 1L;


    boolean isLoggedin;
    VSMCMain main;
    
    @Override
    public void applicationInit() 
    {

        VSMCinit();
        /*
        final Window mainWindow = new Window("VSMClient");
        
        

        mainWindow.addComponent( new GenericMain() );
        setMainWindow(mainWindow);
        setTheme("vsm");
        TM.getDictionary ().setDefaultLanguage ( "de" );

 File themeFolder = new File ( getContext ().getBaseDirectory (),
                String.format ( "VAADIN/themes/%s", getTheme () ) );

        System.out.println(themeFolder.getAbsolutePath());
*/

    }
    void VSMCinit()
    {
        setTheme("vsm");
       
        main = new VSMCMain(this);
        
        setMainWindow(main.getRoot());

    }

    @Override
    public void firstApplicationStartup()
    {
        // TODO Auto-generated method stub
        System.out.println("firstApplicationStartup");
        
    }

    @Override
    public void transactionStart( Application application, Object o )
    {
        super.transactionStart(application, o);
        System.out.println("transactionStart");
    }

    @Override
    public void transactionEnd( Application application, Object o )
    {
        super.transactionEnd(application, o);
        System.out.println("transactionEnd");
    }


    @Override
    public void buttonClick(ClickEvent event)
    {
        // TODO Auto-generated method stub
        
    }    
}
