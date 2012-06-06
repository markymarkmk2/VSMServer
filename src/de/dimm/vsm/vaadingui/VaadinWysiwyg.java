/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vaadingui;

import com.vaadin.terminal.gwt.server.ApplicationServlet;
import de.dimm.vsm.log.Log;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.eclipse.jetty.servlet.ServletHolder;



class VaadinInterfaceServlet implements Servlet
{
    static URL[] getGuiUrls()
    {
        try
        {
            ArrayList<URL> l = new ArrayList<URL>();

            File libDir = new File("dist\\lib");
            if (!libDir.exists())
                libDir = new File("lib");

            if (!libDir.exists())
            {
                System.out.println("Cannot load GUI URLS");
                return null;
            }
            File[] libs = libDir.listFiles();
            for (int i = 0; i < libs.length; i++)
            {
                File file1 = libs[i];
                if (!file1.getName().endsWith(".jar"))
                    continue;

                URL u = file1.toURI().toURL();//new URL("file", "localhost", file.getAbsolutePath());

                l.add(u);
            }
            return l.toArray( new URL[0]);
        }
        catch (MalformedURLException malformedURLException)
        {
        }
        return null;
    }
    
    ApplicationServlet app;


    VaadinInterfaceServlet()
    {
        try
        {
            ClassLoader cl = getClassLoader();
            Class acl = cl.loadClass("de.dimm.vsm.vaadin.AppServlet");
            
            Class main = cl.loadClass("de.dimm.vsm.vaadin.VSMCMain");
           
            app = (ApplicationServlet) acl.newInstance();

            String v = main.getDeclaredMethod("getVersion", (Class[])null).invoke(null, (Object[])null).toString();

            System.out.println("GUI Servlet Version V" + v);
        }
        catch (Exception servletException)
        {
            Log.err("GUI-Servlet kann nicht geladen werden", servletException);
        }        
    }

  
    protected final ClassLoader getClassLoader() throws ServletException
    {
        ClassLoader ldr = null;
        try
        {
            // THIS newInstance-THING DOES THE TRICK, WE LOAD SYSTEMCLASSES FROM SYSTEM CLASSLOADER, THE VAADIN STUFF FROM OUR LOCAL LIBS
            ldr = URLClassLoader.newInstance(getGuiUrls(), this.getClass().getClassLoader());
        }
        catch (Exception e)
        {
            Log.err("GUI-Bibliotheksliste konnte nicht geladen werden", e);
        }
        return ldr;
    }

    @Override
    public void init( ServletConfig config ) throws ServletException
    {
        app.init(config);
    }

    @Override
    public ServletConfig getServletConfig()
    {
        return app.getServletConfig();
    }

    @Override
    public void service( ServletRequest req, ServletResponse res ) throws ServletException, IOException
    {
        app.service(req, res);
    }

    @Override
    public String getServletInfo()
    {
        return app.getServletInfo();
    }

    @Override
    public void destroy()
    {
        app.destroy();
    }
}


public class VaadinWysiwyg
{
   
    public static ServletHolder createGuiServlet( String classname )
    {
        ServletHolder vaadinLoader = new ServletHolder(new VaadinInterfaceServlet());

        vaadinLoader.setInitParameter("application", classname);
        vaadinLoader.setInitParameter("widgetset", "com.example.testvaadin.widgetset.TestvaadinWidgetset");
        vaadinLoader.setInitParameter("productionMode", "true" );

        return vaadinLoader;
    }

    
}
