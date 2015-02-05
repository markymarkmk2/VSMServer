/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.sva.vaadingui;

import com.vaadin.server.VaadinServlet;
import de.dimm.sva.vaadin.SVAClientApplication.MyProjectServlet;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import javax.servlet.ServletException;
import org.apache.commons.logging.Log;
import org.eclipse.jetty.servlet.ServletHolder;


public class VaadinWysiwyg
{
    public static URL[] getGuiUrls()
    {
        try
        {
            ArrayList<URL> l = new ArrayList<>();

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
    
    
    public final ClassLoader getClassLoader() throws ServletException
    {
        ClassLoader ldr = null;
        try
        {
            // THIS newInstance-THING DOES THE TRICK, WE LOAD SYSTEMCLASSES FROM SYSTEM CLASSLOADER, THE VAADIN STUFF FROM OUR LOCAL LIBS
            ldr = URLClassLoader.newInstance(getGuiUrls(), this.getClass().getClassLoader());
        }
        catch (Exception e)
        {
            
        }
        return ldr;
    }

    public static ServletHolder createGuiServlet( String classname )
    {
        VaadinServlet servlet = new MyProjectServlet();
        ServletHolder vaadinLoader = new ServletHolder(servlet);

        //vaadinLoader.setInitParameter("UI", classname);
        //vaadinLoader.setInitParameter("widgetset", "com.example.testvaadin.widgetset.TestvaadinWidgetset");
        //vaadinLoader.setInitParameter("productionMode", "true" );

        return vaadinLoader;
    }
    
}
