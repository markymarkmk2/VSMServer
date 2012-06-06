/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin;

import com.vaadin.terminal.gwt.server.ApplicationServlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

class MAS extends ApplicationServlet
{
    @Override
    public void init( ServletConfig servletConfig ) throws ServletException
    {
        super.init(servletConfig);
    }
}



public class VaadinWysiwyg
{

    public static void main(String[] args) throws Exception
    {

        load_vaadin_client( 8070, "com.example.testvaadin.VaadinApplication");
     
    }
    public static void load_vaadin_client( int port, String classname )
    {
        try
        {
            Server server = new Server(port);
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);


            context.setContextPath("/");
            context.setResourceBase("./src");

            ServletHolder vaadinLoader = new ServletHolder(new MAS());
//            vaadinLoader.setInitParameter("application", "de.dimm.vsm.vaadin.VaadinApplication");
            vaadinLoader.setInitParameter("application", classname);
            vaadinLoader.setInitParameter("widgetset", "com.example.testvaadin.widgetset.TestvaadinWidgetset");

            //vaadinLoader.setInitParameter("widgetset", "/VAADIN/widgetsets/com.vaadin.terminal.gwt.DefaultWidgetSet");

           // vaadinLoader.setInitParameter("application", "de.dimm.vsm.MyEditorApplication");
           // vaadinLoader.setInitParameter("widgetset", "com.vaadin.visualdesigner.server.VisualDesignerApplicationWidgetset");

//        context.addServlet(vaadinLoader, "/edit");

            context.addServlet(vaadinLoader, "/client/*");
            context.addServlet(vaadinLoader, "/VAADIN/*");
           // context.addServlet(vaadinLoader, "/VisualDesigner/WebContent/VAADIN/*");

            server.setHandler(context);

            server.start();
            server.join();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

}