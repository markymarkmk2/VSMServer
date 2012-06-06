/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;


/**
 *
 * @author Administrator
 */
public class TestWebDAV
{

//    static void start_server(int port, boolean ssl, String keystoreFile, String keystorePassword )
//    {
//        Server server = new Server(port);
//        if (ssl)
//        {
//            SslSocketConnector connector = new SslSocketConnector();
//            connector.setPort(port);
//
//            connector.setKeyPassword(keystorePassword);
//            connector.setKeystore(keystoreFile);
//
//
//            server.setConnectors(new Connector[]
//            {
//                connector
//            });
//        }
//
//        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
//
//
//        SessionManager session_manager = new HashSessionManager();
//        SessionHandler session_handler = new SessionHandler(session_manager);
//        context.setSessionHandler(session_handler);
//        server.setHandler(context);
//
//        WebDavServletBean  servlet = new WebDavServletBean();
//        LocalFileSystemStore store = new LocalFileSystemStore(new File("Z:\\webdav"));
//
//        try
//        {
//            servlet.init(store, "", "Hey looser", 1, true);
//        }
//        catch (ServletException servletException)
//        {
//            System.out.println("Oha: " + servletException.getLocalizedMessage());
//        }
//        ServletHolder servletHolder = new ServletHolder(servlet);
//
//        context.addServlet(servletHolder, "/webdav/*");
//
//        try
//        {
//            server.start();
//            server.join();
//        }
//        catch (Exception exception)
//        {
//            exception.printStackTrace();
//        }
//    }
//
//    public static void main( String[] args ) throws Exception
//    {
//        start_server(80, true,  "vsmkeystore2.jks", "123456");
//    }
}
