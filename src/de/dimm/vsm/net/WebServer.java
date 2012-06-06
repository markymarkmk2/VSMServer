/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.server.HessianServlet;
import de.dimm.vsm.net.interfaces.NamedService;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;



class EmptyInStream extends InputStream
{
    int len;

    public EmptyInStream( int len )
    {
        this.len = len;
    }

    @Override
    public int read() throws IOException
    {
        if (len <= 0)
            return -1;

        len--;
        return 0;
    }

    @Override
    public int read( byte[] b ) throws IOException
    {
        int rlen = b.length;
        if (len < b.length)
            rlen = len;
        if (len == 0)
            return -1;


        len -= rlen;
        return rlen;
    }

    @Override
    public int read( byte[] b, int off, int _len ) throws IOException
    {
        int rlen = _len;
        if (len < _len)
            rlen = len;
        if (len == 0)
            return -1;


        len -= rlen;
        return rlen;
    }

    @Override
    public int available() throws IOException
    {
        return len;
    }
}

/**
 *
 * @author Administrator
 */

public class WebServer extends HessianServlet implements NamedService
{

    Server server;
    @Override
    public String getName()
    {
        return "Moin alter Schwede";
    }



    @Override
    protected Hessian2Input createHessian2Input( InputStream is )
    {
        return super.createHessian2Input(is);
    }

    public void start_server(int port, String path, String war)
    {
     /*   server = new Server(8081);

        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.SESSIONS);

        ArrayList<Class<?>> list = new ArrayList<Class<?>>();
        list.add(StoragePool.class);
        RestDataSourceServlet servlet = new RestDataSourceServlet(LogicControl.get_em(), list);

        server.setHandler(context);
        ServletHolder servletHolder = new ServletHolder( servlet);
        context.addServlet(servletHolder, "/datasource");

        try
        {
            server.start();
            server.join();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
        */

        server = new Server(port);


        WebAppContext webappcontext = new WebAppContext();
        webappcontext.setContextPath(path);


        webappcontext.setWar(war);
        webappcontext.setTempDirectory(new File("tmp"));
        webappcontext.setExtractWAR(true);
        webappcontext.setCopyWebDir(true);

        server.setHandler(webappcontext);
               
        try
        {
            server.start();

         
            //server.join();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }
    public void stop_server()
    {
        if (server != null)
        {
            try
            {
                server.stop();
                server.join();
            }
            catch (Exception exception)
            {
            }
            server = null;
        }
    }
    public void join_server()
    {
        if (server != null)
        {
            try
            {
                server.join();
            }
            catch (Exception exception)
            {
            }
        }
    }


    public static void main(String[] args) throws Exception
    {
        WebServer server = new WebServer();
        server.start_server(8080, "/client", "war/VSMClient.war");

        server.join_server();
    }

    static byte[] data = new byte[0];
    @Override
    public byte[] get_data( int len )
    {
        if (data.length != len)
            data = new byte[len];

        return data;
    }


    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    @Override
    public InputStream download( String filename, int len )
    {
        EmptyInStream is = new EmptyInStream(len);
/*        if (data.length != len)
            data = new byte[len];

        bais = new ByteArrayInputStream(data);
        return bais;
 * */
        return is;

    }

    TestNetClass cl = new TestNetClass();
    @Override
    public TestNetClass get_test()
    {
        return cl;
    }

    @Override
    public void put_test( TestNetClass c )
    {
        cl = c;
    }

}
