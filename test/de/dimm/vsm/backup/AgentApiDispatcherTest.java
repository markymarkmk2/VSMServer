/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.interfaces.AgentApi;
import java.io.IOException;
import java.util.Properties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Administrator
 */
public class AgentApiDispatcherTest {

    public AgentApiDispatcherTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    
    /**
     * Test of get_api method, of class AgentApiDispatcher.
     */
    @Test
    public void testGet_api()
    {
        System.out.println("get_api");
        try
        {
            InetAddress addr = InetAddress.getByName("127.0.0.1");
            int port = 8082;
            AgentApiDispatcher instance = new AgentApiDispatcher(/*use_ssl*/false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456", /*tcp*/ true);

            long start1 = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    AgentApiEntry apie = instance.get_api(addr, port);

                    Properties p = apie.getApi().get_properties();

                    apie.close();

                    assertTrue(System.getProperty("os.name").startsWith("Win"));
                    assertTrue(p.getProperty(AgentApi.OP_OS).startsWith("Win"));

                }
                catch (Exception e)
                {
                    fail("Exception at try " + i + ": " + e.getMessage());
                }
            }
            long start2 = System.currentTimeMillis();
             AgentApiEntry apie = instance.get_api(addr, port);
            for (int i = 0; i < 1000; i++)
            {
                try
                {

                    Properties p = apie.getApi().get_properties();
                    assertTrue(System.getProperty("os.name").startsWith("Win"));
                    assertTrue(p.getProperty(AgentApi.OP_OS).startsWith("Win"));

                }
                catch (Exception e)
                {
                    fail("Exception at try " + i + ": " + e.getMessage());
                }
            }
            try
            {
                apie.close();
            }
            catch (IOException iOException)
            {
                fail("Close");
            }
            long end = System.currentTimeMillis();

            System.out.println("1000 opens and calls took " + Long.toString(start2 - start1) + "ms" );
            System.out.println("1000 calls took " + Long.toString(end -start2) + "ms" );

        }
        catch (UnknownHostException unknownHostException)
        {
            fail("InetAddress");
        }
        // TODO review the generated test code and remove the default call to fail.
        
    }

}