/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author Administrator
 */
public class ServerApiImplTest
{
    
    public ServerApiImplTest()
    {
    }
    
     @BeforeClass
    public static void setUpClass() throws Exception
    {
        if (!StoragePoolHandlerTest.init())
            fail( "Cannot load environment" );
    }

    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
    
    List<String> getList()
    {
        List<String> l = new ArrayList<>();
        l.add("1");
        l.add("2");
        System.out.println("getList");    
        return l;
    }

    /**
     * Test of alert method, of class ServerApiImpl.
     */
    @Test
    public void testAlert()
    {
        System.out.println("alert");
        String reason = "";
        String msg = "";
        
        for (String s : getList())
        {
            System.out.println(s);            
        }
        
        ServerApiImpl instance = new ServerApiImpl();
        boolean expResult = false;
        boolean result = instance.alert(reason, msg);
        
    }

    /**
     * Test of alert_list method, of class ServerApiImpl.
     */
    @Test
    public void testAlert_list()
    {
        System.out.println("alert_list");
        List<String> reason = new ArrayList<>();
        reason.add("1");
        reason.add("2");
        String msg = "msg";
        ServerApiImpl instance = new ServerApiImpl();
        boolean expResult = true;
        boolean result = instance.alert_list(reason, msg);
        assertEquals(expResult, result);
    }

    /**
     * Test of get_properties method, of class ServerApiImpl.
     */
    @Test
    public void testGet_properties()
    {
        System.out.println("get_properties");
        ServerApiImpl instance = new ServerApiImpl();        
        Properties result = instance.get_properties();
    }

    /**
     * Test of cdp_call method, of class ServerApiImpl.
     */
    @Ignore @Test
    public void testCdp_call()
    {
        System.out.println("cdp_call");
        CdpEvent file = null;
        CdpTicket ticket = null;
        ServerApiImpl instance = new ServerApiImpl();
        boolean expResult = false;
        boolean result = instance.cdp_call(file, ticket);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of cdp_call_list method, of class ServerApiImpl.
     */
    @Ignore @Test
    public void testCdp_call_list()
    {
        System.out.println("cdp_call_list");
        List<CdpEvent> fileList = null;
        CdpTicket ticket = null;
        ServerApiImpl instance = new ServerApiImpl();
        boolean expResult = false;
        boolean result = instance.cdp_call_list(fileList, ticket);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of vfs_call method, of class ServerApiImpl.
     */

}