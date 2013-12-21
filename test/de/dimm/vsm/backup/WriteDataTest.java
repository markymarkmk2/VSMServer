/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.fsengine.ArrayLazyList;
import de.dimm.vsm.fsengine.DDFS_WR_FileHandle;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Excludes;
import java.net.UnknownHostException;
import java.io.File;
import java.sql.SQLException;

import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.SearchContextManager;
import de.dimm.vsm.net.StoragePoolHandlerContextManager;
import de.dimm.vsm.net.StoragePoolHandlerServlet;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import junit.framework.Assert;
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
public class WriteDataTest {

    String ip = "127.0.0.1";
    int port = 8082;

    public WriteDataTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        if (!StoragePoolHandlerTest.init())
            fail( "Cannot load environment" );
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
     * Test of restore_elem method, of class Restore.
     */
    @Test
    public void testWriteData() throws SQLException, Throwable
    {
        System.out.println("testWriteData");

        String absPath = "/127.0.0.1/8082/z/unittest/unittestdata/a/__start_sync.bat";
        String wrAbsPath = "/127.0.0.1/8082/z/unittest/unittestdata/a/new__start_sync.bat";
        String wrAbsMovedPath = "/127.0.0.1/8082/z/unittest/unittestdata/a/moved__start_sync.bat";
        String fsPath = "z:\\unittest\\unittestdata\\a\\__start_sync.bat";
        String wrFsPath = "z:\\unittest\\unittestdata\\a\\new__start_sync.bat";
        String wrFsMovedPath = "z:\\unittest\\unittestdata\\a\\moved__start_sync.bat";
//        String wrFsPath = "satrt";
//        String wrFsMovedPath = "verzwz";

        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();


        
        SearchContextManager searchMgr = new SearchContextManager();
        StoragePoolHandlerContextManager poolCtxMgr = new StoragePoolHandlerContextManager();
        StoragePoolHandlerServlet servlet = new StoragePoolHandlerServlet(searchMgr, poolCtxMgr);

        StoragePoolWrapper wrapper = poolCtxMgr.createPoolWrapper(pool_handler, ip, port, "Z:");

        // CHECK FOR RD-NODE EXISTANT
        FileSystemElemNode node = pool_handler.resolve_node(absPath);
        if (node == null)
        {
            try
            {
                doBackup();
            }
            catch (Throwable throwable)
            {
                throwable.printStackTrace();
                fail("Backup failed: " + throwable.getMessage());
            }
            node = pool_handler.resolve_node(absPath);
            if (node == null)
                fail("Backup failed ??");
        }

        // CHECK FOR WR-NODE NOT EXISTANT
        FileSystemElemNode wrNode = pool_handler.resolve_node(wrAbsPath);
        if (wrNode != null)
        {
            try
            {
                boolean result = pool_handler.remove_fse_node(wrNode, true);
                assertTrue(result);
                wrNode = pool_handler.resolve_elem_by_path(wrAbsPath);
                assertTrue(wrNode == null);
            }
            catch (PoolReadOnlyException poolReadOnlyException)
            {
                fail("Cannot delete fse node: " + poolReadOnlyException.getMessage());
            }
        }

        // Check read via Servlet
        checkFsExists( servlet, pool_handler, wrapper, absPath, fsPath);


        // Write via Servlet
        long wrFh = servlet.create_fh(wrapper, wrFsPath, FileSystemElemNode.FT_FILE);
        byte[] wrData = "1234567890".getBytes();
        long off = 0;
        //int blocks = 1000*1000;
        int blocks = 1;
        for (int i = 0; i < blocks; i++)
        {
            servlet.writeFile(wrapper, wrFh, wrData, wrData.length, off);
            off += wrData.length;
        }
        servlet.close_fh(wrapper, wrFh);

        // Check written via Servlet
        checkFsExists( servlet, pool_handler, wrapper, wrAbsPath, wrFsPath);

        // Move in VSMFS via Servlet
        servlet.move_fse_node(wrapper, wrAbsPath, wrAbsMovedPath);
        
        // Check moved via Servlet
        checkFsExists( servlet, pool_handler, wrapper, wrAbsMovedPath, wrFsMovedPath);
        // Check moved via Servlet
        checkNotFsExists( servlet, pool_handler, wrapper, wrAbsPath, wrFsPath);

        // Move back
        servlet.move_fse_node(wrapper, wrAbsMovedPath, wrAbsPath);
        
        // Check orig written via Servlet
        checkFsExists( servlet, pool_handler, wrapper, wrAbsPath, wrFsPath);
        checkNotFsExists( servlet, pool_handler, wrapper, wrAbsMovedPath, wrFsMovedPath);

    }
    void checkNotFsExists( StoragePoolHandlerServlet servlet, StoragePoolHandler pool_handler, StoragePoolWrapper wrapper, String absPath, String fsPath )
    {
        try
        {
            //((JDBCEntityManager)pool_handler.getEm()).getCache(JDBCEntityManager.OBJECT_CACHE).getCache().removeAll();
            FileSystemElemNode node = pool_handler.resolve_node(absPath);
            assertNull("Node in DB", node);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            fail(exception.getMessage());
        }        
    }

    void checkFsExists( StoragePoolHandlerServlet servlet, StoragePoolHandler pool_handler, StoragePoolWrapper wrapper, String absPath, String fsPath )
    {
        try
        {
            //((JDBCEntityManager)pool_handler.getEm()).getCache(JDBCEntityManager.OBJECT_CACHE).getCache().removeAll();
            FileSystemElemNode node = pool_handler.resolve_node(absPath);
            assertNotNull("Node nicht in DB", node);
            long fh = servlet.open_fh(wrapper, node.getIdx(), false);
            byte[] data = servlet.read(wrapper, fh, 1024, 0);
            servlet.close_fh(wrapper, fh);

            File rf = new File(fsPath);
            if (rf.exists())
            {
                int dlen = 0;
                if (data != null)
                    dlen = data.length;
                assertEquals("Länge passt nicht", dlen, rf.length());
            }
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            fail(exception.getMessage());
        }
    }

    
    public void doBackup() throws SQLException, Throwable
    {
        System.out.println("doBackup");


        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();

        String ba_path = "z:\\unittest\\unittestdata";

        if (StoragePoolHandlerTest.isMac()) {
            ba_path = "/Users/mw/Documents/VSM/unittest/unittestdata";
        }
   
        AgentApiEntry apiEntry = null;
        FileSystemElemNode node = null;

        Backup backup = new Backup(null);
        try
        {
            apiEntry = LogicControl.getApiEntry(ip, port);

            if (!apiEntry.check_online(false))
            {
                fail("Agent is not online: " + ip + ":" + port);
            }
            RemoteFSElem elem = new RemoteFSElem(ba_path, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);


            ClientInfo info = new ClientInfo();
            ArrayLazyList <Excludes> excl = new ArrayLazyList<>();
            excl.add( new Excludes("exclfile.txt", /*dir*/false, /*fullPath*/false, /*includeM*/false, /*ignoreCase*/true, Excludes.MD_EXACTLY) );
            excl.add( new Excludes("ExclFolder", /*dir*/true, /*fullPath*/false, /*includeM*/false, /*ignoreCase*/false, Excludes.MD_EXACTLY) );
            info.setExclList( excl );
            info.setCompression(true);
            info.setEncryption(true);
            ClientVolume vol = new ClientVolume();
            vol.setVolumePath(elem);


            GenericContext context = backup.init_context( apiEntry, pool_handler, info, vol);
            context.setHashCache( StoragePoolHandlerTest.getNubHandler().getHashCache(pool_handler.getPool()));

            String abs_path = context.getRemoteElemAbsPath( elem );
            node = pool_handler.resolve_node(abs_path);
            while (node != null)
            {
                assertTrue(pool_handler.remove_fse_node(node, true));

                node = pool_handler.resolve_node(abs_path);

            }

            // TEST SPACE CALC
            context.getIndexer().setNoSpaceLeft(100l*1024*1024*1024*1024);
            context.getIndexer().flush();


            try
            {
                Backup.backupRemoteFSElem(context, elem, node, true, true);
                fail("No Space Limit not detected");
            }
            catch (Throwable throwable)
            {
            }
            context.getIndexer().setNoSpaceLeft(100l*1024*1024);
            context.getIndexer().flush();

            Backup.backupRemoteFSElem(context, elem, node, true, true);

            pool_handler.commit_transaction();
            node = pool_handler.resolve_node(abs_path);

            apiEntry.close();

            backup.close_entitymanager();
            
            

        }
        catch (UnknownHostException unknownHostException)
        {
            unknownHostException.printStackTrace(System.out);
            fail("Unknown host: " + unknownHostException.getMessage());
        }
        catch (PoolReadOnlyException poolReadOnlyException)
        {
            poolReadOnlyException.printStackTrace(System.out);
            fail("read only: " + poolReadOnlyException.getMessage());
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.out);
            fail("Backup failed: " + exc.getMessage());
        }
    }
    
    void out( String s )
    {
        System.out.println(s);
    }
    @Test
    public void doFS_WR_Test() throws SQLException, Throwable
    {
        System.out.println("doFS_WR_Test");
        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
          
        try
        {
            String wrAbsPath = "/127.0.0.1/8082/z/unittest/unittestdata/a/__start_sync.bat";
            
            AbstractStorageNode sNode = pool_handler.get_primary_dedup_node_for_write();
            FileSystemElemNode node = pool_handler.resolve_node(wrAbsPath);
            node = pool_handler.resolve_fse_node_from_db(node.getIdx());
            

            FileHandle fh = DDFS_WR_FileHandle.create_fs_handle(sNode, pool_handler, node, false);
            out( String.format("Size: %d", fh.length() ));
            byte[] data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pause".equals(new String(data)));
            
            byte[] wrdata = "jetztKommWasNeues".getBytes();
            fh.writeFile(wrdata, wrdata.length, 0);
            
            byte[] data2 = fh.read( 1024, 0);
            out( String.format("read: %d Data: %s", data2.length, new String(data2) ));
            Assert.assertTrue("Content", new String(wrdata).equals( new String(data2)));
            
            fh.writeFile(data, data.length, 0);
            fh.truncateFile(data.length);
            
            data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            
            
            fh.close();
            
            // Reopen
            fh = DDFS_WR_FileHandle.create_fs_handle(sNode, pool_handler, node, false);
            data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pause".equals(new String(data)));
            
            // 2M
            fh.truncateFile(2*1024*1024);
            fh.writeFile(data, data.length, 1024*1024);
            
            data = fh.read(10, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pause".equals(new String(data, 0, 9)));
            
            data = fh.read(10, 1024*1024);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pause".equals(new String(data, 0, 9)));
            
            data = fh.read(10, 2*1024*1024);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", data.length == 0);
                        
            fh.close();
            
            fh = DDFS_WR_FileHandle.create_fs_handle(sNode, pool_handler, node, true);
            data = "rem pausen".getBytes();
            fh.writeFile(data, data.length, 0);
            
            data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pausen".equals(new String(data)));            
            fh.close();
            
            // Check move
            String wrAbsPath2 = "/127.0.0.1/8082/z/unittest/unittestdata/a/xxx";            
            pool_handler.move_fse_node(node, wrAbsPath, wrAbsPath2);
                        
            FileSystemElemNode node2 = pool_handler.resolve_node(wrAbsPath2);
            node2 = pool_handler.resolve_fse_node_from_db(node2.getIdx());
            
            fh = DDFS_WR_FileHandle.create_fs_handle(sNode, pool_handler, node2, false);
            data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pausen".equals(new String(data)));            
            fh.close();
            
            pool_handler.move_fse_node(node, wrAbsPath2, wrAbsPath);
            
            node = pool_handler.resolve_node(wrAbsPath);
            node = pool_handler.resolve_fse_node_from_db(node.getIdx());
            // Reopen
            fh = DDFS_WR_FileHandle.create_fs_handle(sNode, pool_handler, node, false);
            data = fh.read(1024, 0);
            out( String.format("read: %d Data: %s", data.length, new String(data) ));
            Assert.assertTrue("Content", "rem pausen".equals(new String(data)));
            data = "rem pause".getBytes();
            fh.writeFile(data, data.length, 0);
            fh.close();
            
            pool_handler.remove_fse_node(node, true);
            
            
            
           
        }
        catch (SQLException | PathResolveException | IOException | PoolReadOnlyException interruptedException)
        {
            interruptedException.printStackTrace();
            fail(interruptedException.getMessage());
        }
    }    
    

    private void sleep( int i )
    {
        try
        {
            Thread.sleep(i);
        }
        catch (InterruptedException interruptedException)
        {
        }
    }

}
