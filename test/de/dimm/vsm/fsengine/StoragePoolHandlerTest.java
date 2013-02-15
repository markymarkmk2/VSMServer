/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.auth.User;
import java.sql.SQLException;
import net.sf.ehcache.Element;
import java.util.List;
import net.sf.ehcache.Cache;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import java.io.File;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.IOException;
import java.util.Iterator;
import net.sf.ehcache.statistics.LiveCacheStatistics;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


class TestStoragePoolNubHandler extends StoragePoolNubHandler
{



    @Override
    protected String getIndexPath( StoragePoolNub nub )
    {
        return StoragePoolHandlerTest.jdbcConnectString + "/Index";
    }


}
/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerTest
{
    //static EntityManager em;

    static AbstractStorageNode fs_node;
    //static FileSystemElemNode root_node;
    static StoragePool pool;
    static StoragePoolHandler sp_handler;
    static FileSystemElemNode dir_node;
    static FileSystemElemNode file_node;
    public static final int TESTCNT = 1000;
    static StoragePoolNubHandler nubHandler;

    public static StoragePoolNubHandler getNubHandler()
    {
        return nubHandler;
    }

    public StoragePoolHandlerTest()
    {
    }

    public static StoragePoolHandler getSp_handler()
    {
        return sp_handler;
    }
    
    public static boolean isWin()
    {
        String osName = System.getProperty("os.name");   
        return (osName.startsWith("Win"));
    }
    public static boolean isMac()
    {
        String osName = System.getProperty("os.name");   
        return (osName.startsWith("Mac") || osName.startsWith("Dar"));
    }

    static String jdbcConnectString = "untitTest";
    static boolean rebuild =true;
    public static boolean init()
    {
        // em = LogicControl.get_util_em();

        fs_node = AbstractStorageNode.createFSNode();
        if (isWin())
            fs_node.setMountPoint("z:\\unittest\\testnode");
        if (isMac())
            fs_node.setMountPoint("/Users/mw/Documents/VSM/unittest/testnode");
        fs_node.setNodeMode(AbstractStorageNode.NM_ONLINE);
        fs_node.setName("UnitTestNode1");



        StoragePoolNub nub = new StoragePoolNub();
        nub.setIdx(0);
        
        nub.setJdbcConnectString(jdbcConnectString);

        nubHandler = new TestStoragePoolNubHandler();
        LogicControl.setStorageNubHandler(nubHandler);
        try
        {
            pool = nubHandler.mountPoolDatabase(nub, jdbcConnectString, rebuild);
            
            // REBUILD ONLY ONCE, THIS SPEEDS UP TEST
            if (rebuild)
                rebuild = false;
        }
        catch (Exception exception)
        {
            System.out.println("Creating new Testdatabase");
            try
            {
                pool = nubHandler.createEmptyPoolDatabase(nub, jdbcConnectString);

            }
            catch (Exception iOException)
            {
                iOException.printStackTrace();
                fail("Creating new Testdatabase failed");
            }
        }


        try
        {
            User user = User.createSystemInternal();

            try
            {
                JDBCConnectionFactory conn = nubHandler.getConnectionFactory(pool);
                JDBCEntityManager em = new JDBCEntityManager(pool.getIdx(), conn);
                System.out.println("Open DB Connections: " + nubHandler.getActiveConnections(pool) );                
                sp_handler = new JDBCStoragePoolHandler( em, user, pool, /*rdonly*/ false );

            }
            catch (SQLException sQLException)
            {
                fail("Cannot open Connection:" +  sQLException.getMessage());
            }

            //sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, user, /*rdonly*/ false);

            sp_handler.check_open_transaction();
            pool.setName("UnitTestPool1");
            sp_handler.em_merge(pool);
            sp_handler.commit_transaction();

            try
            {
                AbstractStorageNode pfs_node = sp_handler.createSingleResultQuery("select T1 from AbstractStorageNode T1 where T1.name='" + fs_node.getName() + "'", AbstractStorageNode.class);
                if (pfs_node != null)
                {
                    fs_node = pfs_node;
                    fs_node.setMountPoint("z:\\unittest\\testnode");
                    fs_node.setNodeMode(AbstractStorageNode.NM_ONLINE);
                    sp_handler.em_merge(fs_node);
                }
                else
                {
                    fs_node.setPool(pool);
                    sp_handler.em_persist(fs_node);
                }
            }
            catch (Exception e)
            {
                fail("Cannot load / persist SNode" );
            }
            if (pool.getStorageNodes().isEmpty(sp_handler.getEm()))
                pool.getStorageNodes().add(sp_handler.getEm(), fs_node);

           

            //root_node = pool.getRootDir();
            sp_handler.add_storage_node_handlers();
                     

            sp_handler.commit_transaction();
            sp_handler.check_open_transaction();
        }
        catch (Exception ex)
        {
            System.out.println("Cannot Load Test Env:" + ex.getMessage());
            ex.printStackTrace();
            return false;
        }

        try
        {
            // REMOVE OLD STUFF


            FileSystemElemNode node = sp_handler.resolve_elem_by_path("/Dir/File");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
                node = sp_handler.resolve_elem_by_path("/Dir/File");
            }

            node = sp_handler.resolve_elem_by_path("/Dir/TmpFile");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
                node = sp_handler.resolve_elem_by_path("/Dir/TmpFile");
            }

            node = sp_handler.resolve_elem_by_path("/Dir/TmpDir");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
                node = sp_handler.resolve_elem_by_path("/Dir/TmpDir");
            }

            node = sp_handler.resolve_elem_by_path("/Dir");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
                node = sp_handler.resolve_elem_by_path("/Dir");
            }
            node = sp_handler.resolve_elem_by_path("/Dir2");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
                node = sp_handler.resolve_elem_by_path("/Dir2");
            }

            sp_handler.commit_transaction();
            sp_handler.close_transaction();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println("Cannot remove Test Data:" + ex.getMessage());
            return false;
        }
        return true;
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        init();
        System.out.println("setUpClass");
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
//        sp_handler.close_transaction();
//        em.close();

        System.out.println("tearDownClass");
    }

    @Before
    public void setUp()
    {
        System.out.println("setUp");
    }

    @After
    public void tearDown()
    {
        System.out.println("tearDown");
//        sp_handler.close_transaction();
    }

    @Test
    public void testCache()
    {
        try
        {
            FileSystemElemNode node = sp_handler.resolve_elem_by_path("/");
            JDBCStoragePoolHandler jd = (JDBCStoragePoolHandler) sp_handler;
            Cache c = jd.getJDBCEm().getCache(JDBCEntityManager.OBJECT_CACHE);
            c.put(new Element("1", node));
            Element e = c.get("1");
            assertEquals(e.getValue(), node);
            c.put(new Element("1", jd));
            e = c.get("1");
            assertEquals(e.getObjectValue(), jd);
            c.putIfAbsent(new Element("1", node));
            e = c.get("1");
            assertEquals(e.getObjectValue(), jd);
        }
        catch (SQLException ex)
        {
            fail(ex.getMessage());
        }

    }

    /**
     * Test of getPool method, of class StoragePoolHandler.
     */
    /**
     * Test of getTotalBlocks method, of class StoragePoolHandler.
     */
    @Test
    public void testCreateTestNodes()
    {
        try
        {
            FileSystemElemNode node = sp_handler.resolve_elem_by_path("/Dir");
            if (node != null)
            {
                fail("Node exists: " + node.getName());
            }
            try
            {
                sp_handler.create_fse_node_complete("/Dir", FileSystemElemNode.FT_DIR);
                dir_node = sp_handler.resolve_elem_by_path("/Dir");
            }
            catch (Exception iOException)
            {
                iOException.printStackTrace();
                fail("Cannot create fse node: " + iOException.getMessage());
            }
            try
            {
                node = sp_handler.resolve_elem_by_path("/Dir/File");
                if (node != null)
                {
                    fail("Node exists: " + node.getName());
                }
                sp_handler.create_fse_node_complete("/Dir/File", FileSystemElemNode.FT_FILE);
                file_node = sp_handler.resolve_elem_by_path("/Dir/File");
            }
            catch (Exception iOException)
            {
                iOException.printStackTrace();
                fail("Cannot create fse node: " + iOException.getMessage());
            }
        }
        catch (SQLException ex)
        {
            fail( ex.getMessage());
        }

    }
    long total_n = 0;
    long maxfiles = 0;
    int max_level = 4;

    void createDirFile( int level, int max, String name ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        if (level > max_level)
        {
            return;
        }

        if (sp_handler.resolve_elem_by_path(name) == null)
        {
            sp_handler.create_fse_node_complete(name, FileSystemElemNode.FT_DIR);
        }

        total_n++;
        if (total_n % 1000 == 0)
        {
            System.out.println("created " + name + " " + total_n + " dirs");
        }

        if (total_n >= maxfiles)
        {
            return;
        }

        for (int i = 0; i < max; i++)
        {
            if (total_n >= maxfiles)
            {
                return;
            }
            String childname = name + "/" + level + "_" + i;

            createDirFile(level + 1, max, childname);
        }
    }

    class ht
    {

        long idx;
        String typ;
        String name;

        public ht( long idx, String typ, String name )
        {
            this.idx = idx;
            this.typ = typ;
            this.name = name;
        }
    }

    void JPAreadDirFile( FileSystemElemNode node, int level, String name ) throws IOException, PoolReadOnlyException
    {
        total_n++;
        if (total_n % 1000 == 0)
        {            
            System.out.println("read " + name + " " + total_n + " dirs");
            if (_sp_handler instanceof JDBCStoragePoolHandler)
            {
                JDBCStoragePoolHandler j = (JDBCStoragePoolHandler) _sp_handler;
                Cache c = j.getJDBCEm().getCache(JDBCEntityManager.OBJECT_CACHE);
                LiveCacheStatistics st = c.getLiveCacheStatistics();


                System.out.println("Hits: " + st.getInMemoryHitCount() + "  Miss: " + st.getInMemoryMissCount() + " Size: " + st.getInMemorySize() + " Count: "
                        + st.getPutCount() + " Expired: " + st.getExpiredCount() + " Exicted: " + st.getEvictedCount());
            }
        }

        try
        {
            if (node == null)
            {
                node = node;
            }

            List<FileSystemElemNode> list = node.getChildren(sp_handler.getEm());
            if (list == null)
            {
                list = list;
            }

            if (node.isDirectory() && !list.isEmpty())
            {
                for (Iterator<FileSystemElemNode> it = list.iterator(); it.hasNext();)
                {

                    FileSystemElemNode child_naode = it.next();
                    String cchildname = name + "/" + child_naode.getName();
                    JPAreadDirFile(child_naode, level + 1, cchildname);
                }
            }
            _sp_handler.em_detach(node);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

    void jdbcreadDirFile( PreparedStatement stm, long node_idx, String typ, int level, String name ) throws IOException, PoolReadOnlyException
    {
        total_n++;
        if (total_n % 1000 == 0)
        {
            if (total_n > 10000)
                return;
            System.out.println("read " + name + " " + total_n + " dirs");
        }

        try
        {
            stm.setLong(1, node_idx);
            ResultSet rs = stm.executeQuery();

            ArrayList<ht> list = new ArrayList<ht>();

            while (rs.next())
            {
                list.add(new ht(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
            rs.close();



            if (FileSystemElemNode.isDirectory(typ) && !list.isEmpty())
            {
                for (Iterator<ht> it = list.iterator(); it.hasNext();)
                {
                    ht h = it.next();
                    String cchildname = name + "/" + h.name;
                    jdbcreadDirFile(stm, h.idx, h.typ, level + 1, cchildname);
                }
            }
            list.clear();
            //rs.close();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }
    StoragePoolHandler _sp_handler;

   // @Test
    public void memTestData()
    {

        try
        {
            //StoragePoolHandlerFactory.jpa = false;

            StoragePool ppool = sp_handler.createSingleResultQuery("select T1 from StoragePool T1 where T1.name='Backup'", StoragePool.class);

            User user = User.createSystemInternal();
            _sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(ppool, user, /*rdonly*/ false);
//            EntityManagerFactory emf = LogicControl.get_emf();
//            Object url = emf.getProperties().get("javax.persistence.jdbc.url");
//            Object pwd = emf.getProperties().get("javax.persistence.jdbc.password");
//            //Object user = emf.getProperties().get("javax.persistence.jdbc.user");
//            Object driver = emf.getProperties().get("javax.persistence.jdbc.driver");

//            Connection conn = null;
//            PreparedStatement stm = null;
//            try
//            {
//                // LOAD DRIVER
//                Class cl = Class.forName(driver.toString());
//
//                conn = DriverManager.getConnection(url.toString(), user.toString(), pwd.toString());
//
//                stm = conn.prepareStatement("select f.idx, f.typ, a.name from FileSystemElemNode f, FileSystemElemAttributes a where a.idx=f.attributes_idx and f.parent_idx=?");
//            }
//            catch (Exception e)
//            {
//                System.out.println("Change db failed: " + e.getMessage());
//
//            }
//

//            maxfiles = 100000;
//            System.out.println("Entering Memetest");
//            if (sp_handler.resolve_elem_by_path("/Dir2") == null)
//                sp_handler.create_fse_node_complete("/Dir2", FileSystemElemNode.FT_DIR);
////
//            em.clear();
//
            // createDirFile( 0, 1000, "/Dir2/test" );
            sp_handler.close_transaction(); //check_open_transaction();
            total_n = 0;


//            FileSystemElemNode rootDir = _sp_handler.em_find(FileSystemElemNode.class,ppool.getRootDir().getIdx());
//            rootDir = _sp_handler.em_find(FileSystemElemNode.class,ppool.getRootDir().getIdx());
//            _sp_handler.getPool().setRootDir(rootDir);


            FileSystemElemNode node = _sp_handler.resolve_elem_by_path("/127.0.0.1");

            // Query q = em.createNativeQuery("select f.idx, f.typ from FileSystemElemNode f where  f.parent_idx=?1");
            //Query q = stm..createNativeQuery("select f.idx, f.typ, a.name from FileSystemElemNode f, FileSystemElemAttributes a where a.idx=f.attributes_idx and f.parent_idx=?1");


            //jdbcreadDirFile( stm, node.getIdx(), node.getTyp(), 0, "/Dir2" );
            JPAreadDirFile(node, 0, "/Dir2");

//            int i = 1000000;

            /*            while( i--> 0)
            {
            FileSystemElemNode node = sp_handler.resolve_elem_by_path("/Dir");
            String name = node.getName();
            List<FileSystemElemNode> children = node.getChildren();
            int cnt = children.size();
            em.detach(node);
            em.clear();

            }*/
            System.out.println("Leaving Memetest");


            // REMOVE OLD STUFF
        }
        catch (Exception iOException)
        {
            iOException.printStackTrace();
            fail("Cannot memTestData: " + iOException.getMessage());
        }
    }

    /**
     * Test of getTotalBlocks method, of class StoragePoolHandler.
     */
    @Test
    public void testGetTotalBlocks()
    {
        System.out.println("getTotalBlocks");


        StoragePoolHandler instance = sp_handler;
        StorageNodeHandler sn_h = instance.get_handler_for_node(fs_node);
        long expResult = sn_h.getTotalBlocks();
        long result = instance.getTotalBlocks();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUsedBlocks method, of class StoragePoolHandler.
     */
    @Test
    public void testGetUsedBlocks()
    {
        System.out.println("getUsedBlocks");

        StoragePoolHandler instance = sp_handler;
        StorageNodeHandler sn_h = instance.get_handler_for_node(fs_node);
        long expResult = sn_h.getUsedBlocks();
        long result = instance.getUsedBlocks();
        assertEquals(expResult, result);

    }

    /**
     * Test of getBlockSize method, of class StoragePoolHandler.
     */
    @Test
    public void testGetBlockSize()
    {
        System.out.println("getBlockSize");


        StoragePoolHandler instance = sp_handler;
        StorageNodeHandler sn_h = instance.get_handler_for_node(fs_node);
        long expResult = sn_h.getBlockSize();
        int result = instance.getBlockSize();
        assertEquals(expResult, result);

    }

    /**
     * Test of resolve_dir_node method, of class StoragePoolHandler.
     */
    @Test
    public void testResolve_dir_node()
    {
        try
        {
            System.out.println("resolve_dir_node");
            StoragePoolHandler instance = sp_handler;
            FileSystemElemNode expResult = dir_node;
            FileSystemElemNode result = instance.resolve_elem_by_path("/Dir");
            assertSame(expResult, result);
            expResult = file_node;
            result = instance.resolve_elem_by_path("/Dir/File");
            assertSame(expResult, result);
        }
        catch (SQLException ex)
        {
            fail( ex.getMessage() );
        }
    }

    /**
     * Test of resolve_parent_dir_node method, of class StoragePoolHandler.
     */
    @Test
    public void testResolve_parent_dir_node()
    {
        System.out.println("resolve_parent_dir_node");


        StoragePoolHandler instance = null;

        instance = sp_handler;

        long expResult = dir_node.getIdx();
        FileSystemElemNode _result = instance.resolve_parent_dir_node("/Dir/File");
        long result = _result.getIdx();
        assert (expResult == result);
        expResult = pool.getRootDir().getIdx();
        result = instance.resolve_parent_dir_node("/Dir").getIdx();
        assert (expResult == result);
    }

    /**
     * Test of build_virtual_path method, of class StoragePoolHandler.
     */
    @Test
    public void testBuild_virtual_path() throws Exception
    {
        System.out.println("build_virtual_path");

        StringBuilder sb = new StringBuilder();
        StoragePoolHandler instance = null;

        instance = sp_handler;
        instance.build_virtual_path(file_node, sb);
        // TODO review the generated test code and remove the default call to fail.
        assertEquals("Path resolve", "/UnitTestPool1/Dir/File", sb.toString());
    }

   

    /**
     * Test of remove_fse_node method, of class StoragePoolHandler.
     */
    @Test
    public void testRemove_fse_node()
    {

        try
        {
            System.out.println("remove_fse_node");
            //FileSystemElemNodeHandler f = null;
            StoragePoolHandler instance = sp_handler;
            boolean expResult = false;
            String tmp_file = "/Dir/TmpFile";
            sp_handler.check_open_transaction();
            FileSystemElemNode node = sp_handler.resolve_elem_by_path(tmp_file);
            assertTrue(node == null);
            try
            {
                sp_handler.create_fse_node_complete(tmp_file, FileSystemElemNode.FT_FILE);
            }
            catch (Exception iOException)
            {
                fail("Cannot create fse node: " + iOException.getMessage());
            }
            sp_handler.commit_transaction();
            node = sp_handler.resolve_elem_by_path(tmp_file);
            assertTrue(node != null);
            try
            {
                boolean result = instance.remove_fse_node(node, true);
                assertTrue(result);
                node = sp_handler.resolve_elem_by_path(tmp_file);
                assertTrue(node == null);
            }
            catch (PoolReadOnlyException poolReadOnlyException)
            {
                fail("Cannot delete fse node: " + poolReadOnlyException.getMessage());
            }
        }
        catch (SQLException ex)
        {
            fail( ex.getMessage() );
        }
    }

    /**
     * Test of mkdir method, of class StoragePoolHandler.
     */
    @Test
    public void testMkdir() throws Exception
    {
        System.out.println("mkdir");
        String pathName = "";
        String tmp_file = "/Dir/TmpDir";

        StoragePoolHandler instance = sp_handler;

        sp_handler.check_open_transaction();

        FileSystemElemNode node = sp_handler.resolve_elem_by_path(tmp_file);
        assertTrue(node == null);

        try
        {
            instance.mkdir(tmp_file);
        }
        catch (IOException iOException)
        {
            fail("Cannot create fse node: " + iOException.getMessage());
        }
        node = sp_handler.resolve_elem_by_path(tmp_file);
        assertTrue("Found new dir ", node != null);
        assertTrue(" Is dir class", node.isDirectory());

        sp_handler.commit_transaction();

        boolean result = instance.remove_fse_node(node, true);

        assertTrue("Removed new dir ", result);

        node = sp_handler.resolve_elem_by_path(tmp_file);
        assertTrue("Not Found new dir ", node == null);

    }

    /**
     * Test of create_fse_node method, of class StoragePoolHandler.
     */
    /* @Test
    public void testCreate_fse_node_String_String() throws Exception
    {
    System.out.println("create_fse_node");
    String fileName = "";
    String type = "";
    StoragePoolHandler instance = sp_handler;
    instance.create_fse_node(fileName, type);

    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
    }*/
    /**
     * Test of move_fse_node method, of class StoragePoolHandler.
     */
    //@Test
    // TODO: TEST MOVE
    public void testMove_fse_node() throws Exception
    {
        System.out.println("move_fse_node");
        FileSystemElemNode from_node;
        String from = "/Dir/File";
        String to = "/Dir/TmpFile";

        sp_handler.check_open_transaction();

        StoragePoolHandler instance = sp_handler;
        from_node = instance.resolve_node(from);
        instance.move_fse_node(from_node, from, to);

        StringBuilder sb = new StringBuilder();
        instance.build_virtual_path(from_node, sb);
        assertEquals(sb.toString(), "/" + pool.getName() + to);

        from_node = instance.resolve_node(to);
        instance.move_fse_node(from_node, to, from);

        instance.build_virtual_path(from_node, sb);
        assertEquals(sb.toString(), "/" + pool.getName() + from);

    }

    /**
     * Test of open_file_handle method, of class StoragePoolHandler.
     */
    @Test
    public void testOpen_file_handle() throws Exception
    {
        System.out.println("open_file_handle");
        String path = "/Dir/File";

        sp_handler.check_open_transaction();

        StoragePoolHandler instance = sp_handler;

        FileSystemElemNode node = instance.resolve_node(path);
        FileHandle result = instance.open_file_handle(node, /*create*/ true);

        assertTrue( result instanceof FileHandle);

        FileHandle ffh = (FileHandle) result;

        String t = "Testdata!äöüßÖÄÜ";
        byte[] b = t.getBytes("UTF-8");


        try
        {
            ffh.truncateFile(0);
            ffh.writeFile(b, b.length, 0);
            fail("Not allowed Operation");
        }
        catch (PoolReadOnlyException poolReadOnlyException)
        {
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.err);
            fail("Not allowed Exception");
        }

        ffh.close();

       

    }

       /**
     * Test of getTotalBlocks method, of class StoragePoolHandler.
     */
    @Test
    public void testSwitchNodeTempOffline()
    {
       StoragePoolHandler instance = sp_handler;
       
        AbstractStorageNode node = instance.get_primary_dedup_node_for_write();
        String orig = node.getMountPoint();
        assertTrue("is Online", node.isOnline());
        node.setMountPoint("z:\\unittest\\testnodedfsfhdksfkdshdfjhksjfd");
        long space = instance.checkStorageNodeSpace();
        assertTrue("No space", space == 0);
        assertTrue("is TempOffline", node.isTempOffline());
        node.setMountPoint(orig);
        space = instance.checkStorageNodeSpace();
        assertTrue("No space", space != 0);
        assertTrue("is Online", node.isOnline());
    }
    

    @Test
    public void removeTestData()
    {
        try
        {
            // REMOVE OLD STUFF

            sp_handler.commit_transaction();
            
            FileSystemElemNode node = sp_handler.resolve_elem_by_path("/Dir/File");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
            }



            node = sp_handler.resolve_elem_by_path("/Dir/TmpFile");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
            }

            node = sp_handler.resolve_elem_by_path("/Dir/TmpDir");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
            }

            node = sp_handler.resolve_elem_by_path("/Dir");
            if (node != null)
            {
                sp_handler.remove_fse_node(node, true);
            }

            node = sp_handler.resolve_elem_by_path("/Dir/File");
            if (node != null)
            {
                fail("Cannot delete " + node.getName());
            }

            node = sp_handler.resolve_elem_by_path("/Dir/TmpFile");
            if (node != null)
            {
                fail("Cannot delete " + node.getName());
            }

            node = sp_handler.resolve_elem_by_path("/Dir/TmpDir");
            if (node != null)
            {
                fail("Cannot delete " + node.getName());
            }

            node = sp_handler.resolve_elem_by_path("/Dir");
            if (node != null)
            {
                fail("Cannot delete " + node.getName());
            }

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println("Cannot remove Test Data:" + ex.getMessage());
        }

    }
}
