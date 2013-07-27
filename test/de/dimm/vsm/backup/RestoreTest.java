/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.fsengine.ArrayLazyList;
import java.nio.file.Path;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.io.File;
import de.dimm.vsm.LogicControl;
import java.sql.SQLException;
import java.util.List;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Excludes;
import de.dimm.vsm.records.FileSystemElemNode;
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
public class RestoreTest {

    public RestoreTest() {
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

    public static String getLongPath( String fpath )
    {
//        if (fpath.length() > 200)
//        {
//            // DRIVE ?
//            if (fpath.charAt(1) == ':')
//            {
//               fpath = "\\\\?\\" + fpath;
//            }
//            // UNC
//            else if (fpath.startsWith("//") || fpath.startsWith("\\\\"))
//            {
//               fpath = "\\\\?\\UNC\\" + fpath;
//            }
//        }
        return fpath;
    }
    
    @Test
    public void accessLongPath() throws  Throwable
    {
        File f = new File("z:\\unittest\\unittestdata\\a\\444440497501_593085\\zm123456789012345678901234567890\\referenzen123456789012345678901234567890\\444440497500_582452\\angeliefert\\07.03.11\\29831_DF_PHILADELPHIA_Snack_MP_Milka\\29831 DF PHILADELPHIA Snack Milka MP Collection\\._29831 DF PHILADELPHIA Snack Milka MP.ai");

        try
        {
            if (StoragePoolHandlerTest.isWin()) {
            assertTrue(f.exists());

//            FileSystem fs = FileSystems.getDefault();
//            Path file = fs.getPath(f.getAbsolutePath());
//            assertTrue(file.exists());

            String fpath = getLongPath( f.getAbsolutePath() );

            f = new File( fpath );

            assertTrue(f.exists());

            }

//            file = fs.getPath(f.getAbsolutePath());
//            assertTrue(file.exists());
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.out);
            fail("Longnames failed: " + exc.getMessage());
        }


    }
    /**
     * Test of restore_elem method, of class Restore.
     */
    @Test
    public void testExcludes()
    {
        String ut = "Z:\\unittest\\unittestdata";
        if (StoragePoolHandlerTest.isMac()) 
            ut = "/Users/mw/Documents/VSM/unittest/unittestdata";
        assertTrue(Excludes.checkExclude(new Excludes("excl", /*dir*/true, /*fullPath*/false, /*includeM*/false, /*ignoreCase*/true, Excludes.MD_BEGINS_WITH) ,
                new RemoteFSElem(new File(ut + "/a/ExclFolder"))));
        assertTrue(Excludes.checkExclude(new Excludes(ut + "/a/Excl", /*dir*/true, /*fullPath*/true, /*includeM*/false, /*ignoreCase*/false, Excludes.MD_BEGINS_WITH) ,
                new RemoteFSElem(new File(ut + "/a/ExclFolder"))));
        assertFalse(Excludes.checkExclude(new Excludes(ut + "/a/Excl", /*dir*/true, /*fullPath*/true, /*includeM*/true, /*ignoreCase*/false, Excludes.MD_BEGINS_WITH) ,
                new RemoteFSElem(new File(ut + "/a/ExclFolder"))));
        assertTrue(Excludes.checkExclude(new Excludes("jpg", /*dir*/false, /*fullPath*/false, /*includeM*/false, /*ignoreCase*/true, Excludes.MD_ENDS_WITH) ,
                new RemoteFSElem(new File("Blah.JpG"))));

    }

    /**
     * Test of restore_elem method, of class Restore.
     */
    @Test
    public void testRestore_elem() throws SQLException, Throwable
    {
        System.out.println("restore_elem");


        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();

        String ba_path = "z:\\unittest\\unittestdata";
        String restore_path = "z:\\unittest\\unittestrestore";
        String restore_path_data = "z:\\unittest\\unittestrestore\\unittestdata";
        if (StoragePoolHandlerTest.isMac()) {
            ba_path = "/Users/mw/Documents/VSM/unittest/unittestdata";
             restore_path = "/Users/mw/Documents/VSM/unittest/unittestrestore";
             restore_path_data = "/Users/mw/Documents/VSM/unittest/unittestrestore/unittestdata";

        }
            
        
        String ip = "127.0.0.1";
        int port = 8082;

        List<RemoteFSElem> expResult = null;
        List<RemoteFSElem> restoreResult = null;

//        Main main = new Main();
//        main.init();

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

            expResult = apiEntry.getApi().list_dir(elem, true);

            ClientInfo info = new ClientInfo();
            ArrayLazyList <Excludes> excl = new ArrayLazyList<Excludes>();
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
            if (node != null)
            {
                assertTrue(pool_handler.remove_fse_node(node, true));
               
                node = pool_handler.resolve_node(abs_path);
                
                assertNull(node);
                
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

        assertNull("Excluded Elem",  pool_handler.resolve_node_by_remote_elem(new RemoteFSElem(new File("/Users/mw/Documents/VSM/unittest/unittestdata/a/ExclFile.txt"))) );
    
        assertNull("Excluded Elem",  pool_handler.resolve_node_by_remote_elem(new RemoteFSElem(new File("/Users/mw/Documents/VSM/unittest/unittestdata/a/ExclFolder"))) );

        
        int flags = GuiServerApi.RF_RECURSIVE;
        StoragePoolQry qry = pool_handler.getPoolQry();

        boolean result = false;
        try
        {
            InetAddress targetIP = InetAddress.getByName(ip);
            int targetPort = port;
            RemoteFSElem target = new RemoteFSElem(restore_path, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);

            Restore instance = new Restore(pool_handler, node, flags, qry, targetIP, targetPort, target);

            instance.run_restore();

            assertTrue("RestoreElem:", instance.actualContext.getResult());

            RemoteFSElem restorelist = new RemoteFSElem(restore_path_data, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            apiEntry = LogicControl.getApiEntry(targetIP, targetPort);
            restoreResult = apiEntry.getApi().list_dir(restorelist, true);

            //instance.close_entitymanager();

        }
        catch (UnknownHostException unknownHostException)
        {
            fail("Unknown host: " + unknownHostException.getMessage());
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.out);
            fail("Unknown Exception: " + exc.getMessage());
        }

        compareDirLists( apiEntry, expResult, restoreResult, /*mtime*/ true, /*atime*/StoragePoolHandlerTest.isWin());

        flags = GuiServerApi.RF_RECURSIVE | GuiServerApi.RF_COMPRESSION | GuiServerApi.RF_ENCRYPTION;


        result = false;
        try
        {
            InetAddress targetIP = InetAddress.getByName(ip);
            int targetPort = port;
            RemoteFSElem target = new RemoteFSElem(restore_path, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);

            Restore instance = new Restore(pool_handler, node, flags, qry, targetIP, targetPort, target);

            instance.run_restore();

            assertTrue("RestoreElem:", instance.actualContext.getResult());

            RemoteFSElem restorelist = new RemoteFSElem(restore_path_data, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            apiEntry = LogicControl.getApiEntry(targetIP, targetPort);
            restoreResult = apiEntry.getApi().list_dir(restorelist, true);

            //instance.close_entitymanager();

        }
        catch (UnknownHostException unknownHostException)
        {
            fail("Unknown host: " + unknownHostException.getMessage());
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.out);
            fail("Unknown Exception: " + exc.getMessage());
        }

        compareDirLists( apiEntry, expResult, restoreResult, /*mtime*/ true, /*atime*/StoragePoolHandlerTest.isWin());
    }
    
    public static void compareDirLists( AgentApiEntry apiEntry, List<RemoteFSElem> l1, List<RemoteFSElem> l2, boolean check_mtime, boolean check_atime )
    {
        for (int i = 0; i < l1.size(); i++)
        {
            RemoteFSElem remoteFSElem = l1.get(i);
            if (remoteFSElem.getName().equalsIgnoreCase("ExclFile.txt"))
            {
                l1.remove(remoteFSElem);
                i--;
            }
            if (remoteFSElem.getName().equalsIgnoreCase("ExclFolder"))
            {
                l1.remove(remoteFSElem);
                i--;
            }
        }
        assertEquals("DirSize", l1.size(), l2.size());

        for (int i = 0; i < l1.size(); i++)
        {

            RemoteFSElem r1 = l1.get(i);
            RemoteFSElem r2 = l2.get(i);

            String oname = r1.getPath() + " ";
            assertEquals(oname + "Types", r1.isDirectory(), r2.isDirectory());
            assertEquals(oname + "Types", r1.isFile(), r2.isFile());
            assertEquals(oname + "Types", r1.isHardLink(), r2.isHardLink());
            assertEquals(oname + "Types", r1.isSymbolicLink(), r2.isSymbolicLink());

            if (r1.isDirectory())
            {
                 List<RemoteFSElem> ll1 = apiEntry.getApi().list_dir(r1, true);
                 List<RemoteFSElem> ll2 = apiEntry.getApi().list_dir(r2, true);
                 compareDirLists( apiEntry, ll1, ll2, check_mtime, check_atime );
            }
            else
            {
                assertEquals(oname + "FSize", r1.getDataSize(), r2.getDataSize());
            }

            assertEquals(oname + "Filenames", r1.getName(), r2.getName());
            if (check_atime)
                assertEquals(oname + "Atime", r1.getAtimeMs(), r2.getAtimeMs());
            if (check_mtime)
                assertEquals(oname + "Mtime", r1.getMtimeMs(), r2.getMtimeMs());
            if ( r1.getStreamSize() != r2.getStreamSize())
                System.out.println("XSize Differs: " + r1.getStreamSize() + " " + r2.getStreamSize());
            assertEquals(oname + "AclInfo", r1.getAclinfo(), r2.getAclinfo());
            assertEquals(oname + "AclInfoData", r1.getAclinfoData(), r2.getAclinfoData());
            //assertEquals("", r1.getBase_ts(), r2.getBase_ts());
            assertEquals(oname + "GID", r1.getGid(), r2.getGid());
            assertEquals(oname + "PosixMode", r1.getPosixMode(), r2.getPosixMode());
            assertEquals(oname + "StreamInfo", r1.getStreaminfo(), r2.getStreaminfo());
            assertEquals(oname + "UID", r1.getUid(), r2.getUid());

        }

    }

   

}