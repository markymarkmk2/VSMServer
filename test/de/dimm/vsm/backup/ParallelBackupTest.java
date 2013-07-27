/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import org.junit.Ignore;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Excludes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.net.UnknownHostException;
import java.sql.SQLException;
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
public class ParallelBackupTest {

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
    @Ignore
    public void testRestore_elem() throws SQLException, Throwable
    {
        System.out.println("Backup_elem");
        String ba_path = "z:\\";

        singleBackup( ba_path );

    }
    public void singleBackup(String ba_path) throws SQLException, Throwable
    {
        StoragePoolHandler pool_handler = StoragePoolHandlerTest.createInternalPoolHandler();
       
        if (StoragePoolHandlerTest.isMac()) {
            ba_path = "/Users/mw";
        }

        String ip = "127.0.0.1";
        int port = 8082;

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


}
