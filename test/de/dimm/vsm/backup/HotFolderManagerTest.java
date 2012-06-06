/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import java.util.List;
import de.dimm.vsm.backup.hotfolder.HotFolderManager;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import java.util.ArrayList;
import java.io.File;
import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HotFolder;
import java.io.FileOutputStream;
import java.io.IOException;
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
public class HotFolderManagerTest {

    public HotFolderManagerTest() {
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

    @Test
    public void testSomeMethod()
    {
        HotFolderManager hfm = new HotFolderManager(StoragePoolHandlerTest.getNubHandler());
//        Main main = new Main();
//        try
//        {
//            main.init();
//        }
//        catch (SQLException sQLException)
//        {
//            fail( sQLException.getMessage() );
//        }
        HotFolder hotFolder = new HotFolder();
        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
        RemoteFSElem fse = new RemoteFSElem("z:/unittest/hotfolder", FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);

        String data = "DummeSau,Du!!";
        hotFolder.setIp("127.0.0.1");
        hotFolder.setPort(8082);
        hotFolder.setPoolIdx(pool_handler.getPool().getIdx());
        hotFolder.setMountPath( fse );
        hotFolder.setAtomicEval(true);
        hotFolder.setAcceptString(HotFolder.HF_DIRS);
        hotFolder.setSettleTime(5);
        hotFolder.setCreateDateSubdir(true);

        //LogicControl.get_base_util_em().nativeCall("delete * from HotFolderError where hotfolder_idx=0" );

        File f = new File("z:/unittest/hotfolder/A123456");
        if (!f.exists())
            f.mkdirs();
        f = new File("z:/unittest/hotfolder/A123456/Data.txt");
        try
        {
            FileOutputStream fis = new FileOutputStream(f);
            fis.write(data.getBytes());
            fis.close();
        }
        catch (IOException iOException)
        {
            fail("Cannot write test data.");
        }
        

        int cnt = 0;
        ArrayList<String> folders = null;
        while (cnt < 30)
        {
            folders = hfm.checkHotfolder(  hotFolder );
            if (!folders.isEmpty())
                break;
            cnt++;
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException interruptedException)
            {
            }
        }
        if (cnt >= 30)
        {
            fail( "Hotfolder was not found" );
        }
        FileSystemElemNode node = null;
        try
        {
            node = pool_handler.resolve_node(folders.get(0));
        }
        catch (SQLException ex)
        {
              fail( ex.getMessage() );
        }
        if (node == null)
            fail( "Hotfolder not found in db" );
        
        if (node.getChildren(pool_handler.getEm()).size() != 1)
            fail( "Hotfolder wrong data 1" );

        if (!node.getChildren(pool_handler.getEm()).get(0).getName().equals("Data.txt"))
            fail( "Hotfolder wrong data 2" );
        if (node.getChildren(pool_handler.getEm()).get(0).getAttributes().getFsize() != data.length())
            fail( "Hotfolder wrong data 3" );

        try
        {

            List<ArchiveJob> jobList = pool_handler.createQuery("select T1 from ArchiveJob where T1.sourceIdx=" + hotFolder.getIdx() + " and T1.sourceType='h'", ArchiveJob.class );
            for (int i = 0; i < jobList.size(); i++)
            {
                ArchiveJob job = jobList.get(i);

                pool_handler.remove_job(job);
//                job.setDirectory(null);
//                pool_handler.em_merge(job);
//                pool_handler.em_remove(job);
            }
            assertNull( pool_handler.resolve_node(folders.get(0)) );
            pool_handler.remove_fse_node(node.getChildren(pool_handler.getEm()).get(0), true);
            pool_handler.remove_fse_node(node, true);
        }
        catch (Exception poolReadOnlyException)
        {
            poolReadOnlyException.printStackTrace();
            fail( "Cannot Remove HF" );
        }





        // TODO review the generated test code and remove the default call to fail.
        
    }

}