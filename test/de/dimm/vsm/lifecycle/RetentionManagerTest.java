/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.lifecycle;



import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.Retention;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Date;
import java.io.File;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.backup.GenericContext;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.backup.RestoreTest;
import de.dimm.vsm.fsengine.FS_FileHandle;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerTest;
import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.Snapshot;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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
public class RetentionManagerTest {

    public RetentionManagerTest() {
    }


    @BeforeClass
    public static void setUpClass() throws Exception
    {
        StoragePoolHandlerTest.init();
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

    void writeTestFile(File f, long len, byte val)
    {
        
        try
        {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            byte[] buff = new byte[8192];
            for (int i = 0; i < buff.length; i++)
            {
                buff[i] = val;

            }
            while (len >  0)
            {
                int wlen = buff.length;
                if (wlen > len)
                    wlen = (int)len;

                raf.write(buff, 0, wlen);

                len -= wlen;
            }
            raf.close();
        }
        catch (IOException iOException)
        {
            fail( "Cannot create test file " +  f.getAbsolutePath() );
        }
    }
    void updateTestFile(File f, long pos, byte val)
    {

        try
        {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(pos);
            raf.write(val);
            raf.close();
        }
        catch (IOException iOException)
        {
            fail( "Cannot update test file " + f.getAbsolutePath() );
        }
    }
    byte readTestFile(File f, long pos)
    {
        byte val = 0;
        try
        {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(pos);
            val = (byte)raf.read();
            raf.close();
        }
        catch (IOException iOException)
        {
            fail( "Cannot update test file " + f.getAbsolutePath() );
        }
        return val;
    }
    private static Snapshot createSnapshot()
    {
        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
        Snapshot sn = new Snapshot();
        sn.setCreation( new Date() );
        sn.setName("S" + sn.getCreation().toString());
        sn.setPool( pool_handler.getPool());
        
        return sn;
    }
//    private static void deleteRetentionsAndSnapshots()
//    {
//        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
//        assertFalse( pool_handler.nativeCall("delete from Wrksldlasfhd where pool_idx=" + pool_handler.getPool().getIdx()) );
//        pool_handler.nativeCall("delete from Snapshot where pool_idx=" + pool_handler.getPool().getIdx());
//        pool_handler.nativeCall("delete from Retention where pool_idx=" + pool_handler.getPool().getIdx());
//    }


    static void sortHashBlocks( List<HashBlock> hash_block_list)
    {
        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hash_block_list, new Comparator<HashBlock>()
        {

            @Override
            public int compare( HashBlock o1, HashBlock o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });
    }

    boolean equalsHashBlock( HashBlock hb1, HashBlock hb2)
    {
        if (hb1.getBlockLen() != hb2.getBlockLen())
            return false;
        if (hb1.getBlockOffset() != hb2.getBlockOffset())
            return false;
        if (hb1.getTs() != hb2.getTs())
            return false;
        if (hb1.getFileNode().getIdx() != hb2.getFileNode().getIdx())
            return false;
        if (!hb1.getHashvalue().equals( hb2.getHashvalue()))
            return false;

        return true;
    }

    int filterRetentionResultList( RetentionResultList retentionResult, long fidx )
    {
        int cnt = 0;
        List<Object[]> l = retentionResult.list;
        for (int i = 0; i < l.size(); i++)
        {
            Object[] objects = l.get(i);
            if (fidx == (Long)objects[0])
                cnt++;

        }
        return cnt;
    }

    void add_and_sort_snapshots( Snapshot s, List<Snapshot> snapshots )
    {
        snapshots.add(s);
        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(snapshots, new Comparator<Snapshot>()
        {

            @Override
            public int compare( Snapshot o1, Snapshot o2 )
            {
                return ((o2.getCreation().getTime() - o1.getCreation().getTime()) > 0) ? 1 : -1;
            }
        });

    }

    FS_FileHandle  get_fs_handle_for_dedupblock( HashBlock hb2 )
    {
        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
        assertNotNull(hb2.getDedupBlock());
        List<PoolNodeFileLink> link_list = pool_handler.get_pool_node_file_links( hb2.getFileNode() );
        assertNotNull(link_list);
        assertEquals(link_list.size(), 1);


        PoolNodeFileLink poolNodeFileLink = link_list.get(0);

        AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();

        StorageNodeHandler sn_handler = pool_handler.get_handler_for_node(s_node);
        FileHandle fh = null;
        try
        {
            fh = sn_handler.create_file_handle(hb2.getDedupBlock(), /*create*/ false);
        }
        catch (Exception pathResolveException)
        {
            fail("create_file_handle failed: " + pathResolveException.getMessage());
        }
        assert( fh instanceof FS_FileHandle);
        FS_FileHandle sfh = (FS_FileHandle)fh;
        return sfh;
    }


    @Test
    public void testRetention() throws SQLException, Throwable
    {
        System.out.println("testRetention");


        StoragePoolHandler pool_handler = StoragePoolHandlerTest.getSp_handler();
        DerbyStoragePoolNubHandler nubHandler = StoragePoolHandlerTest.getNubHandler();

        String ba_path = "z:\\unittest\\retentiondata";
        String restore_path = "z:\\unittest\\unittestrestore";
        String restore_path_data = "z:\\unittest\\unittestrestore\\retentiondata";
        
                if (StoragePoolHandlerTest.isMac()) {
            ba_path = "/Users/mw/Documents/VSM/unittest/retentiondata";
             restore_path = "/Users/mw/Documents/VSM/unittest/unittestrestore";
             restore_path_data = "/Users/mw/Documents/VSM/unittest/unittestrestore/retentiondata";

        }

        File rf = new File( restore_path_data );
        rf.mkdir();


        String ip = "127.0.0.1";
        int port = 8082;

        List<RemoteFSElem> expResult = null;
        List<RemoteFSElem> restoreResult = null;

//        Main main = new Main();
//        main.init();

        AgentApiEntry apiEntry = null;
        FileSystemElemNode fileNode = null;

        Snapshot s0 = null;
        Snapshot s1 = null;
        Snapshot s2 = null;

        int restoreFlags = GuiServerApi.RF_RECURSIVE;
        
        File restoreTestfile = new File(restore_path_data, "testfile.dat");

        //deleteRetentionsAndSnapshots();

        Backup backup = new Backup(null);
        StoragePool pool = pool_handler.getPool();
        File testfile = new File( ba_path, "testfile.dat" );
        int bs = 0;
        long TsBeforebackup = System.currentTimeMillis();
        sleep( 1 );

        try
        {
            apiEntry = LogicControl.getApiEntry(ip, port);
            RemoteFSElem direlem = new RemoteFSElem(ba_path, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            RemoteFSElem fileelem = new RemoteFSElem(ba_path + "/testfile.dat", FileSystemElemNode.FT_FILE, 0, 0, 0, 0, 0);

            expResult = apiEntry.getApi().list_dir(direlem, true);

            GenericContext context = backup.init_context( apiEntry, pool_handler, null, null);
            context.setHashCache( StoragePoolHandlerTest.getNubHandler().getHashCache(pool_handler.getPool()));

            String backup_abs_path = context.getRemoteElemAbsPath( direlem );
            String file_abs_path = context.getRemoteElemAbsPath( fileelem );
            bs = context.getHashBlockSize();

            
            sleep(10);
            writeTestFile(testfile , 10*bs, (byte)'a');

            // DELETE EXISTING
            FileSystemElemNode dirNode = pool_handler.resolve_node(backup_abs_path);
            if (dirNode != null)
            {
                assertTrue(pool_handler.remove_fse_node(dirNode, true));
                
                dirNode = pool_handler.resolve_node(backup_abs_path);
                assertNull(dirNode);
            }

            //  NODE IS NULL -> NEW FILE
            Backup.backupRemoteFSElem(context, direlem, dirNode, true, true);
            pool_handler.commit_transaction();
            context.getStatCounter().check_stat(true);

            // CREATE SNAPSHOT AFTER FIRST UPDATE
            sleep(1000);
            s0 = createSnapshot();
            

            fileNode = pool_handler.resolve_node(file_abs_path);
            assertNotNull(fileNode);
            dirNode = pool_handler.resolve_node(backup_abs_path);
            assertNotNull(dirNode);

            // GET AND SAVE ORIG LIST OF BLOCKS
            List<HashBlock> hb_orig = new ArrayList<HashBlock>(fileNode.getHashBlocks().getList(pool_handler.getEm()));


            // UPDATE FILE AT FIRST BYTE OF SECOND BLOCK AND BACKUP FILE
            sleep(1000);
            updateTestFile(testfile, 1*bs, (byte)'b');
            Backup.backupRemoteFSElem(context, direlem, dirNode, true, true);
            pool_handler.commit_transaction();
            context.getStatCounter().check_stat(true);

            // CHECK HBLIST
            fileNode = pool_handler.resolve_node(file_abs_path);
            List<HashBlock> hb_ba1 = new ArrayList<HashBlock>(fileNode.getHashBlocks().getList(pool_handler.getEm()));
            assertEquals(hb_orig.size() + 1, hb_ba1.size());

            // SORT BLOCKS ORDER BY OFFSET, TS
            sortHashBlocks( hb_ba1 );

            // FETCH SECOND BLOCK, THIS MUST BE THE UPDATE,
            HashBlock hb1 = hb_ba1.get(1);
            assertNotNull(hb1.getDedupBlock());
            assertEquals(hb1.getBlockOffset(), 1*bs);
            
            // CREATE SNAPSHOT AFTER FIRST UPDATE
            sleep(1000);
            s1 = createSnapshot();

            // UPDATE FILE AT FIRST BYTE OF THIRD BLOCK AND BACKUP FILE
            sleep(1000);
            updateTestFile(testfile, 2*bs, (byte)'c');
            Backup.backupRemoteFSElem(context, direlem, dirNode, true, true);
            pool_handler.commit_transaction();
            context.getStatCounter().check_stat(true);

            // CREATE SNAPSHOT AFTER SECOND UPDATE
            sleep(1000);
            s2 = createSnapshot();

            // CHECK HBLIST
            fileNode = pool_handler.resolve_node(file_abs_path);
            List<HashBlock> hb_ba2  = new ArrayList<HashBlock>( fileNode.getHashBlocks().getList(pool_handler.getEm()) );
            assertEquals(hb_orig.size() + 2, hb_ba2.size());

            // SORT BLOCKS ORDER BY OFFSET, TS
            sortHashBlocks( hb_ba2 );

            // FETCH FORTH BLOCK, THIS MUST BE THE LAST UPDATE: ORIG B0, UPD B1, ORIG B1, UPD B2, ...
            HashBlock hb2 = hb_ba2.get(3);
            assertNotNull(hb2.getDedupBlock());
            assertEquals(hb2.getBlockOffset(), 2*bs);

            // NOW CREATE RETENTION
            Retention retention = new Retention();
            retention.setName("Ret1");
            retention.setPool(pool);
            retention.setCreation(new Date());

            
            retention.setArgOp(Retention.OP_LE);
            retention.setArgType(Retention.ARG_TS);
            // RETENTIONTIME VEFORE BACKUP, NOTHING TO DELETE
            retention.setArgValue( Long.toString(System.currentTimeMillis() - TsBeforebackup) );
            retention.setFollowAction(Retention.AC_DELETE);
            retention.setFollowActionParams("");
            retention.setMode(Retention.MD_ARCHIVE);

            RetentionManager rm = new RetentionManager(nubHandler);

            // BA
            // S0
            // UPD Block1
            // S1
            // UPD Block2
            // S2

            // CREATE RETENTIONRESULT
            RetentionResultList retentionResult = rm.createRetentionResult(retention, pool, 0, 100, System.currentTimeMillis());
            // THIS SHOULD BE NOTHING, RETENTION IS OLDER THAN OLDEST FILE
            assertEquals(filterRetentionResultList(retentionResult, fileNode.getIdx()), 0);

            // EVERYTHING OLDER THAN NOW WILL BE HANDLED
            retention.setArgValue( Long.toString(0) );

            retentionResult = rm.createRetentionResult(retention, pool, 0, 100, System.currentTimeMillis());

            // THIS SHOULD BE ALL ENTRIES, ORIG + 2 UPDATES
            assertEquals(filterRetentionResultList(retentionResult, fileNode.getIdx()), 3);
            ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();


            RetentionResultList snapshotRetentionResult = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            // THIS SHOULD BE ALL ENTRIES, ORIG + 2 UPDATES
            assertEquals(filterRetentionResultList(snapshotRetentionResult, fileNode.getIdx()), 3);


            add_and_sort_snapshots( s1, snapshots );
            RetentionResultList snapshotRetentionResult_s1 = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            // ONLY S1: THIS SHOULD BE ORIG Block1 + UPD Block2
            assertEquals(filterRetentionResultList(snapshotRetentionResult_s1, fileNode.getIdx()), 2);

            snapshots.clear();
            add_and_sort_snapshots( s2, snapshots );
            RetentionResultList snapshotRetentionResult_s2only = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            // ONLY S2: THIS SHOULD BE ONE ORIG Block1 AND ORIG BLock2
            assertEquals(filterRetentionResultList(snapshotRetentionResult_s2only, fileNode.getIdx()), 2);


            add_and_sort_snapshots( s1, snapshots );
            RetentionResultList snapshotRetentionResult_s2 = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            // S1 AND S2: THIS SHOULD BE ONLY ORIG Block1, ORIG BLOCK 2 IS NEEDED FOR S1
            assertEquals(filterRetentionResultList(snapshotRetentionResult_s2, fileNode.getIdx()), 1);


            add_and_sort_snapshots( s0, snapshots );
            RetentionResultList snapshotRetentionResult_s0 = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            // THIS SHOULD BE NO ENTRY, EVERYTHING IS PROTECTED BY SNAPHOTS
            assertEquals(filterRetentionResultList(snapshotRetentionResult_s0, fileNode.getIdx()), 0);



            // NOW GET THE BLOCK WHICH SHOULD BE RECYCLED
            assertNotNull(hb2.getDedupBlock());
            FS_FileHandle sfh = get_fs_handle_for_dedupblock( hb2 );

            // AND CHECK IF EXISTS BEFORE RETENTION
            assertTrue( sfh.get_fh().exists() );

            // RESTORE LAST VERSION, CHECK IF MODIFICATIONS ARE ONLINE

            RemoteFSElem target = new RemoteFSElem(restore_path_data, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            StoragePoolQry live_qry = StoragePoolQry.createActualRdOnlyStoragePoolQry(User.createSystemInternal(), /*del*/false);
            
            Restore instance = new Restore(pool_handler, fileNode, restoreFlags, live_qry,  InetAddress.getByName(ip), port, target);

            instance.run_restore();
            assertTrue(instance.getResult());
            byte b1 = readTestFile( restoreTestfile, 1*bs ); // FIRST UPDATE, PROTECTED BY S1
            byte b2 = readTestFile( restoreTestfile, 2*bs );// SECOND UPDATE, PROTECTED BY S2

            // BEFORE RETENTION S1 FIRST UPDATE IS THERE, SECOND UPDATE IS STILL THERE TOO,
            assertEquals(b1, 'b' );
            assertEquals(b2, 'c' );

            StoragePoolQry ts_qry = StoragePoolQry.createTimestampStoragePoolQry(User.createSystemInternal(), s0.getCreation().getTime());
            instance.setRestoreParam(pool_handler, fileNode, restoreFlags, ts_qry,  InetAddress.getByName(ip), port, target);
            instance.run_restore();
            assertTrue(instance.getResult());
            b1 = readTestFile( restoreTestfile, 1*bs ); // FIRST UPDATE, PROTECTED BY S1
            b2 = readTestFile( restoreTestfile, 2*bs );// SECOND UPDATE, PROTECTED BY S2

            // BEFORE RETENTION S1 FIRST UPDATE IS THERE, SECOND UPDATE IS STILL THERE TOO,
            assertEquals(b1, 'a' );
            assertEquals(b2, 'a' );

            ts_qry = StoragePoolQry.createTimestampStoragePoolQry(User.createSystemInternal(), s1.getCreation().getTime());
            instance.setRestoreParam(pool_handler, fileNode, restoreFlags, ts_qry,  InetAddress.getByName(ip), port, target);
            instance.run_restore();
            assertTrue(instance.getResult());
            b1 = readTestFile( restoreTestfile, 1*bs ); // FIRST UPDATE, PROTECTED BY S1
            b2 = readTestFile( restoreTestfile, 2*bs );// SECOND UPDATE, PROTECTED BY S2

            // BEFORE RETENTION S1 FIRST UPDATE IS THERE, SECOND UPDATE IS STILL THERE TOO,
            assertEquals(b1, 'b' );
            assertEquals(b2, 'a' );

            ts_qry = StoragePoolQry.createTimestampStoragePoolQry(User.createSystemInternal(), s2.getCreation().getTime());
            instance.setRestoreParam(pool_handler, fileNode, restoreFlags, ts_qry,  InetAddress.getByName(ip), port, target);
            instance.run_restore();
            assertTrue(instance.getResult());
            b1 = readTestFile( restoreTestfile, 1*bs ); // FIRST UPDATE, PROTECTED BY S1
            b2 = readTestFile( restoreTestfile, 2*bs );// SECOND UPDATE, PROTECTED BY S2

            // BEFORE RETENTION S1 FIRST UPDATE IS THERE, SECOND UPDATE IS STILL THERE TOO,
            assertEquals(b1, 'b' );
            assertEquals(b2, 'c' );


            
            restoreTestfile.delete();


            // NOW HANDLE RETENTION FOR S1, THAT MEANS WE LOOSE LAST UPDATE AND WE LOOSE ORIGINAL BLOCK OF UPDATE
            RetentionResult localret = rm.handleRetentionList(pool_handler, snapshotRetentionResult_s1);

            // AND CHECK IF NOT EXISTS AFTER RETENTION
            assert( !sfh.get_fh().exists() );

            // ONE FILE, TWO BLOCKS
            assertEquals(localret.files, 1 );
            assertEquals(localret.size, 2*bs );
            


            // RESULTS AFTER RETENTION SHOULD BE SAME AS AFTER FIRST UPDATE, ONLY THE ORIGINAL OF SECOND BLOCK IS MISSING
            fileNode = pool_handler.resolve_node(file_abs_path);
            fileNode = pool_handler.em_find(FileSystemElemNode.class, fileNode.getIdx());
            List<HashBlock> hb_ba3  = new ArrayList<HashBlock>(fileNode.getHashBlocks().getList(pool_handler.getEm()));
            sortHashBlocks( hb_ba3 );
            assertEquals( hb_ba1.size(), hb_ba3.size() + 1);

            assertTrue( equalsHashBlock( hb_ba1.get(0), hb_ba3.get(0) ) );
            assertTrue( equalsHashBlock( hb_ba1.get(1), hb_ba3.get(1) ) );
            assertTrue( equalsHashBlock( hb_ba1.get(3), hb_ba3.get(2) ) );
            assertTrue( equalsHashBlock( hb_ba1.get(4), hb_ba3.get(3) ) );


            // REMOVE EVERYTHING
            snapshots.clear();
            snapshotRetentionResult = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
            localret = rm.handleRetentionList(pool_handler, snapshotRetentionResult_s1);
            
            context.close();
        }

        catch (UnknownHostException unknownHostException)
        {
            fail("Unknown host: " + unknownHostException.getMessage());
        }
        catch (PoolReadOnlyException poolReadOnlyException)
        {
            fail("read only: " + poolReadOnlyException.getMessage());
        }
        catch (Exception exc)
        {
            exc.printStackTrace();
            fail("Backup failed: " + exc.getMessage());
        }


        try
        {            
            // RETSORE FROM LIVE POOL-QRY
            StoragePoolQry qry = StoragePoolQry.createActualRdOnlyStoragePoolQry(User.createSystemInternal(), /*del*/ false);
            RemoteFSElem target = new RemoteFSElem(restore_path_data, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            Restore instance = new Restore(pool_handler, fileNode, restoreFlags, qry,  InetAddress.getByName(ip), port, target);
            
            instance.run_restore();

            assertTrue("RestoreElem:", instance.getResult());
            

            RemoteFSElem restorelist = new RemoteFSElem(restore_path_data, FileSystemElemNode.FT_DIR, 0, 0, 0, 0, 0);
            apiEntry = LogicControl.getApiEntry(InetAddress.getByName(ip), port);
            restoreResult = apiEntry.getApi().list_dir(restorelist, true);

            //instance.close_entitymanager();

        }
        catch (UnknownHostException unknownHostException)
        {
            fail("Unknown host: " + unknownHostException.getMessage());
        }

        RestoreTest.compareDirLists( apiEntry, expResult, restoreResult, /*mtime*/ false, /*atime*/StoragePoolHandlerTest.isWin());

        byte b1 = readTestFile( restoreTestfile, 1*bs ); // FIRST UPDATE, PROTECTED BY S1
        byte b2 = readTestFile( restoreTestfile, 2*bs );// SECOND UPDATE, PROTECTED BY S2

        // AFTER RETENTION S1 FIRST UPDATE IS THERE, SECOND UPDATE IS GONE, 
        assertEquals(b1, 'b' );
        assertEquals(b2, 'a' );
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