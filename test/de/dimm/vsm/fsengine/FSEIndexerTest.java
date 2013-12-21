/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.net.SearchEntry;
import de.dimm.vsm.search.IndexImpl;
import java.util.ArrayList;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.util.List;
import java.sql.SQLException;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermsFilter;
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
public class FSEIndexerTest
{

    static FileSystemElemNode dir_node;
    static FileSystemElemNode file_node;

    public FSEIndexerTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        if (!StoragePoolHandlerTest.init())
        {
            fail("Cannot load environment");
        }

    }

    @AfterClass
    public static void tearDownClass() throws Exception
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
    public static final String fileName = "JvM 66_67_Sport-Paket Exterieur, Kühlergrill .png";

    
    @Test
    public void testCreateTestNodes()
    {
        StoragePoolHandler sp_handler = StoragePoolHandlerTest.getSp_handler();
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
            catch (IOException | PoolReadOnlyException | PathResolveException | SQLException iOException)
            {
                iOException.printStackTrace();
                fail("Cannot create fse node: " + iOException.getMessage());
            }
            try
            {
                node = sp_handler.resolve_elem_by_path("/Dir/" + fileName);
                if (node != null)
                {
                    fail("Node exists: " + node.getName());
                }
                sp_handler.create_fse_node_complete("/Dir/" + fileName, FileSystemElemNode.FT_FILE);
                file_node = sp_handler.resolve_elem_by_path("/Dir/" + fileName);
                sp_handler.create_fse_node_complete("/Dir/" + "dummy", FileSystemElemNode.FT_FILE);
            }
            catch (SQLException | IOException | PoolReadOnlyException | PathResolveException iOException)
            {
                iOException.printStackTrace();
                fail("Cannot create fse node: " + iOException.getMessage());
            }
        }
        catch (SQLException ex)
        {
            fail(ex.getMessage());
        }

    }

    @Test
    public void testSomeMethod()
    {
        StoragePoolNubHandler handler = StoragePoolHandlerTest.getNubHandler();

        StoragePoolHandler sp_handler = StoragePoolHandlerTest.getSp_handler();
        FSEIndexer fsi = handler.getIndexer(sp_handler.getPool());

        assertTrue(fsi.open());

        assertTrue(fsi.isOpen());

        ArchiveJob job = new ArchiveJob();
        job.setIdx(42);

        try
        {
            fsi.removeNodes("");
//            fsi.removeNodes("jobidx:"+ IndexImpl.to_hex_field(42));
            fsi.flushSync();
            fsi.updateReadIndex();
            
            assertEquals(0, fsi.searchFSEDocuments("", null, 10).size());

            file_node.getAttributes().setAccessDateMs(1);
            fsi.flushSync();
            fsi.addToIndex(file_node.getAttributes(), job); 
            fsi.flushSync();
            fsi.updateReadIndex();


            TermsFilter tf = new TermsFilter();
            tf.addTerm(new Term("jobidx", Long.toString(job.getIdx())));
            tf.addTerm(new Term("jobidx", IndexImpl.to_hex_field(job.getIdx())));
            fsi.searchFSEDocuments("", null, 10);

            List<FileSystemElemNode> ret = fsi.searchNodes(sp_handler, "nodeidx:" + IndexImpl.to_hex_field(file_node.getIdx()), 10);
            assertEquals(1, ret.size());
            assertEquals(ret.get(0).getIdx(), file_node.getIdx());

            // "FileHandler 08Test.txt";
            assertEquals(fsi.searchNodes(sp_handler, "name:jvm", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:66", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:67", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:sport", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:paket", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:png", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "name:kühlergrill name:exterieur", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "", job, 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "jobidx: [0 TO " + IndexImpl.to_hex_field(Long.MAX_VALUE) + "]", 10).size(), 1);
            assertEquals(fsi.searchNodes(sp_handler, "accessDateMs:1", 10).size(), 1 );
            job.setIdx(43);
            assertEquals(fsi.searchNodes(sp_handler, "jobidx:" + IndexImpl.to_hex_field(43), 10).size(), 0);
            assertEquals(fsi.searchNodes(sp_handler, "", job, 10).size(), 0);
            assertEquals(ret.size(), 1);
            assertEquals(ret.get(0).getIdx(), file_node.getIdx());

            System.out.println("Testing update Index");
            file_node.getAttributes().setAccessDateMs(2);
            fsi.updateIndex(file_node.getAttributes());
            fsi.flushSync();
            fsi.updateReadIndex();
            assertEquals(0, fsi.searchNodes(sp_handler, "accessDateMs:1", 10).size());
            assertEquals(1, fsi.searchNodes(sp_handler, "accessDateMs:2", 10).size() );


        }
        catch (SQLException sQLException)
        {
            sQLException.printStackTrace();
            fail(sQLException.getMessage());
        }
        fsi.close();
        assertFalse(fsi.isOpen());

    }
    // REMOVE OLD STUFF

    @Test
    public void buildLuceneQry()
    {
        ArrayList<SearchEntry> slist = new ArrayList<SearchEntry>();
        slist.add(new SearchEntry("test", null, SearchEntry.ARG_NAME, SearchEntry.OP_EQUAL, false, false, true, null));

        StoragePoolHandler instance = StoragePoolHandlerTest.getSp_handler();
        StoragePoolNubHandler handler = StoragePoolHandlerTest.getNubHandler();

        FSEIndexer fsi = handler.getIndexer(instance.getPool());

        String qry = fsi.buildLuceneQry(slist);
        QueryParser qp = new QueryParser(Version.LUCENE_30, "", new WhitespaceAnalyzer());

        Query q = null;
        try
        {
            q = qp.parse(qry);
            assertNotNull(q);
        }
        catch (ParseException parseException)
        {
            fail(parseException.getMessage());
        }

        slist.add(new SearchEntry(Long.toString(System.currentTimeMillis()), null, SearchEntry.ARG_MDATE, SearchEntry.OP_LT, false, false, true, null));
        qry = fsi.buildLuceneQry(slist);
        try
        {
            q = qp.parse(qry);
            assertNotNull(q);
        }
        catch (ParseException parseException)
        {
            fail(parseException.getMessage());
        }
        slist.add(new SearchEntry(Long.toString(System.currentTimeMillis()), Long.toString(System.currentTimeMillis() + 1), SearchEntry.ARG_CDATE, SearchEntry.OP_BETWEEN, false, false, true, null));
        qry = fsi.buildLuceneQry(slist);
        try
        {
            q = qp.parse(qry);
            assertNotNull(q);
        }
        catch (ParseException parseException)
        {
            fail(parseException.getMessage());
        }


    }

    @Test
    public void testRemoveTestNodes() throws SQLException, PoolReadOnlyException
    {
        StoragePoolHandler sp_handler = StoragePoolHandlerTest.getSp_handler();

        sp_handler.commit_transaction();

        FileSystemElemNode node = sp_handler.resolve_elem_by_path("/Dir/" + fileName);
        if (node != null)
        {
            sp_handler.remove_fse_node(node, true);
        }
        node = sp_handler.resolve_elem_by_path("/Dir/" + "dummy");
        if (node != null)
        {
            sp_handler.remove_fse_node(node, true);
        }


        node = sp_handler.resolve_elem_by_path("/Dir");
        if (node != null)
        {
            sp_handler.remove_fse_node(node, true);
        }
        sp_handler.commit_transaction();
    }
}
