/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Utilities.QueueElem;
import de.dimm.vsm.Utilities.QueueRunner;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.SearchEntry;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.search.IndexImpl;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermsFilter;
import org.apache.lucene.util.Version;

class HexLongParser implements FieldCache.LongParser
 {

    @Override
    public long parseLong( String arg0 )
    {
        return Long.parseLong(arg0, 16);
    }

 }


class IndexQueueElem extends QueueElem
{
    FileSystemElemAttributes attr;
    ArchiveJob job;
    FSEIndexer index;

    public IndexQueueElem( FileSystemElemAttributes attr, ArchiveJob job, FSEIndexer index )
    {
        this.attr = attr;
        this.job = job;
        this.index = index;
    }

    @Override
    public boolean run()
    {
        return index.addToIndex(attr, job);
    }
}
class IndexJobQueueElem extends QueueElem
{
    ArchiveJob job;
    FSEIndexer index;

    public IndexJobQueueElem( ArchiveJob attr, FSEIndexer index )
    {
        this.job = attr;
        this.index = index;
    }

    @Override
    public boolean run()
    {
        return index.addToIndex(job);
    }
}
class UpdateQueueElem extends QueueElem
{
    FileSystemElemAttributes attr;
    FSEIndexer index;

    public UpdateQueueElem( FileSystemElemAttributes attr, FSEIndexer index )
    {
        this.attr = attr;
        this.index = index;
    }

    @Override
    public boolean run()
    {
        return index.updateIndex(attr);
    }
}
class UpdateJobQueueElem extends QueueElem
{
    ArchiveJob attr;
    FSEIndexer index;

    public UpdateJobQueueElem( ArchiveJob attr, FSEIndexer index )
    {
        this.attr = attr;
        this.index = index;
    }

    @Override
    public boolean run()
    {
        return index.updateIndex(attr);
    }
}

class FlushQueueElem extends QueueElem
{
    FSEIndexer index;

    public FlushQueueElem( FSEIndexer index )
    {
        this.index = index;
    }

    @Override
    public boolean run()
    {
        index.flushSync();
        return true;
    }
}


/**
 *
 * @author Administrator
 */
public class FSEIndexer
{
    IndexImpl indexImpl;
    
    //ThreadPoolExecutor tpe;

    QueueRunner qr;
    int autoflushCnt = 5000;

    public static final long MAX_FLUSH_CYCLE_MS = 60*60*1000;
    int n = 0;

    Document fseDoc;
    Document archiveJobDoc;

  Field f_idx;
  Field f_nodeidx;
  Field f_jobidx;
  Field f_isjob;
  Field f_typ;
  Field f_name;
  Field f_creationDateMs;
  Field f_modificationDateMs;
  Field f_accessDateMs;
  Field f_ts;
  Field f_size;

  Field f_jobname;
  Field f_startTime;
  Field f_endTime;
  Field f_sourceIdx;
  Field f_ok;
  Field f_sourceType;

  QueryParser qParser;




    public FSEIndexer( String path)
    {
        File indexPath = new File(path);

        indexImpl = new IndexImpl(indexPath);

        qr = new QueueRunner("IndexQueueRunner", 500);

        Analyzer ana = indexImpl.create_analyzer("de", false);
        qParser = new QueryParser(Version.LUCENE_30,  "", ana );
        qParser.setAllowLeadingWildcard( true );
        

        createDocs();
    }

    final void createDocs()
    {
        fseDoc = new Document();

        f_idx = new Field("idx", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_nodeidx = new Field( "nodeidx", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_jobidx = new Field( "jobidx", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_isjob = new Field( "isjob", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_typ = new Field( "typ", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
        f_name = new Field( "name", "", Field.Store.YES, Field.Index.ANALYZED) ;
        f_creationDateMs = new Field( "creationDateMs", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_modificationDateMs = new Field( "modificationDateMs", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_accessDateMs = new Field( "accessDateMs", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_ts = new Field( "ts", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_size = new Field( "size", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;

        fseDoc.add( f_idx);
        fseDoc.add( f_nodeidx);
        fseDoc.add( f_jobidx );
        fseDoc.add( f_isjob);
        fseDoc.add( f_typ);
        fseDoc.add( f_name);
        fseDoc.add( f_creationDateMs);
        fseDoc.add( f_modificationDateMs);
        fseDoc.add( f_accessDateMs);
        fseDoc.add( f_ts );
        fseDoc.add( f_size );


        archiveJobDoc = new Document();

        f_startTime = new Field( "startTime", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_endTime = new Field( "endTime", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_sourceType = new Field( "sourceType", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_ok = new Field( "ok", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_sourceIdx = new Field( "sourceIdx", "", Field.Store.YES, Field.Index.NOT_ANALYZED) ;
        f_jobname = new Field( "jobname", "", Field.Store.YES, Field.Index.ANALYZED) ;

        archiveJobDoc.add( f_idx);
        archiveJobDoc.add( f_typ);
        archiveJobDoc.add( f_jobname);
        archiveJobDoc.add( f_startTime);
        archiveJobDoc.add( f_endTime);
        archiveJobDoc.add( f_size );
        archiveJobDoc.add( f_sourceType );
        archiveJobDoc.add( f_ok );
        archiveJobDoc.add( f_sourceIdx);

    }

    public boolean open()
    {
        if (indexImpl.isOpen())
            return true;
        
        n = 0;
        try
        {
            indexImpl.open();
        }
        catch (IOException iOException)
        {
            try
            {
                indexImpl.create();
            }
            catch (IOException iOException1)
            {
                LogManager.msg_index(LogManager.LVL_ERR, "Index kann nicht geöffnet werden", iOException);
                return false;
            }
        }
        return true;
    }

    public boolean isOpen()
    {
        return indexImpl.isOpen();
    }

    public void updateReadIndex()
    {
        indexImpl.updateReadIndex();
    }

    public void addToIndexAsync( final ArchiveJob job )
    {
        QueueElem elem = new IndexJobQueueElem(job, this);
        qr.addElem(elem);

        // FLUSH AFTER EVERY JOB TO UPDATE VIEW IN ARCHIVE WINDOW
        qr.addElem( new FlushQueueElem(this));
        
    }
    public void flushAsync( )
    {
        QueueElem elem = new FlushQueueElem(this);
        qr.addElem(elem);
    }
    public void flush( )
    {
        QueueElem elem = new FlushQueueElem(this);
        qr.addElem(elem);
        qr.flush();
    }
    
    void setLongValue( Field f , long v )
    {
        f.setValue(IndexImpl.to_hex_field(v));
    }

    public boolean addToIndex( FileSystemElemAttributes attr,  ArchiveJob job )
    {
        FileSystemElemNode node = attr.getFile();

        return addToIndex(attr.getIdx(),  node.getIdx(),(job != null ? job.getIdx() : 0),(job != null), node.getTyp(), attr.getName(), attr.getCreationDateMs(),
                attr.getModificationDateMs(), attr.getAccessDateMs(), attr.getTs(), attr.getFsize());
    }
    public boolean addToIndex( long idx, long nodeidx, long jobidx, boolean isJob, String typ, String name, long cdate, long mdate, long adate, long ts, long size )
    {

        setLongValue(f_idx, idx );
        setLongValue(f_nodeidx, nodeidx);
        setLongValue(f_jobidx, jobidx );
        f_isjob.setValue(   isJob ? "1" : "0");
        f_typ.setValue(typ);
        f_name.setValue(name);
        setLongValue(f_creationDateMs, cdate );
        setLongValue(f_modificationDateMs, mdate );
        setLongValue(f_accessDateMs, adate );
        setLongValue(f_ts, ts);
        setLongValue(f_size, size);

        try
        {
            indexImpl.writeDocument(fseDoc);

            return true;
        }
        catch (IOException iOException)
        {
            Log.err( "Dokument kann nicht indiziert werden", name, iOException);
        }
        return false;
    }
    public boolean addJobToIndex( long idx, String typ, String name, long startTime, long endTime, long size, boolean ok, String sourceType, long sourceIdx )
    {
        setLongValue( f_idx, idx);
        f_typ.setValue("job");
        f_jobname.setValue(name);
        setLongValue( f_startTime, startTime);
        setLongValue( f_endTime, endTime);
        setLongValue( f_sourceIdx, sourceIdx);
        setLongValue( f_size, size);
        f_sourceType.setValue(sourceType);
        f_ok.setValue(ok? "1" : "0");


        try
        {

            indexImpl.writeDocument(archiveJobDoc);

            return true;
        }
        catch (IOException iOException)
        {
            Log.err( "ArchivJob kann nicht indiziert werden", name, iOException);
        }
        return false;

    }

    public boolean addToIndex( ArchiveJob job )
    {
        setLongValue( f_idx, job.getIdx());
        f_typ.setValue("job");
        f_jobname.setValue(job.getName());
        setLongValue( f_startTime, job.getStartTime().getTime());
        setLongValue( f_endTime, job.getEndTime().getTime());
        setLongValue( f_sourceIdx, job.getSourceIdx());
        setLongValue( f_size, job.getTotalSize());
        f_sourceType.setValue(job.getSourceType());
        f_ok.setValue(job.isOk() ? "1" : "0");

        
        try
        {
           
            indexImpl.writeDocument(archiveJobDoc);

            return true;
        }
        catch (IOException iOException)
        {
            Log.err( "ArchivJob kann nicht indiziert werden", job.getName(), iOException);
        }
        return false;
    }

    Query buildUniqueIndexQuery( Document doc )
    {
        TermQuery t1 = new TermQuery(new Term( f_idx.name(), doc.getField(f_idx.name()).stringValue() ) );
        TermQuery t2 = new TermQuery(new Term( f_typ.name(), doc.getField(f_typ.name()).stringValue() ) );
        BooleanQuery bq = new BooleanQuery();
        bq.add( t1, Occur.MUST);
        bq.add( t2, Occur.MUST);
        return bq;
    }
    public boolean updateIndex( FileSystemElemAttributes attr  )
    {
        FileSystemElemNode node = attr.getFile();

        TermsFilter typFilter = new TermsFilter();
        typFilter.addTerm( new Term(f_typ.name(), "file" ) );
        typFilter.addTerm( new Term(f_typ.name(), "dir" ) );

        TermsFilter idxFilter = new TermsFilter();
        idxFilter.addTerm( new Term(f_idx.name(), IndexImpl.to_hex_field(attr.getIdx())) );

        BooleanFilter bf = new BooleanFilter();

        bf.add( new FilterClause(idxFilter, Occur.MUST));
        bf.add( new FilterClause(typFilter, Occur.MUST));

        List<Document> ret = null;
        try
        {
            Query q = new MatchAllDocsQuery();
            ret = indexImpl.searchDocument(q, bf, 2, Sort.INDEXORDER);

        }
        catch (Exception exc)
        {
            Log.err( "Node kann nicht gesucht werden", attr.getName(), exc);
            return false;
        }

        if (ret.size() == 1)
        {
            Document doc = ret.get(0);
            doc.getField(f_name.name()).setValue( attr.getName());
            setLongValue( doc.getField(f_creationDateMs.name()), attr.getCreationDateMs());
            setLongValue( doc.getField(f_modificationDateMs.name()), attr.getModificationDateMs());
            setLongValue( doc.getField(f_accessDateMs.name()), attr.getAccessDateMs());
            setLongValue( doc.getField(f_ts.name()), attr.getTs());
            setLongValue( doc.getField(f_size.name()), attr.getFsize());
       
            try
            {
                Query qry = buildUniqueIndexQuery( doc );
                indexImpl.updateDocument(doc, qry);
                return true;
            }
            catch (IOException iOException)
            {
                Log.err( "Dokument kann nicht indiziert werden", attr.getName(), iOException);
                return false;
            }
        }
        LogManager.msg_index(LogManager.LVL_ERR, "Update nicht möglich: Ret=" + ret.size());
        return false;
    }

    public boolean updateIndex( ArchiveJob job  )
    {
        TermsFilter typFilter = new TermsFilter();
        typFilter.addTerm( new Term(f_typ.name(), "job" ) );

        TermsFilter idxFilter = new TermsFilter();
        idxFilter.addTerm( new Term(f_idx.name(), IndexImpl.to_hex_field(job.getIdx())) );

        BooleanFilter bf = new BooleanFilter();

        bf.add( new FilterClause(idxFilter, Occur.MUST));
        bf.add( new FilterClause(typFilter, Occur.MUST));
        
        List<Document> ret = null;
        try
        {
            Query q = new MatchAllDocsQuery();
            ret = indexImpl.searchDocument(q, bf, 2, Sort.INDEXORDER);
        }
        catch (Exception exc)
        {
            Log.err( "ArchiveJob kann nicht gesucht werden", job.getName(), exc);
            return false;
        }
        

        if (ret.size() == 1)
        {
            Document doc = ret.get(0);

            doc.getField(f_jobname.name()).setValue( job.getName());
            setLongValue( doc.getField(f_startTime.name()), job.getStartTime().getTime());
            setLongValue( doc.getField(f_endTime.name()), job.getEndTime().getTime());
            doc.getField(f_ok.name()).setValue( job.isOk() ? "1" : "0");
            setLongValue( doc.getField(f_size.name()), job.getTotalSize());

            try
            {
                Query qry = buildUniqueIndexQuery( doc );                
                indexImpl.updateDocument(doc, qry);
                return true;
            }
            catch (IOException iOException)
            {
                Log.err( "ArchiveJob kann nicht indiziert werden", job.getName(), iOException);
                return false;
            }
        }
        LogManager.msg_index(LogManager.LVL_ERR, "Update job nicht möglich: Ret=" + ret.size());
        return false;
    }


    void flushSync()
    {
        try
        {            
            indexImpl.flush(false);
        }
        catch (IOException iOException)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Index kann nicht geflushed werden", iOException);
        }
    }

    public void close()
    {
        qr.flush();
        try
        {            
            indexImpl.close();
        }
        catch (IOException iOException)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Index kann nicht geschlossen werden", iOException);
        }
    }

    boolean hasFileNameSearch(  ArrayList<SearchEntry> slist )
    {
        for (int i = 0; i < slist.size(); i++)
        {
            SearchEntry searchEntry = slist.get(i);
            if (searchEntry.getArgType().equals(SearchEntry.ARG_NAME))
                return true;
        }
        return false;
    }
    boolean hasJobNameSearch(  ArrayList<SearchEntry> slist )
    {
        for (int i = 0; i < slist.size(); i++)
        {
            SearchEntry searchEntry = slist.get(i);
            if (searchEntry.getArgType().equals(SearchEntry.ARG_JOBNAME))
                return true;
        }
        return false;
    }

    public String buildLuceneQry(  ArrayList<SearchEntry> slist )
    {
        StringBuilder sb_where = new StringBuilder();

        for (int i = 0; i < slist.size(); i++)
        {
            SearchEntry searchEntry = slist.get(i);
            if ( i > 0 )
            {
                if (searchEntry.isPrevious_or())
                    sb_where.append( " OR (" );
                else
                    sb_where.append( " AND (" );
            }

            String v = searchEntry.getArgValue();
            String[] argList = argList = new String[] { v };

            if (v.contains(" "))
            {
                if ((v.charAt(0) == '\"' || v.charAt(0) == '\'') && (v.charAt( v.length() - 2) == '\"' || v.charAt( v.length() - 2) == '\''))
                {
                    argList = new String[] { v.substring(1, v.length() - 2) };
                }
                else
                {
                    argList = v.split(" ");
                }
            }

            for (int j = 0; j < argList.length; j++)
            {
                if (j > 0)
                    sb_where.append(" AND ");

                String string = argList[j];

                if (searchEntry.isStringArgType())
                {
                    if(searchEntry.getArgOp().equals(SearchEntry.OP_BEGINS))
                    {
                        sb_where.append("( ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, SearchEntry.OP_EQUAL) );
                        sb_where.append(" OR ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, searchEntry.getArgOp()) );
                        sb_where.append(") ");
                    }
                    if (searchEntry.getArgOp().equals(SearchEntry.OP_ENDS))
                    {
                        sb_where.append("( ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, SearchEntry.OP_EQUAL) );
                        sb_where.append(" OR ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, searchEntry.getArgOp()) );
                        sb_where.append(") ");
                    }
                    else if(searchEntry.getArgOp().equals(SearchEntry.OP_CONTAINS))
                    {
                        sb_where.append("( ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, SearchEntry.OP_EQUAL) );
                        sb_where.append(" OR ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, SearchEntry.OP_BEGINS) );
                        sb_where.append(" OR ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, SearchEntry.OP_ENDS) );
                        sb_where.append(" OR ");
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, searchEntry.getArgOp()) );
                        sb_where.append(") ");
                    }
                    else
                    {
                        sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                        sb_where.append(":");
                        sb_where.append(searchEntry.getLuceneVal(string, searchEntry.getArgOp()) );
                    }

                }
                else
                {
                    sb_where.append( searchEntry.getLuceneFieldforArgtype() );
                    sb_where.append(":");
                    String val = searchEntry.getLuceneVal(string, searchEntry.getArgOp());
                    sb_where.append(val);
                }

            }
            if ( i > 0 )
                sb_where.append(")");

        }
        return sb_where.toString();
    }

    public List<FileSystemElemNode> searchNodes(StoragePoolHandler sp, String qry, int maxCnt) throws SQLException
    {
        return searchNodes(sp, qry, null, maxCnt);
    }
    public List<FileSystemElemNode> searchNodes(StoragePoolHandler sp, ArrayList<SearchEntry> slist, int maxCnt) throws SQLException
    {
        return searchNodes(sp, slist, null, maxCnt);
    }
    public List<FileSystemElemNode> searchNodes(StoragePoolHandler sp, ArrayList<SearchEntry> slist, ArchiveJob job, int maxCnt) throws SQLException
    {
        String qry = buildLuceneQry( slist );
        return searchNodes(sp, qry, job, maxCnt);
    }

    public List<FileSystemElemNode> searchNodes(StoragePoolHandler sp, String qry, ArchiveJob job, int maxCnt) throws SQLException
    {
        List<FileSystemElemNode> ret = new ArrayList<FileSystemElemNode>();
        
        try
        {
            Query q;
            if (qry.length() > 0)
                q = qParser.parse(qry);
            else
                q = new MatchAllDocsQuery();


            Filter filter;
            TermsFilter f = new TermsFilter();
            f.addTerm( new Term(f_typ.name(), FileSystemElemNode.FT_FILE));
            f.addTerm( new Term(f_typ.name(), FileSystemElemNode.FT_DIR));


            if (job == null)
                filter = f;
            else
            {
                BooleanFilter bf = new BooleanFilter();
                TermsFilter jobtf = new TermsFilter();
                jobtf.addTerm( new Term(f_jobidx.name(), IndexImpl.to_hex_field(job.getIdx())));
                bf.add( new FilterClause(jobtf, Occur.MUST));
                bf.add( new FilterClause(f, Occur.MUST));
                filter = bf;
            }

            Sort sort = new Sort( new SortField(f_modificationDateMs.name(), SortField.STRING_VAL, true));

            //index.searchDocument(q, null, maxCnt, Sort.INDEXORDER);

            List<Document> l = indexImpl.searchDocument(q, filter, maxCnt, Sort.INDEXORDER);

            for (int i = 0; i < l.size(); i++)
            {
                Document document = l.get(i);

                long nodeIdx = IndexImpl.doc_get_hex_long(document, f_nodeidx.name() );
                FileSystemElemNode node = sp.resolve_fse_node_from_db(nodeIdx);
                if (node != null)
                {
                    ret.add(node);
                }
            }
            return ret;
        }
        catch (ParseException exc)
        {
            Log.err( "Suchauftrag schlug fehl", qry, exc);
        }
        return null;
    }
    public List<ArchiveJob> searchJobs(StoragePoolHandler sp, ArrayList<SearchEntry> slist, int maxCnt) throws SQLException
    {
        String qry = buildLuceneQry( slist );

        List<ArchiveJob> ret = new ArrayList<ArchiveJob>();

        HashMap<Long,ArchiveJob> jobMap = new HashMap<Long, ArchiveJob>();

        try
        {
            Query q;
            if (qry.length() > 0)
                q = qParser.parse(qry);
            else
                q = new MatchAllDocsQuery();

            TermsFilter f = new TermsFilter();

            if (hasFileNameSearch(slist))
            {
                // ALL NODES THAT BELONG TO A JOB
                f.addTerm( new Term("isjob", "1"));
            }
            else
            {
                // ONLY ARCHIVEJOBS
                f.addTerm( new Term(f_typ.name(), "job"));
            }

            // SORT DATE DESCENDING
            SortField hex_long_field = new SortField(f_startTime.name(), new HexLongParser(), /*reverse*/ true);
            Sort sort = new Sort( hex_long_field);

            
            List<Document> l = indexImpl.searchDocument(q, f, maxCnt, sort);

            for (int i = 0; i < l.size(); i++)
            {
                Document document = l.get(i);

                String typ = document.get(f_typ.name());
                if (typ.equals("job"))
                {
                    long nodeIdx = IndexImpl.doc_get_hex_long(document,f_idx.name() );
                    if (!jobMap.containsKey(nodeIdx))
                    {
                        ArchiveJob job = sp.em_find(ArchiveJob.class, nodeIdx);
                        if (job != null)
                        {
                            jobMap.put(nodeIdx, job);
                            ret.add(job);
                        }
                    }
                }
                else
                {
                    long nodeIdx = IndexImpl.doc_get_hex_long(document, f_jobidx.name() );
                    if (nodeIdx > 0 && !jobMap.containsKey(nodeIdx))
                    {
                        ArchiveJob job = sp.em_find(ArchiveJob.class, nodeIdx);
                        if (job != null)
                        {
                            jobMap.put(nodeIdx, job);
                            ret.add(job);
                        }
                    }
                }
            }
            return ret;
        }
        catch (ParseException exc)
        {
            Log.err( "Suchauftrag schlug fehl", qry, exc);
        }
        return null;
    }
    

    public List<Document> searchFSEDocuments(String qry, Filter addFilter, int maxCnt)
    {

        try
        {
            Query q;
            if (qry.length() > 0)
                q = qParser.parse(qry);
            else
                q = new MatchAllDocsQuery();


            Filter filter = null;

            TermsFilter f = new TermsFilter();
            f.addTerm( new Term(f_typ.name(), FileSystemElemNode.FT_FILE));
            f.addTerm( new Term(f_typ.name(), FileSystemElemNode.FT_DIR));
            if (addFilter == null)
                filter = f;
            else
            {
                BooleanFilter bf = new BooleanFilter();
                bf.add( new FilterClause(addFilter, Occur.MUST));
                bf.add( new FilterClause(f, Occur.MUST));
                filter = bf;
            }
            filter = addFilter;

            List<Document> l = indexImpl.searchDocument(q, filter, maxCnt, Sort.INDEXORDER);

            return l;
        }
        catch (ParseException parseException)
        {
        }
        return null;
    }
    public boolean removeNodes(String qry) throws SQLException
    {
        try
        {
            Query q = null;
            if (qry.isEmpty())
            {
                q = new MatchAllDocsQuery();
            }
            else
            {
                q = qParser.parse(qry);
            }

            return removeNodes( q );
        }
        catch (Exception parseException)
        {
            Log.err("Dokument kann icht aus Index entfernt werden", parseException);
        }
        return false;

    }
    public boolean removeNodes(Query q) throws SQLException
    {
        try
        {
            indexImpl.removeDocument( q);

            return true;
        }
        catch (Exception parseException)
        {
            Log.err("Dokument kann icht aus Index entfernt werden", parseException);
        }
        return false;

    }
    public boolean removeNodes(FileSystemElemNode node) throws SQLException
    {
        try
        {
            Query q = new TermQuery( new Term(f_nodeidx.name(), IndexImpl.to_hex_field(node.getIdx())) );
            indexImpl.removeDocument( q);

            return true;
        }
        catch (Exception parseException)
        {
            Log.err("Node kann icht aus Index entfernt werden", parseException);
        }
        return false;

    }
    public boolean removeJob( ArchiveJob job )
    {
        try
        {
            Query q1 = new TermQuery( new Term(f_idx.name(), IndexImpl.to_hex_field(job.getIdx())) );
            Query q2 = new TermQuery( new Term(f_typ.name(), "job") );

            BooleanQuery bq = new BooleanQuery();
            bq.add(q1, Occur.MUST);
            bq.add(q2, Occur.MUST);

            indexImpl.removeDocument( bq );

            return true;
        }
        catch (Exception parseException)
        {
            Log.err("Job kann icht aus Index entfernt werden", parseException);
        }
        return false;
    }


    public void rebuildIndex( Connection conn  )
    {
        try
        {
            open();

            Statement st = conn.createStatement();


            HashMap<Long,Long> archiveLinkMap = new HashMap<Long, Long>();
            String qry = "select fileNode_idx, archiveJob_idx from ArchiveJobFileLink";
            ResultSet rs = st.executeQuery(qry);
            int n = 0;
            while (rs.next())
            {
                long nodeidx = rs.getLong(1);
                long jobidx = rs.getLong(2);
                archiveLinkMap.put(nodeidx, jobidx);
                n++;
                if (n % 1000 == 0)
                {
                    System.out.println("Added " + n + " job links");
                    flushSync();
                }
            }
            rs.close();

            qry = "select a.idx, a.file_idx, f.typ, a.name, a.creationDateMs, a.modificationDateMs, a.accessDateMs, a.ts, a.fsize from FileSystemElemAttributes a, FileSystemElemNode f where f.idx=a.file_idx";
            rs = st.executeQuery(qry);

            n = 0;
            while (rs.next())
            {
                long idx = rs.getLong(1);
                long nodeidx = rs.getLong(2);
                String typ = rs.getString(3);
                String name = rs.getString(4);
                long cdate = rs.getLong(5);
                long mdate = rs.getLong(6);
                long adate = rs.getLong(7);
                long ts = rs.getLong(8);
                long size = rs.getLong(9);

                long jobidx = 0;
                boolean isJob = false;
                Long job = archiveLinkMap.get(idx);
                if (job != null)
                {
                    jobidx = job.longValue();
                    isJob = true;
                }


                addToIndex(idx, nodeidx, jobidx, isJob, typ, name, cdate, mdate, adate, ts, size);
                n++;
                if (n % 1000 == 0)
                {
                    System.out.println("Added " + n + " entries");
                    flushSync();
                }
            }
            rs.close();
            
            String aqry = "select idx, name, startTime, endTime, totalSize, ok, sourceType, sourceIdx from ArchiveJob";
            rs = st.executeQuery(aqry);

            n = 0;
            while (rs.next())
            {
                long idx = rs.getLong(1);
                
                String typ = "job";
                String name = rs.getString(2);
                Date startTime = rs.getTimestamp(3);
                Date endTime = rs.getTimestamp(4);
                long size = rs.getLong(5);
                boolean ok = rs.getBoolean(6);
                String sourceType = rs.getString(7);
                long sourceIdx = rs.getLong(8);
                long lst = 0;
                if (startTime != null)
                    lst = startTime.getTime();
                long let = 0;
                if (endTime != null)
                    let = endTime.getTime();
                               
                addJobToIndex(idx, typ, name, lst, let, size, ok, sourceType, sourceIdx);
                n++;
                if (n % 1000 == 0)
                {
                    System.out.println("Added " + n + " jobs");
                    flushSync();
                }
            }
            rs.close();
            st.close();
            
        }
        catch (Exception sQLException)
        {
            Log.err("Fehler beim Neuaufbau des Indexes", sQLException);
        }
        finally
        {
            flushSync();
            close();
        }
    }

    public boolean noSpaceLeft()
    {
        return indexImpl.no_space_left();
    }
    public boolean lowSpaceLeft()
    {
        return indexImpl.low_space_left();
    }

    public void setNoSpaceLeft(long l)
    {
        indexImpl.setMinFreeSpace(l);
    }
    public void setLowSpaceLeft(long l)
    {
        indexImpl.setLowFreeSpace(l);
    }

    public void addToIndexAsync( final FileSystemElemAttributes attr, final ArchiveJob job )
    {
        QueueElem elem = new IndexQueueElem(attr, job, this);
        qr.addElem(elem);
        n++;
        if (n >= autoflushCnt)
        {
            n = 0;
            qr.addElem( new FlushQueueElem(this));
            lastFlush = System.currentTimeMillis();
        }
    }

    public void updateJobAsync( ArchiveJob job )
    {
        QueueElem elem = new UpdateJobQueueElem(job, this);
        qr.addElem(elem);
        n++;
        if (n >= autoflushCnt)
        {
            n = 0;
            qr.addElem( new FlushQueueElem(this));
            lastFlush = System.currentTimeMillis();
        }
    }
    public void updateIndexAsync( final FileSystemElemAttributes attr)
    {
        QueueElem elem = new UpdateQueueElem(attr, this);
        qr.addElem(elem);
        n++;
        if (n >= autoflushCnt)
        {
            n = 0;
            qr.addElem( new FlushQueueElem(this));
            lastFlush = System.currentTimeMillis();
        }
    }

    long lastFlush = System.currentTimeMillis();
    public void checkFlushAsync()
    {
        long now = System.currentTimeMillis();
        if (now - lastFlush > MAX_FLUSH_CYCLE_MS) // ONCE EVERY HOUR AT LEAST
        {
            n = 0;
            lastFlush = now;
            flushAsync();
        }
    }


}
