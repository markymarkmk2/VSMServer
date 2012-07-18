/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 *
 * @author Administrator
 */
public class SearchContext
{

    StoragePoolHandler sp_handler;
    ArrayList<SearchEntry> slist;
//    Thread searchThread;
    List<FileSystemElemNode> resultList;
    List<ArchiveJob> jobResultList;
    Date lastUsage;
    int max;

    
    public SearchContext( StoragePoolHandler handler, ArrayList<SearchEntry> slist, int max )
    {
        this.sp_handler = handler;
        this.slist = slist;
        this.max = max;
        resultList = new ArrayList<FileSystemElemNode>();
        jobResultList = new ArrayList<ArchiveJob>();


        // REGISTER OURSELVES AT HANDLER
        handler.setSearchContext(this);

        // AND REGISTER NEW PATH REOLVER FOR OUR VIRTUAL SEARCH FILESYSTEM
        SearchPathResolver resolver = new SearchPathResolver(this, handler);
        handler.setPathResolver(resolver);
        
//        searchThread = new Thread(new Runnable()
//        {
//
//            @Override
//            public void run()
//            {
//                runSearch();
//            }
//        }, "SearchThread");

        lastUsage = new Date();
    }
    public void touch()
    {
        lastUsage = new Date();
    }
    public boolean isExpired()
    {
        return (System.currentTimeMillis() - lastUsage.getTime() > SearchContextManager.EXPIRE_MS);
    }
   

    void startSearch()
    {
        //searchThread.start();
        runSearch();
    }
    void startSearchJob()
    {
        //searchThread.start();
        runSearchJob();
    }

    void runSearch()
    {
        resultList.clear();
        List<FileSystemElemNode> ret = null;
        try
        {
            ret = sp_handler.search(slist, max);
        }
        catch (SQLException sQLException)
        {
            Log.err("Abbruch bei der Suche", sQLException);
        }
        if (ret != null)
        {
           resultList.addAll(ret);
        }
    }
    void runSearchJob()
    {
        resultList.clear();
        List<ArchiveJob> ret = null;
        try
        {
            ret = sp_handler.searchJob(slist, max);
        }
        catch (SQLException sQLException)
        {
            Log.err("Abbruch bei der Suche", sQLException);
        }
        if (ret != null)
        {
           jobResultList.addAll(ret);
        }
    }

    boolean isBusy()
    {
        return false;
        //return searchThread.isAlive();
    }
    public List<ArchiveJob> getJobResultList()
    {
        return jobResultList;
    }

    public List<RemoteFSElem> getResultList()
    {
        List<RemoteFSElem> ret = new ArrayList<RemoteFSElem>();
        for (int i = 0; i < resultList.size(); i++)
        {
            FileSystemElemNode fileSystemElemNode = resultList.get(i);


            // THIS IS THE NEWEST ENTRY FOR THIS FILE
            FileSystemElemAttributes attr = sp_handler.getActualFSAttributes(fileSystemElemNode, sp_handler.getPoolQry());


            // OBVIOUSLY THE FILE WAS CREATED AFTER TS
            if (attr == null)
            {
                continue;
            }

            // FILE WAS DELETED AT TS
            if (attr.isDeleted())
            {
                continue;
            }

            ret.add(StoragePoolHandlerServlet.genRemoteFSElemfromNode(fileSystemElemNode, attr));
        }

        return ret;
    }

    void close()
    {
//        if (isBusy())
//        {
//            try
//            {
//                searchThread.interrupt();
//            }
//            catch (Exception e)
//            {
//            }
//        }
        if (resultList != null)
        {
            resultList.clear();
        }
        try
        {
            sp_handler.close_transaction();
        }
        catch (SQLException sQLException)
        {
        }
        sp_handler.close_entitymanager();
    }

    // THIS CALS SIZE OF OBJECTS ON FIRST LEVEL, IF WE DO IT RECURSIVELY, WE COULD END UP SCANNING THE WHOLE FS...
    long getLevel1Size()
    {
        long size = 0;
        for (int i = 0; i < resultList.size(); i++)
        {
            FileSystemElemNode n = resultList.get(i);            
            size += n.getAttributes().getFsize() + n.getAttributes().getStreamSize();
        }
        return size;
    }
    private static final int BS = 4096;
    public long getTotalBlocks()
    {
        return getLevel1Size() / BS;
    }

    public long getUsedBlocks()
    {
        return getLevel1Size() / BS;
    }

    public int getBlockSize()
    {
        return BS;
    }
}
