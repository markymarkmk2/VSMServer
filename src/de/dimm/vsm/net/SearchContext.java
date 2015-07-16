/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import static de.dimm.vsm.LogicControl.getKeyPwd;
import static de.dimm.vsm.LogicControl.getKeyStore;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.IndexResult;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


/**
 *
 * @author Administrator
 */
public class SearchContext
{
    StoragePoolHandler sp_handler;
    ArrayList<SearchEntry> slist;
//    Thread searchThread;
    List<IndexResult> resultList;
    List<ArchiveJob> jobResultList;
    Date lastUsage;
    int max;
    NetServer webDavServer;
    

    public SearchContext( StoragePoolHandler handler, ArrayList<SearchEntry> slist, int max )
    {
        this.sp_handler = handler;
        this.slist = slist;
        this.max = max;
        resultList = new ArrayList<>();
        jobResultList = new ArrayList<>();


        // REGISTER OURSELVES AT HANDLER
        handler.setSearchContext(this);

        // AND REGISTER NEW PATH REOLVER FOR OUR VIRTUAL SEARCH FILESYSTEM
        SearchPathResolver resolver = new SearchPathResolver(this, handler);
        handler.setPathResolver(resolver);
        

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
        List<IndexResult> ret = null;
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
        List<RemoteFSElem> ret = new ArrayList<>();
        for (int i = 0; i < resultList.size(); i++)
        {
            IndexResult result = resultList.get(i);


            // THIS IS THE NEWEST ENTRY FOR THIS FILE
            FileSystemElemAttributes attr = result.getAttributes(sp_handler.getEm());

            // OBVIOUSLY THE FILE WAS CREATED AFTER TS
            if (attr == null)
            {
                continue;
            }
            
            RemoteFSElem elem = StoragePoolHandlerServlet.genRemoteFSElemfromNode(result.getNode(), attr);
            if (result.getNode().getHistory().size() > 1)
                elem.setMultVersions(true);                    
            ret.add(elem);
        }

        return ret;
    }

    void close()
    {
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
        if (webDavServer != null) {
            webDavServer.stop_server();
        }        
    }

    // THIS CALS SIZE OF OBJECTS ON FIRST LEVEL, IF WE DO IT RECURSIVELY, WE COULD END UP SCANNING THE WHOLE FS...
    long getLevel1Size()
    {
        long size = 0;
        for (int i = 0; i < resultList.size(); i++)
        {
            FileSystemElemNode n = resultList.get(i).getNode();            
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

    public long getTs()
    {
        return lastUsage.getTime();
    }
    
    NetServer createWebDavServer( SearchWrapper wrapper, long loginIdx, long wrapperIdx, int webDavPort ) throws IOException {
        // Aus dem User das Logintoken holen -> das ist unser WebDavToken: eindeutig je User 
        User usr = Main.get_control().getLoginManager().getUser(loginIdx);              
        wrapper.setWebDavToken(usr.getLoginToken());
        
        webDavServer = new NetServer();
        DefaultServlet servlet = new DefaultServlet();
        ServletHolder htmlSH = new ServletHolder(servlet);
        webDavServer.addServletHolder("*", htmlSH);
        ServletContextHandler context = webDavServer.getContext();
        FilterHolder fh = new FilterHolder(new VSMWebDavFilter(loginIdx, wrapperIdx, /*isSearch*/ true));        
        
        // Filter ist UserToken
        context.addFilter(fh, "/" + usr.getLoginToken() + "/*", null);
                
        if (webDavServer.start_server(webDavPort, false, getKeyStore(), getKeyPwd())) {
            return webDavServer;
        }
        webDavServer.stop_server();
        return null;
    }    
}
