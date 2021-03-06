/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 *
 * @author Administrator
 */
public class SearchContextManager extends WorkerParent
{
    int lastCnt = -1;
    private int maxHandlers = 100;
    final Map<SearchWrapper, SearchContext> handlerMap;
    final Map<Long,NetServer> webDavServerMap;
    private static final int WEB_DAV_USER_SEARCH_PORT_BASE = 59080;
    public static long EXPIRE_MS = 3600*1000l;  // 1h

    public SearchContextManager()
    {
        super( "SearchContextManager" );
        handlerMap = new HashMap<>();  
    webDavServerMap = new HashMap<>();

        maxHandlers = Main.get_int_prop(GeneralPreferences.MAX_OPEN_POOLHANDLERS, maxHandlers); 

    }

    @Override
    public boolean isVisible()
    {
        return false;
    }



    public SearchWrapper createSearchWrapper( User user, StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = StoragePoolQry.createSearchlistStoragePoolQry(user, slist);
                SearchWrapper w = new SearchWrapper(newIdx, pool.getIdx(), qry);

                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, qry);

                SearchContext context = new SearchContext(handler, slist, max);

                handlerMap.put(w, context);
                return w;
            }
            catch (Exception iOException)
            {
                Log.err(Main.Txt("Abbruch in createSearchWrapper") , iOException);
            }
            return null;
        }
    }
    
    SearchContext getContext(SearchWrapper wrapper)
    {
        synchronized( handlerMap )
        {
            SearchContext ret =  handlerMap.get(wrapper);
            if (ret != null)
                ret.touch();

            return ret;
        }
    }

    SearchContext getValidContext(SearchWrapper wrapper) throws SQLException
    {
        synchronized( handlerMap )
        {
            SearchContext ret =  handlerMap.get(wrapper);
            if (ret == null)
                throw new SQLException(Main.Txt("Der Sucheintrag ist nicht mehr gültig"));
            ret.touch();

            return ret;
        }
    }


    public StoragePoolHandler getHandlerbyWrapper( SearchWrapper pool )
    {
        SearchContext context = getContext(pool);
        if (context != null)
        {
            return context.sp_handler;
        }

        return null;
    }

    List<RemoteFSElem> getSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        SearchContext context = getContext(wrapper);

        if (context != null)
        {
            return context.getResultList();
        }

        return null;
    }

    List<ArchiveJob> getJobSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        SearchContext context = getContext(wrapper);

        if (context != null)
        {
            return context.jobResultList;
        }

        return null;
    }

    SearchStatus getSearchStatus( SearchWrapper wrapper )
    {
        SearchStatus st = null;

        SearchContext context = getContext(wrapper);
        if (context != null)
        {
            if (context.isBusy())
            {
                st = new SearchStatus(Main.Txt("Busy"), true, 0);
            }
            else
            {
                if (context.getResultList() == null)
                {
                    st = new SearchStatus(Main.Txt("Aborted"), false, 0);
                }
                else
                {
                    st = new SearchStatus(Main.Txt("Finished"), false, context.getResultList().size());
                }
            }
        }
        return st;
    }

    void closeSearch( SearchWrapper wrapper )
    {
        SearchContext context = handlerMap.remove(wrapper);
        if (context != null)
        {
            context.close();
        }
        webDavServerMap.remove(wrapper.getWrapperIdx());  
    }

    SearchWrapper search( User user, StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        SearchWrapper wrapper = createSearchWrapper(user, pool, slist, max);
        SearchContext context = handlerMap.get(wrapper);
        context.startSearch();

        return wrapper;
    }
    SearchWrapper searchJob( User user, StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        SearchWrapper wrapper = createSearchWrapper(user, pool, slist, max);
        SearchContext context = handlerMap.get(wrapper);
        context.startSearchJob();
        return wrapper;
    }

    List<RemoteFSElem> get_child_nodes( SearchWrapper wrapper, RemoteFSElem path ) throws SQLException
    {
        SearchContext context = getValidContext(wrapper);

        return Main.get_control().getPoolHandlerServlet().get_child_nodes(context.sp_handler, path);
    }

    boolean restoreFSElem( SearchWrapper wrapper, List<RemoteFSElem> paths, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, IOException
    {
        SearchContext context = getValidContext(wrapper);

        return Main.get_control().getPoolHandlerServlet().restoreFSElem(context.sp_handler, paths, targetIP, targetPort, targetPath, flags, user);
    }
    boolean restoreJob( SearchWrapper searchWrapper, ArchiveJob job, String targetIP, int targetPort, String targetPath, int rflags, User user ) throws SQLException
    {
        SearchContext context = getValidContext(searchWrapper);
        return Main.get_control().getPoolHandlerServlet().restoreJob(context.sp_handler, job, targetIP, targetPort, targetPath, rflags, user);
    }

    boolean removeJob( SearchWrapper searchWrapper, ArchiveJob job ) throws  SQLException, PoolReadOnlyException
    {
        SearchContext context = getValidContext(searchWrapper);
        return Main.get_control().getPoolHandlerServlet().removeJob(context.sp_handler, job);
    }


    StoragePoolWrapper createPoolWrapper( String ip, int port, SearchWrapper searchWrapper, User user, String drive )
    {
        StoragePoolHandlerContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getContextManager();

        // GET CONTEXT FOR THIS SEARCH
        SearchContext context = getContext(searchWrapper);

        // OPEN A NEW POOLHANDLERWRAPPER FOR OUR SP_HANDLER
        StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper(context.sp_handler, ip, port, drive);
        
        return poolWrapper;
    }

    void reSearch( SearchWrapper wrapper, StoragePool pool, ArrayList<SearchEntry> slist ) throws SQLException
    {
        SearchContext context = getValidContext(wrapper);

        context.slist = slist;        
        context.startSearch();        
    }


    @Override
    public boolean initialize()
    {
        return true;
    }



    @Override
    public void run()
    {
        is_started = true;

        while(!isShutdown())
        {
            LogicControl.sleep(1000);

            checkExpired();
        }
        finished = true;
    }

    private void checkExpired()
    {
        synchronized( handlerMap )
        {
            if (handlerMap.size() != lastCnt)
            {
                lastCnt = handlerMap.size();
                Log.debug("Open SearchContexts " + lastCnt);
            }

            Set<Entry<SearchWrapper, SearchContext>> vals = handlerMap.entrySet();
            for (Entry<SearchWrapper, SearchContext> entry : vals)
            {
                if (entry.getValue().isExpired())
                {
                    Log.debug(Main.Txt("Entferne abgelaufenen SuchContext") + " "  + entry.getKey().qry.getUser());
                    entry.getValue().close();
                    handlerMap.remove(entry.getKey());
                    webDavServerMap.remove(entry.getKey().getWrapperIdx());  
                    break;
                }
            }
            while (handlerMap.size() > maxHandlers)
            {
                Entry<SearchWrapper,SearchContext> oldestEntry = null;
                for (Entry<SearchWrapper, SearchContext> entry : vals)
                {
                    if (oldestEntry == null || oldestEntry.getValue().getTs() > entry.getValue().getTs())
                    {
                        oldestEntry = entry;
                    }
                }
                if (oldestEntry == null)
                    break;
                
                Log.debug("SearchContext is removed because HandlerMap is full " + handlerMap.size() + "/" + maxHandlers + ": " + oldestEntry.getKey().qry.getUser());
                oldestEntry.getValue().close();
                handlerMap.remove(oldestEntry.getKey());
                webDavServerMap.remove(oldestEntry.getKey().getWrapperIdx());  
            }            
        }
    }

    public int createWebDavServer( SearchWrapper wrapper, long loginIdx) throws IOException {
        synchronized( handlerMap )
        {
            SearchContext ctx = handlerMap.get(wrapper);
            int webDavPort = Main.get_int_prop(GeneralPreferences.WEB_DAV_SEARCH_PORT, WEB_DAV_USER_SEARCH_PORT_BASE) + (int)(wrapper.getWrapperIdx());       
            
            if (webDavServerMap.containsKey(wrapper.getWrapperIdx())){
                webDavServerMap.get(wrapper.getWrapperIdx()).stop_server();
            }
            NetServer server = ctx.createWebDavServer( wrapper, loginIdx, wrapper.getWrapperIdx(), webDavPort);
            if (server != null) {
                webDavServerMap.put(wrapper.getWrapperIdx(), server);                        
                return webDavPort;
            }
            return 0;
        }
    }  
    
    public SearchWrapper getSearchWrapper( long wrIdx ) {
        synchronized( handlerMap )
        {

            if (handlerMap.size() != lastCnt)
            {
                lastCnt = handlerMap.size();
                Log.debug("Open StoragePoolHandlerContexts " + lastCnt);
            }

            Set<Entry<SearchWrapper,SearchContext>> vals = handlerMap.entrySet();
            for (Entry<SearchWrapper,SearchContext> entry : vals)
            {
                if (entry.getKey().getWrapperIdx() == wrIdx)
                {
                    return entry.getKey();
                }
            }
        }
        return null;
    }    
}
