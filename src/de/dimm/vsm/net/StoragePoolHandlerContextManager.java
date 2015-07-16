/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.FileSystemElemNode;
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
public class StoragePoolHandlerContextManager extends WorkerParent
{
    final Map<StoragePoolWrapper, StoragePoolHandlerContext> handlerMap;
    final Map<Long,NetServer> webDavServerMap;

    private static final int WEB_DAV_USER_PORT_BASE = 58080;
    public static long EXPIRE_MS = 3600*1000l;  // 1h
    private int maxHandlers = 100;
    int lastCnt = -1;

    public StoragePoolHandlerContextManager()
    {
        super("StoragePoolHandlerContextManager");
        handlerMap = new HashMap<>();
        webDavServerMap = new HashMap<>();
        
        maxHandlers = Main.get_int_prop(GeneralPreferences.MAX_OPEN_POOLHANDLERS, maxHandlers);       
        
    }

    @Override
    public boolean isVisible()
    {
        return false;
    }

    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, StoragePoolQry qry, FileSystemElemNode node, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();                
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, true);
                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, qry);
                handler.setPathResolver( new NodePathResolver(node, handler));
                handler.em_refresh(node);

                StoragePoolHandlerContext context = new StoragePoolHandlerContext(handler, drive, agentIp, port);
                handlerMap.put(w, context);
                return w;
            }
            catch (Exception iOException)
            {
                Log.err(Main.Txt("Abbruch in createPoolWrapper") , iOException);
            }
            return null;
        }
    }
    
    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, StoragePoolQry qry, String subPath, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                //StoragePoolQry qry = new StoragePoolQry(user, rdonly, timestamp, showDeleted);
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, true);
                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, qry);
                if (handler.isInsideMappingDir( subPath ))
                {
                    subPath = handler.resolveMappingDir(subPath);
                }
                
                FileSystemElemNode node = handler.resolve_node( subPath );
                // Bei schreibenden Mounts das Erstellen des VSM-Pfads zulassen
                if (node == null && !qry.isReadOnly())
                {
                    boolean mf = qry.isUseMappingFilter();
                    // ALL FOLLOWING REQUESTS ARE IN REAL VSM-NAMESPACE
                    qry.setUseMappingFilter(false);
                    FileSystemElemNode parent = handler.resolve_parent_dir_node( subPath);
                    // Fehlende Parent-Ordner erzeugen
                    if (parent == null && !subPath.equals( "/"))
                    {
                        parent = handler.create_parent_dir_node(subPath);
                    }
                    // Ordner selber erzeugen (darf auch root-Path sein!)
                    if (parent != null || subPath.equals( "/"))
                    {
                        handler.create_fse_node_complete( subPath, FileSystemElemNode.FT_DIR);
                    }
                    qry.setUseMappingFilter(mf);
                    node = handler.resolve_node( subPath );    
                    if (node == null)
                    {
                        throw new IOException("VSM-Pfad kann nicht angelegt werden: " + subPath );                        
                    }
                }
                handler.setPathResolver( new NodePathResolver(node, handler));
                handler.em_refresh(node);

                StoragePoolHandlerContext context = new StoragePoolHandlerContext(handler, drive, agentIp, port);
                handlerMap.put(w, context);
                return w;
            }
            catch (IOException | SQLException | PoolReadOnlyException | PathResolveException iOException)
            {
                Log.err(Main.Txt("Abbruch in createPoolWrapper") , iOException);
            }
            return null;
        }
    }

    public StoragePoolWrapper createPoolWrapper( StoragePoolHandler handler, String agentIp, int port, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = handler.getPoolQry();
                StoragePool pool = handler.getPool();
                handler.em_refresh(pool.getRootDir());
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, false);


                StoragePoolHandlerContext context = new StoragePoolHandlerContext(handler, drive, agentIp, port);
                handlerMap.put(w, context);
                Log.debug( "Adding StoragePoolWrapper " + w.toString());
                return w;
            }
            catch (Exception iOException)
            {
                Log.err(Main.Txt("Abbruch in createPoolWrapper") , iOException);
            }
            return null;
        }
    }
    public void removePoolWrapper( StoragePoolWrapper poolWrapper )
    {
        synchronized (handlerMap)
        {
            Log.debug( "Removing StoragePoolWrapper " + poolWrapper.toString());
            StoragePoolHandlerContext ctx = handlerMap.remove(poolWrapper);
            if (ctx != null && poolWrapper.isPoolHandlerCreated())
            {
                ctx.closePoolHandler();
                webDavServerMap.remove(poolWrapper.getWrapperIdx());
            }
        }
    }

    public StoragePoolHandler getHandlerbyWrapper( StoragePoolWrapper pool )
    {
        synchronized (handlerMap)
        {
            StoragePoolHandlerContext context = handlerMap.get(pool);
            if (context != null) 
            {
                context.touch();
                return context.handler;
            }
        }
        return null;
    }
    public String getAgentIPbyWrapper( StoragePoolWrapper pool )
    {
        synchronized (handlerMap)
        {
            StoragePoolHandlerContext context = handlerMap.get(pool);
            if (context != null)
            {
                context.touch();
                return context.agentIp;
            }
        }
        return null;
    }
    public int getPortbyWrapper( StoragePoolWrapper pool )
    {
        synchronized (handlerMap)
        {
            StoragePoolHandlerContext context = handlerMap.get(pool);
            if (context != null)
            {
                context.touch();
                return context.port;
            }
        }
        return -1;
    }

    public String getDrivebyWrapper( StoragePoolWrapper pool )
    {
        synchronized (handlerMap)
        {
            StoragePoolHandlerContext context = handlerMap.get(pool);
            if (context != null)
            {
                context.touch();
                return context.drive;
            }
        }
        return null;
    }
    public List<StoragePoolWrapper> getPoolWrappers( String agentIp, int port )
    {

        ArrayList<StoragePoolWrapper> list = new ArrayList<>();
        synchronized (handlerMap)
        {
        Set<Entry<StoragePoolWrapper,StoragePoolHandlerContext>> vals = handlerMap.entrySet();
        for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
        {
            StoragePoolHandlerContext storagePoolHandlerContext = entry.getValue();

            if (storagePoolHandlerContext.agentIp.equals(agentIp) && storagePoolHandlerContext.port == port)
            {
                list.add(entry.getKey());
            }

        }
        }
        return list;
    }
    public List<StoragePoolWrapper> getPoolWrappers( String agentIp, int port, StoragePool pool )
    {
        ArrayList<StoragePoolWrapper> list = new ArrayList<>();
        synchronized (handlerMap)
        {
        Set<Entry<StoragePoolWrapper,StoragePoolHandlerContext>> vals = handlerMap.entrySet();
        for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
        {
            StoragePoolHandlerContext storagePoolHandlerContext = entry.getValue();

            if (storagePoolHandlerContext.agentIp.equals(agentIp)
                    && storagePoolHandlerContext.port == port
                    && storagePoolHandlerContext.handler.getPool().getIdx() == pool.getIdx())
            {
                list.add(entry.getKey());
            }

        }
        }
        return list;
    }
    public List<StoragePoolWrapper> getPoolWrappers()
    {
        ArrayList<StoragePoolWrapper> list = new ArrayList<>();
        synchronized (handlerMap)
        {
        Set<Entry<StoragePoolWrapper,StoragePoolHandlerContext>> vals = handlerMap.entrySet();
        for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
        {
            list.add(entry.getKey());
        }
        }
        return list;
    }


    @Override
    public boolean initialize()
    {
        return true;
    }

    @Override
    public  void run()
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
                Log.debug("Open StoragePoolHandlerContexts " + lastCnt);
            }

            Set<Entry<StoragePoolWrapper,StoragePoolHandlerContext>> vals = handlerMap.entrySet();
            for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
            {
                if (entry.getValue().isExpired())
                {
                    Log.debug(Main.Txt("Entferne abgelaufenen StoragePoolContext")  + " " + entry.getKey().qry.getUser());
                    entry.getValue().closePoolHandler();
                    handlerMap.remove(entry.getKey());
                    webDavServerMap.remove(entry.getKey().getWrapperIdx());       
                    break;
                }
            }
            
            while (handlerMap.size() > maxHandlers)
            {
                Entry<StoragePoolWrapper,StoragePoolHandlerContext> oldestEntry = null;
                for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
                {
                    if (oldestEntry == null || oldestEntry.getValue().getTs() > entry.getValue().getTs())
                    {
                        oldestEntry = entry;
                    }
                }
                if (oldestEntry == null)
                    break;
                
                Log.debug("StoragePoolWrapper is removed because HandlerMap is full " + handlerMap.size() + "/" + maxHandlers + ": " + oldestEntry.getKey().qry.getUser());
                oldestEntry.getValue().closePoolHandler();
                handlerMap.remove(oldestEntry.getKey());
                webDavServerMap.remove(oldestEntry.getKey().getWrapperIdx());  
            }
        }
    }
    
    public void touch( StoragePoolWrapper poolWrapper)
    {
        synchronized( handlerMap )
        {
            StoragePoolHandlerContext ctx = handlerMap.get(poolWrapper);
            if (ctx != null)
            {
                ctx.touch();
            }
        }        
    }
    
    void updateContext( StoragePoolWrapper poolWrapper, String agentIp, int agentPort, String drive )
    {
        synchronized( handlerMap )
        {
            if (handlerMap.size() != lastCnt)
            {
                lastCnt = handlerMap.size();
                Log.debug("Open StoragePoolHandlerContexts " + lastCnt);
            }

            StoragePoolHandlerContext ctx = handlerMap.get(poolWrapper);
            if (ctx != null)
            {
                ctx.agentIp = agentIp;
                ctx.port = agentPort;
                ctx.drive = drive;
            }
        }
    }

    public StoragePoolWrapper getPoolWrapper( long wrIdx ) {
        synchronized( handlerMap )
        {

            if (handlerMap.size() != lastCnt)
            {
                lastCnt = handlerMap.size();
                Log.debug("Open StoragePoolHandlerContexts " + lastCnt);
            }

            Set<Entry<StoragePoolWrapper,StoragePoolHandlerContext>> vals = handlerMap.entrySet();
            for (Entry<StoragePoolWrapper, StoragePoolHandlerContext> entry : vals)
            {
                if (entry.getKey().getWrapperIdx() == wrIdx)
                {
                    return entry.getKey();
                }
            }
        }
        return null;
    }
    
    public int createWebDavServer( StoragePoolWrapper wrapper, long loginIdx) throws IOException {
        synchronized( handlerMap )
        {
            StoragePoolHandlerContext ctx = handlerMap.get(wrapper);
            int webDavPort = Main.get_int_prop(GeneralPreferences.WEB_DAV_PORT, WEB_DAV_USER_PORT_BASE) + (int)(wrapper.getWrapperIdx());       
            
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
}
