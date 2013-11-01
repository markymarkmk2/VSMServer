/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

class StoragePoolHandlerContext
{
    StoragePoolHandler handler;
    String drive;
    String agentIp;
    int port;
    Date lastUsage;

    public StoragePoolHandlerContext( StoragePoolHandler handler, String drive, String agentIp, int port )
    {
        this.handler = handler;
        this.drive = drive;
        this.agentIp = agentIp;
        this.port = port;
        lastUsage = new Date();
    }
    public void touch()
    {
        lastUsage = new Date();
    }
    public boolean isExpired()
    {
        return (System.currentTimeMillis() - lastUsage.getTime() > StoragePoolHandlerContextManager.EXPIRE_MS);
    }
    void closePoolHandler()
    {
        handler.close_entitymanager();
    }
}

/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerContextManager extends WorkerParent
{
    final HashMap<StoragePoolWrapper, StoragePoolHandlerContext> handlerMap;

    public static long EXPIRE_MS = 3600*1000l;  // 1h

    public StoragePoolHandlerContextManager()
    {
        super("StoragePoolHandlerContextManager");
        handlerMap = new HashMap<StoragePoolWrapper, StoragePoolHandlerContext>();

        // TODO : BACKGROUND TASK TO CLEANUP UNUSED ENTRIES
    }

    @Override
    public boolean isVisible()
    {
        return false;
    }



    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, boolean rdonly, boolean showDeleted, String subPath, User user, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = new StoragePoolQry(user, rdonly, -1, showDeleted);
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, true);
                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, qry);

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
    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, long timestamp, boolean rdonly, boolean showDeleted, FileSystemElemNode node, User user, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = new StoragePoolQry(user, rdonly, timestamp, showDeleted);
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
    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, long timestamp, boolean rdonly, boolean showDeleted, String subPath, User user, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = new StoragePoolQry(user, rdonly, timestamp, showDeleted);
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, true);
                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, qry);
                if (handler.isInsideMappingDir( subPath ))
                {
                    subPath = handler.resolveMappingDir(subPath);
                }
                // ALL FOLLOWING REQUESTS ARE IN REAL VSM-NAMESPACE
                qry.setUseMappingFilter(false);
                FileSystemElemNode node = handler.resolve_node( subPath );
                // Bei schreibenden Mounts das Erstellen des VSM-Pfads zulassen
                if (node == null && !rdonly)
                {
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
            catch (Exception iOException)
            {
                Log.err(Main.Txt("Abbruch in createPoolWrapper") , iOException);
            }
            return null;
        }
    }
    public StoragePoolWrapper createPoolWrapper( String agentIp, int port, StoragePool pool, long ts, String subPath, User user, String drive )
    {
        synchronized (handlerMap)
        {
            try
            {
                long newIdx = handlerMap.size();
                StoragePoolQry qry = new StoragePoolQry(user, true, ts, false);
                StoragePoolWrapper w = new StoragePoolWrapper(newIdx, pool.getIdx(), agentIp, port, qry, true);
                StoragePoolHandler handler = StoragePoolHandlerFactory.createStoragePoolHandler( pool, qry);
                if (handler.isInsideMappingDir( subPath ))
                {
                    subPath = handler.resolveMappingDir(subPath);
                }
                // ALL FOLLOWING REQUESTS ARE IN REAL VSM-NAMESPACE
                qry.setUseMappingFilter(false);

                handler.em_refresh(pool.getRootDir());

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

        ArrayList<StoragePoolWrapper> list = new ArrayList<StoragePoolWrapper>();
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
        ArrayList<StoragePoolWrapper> list = new ArrayList<StoragePoolWrapper>();
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
        ArrayList<StoragePoolWrapper> list = new ArrayList<StoragePoolWrapper>();
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

    int lastCnt = -1;

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
                    break;
                }
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

}
