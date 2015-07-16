/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.backup.hotfolder.MMImportManager;
import de.dimm.vsm.fsengine.FSEIndexer;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMFSInputStream;
import de.dimm.vsm.fsengine.fixes.FixBootstrapEntries;
import de.dimm.vsm.jobs.JobEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.lifecycle.NodeMigrationManager;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.preview.IPreviewData;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HotFolder;
import de.dimm.vsm.records.MessageLog;
import de.dimm.vsm.records.MountEntry;
import de.dimm.vsm.records.Role;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.recovery.RecoveryManager;
import de.dimm.vsm.tasks.TaskEntry;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class GuiServerApiImpl implements GuiServerApi
{
    long loginTime;
    long loginIdx;
    Role role;
    User user;
    LogicControl control;

    
    
    public GuiServerApiImpl( long loginTime, Role role, User user)
    {
        this.loginTime = loginTime;
        this.role = role;
        this.user = user;
        this.loginIdx = -1;
        control = Main.get_control();
    }

    public void clear()
    {
    }

    public long getLoginIdx() {
        return loginIdx;
    }

    public void setLoginIdx( long loginIdx ) {
        this.loginIdx = loginIdx;
    }
    
        

    @Override
    public boolean abortBackup( final Schedule sched )
    {
        return control.abortScheduler(sched);

    }

    public void mountVolume( AgentApiEntry apiEntry, final StoragePoolWrapper poolWrapper, final String drive ) throws IOException
    {
        InetAddress adr = Main.getServerAddress();

        try
        {
            Boolean ret = apiEntry.getApi().mountVSMFS(adr, Main.getServerPort(), poolWrapper/*, timestamp, subPath, user*/, drive);
            
            poolWrapper.setPhysicallyMounted(ret);

            Log.debug("Mount finished " + ret.toString());
        }
        catch (Exception exc)
        {
            String txt =  exc.getMessage();
            if (exc instanceof UndeclaredThrowableException) 
            {
                UndeclaredThrowableException ute = (UndeclaredThrowableException)exc;
                txt = ute.getUndeclaredThrowable().getMessage();
            }
            Log.err(Main.Txt("Mount schlug fehl"), drive, exc);
            throw new IOException( Main.Txt("Mount schlug fehl") + ": " + drive + ": " + txt);
        }
    }

    @Override
    public StoragePoolWrapper mountVolume( final String agentIp, final int agentPort, final StoragePoolWrapper poolWrapper, final String drive ) throws IOException
    {
        AgentApiEntry apiEntry = null;
        try
        {
            apiEntry = LogicControl.getApiEntry(agentIp, agentPort);
            if (!apiEntry.isOnline())
            {
                throw new IOException( Main.Txt("Mount schlug fehl, Agent ist nicht erreichbar") + ": " + agentPort + ":" + agentIp);
            }

            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
            contextMgr.updateContext( poolWrapper, agentIp, agentPort, drive );


            InetAddress adr = Main.getServerAddress();
            mountVolume( apiEntry, poolWrapper, drive );

            return poolWrapper;
        }
        catch (Exception exc)
        {
            Log.err(Main.Txt("Mount schlug fehl"), drive, exc);
            throw new IOException( Main.Txt("Mount schlug fehl") + ": " + drive + ": " + exc.getMessage());
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }
    }

    @Override
    public StoragePoolWrapper mountVolume( final String agentIp, final int agentPort, final StoragePool pool, final Date timestamp, final String subPath, final User user, final String drive )
    {
        /*  Thread thr = new Thread(new Runnable()
        {192.168.1.160

        @Override
        public void run()
        {*/
        AgentApiEntry apiEntry = null;
        try
        {
            apiEntry = LogicControl.getApiEntry(agentIp, agentPort);
            if (!apiEntry.isOnline())
            {
                throw new IOException( Main.Txt("Mount schlug fehl, Agent ist nicht erreichbar") + ": " + agentPort + ":" + agentIp);
            }
            
            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();

            StoragePoolQry qry = StoragePoolQry.createTimestampStoragePoolQry(user, timestamp.getTime());
            final StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper(agentIp, agentPort,
                    pool, qry, subPath, drive);

            poolWrapper.setCloseOnUnmount(true);

            mountVolume( apiEntry, poolWrapper, drive );

            return poolWrapper;
        }
        catch (Exception exc)
        {
            Log.err(Main.Txt("Mount schlug fehl"), drive, exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }

        return null;
        /*        }
        });
        thr.start();
        return true;*/
    }

    @Override
    public boolean unmountVolume( StoragePoolWrapper poolWrapper )
    {
        boolean ret = false;
        AgentApiEntry apiEntry = null;
        try
        {
            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();

            String agentIp = contextMgr.getAgentIPbyWrapper(poolWrapper);
            int agentPort = contextMgr.getPortbyWrapper(poolWrapper);

            apiEntry = LogicControl.getApiEntry(agentIp, agentPort);

            InetAddress adr = Main.getServerAddress();

            ret = apiEntry.getApi().unmountVSMFS(adr, Main.getServerPort(), poolWrapper);

            if (poolWrapper.isCloseOnUnmount())
            {
                contextMgr.removePoolWrapper(poolWrapper);
            }

        }
        catch (Exception exc)
        {
            Log.err(Main.Txt("UnMount schlug fehl"), exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }


        return ret;
    }

    @Override
    public boolean remountVolume( StoragePoolWrapper poolWrapper )
    {
        boolean ret = false;
        AgentApiEntry apiEntry = null;
        try
        {
            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();

            String agentIp = contextMgr.getAgentIPbyWrapper(poolWrapper);
            int agentPort = contextMgr.getPortbyWrapper(poolWrapper);
            String drive = contextMgr.getDrivebyWrapper(poolWrapper);

            apiEntry = LogicControl.getApiEntry(agentIp, agentPort);

            InetAddress adr = Main.getServerAddress();

            Boolean mret = apiEntry.getApi().mountVSMFS(adr, Main.getServerPort(), poolWrapper,/*, timestamp, subPath, user, */ drive);

            poolWrapper.setPhysicallyMounted(mret);

            Log.debug("ReMount abgeschlossen", mret.toString());

            return true;
        }
        catch (Exception exc)
        {            
            Log.err("ReMount wurde abgebrochen", exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }

        return ret;
    }

    @Override
    public StoragePoolWrapper getMounted( String agentIp, int agentPort, StoragePool pool )
    {
        boolean ret = false;
        AgentApiEntry apiEntry = null;
        try
        {
            apiEntry = LogicControl.getApiEntry(agentIp, agentPort, /*withMsg*/ false);

            InetAddress adr = Main.getServerAddress();

            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();

            List<StoragePoolWrapper> poolWrappers = contextMgr.getPoolWrappers(agentIp, agentPort, pool);
            if (poolWrappers.isEmpty())
            {
                return null;
            }

            for (int i = 0; i < poolWrappers.size(); i++)
            {
                StoragePoolWrapper storagePoolWrapper = poolWrappers.get(i);
                boolean uret = apiEntry.getApi().isMountedVSMFS(adr, Main.getServerPort(), storagePoolWrapper);
                storagePoolWrapper.setPhysicallyMounted(uret);
                return storagePoolWrapper;
            }
        }
        catch (Exception exc)
        {
            Log.err("GetMounted wurde abgebrochen", exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }
        return null;
    }

    @Override
    public boolean unmountAllVolumes()
    {
        boolean ret = true;
        AgentApiEntry apiEntry = null;
        try
        {
            InetAddress adr = Main.getServerAddress();

            StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();

            List<StoragePoolWrapper> poolWrappers = contextMgr.getPoolWrappers();
            if (poolWrappers.isEmpty())
            {
                return ret;
            }

            for (int i = 0; i < poolWrappers.size(); i++)
            {
                StoragePoolWrapper storagePoolWrapper = poolWrappers.get(i);

                String agentIp = contextMgr.getAgentIPbyWrapper(storagePoolWrapper);
                int agentPort = contextMgr.getPortbyWrapper(storagePoolWrapper);

                apiEntry = LogicControl.getApiEntry(agentIp, agentPort);

                boolean uret = apiEntry.getApi().unmountVSMFS(adr, Main.getServerPort(), storagePoolWrapper);
                if (uret)
                {
                    ret = false;
                }

                contextMgr.removePoolWrapper(storagePoolWrapper);
            }
        }
        catch (Exception exc)
        {
            Log.err("UnMount wurde abgebrochen", exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }

        return ret;
    }
    @Override
    public StoragePoolWrapper openPoolView( StoragePool pool, StoragePoolQry qry, String subPath )
    {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();        
        final StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper("", 0, pool, qry, subPath, "");
        return poolWrapper;
    }


    @Override
    public StoragePoolWrapper openPoolView( StoragePool pool, StoragePoolQry qry, FileSystemElemNode node )
    {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();        
        final StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper("", 0, pool, qry, node, "");
        return poolWrapper;
    }
    
    

    @Override
    public List<RemoteFSElem> listDir( IWrapper wrapper, RemoteFSElem path ) throws SQLException
    {
        return control.getPoolHandlerServlet().get_child_nodes(wrapper, path);
    }

    @Override
    public void closePoolView( StoragePoolWrapper wrapper )
    {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
        try
        {
            control.getPoolHandlerServlet().closeTransactions(wrapper);
        }
        catch (SQLException sQLException)
        {
        }
        contextMgr.removePoolWrapper(wrapper);
    }

    @Override
    public boolean removeFSElem( IWrapper wrapper, RemoteFSElem path ) throws PoolReadOnlyException, SQLException
    {
        return control.getPoolHandlerServlet().removeFSElem(wrapper, path);
    }
    @Override
    public boolean undeleteFSElem( IWrapper wrapper, RemoteFSElem path ) throws PoolReadOnlyException, SQLException
    {
        return control.getPoolHandlerServlet().undeleteFSElem(wrapper, path);
    }
    @Override
    public boolean deleteFSElem( IWrapper wrapper, RemoteFSElem path ) throws PoolReadOnlyException, SQLException
    {
        return control.getPoolHandlerServlet().deleteFSElem(wrapper, path);
    }

    @Override
    public String checkRestoreErrFSElem( IWrapper wrapper, RemoteFSElem path )  {
        return control.getPoolHandlerServlet().checkRestoreErrFSElem(wrapper, path);
    }

    @Override
    public boolean restoreFSElem( IWrapper wrapper, RemoteFSElem path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws PoolReadOnlyException, SQLException, IOException
    {
        List<RemoteFSElem> list = new ArrayList<>();
        list.add(path);
        return control.getPoolHandlerServlet().restoreFSElem(wrapper, list, targetIP, targetPort, targetPath, flags, user);
    }
    @Override
    public boolean restoreVersionedFSElem( IWrapper wrapper, RemoteFSElem path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws PoolReadOnlyException, SQLException, IOException
    {
        List<RemoteFSElem> list = new ArrayList<>();
        list.add(path);
        return control.getPoolHandlerServlet().restoreVersionedFSElem(wrapper, list, targetIP, targetPort, targetPath, flags, user);
    }
    @Override
    public boolean restoreFSElems( IWrapper wrapper, List<RemoteFSElem> paths, String targetIP, int targetPort, String targetPath, int flags, User user ) throws PoolReadOnlyException, SQLException, IOException
    {
        return control.getPoolHandlerServlet().restoreFSElem(wrapper, paths, targetIP, targetPort, targetPath, flags, user);
    }
    @Override
    public boolean restoreVersionedFSElems( IWrapper wrapper, List<RemoteFSElem> paths, String targetIP, int targetPort, String targetPath, int flags, User user ) throws PoolReadOnlyException, SQLException, IOException
    {
        return control.getPoolHandlerServlet().restoreVersionedFSElem(wrapper, paths, targetIP, targetPort, targetPath, flags, user);
    }
    
    @Override
    public boolean isWrapperValid( IWrapper wrapper)
    {
        return StoragePoolHandlerServlet.getPoolHandlerByWrapper(wrapper) != null;        
    }

    @Override
    public FileSystemElemNode createFileSystemElemNode( StoragePool pool, String path, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
        StoragePoolQry qry = StoragePoolQry.createActualRdWrStoragePoolQry(User.createSystemInternal(), false);
        final StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper("", 0, pool, qry, "", "" );
        StoragePoolHandler handler = contextMgr.getHandlerbyWrapper(poolWrapper);
        FileSystemElemNode node = handler.create_fse_node_complete(path, type);

        return node;
    }
    @Override
    public FileSystemElemNode createFileSystemElemNode( StoragePoolWrapper poolWrapper, String path, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
        StoragePoolHandler handler = contextMgr.getHandlerbyWrapper(poolWrapper);
        FileSystemElemNode node = handler.create_fse_node_complete(path, type);

        return node;
    }

    @Override
    public List<ScheduleStatusEntry> listSchedulerStats()
    {
        return control.listSchedulerStats();
    }

    @Override
    public Properties getAgentProperties( String ip, int port, boolean withMsg )
    {
        AgentApiEntry apiEntry = null;
        try
        {
            apiEntry = LogicControl.getApiEntry(ip, port);
            Properties p = apiEntry.getApi().get_properties();
            return p;
        }
        catch (Exception exc)
        {
            if (withMsg)
                Log.warn("AgentProperties lesen schlug fehl", exc);
        }
        finally
        {
            try
            {
                if (apiEntry != null)
                    apiEntry.close();
            }
            catch (Exception exception)
            {
            }
        }

        return null;
    }

    @Override
    public SearchWrapper search(  StoragePool pool, ArrayList<SearchEntry> slist )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        final SearchWrapper wrapper = contextMgr.search( user, pool, slist, 0);
        return wrapper;

    }

    @Override
    public SearchWrapper search(  StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        final SearchWrapper wrapper = contextMgr.search( user, pool, slist, max);
        return wrapper;
    }

    @Override
    public SearchWrapper searchJob(  StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        final SearchWrapper wrapper = contextMgr.searchJob( user, pool, slist, max);
        return wrapper;
    }

    @Override
    public List<RemoteFSElem> getSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.getSearchResult( wrapper, start, limit );
    }
    @Override
    public List<ArchiveJob> getJobSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.getJobSearchResult( wrapper, start, limit );
    }

    @Override
    public SearchStatus getSearchStatus( SearchWrapper wrapper )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.getSearchStatus( wrapper );
    }

    @Override
    public void closeSearch( SearchWrapper wrapper )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        contextMgr.closeSearch( wrapper );
    }
    @Override
    public List<RemoteFSElem> listSearchDir( SearchWrapper wrapper, RemoteFSElem path ) throws SQLException
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.get_child_nodes(wrapper, path);
    }

//    @Override
//    public boolean restoreFSElem( IWrapper wrapper, RemoteFSElem path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
//    {
//        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
//        List<RemoteFSElem> list = new ArrayList<RemoteFSElem>();
//        list.add(path);
//
//        return contextMgr.restoreFSElem(wrapper, list, targetIP, targetPort, targetPath, flags, user);
//    }
//    @Override
//    public boolean restoreFSElems( IWrapper wrapper, List<RemoteFSElem> paths, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
//    {
//        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
//
//        return contextMgr.restoreFSElem(wrapper, paths, targetIP, targetPort, targetPath, flags, user);
//    }

    @Override
    public boolean restoreJob( SearchWrapper searchWrapper, ArchiveJob job, String targetIP, int targetPort, String targetPath, int rflags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.restoreJob(searchWrapper, job, targetIP, targetPort, targetPath, rflags, user);
    }

    @Override
    public boolean removeJob( SearchWrapper searchWrapper, ArchiveJob job ) throws  SQLException, PoolReadOnlyException
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.removeJob(searchWrapper, job);
    }



    @Override
    public StoragePoolWrapper mountVolume( String ip, int port, SearchWrapper searchWrapper, User user, String drive )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        
        final StoragePoolWrapper poolWrapper = contextMgr.createPoolWrapper(ip, port, searchWrapper, user, drive);
        

        // OURSELF
        InetAddress adr = Main.getServerAddress();

        try
        {
            AgentApiEntry apiEntry = LogicControl.getApiEntry(ip, port);

            Boolean ret = apiEntry.getApi().mountVSMFS(adr, Main.getServerPort(), poolWrapper/*, timestamp, subPath, user*/, drive);
            if (ret)
            {

                poolWrapper.setPhysicallyMounted(ret);

                Log.info("Mount abgeschlossen", ip + ":" + port + "/" + drive);

                return poolWrapper;
            }
            else
            {
                Log.err("Mount schlug fehl");
            }
        }
        catch (Exception exc)
        {
             Log.err("Mount wurde abgebrochen", exc);
        }
        control.getPoolHandlerServlet().getContextManager().removePoolWrapper(poolWrapper);
       
        return null;
    }

    @Override
    public StoragePoolWrapper getMounted( String ip, int port, SearchWrapper searchWrapper )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(searchWrapper);
        if (sp_handler != null)
        {
            return getMounted(ip, port, sp_handler.getPool());
        }
        return null;
    }

    @Override
    public void reSearch( SearchWrapper searchWrapper, ArrayList<SearchEntry> slist )
    {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(searchWrapper);
        try
        {
            contextMgr.reSearch(searchWrapper, sp_handler.getPool(), slist);
        }
        catch (Exception exc)
        {
            // TODO IN TRUNK, TAG 1.0 FINISHES HERE
        }
    }

    @Override
    public JobEntry[] listJobs(User user)
    {
        JobEntry[] arr = control.getJobManager().getJobArray(user);
        return arr;
    }

    @Override
    public TaskEntry[] listTasks()
    {
        TaskEntry[] arr = control.getTaskArray();
        return arr;
    }


    @Override
    public void emptyNode( AbstractStorageNode node, User user ) throws SQLException
    {
        JobManager jm = control.getJobManager();
        jm.addJobEntry( NodeMigrationManager.createEmptyJob(node, user));
    }

    @Override
    public void moveNode( AbstractStorageNode node, AbstractStorageNode toNode, User user )
    {
        JobManager jm = control.getJobManager();
        jm.addJobEntry( NodeMigrationManager.createMoveJob(node, toNode, user));
    }

    @Override
    public boolean startBackup( final Schedule sched, User user ) throws Exception
    {
        JobManager jm = control.getJobManager();
        if (jm.isBackupRunning(sched))
        {
            throw new IOException(Main.Txt("Schedule ist bereits gestartet"));
        }
        jm.addJobEntry( Backup.createbackupJob(sched, user));
        return true;
    }


    public User getUser()
    {
        return user;
    }

    @Override
    public MessageLog[] listLogs( int cnt, long offsetIdx, LogQuery lq )
    {
        MessageLog[] ret =  control.listLogs( cnt, offsetIdx,  lq );
        return ret;
    }

    @Override
    public MessageLog[] listLogsSince( long idx, LogQuery lq )
    {
        MessageLog[] ret =  control.listLogsSinceIdx( idx,  lq );
        return ret;
    }
    @Override
    public long getLogCounter()
    {
        long ret =  control.getLogCounter();
        return ret;
    }

//    @Override
//    public InputStream openStream( SearchWrapper wrapper, RemoteFSElem path )
//    {
//        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
//        StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(wrapper);
//        VSMFSInputStream is = new VSMFSInputStream(sp_handler, path.getIdx());
//        return is;
//    }

    @Override
    public InputStream openStream( IWrapper wrapper, RemoteFSElem path )
    {
        StoragePoolHandler sp_handler = control.getPoolHandlerServlet().getPoolHandlerByWrapper(wrapper);
        if (sp_handler == null)
        {
            return null;
        }
        
        VSMFSInputStream is = new VSMFSInputStream(sp_handler, path.getIdx(), path.getAttrIdx());
        return is;
    }

    @Override
    public String resolvePath( IWrapper wrapper, RemoteFSElem path ) throws SQLException, PathResolveException
    {
        StoragePoolHandler sp_handler = control.getPoolHandlerServlet().getPoolHandlerByWrapper(wrapper);
        if (path.getIdx() >= 0)
        {
            FileSystemElemNode node = sp_handler.resolve_fse_node_from_db(path.getIdx());
            if (node != null)
            {
                StringBuilder sb = new StringBuilder();
                sp_handler.build_virtual_path(node, sb);
                return sb.toString();
            }
        }
        return "/_NO_POOL_/";
    }

//    @Override
//    public String resolvePath( StoragePoolWrapper wrapper, RemoteFSElem path ) throws SQLException, PathResolveException
//    {
//        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
//        StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(wrapper);
//        if (path.getIdx() >= 0)
//        {
//            FileSystemElemNode node = sp_handler.resolve_fse_node_from_db(path.getIdx());
//            if (node != null)
//            {
//                StringBuilder sb = new StringBuilder();
//                sp_handler.build_virtual_path(node, sb);
//                return sb.toString();
//            }
//        }
//        return "Unknown";
//    }

    @Override
    public boolean importMMArchiv( HotFolder node, long fromIdx, long tillIdx, boolean withOldJobs, User user ) throws IOException
    {
        JobManager jm = control.getJobManager();
        if (jm.isImportRunning(node))
        {
            throw new IOException(Main.Txt("Import ist bereits gestartet"));
        }
        jm.addJobEntry( MMImportManager.createImportJob(node, fromIdx, tillIdx, withOldJobs, user));
        return true;
    }

    @Override
    public void updateReadIndex( StoragePool pool )
    {
        FSEIndexer fse = LogicControl.getStorageNubHandler().getIndexer(pool);
        fse.updateReadIndex();
    }

    @Override
    public void syncNode( AbstractStorageNode t, AbstractStorageNode cloneNode, User user  ) throws SQLException, IOException
    {
        JobManager jm = control.getJobManager();
        jm.addJobEntry( NodeMigrationManager.createSyncNodeJob(t, cloneNode, user));
    }

    @Override
    public boolean isBusyNode( AbstractStorageNode node )
    {
        JobManager jm = control.getJobManager();
        return jm.isMigrationJobBusy( node );
    }

    @Override
    public boolean initNode( AbstractStorageNode node, User user )
    {
        return StorageNodeHandler.initNode(node);
    }

    @Override
    public void initCheck(User user, String checkName, Object arg, Object optArg) {
        
        JobManager jm = control.getJobManager();
        JobInterface job = control.getCheckManager().createCheckJob(checkName, arg, optArg, user);
        if (job != null ) {
            jm.addJobEntry( job);      
        }
          
    }

    @Override
    public List<String> getCheckNames(Class<?> clazz) {
        return control.getCheckManager().getCheckNames(clazz);
    }

    @Override
    public List<MountEntry> getMountedMountEntries()
    {
        return control.getAutoMountManager().getMountList();
    }
    @Override
    public List<MountEntry> getAllMountEntries()
    {
        return control.getAutoMountManager().getMountEntryList();
    }

    @Override
    public StoragePoolWrapper mountEntry( User user, MountEntry mountEntry ) throws IOException
    {
        return control.getAutoMountManager().mountEntry( user, this, mountEntry );
    }

    @Override
    public void unMountEntry( MountEntry mountEntry )
    {
        control.getAutoMountManager().unMountEntry( this, mountEntry );
    }

    @Override
    public Properties getProperties()
    {
        Properties props = new Properties();
        props.setProperty("Version", Main.get_version_str());
        return props;
    }

    @Override
    public void scanDatabase(  User user, AbstractStorageNode node )
    {        
        JobInterface ji = RecoveryManager.createJobInterface(user, node);
        Main.get_control().getJobManager().addJobEntry(ji);
    }
    @Override
    public void rebuildBootstraps( User user, StoragePool pool )
    {
        JobInterface ji = FixBootstrapEntries.createFixJobInterface(user, pool);
        Main.get_control().getJobManager().addJobEntry(ji);
    }

    @Override
    public List<RemoteFSElem> listVersions( IWrapper wrapper, RemoteFSElem path ) throws SQLException, IOException
    {
        StoragePoolHandler sp_handler = StoragePoolHandlerServlet.getPoolHandlerByWrapper(wrapper);
        return control.getPoolHandlerServlet().get_versions(sp_handler, path);
    }

    @Override
    public List<IPreviewData> getPreviewData( IWrapper wrapper, List<RemoteFSElem> path ) throws SQLException, IOException {
        StoragePoolHandler sp_handler = StoragePoolHandlerServlet.getPoolHandlerByWrapper(wrapper);
        
        return sp_handler.getPreviewData( path );
    }

    @Override
    public int createWebDavServer( StoragePoolWrapper wrapper ) throws IOException, PoolReadOnlyException, PathResolveException {
        StoragePoolHandlerContextManager contextMgr = control.getPoolHandlerServlet().getContextManager();
        return contextMgr.createWebDavServer(wrapper, getLoginIdx());        
    }    
    @Override
    public int createWebDavSearchServer( SearchWrapper wrapper ) throws IOException, PoolReadOnlyException, PathResolveException {
        SearchContextManager contextMgr = control.getPoolHandlerServlet().getSearchContextManager();
        return contextMgr.createWebDavServer(wrapper, getLoginIdx());        
    }    
}
