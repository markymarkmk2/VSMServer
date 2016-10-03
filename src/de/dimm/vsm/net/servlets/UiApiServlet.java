/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Deserializer;
import com.caucho.hessian.io.HessianProtocolException;
import com.caucho.hessian.io.Serializer;
import com.caucho.hessian.server.HessianServlet;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.JDBCLazyList;
import de.dimm.vsm.hessian.FileSystemElementNodeDeserializer;
import de.dimm.vsm.hessian.FileSystemElementNodeSerializer;
import de.dimm.vsm.hessian.JDBCLazyListDeserializer;
import de.dimm.vsm.hessian.JDBCLazyListSerializer;
import de.dimm.vsm.jobs.JobEntry;
import de.dimm.vsm.net.LogQuery;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.ScheduleStatusEntry;
import de.dimm.vsm.net.SearchEntry;
import de.dimm.vsm.net.SearchStatus;
import de.dimm.vsm.net.SearchWrapper;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.preview.IPreviewData;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HotFolder;
import de.dimm.vsm.records.MessageLog;
import de.dimm.vsm.records.MountEntry;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.tasks.TaskEntry;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Administrator
 */
public class UiApiServlet  extends HessianServlet implements GuiServerApi
{
    
    
    GuiApiEntry entry;

    public UiApiServlet()
    {
         initHessian();         
    }
    
    
    private void initHessian()
    {
        AbstractSerializerFactory FSENodeFactory = new AbstractSerializerFactory(){

            @Override
            public Serializer getSerializer( Class cl ) throws HessianProtocolException
            {
                if (cl.getCanonicalName().equals( FileSystemElemNode.class.getCanonicalName()))
                {
                    return new FileSystemElementNodeSerializer();
                }
                return null;
            }

            @Override
            public Deserializer getDeserializer( Class cl ) throws HessianProtocolException
            {
                if (cl.getCanonicalName().equals( FileSystemElemNode.class.getCanonicalName()))
                {
                    return new FileSystemElementNodeDeserializer();
                }
                return null;
            }
        
        };
        AbstractSerializerFactory lazyListFactory = new AbstractSerializerFactory(){

            @Override
            public Serializer getSerializer( Class cl ) throws HessianProtocolException
            {
                if (cl.getCanonicalName().equals( JDBCLazyList.class.getCanonicalName()))
                {
                    return new JDBCLazyListSerializer();
                }
                return null;
            }

            @Override
            public Deserializer getDeserializer( Class cl ) throws HessianProtocolException
            {
                if (cl.getCanonicalName().equals( JDBCLazyList.class.getCanonicalName()))
                {
                    return new JDBCLazyListDeserializer();
                }
                return null;
            }
        
        };
        
        this.getSerializerFactory().addFactory(lazyListFactory);
    }
    
        
         
    @Override
    public void service(ServletRequest request, ServletResponse response)    throws IOException, ServletException
    {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        
        String txt = req.getContextPath();
        long id = readIdFromUrl(req.getQueryString());
        if (id == -1)
        {
            res.sendError(500, "Nicht authorisiert");
            return;
        }
        
        
        GuiServerApi api = Main.get_control().getLoginManager().getApi(id);
        if (api == null)
        {
            res.sendError(500, "Session wurde geschlossen");
            return;
        }
        
        entry = new GuiApiEntry(api, null);      
        
        super.service(request, response);
  
    }  
    
    long readIdFromUrl( String url)
    {
        long ret = -1;
        
        int idx = url.indexOf("Id=");
        if (idx >= 0)
        {
            String[] args = url.substring(idx + 3).split("[,;& ]");
            if (args.length > 0)
            {
                try
                {
                    ret = Long.parseLong(args[0]);
                }
                catch (NumberFormatException numberFormatException)
                {
                }
            }
        }
        return ret;
    }

    @Override
    public boolean startBackup( Schedule sched, User user ) throws Exception
    {
        return entry.getApi().startBackup(sched, user);
    }

    @Override
    public boolean abortBackup( Schedule sched )
    {
        return entry.getApi().abortBackup(sched);
    }

    @Override
    public StoragePoolWrapper mountVolume( String agentIp, int agentPort, StoragePool pool, Date timestamp, String subPath, User user, String drive ) throws IOException
    {
        return entry.getApi().mountVolume(agentIp, agentPort, pool, timestamp, subPath, user, drive);
    }

    @Override
    public StoragePoolWrapper mountVolume( String agentIp, int agentPort, StoragePoolWrapper poolWrapper, String drive ) throws IOException
    {
        return entry.getApi().mountVolume(agentIp, agentPort, poolWrapper, drive);
    }

    @Override
    public boolean unmountVolume( StoragePoolWrapper wrapper )
    {
        return entry.getApi().unmountVolume(wrapper);
    }

    @Override
    public boolean unmountAllVolumes()
    {
        return entry.getApi().unmountAllVolumes();
    }

    @Override
    public StoragePoolWrapper getMounted( String agentIp, int agentPort, StoragePool pool )
    {
        return entry.getApi().getMounted(agentIp, agentPort, pool);
    }

    @Override
    public boolean remountVolume( StoragePoolWrapper wrapper )
    {
        return entry.getApi().remountVolume(wrapper);
    }
    @Override
    public StoragePoolWrapper openPoolView( StoragePool pool, StoragePoolQry qry, String subPath )
    {
        return entry.getApi().openPoolView(pool, qry, subPath);
    }
    @Override
    public StoragePoolWrapper openPoolView( StoragePool pool, StoragePoolQry qry, FileSystemElemNode node )
    {
        return entry.getApi().openPoolView(pool, qry, node);
    }

    @Override
    public List<RemoteFSElem> listDir( IWrapper wrapper, RemoteFSElem path ) throws SQLException
    {
        return entry.getApi().listDir(wrapper, path);
    }

    @Override
    public void closePoolView( StoragePoolWrapper wrapper )
    {
        entry.getApi().closePoolView(wrapper);
    }

    @Override
    public boolean removeFSElem( IWrapper wrapper, RemoteFSElem path ) throws SQLException, PoolReadOnlyException
    {
        return entry.getApi().removeFSElem(wrapper, path);
    }

    @Override
    public boolean undeleteFSElem( IWrapper wrapper, RemoteFSElem path ) throws SQLException, PoolReadOnlyException
    {
        return entry.getApi().undeleteFSElem(wrapper, path);
    }

    @Override
    public boolean deleteFSElem( IWrapper wrapper, RemoteFSElem path ) throws SQLException, PoolReadOnlyException
    {
        return entry.getApi().deleteFSElem(wrapper, path);
    }

    @Override
    public boolean restoreFSElem( IWrapper wrapper, RemoteFSElem path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        return entry.getApi().restoreFSElem(wrapper, path, targetIP, targetPort, targetPath, flags, user);
    }

    @Override
    public boolean restoreFSElems( IWrapper wrapper, List<RemoteFSElem> path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        return entry.getApi().restoreFSElems(wrapper, path, targetIP, targetPort, targetPath, flags, user);
    }

    @Override
    public FileSystemElemNode createFileSystemElemNode( StoragePool pool, String path, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        return entry.getApi().createFileSystemElemNode(pool, path, type);
    }

    @Override
    public FileSystemElemNode createFileSystemElemNode( StoragePoolWrapper wrapper, String path, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        return entry.getApi().createFileSystemElemNode(wrapper, path, type);
    }

    @Override
    public List<ScheduleStatusEntry> listSchedulerStats()
    {
        return entry.getApi().listSchedulerStats();
    }

    @Override
    public Properties getAgentProperties( String ip, int port, boolean withMsg )
    {
        return entry.getApi().getAgentProperties(ip, port, withMsg);
    }

    @Override
    public SearchWrapper search( StoragePool pool, ArrayList<SearchEntry> slist )
    {
        return entry.getApi().search(pool, slist);
    }

    @Override
    public SearchWrapper search( StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        return entry.getApi().search(pool, slist, max);
    }

    @Override
    public SearchWrapper searchJob( StoragePool pool, ArrayList<SearchEntry> slist, int max )
    {
        return entry.getApi().searchJob(pool, slist, max);
    }

    @Override
    public List<RemoteFSElem> getSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        return entry.getApi().getSearchResult(wrapper, start, limit);
    }

    @Override
    public List<ArchiveJob> getJobSearchResult( SearchWrapper wrapper, int start, int limit )
    {
        return entry.getApi().getJobSearchResult(wrapper, start, limit);
    }

    @Override
    public SearchStatus getSearchStatus( SearchWrapper wrapper )
    {
        return entry.getApi().getSearchStatus(wrapper);
    }

    @Override
    public void updateReadIndex( StoragePool pool )
    {
        entry.getApi().updateReadIndex(pool);                
    }

    @Override
    public List<RemoteFSElem> listSearchDir( SearchWrapper wrapper, RemoteFSElem path ) throws SQLException
    {
        return entry.getApi().listSearchDir(wrapper, path);
    }

    @Override
    public void closeSearch( SearchWrapper wrapper )
    {
        entry.getApi().closeSearch(wrapper);
    }

    @Override
    public StoragePoolWrapper getMounted( String ip, int port, SearchWrapper searchWrapper )
    {
        return entry.getApi().getMounted(ip, port, searchWrapper);
    }

    @Override
    public StoragePoolWrapper mountVolume( String ip, int port, SearchWrapper searchWrapper, User object, String drive )
    {
        return entry.getApi().mountVolume(ip, port, searchWrapper, object, drive);
    }

    @Override
    public void reSearch( SearchWrapper searchWrapper, ArrayList<SearchEntry> slist )
    {
        entry.getApi().reSearch(searchWrapper, slist);
    }

    @Override
    public JobEntry[] listJobs( User user )
    {
        return entry.getApi().listJobs(user);
    }

    @Override
    public void moveNode( AbstractStorageNode node, AbstractStorageNode toNode, User user ) throws SQLException
    {
        entry.getApi().moveNode(node, toNode, user);
    }

    @Override
    public void emptyNode( AbstractStorageNode node, User user ) throws SQLException
    {
        entry.getApi().emptyNode(node, user);
    }

    @Override
    public TaskEntry[] listTasks()
    {
        return entry.getApi().listTasks();
    }

    @Override
    public MessageLog[] listLogs( int cnt, long offsetIdx, LogQuery lq )
    {
        return entry.getApi().listLogs(cnt, offsetIdx, lq);
    }

    @Override
    public MessageLog[] listLogsSince( long idx, LogQuery lq )
    {
        return entry.getApi().listLogsSince(idx, lq);
    }

    @Override
    public long getLogCounter()
    {
        return entry.getApi().getLogCounter();
    }

    @Override
    public InputStream openStream( IWrapper wrapper, RemoteFSElem path )
    {
        return entry.getApi().openStream(wrapper, path);
    }

    @Override
    public String resolvePath( IWrapper wrapper, RemoteFSElem path ) throws SQLException, PathResolveException
    {
        return entry.getApi().resolvePath(wrapper, path);
    }

    @Override
    public boolean importMMArchiv( HotFolder node, long fromIdx, long tillIdx, boolean withOldJobs, User user ) throws Exception
    {
        return entry.getApi().importMMArchiv(node, fromIdx, tillIdx, withOldJobs, user);
    }

    @Override
    public boolean restoreJob( SearchWrapper searchWrapper, ArchiveJob job, String ip, int port, String path, int rflags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        return entry.getApi().restoreJob(searchWrapper, job, path, port, path, rflags, user);
    }

    @Override
    public boolean removeJob( SearchWrapper searchWrapper, ArchiveJob job ) throws SQLException, PoolReadOnlyException
    {
        return entry.getApi().removeJob(searchWrapper, job);
    }

    @Override
    public void syncNode( AbstractStorageNode t, AbstractStorageNode cloneNode, User user ) throws SQLException, IOException
    {
        entry.getApi().syncNode(t, cloneNode, user);
    }

    @Override
    public boolean isBusyNode( AbstractStorageNode node )
    {
        return entry.getApi().isBusyNode(node);
    }

    @Override
    public boolean initNode( AbstractStorageNode node, User user )
    {
        return entry.getApi().initNode(node, user);
    }

    @Override
    public void initCheck( User user, String checkName, Object arg, Object optArg )
    {
        entry.getApi().initCheck(user, checkName, arg, optArg);
    }

    @Override
    public List<String> getCheckNames( Class<?> clazz )
    {
        return entry.getApi().getCheckNames(clazz);
    }

    @Override
    public List<MountEntry> getAllMountEntries()
    {
        return entry.getApi().getAllMountEntries();
    }

    @Override
    public List<MountEntry> getMountedMountEntries()
    {
        return entry.getApi().getMountedMountEntries();
    }

    @Override
    public void unMountEntry( MountEntry mountEntry )
    {
        entry.getApi().unMountEntry(mountEntry);
    }

    @Override
    public StoragePoolWrapper mountEntry( User user, MountEntry mountEntry ) throws IOException
    {
        return entry.getApi().mountEntry(user, mountEntry);
    }        

    @Override
    public Properties getProperties()
    {
        return entry.getApi().getProperties();
    }

    @Override
    public void scanDatabase( User user, AbstractStorageNode node )
    {
        entry.getApi().scanDatabase(user, node);
    }
    @Override
    public void rebuildBootstraps( User user, StoragePool pool )
    {
        entry.getApi().rebuildBootstraps( user, pool );
    }

    @Override
    public List<RemoteFSElem> listVersions( IWrapper wrapper, RemoteFSElem path ) throws SQLException, IOException
    {
        return entry.getApi().listVersions(wrapper, path);
    }
    
    @Override
    public boolean restoreVersionedFSElem( IWrapper wrapper, RemoteFSElem path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        return entry.getApi().restoreVersionedFSElem(wrapper, path, targetIP, targetPort, targetPath, flags, user);
    }

    @Override
    public boolean restoreVersionedFSElems( IWrapper wrapper, List<RemoteFSElem> path, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, PoolReadOnlyException, IOException
    {
        return entry.getApi().restoreVersionedFSElems(wrapper, path, targetIP, targetPort, targetPath, flags, user);
    }

    @Override
    public boolean isWrapperValid( IWrapper wrapper )
    {
         return entry.getApi().isWrapperValid(wrapper);
    }

    @Override
    public List<IPreviewData> getPreviewData( IWrapper wrapper, List<RemoteFSElem> path, Properties props ) throws SQLException, IOException {
        return entry.getApi().getPreviewData(wrapper, path, props);
    }

    @Override
    public int createWebDavServer( StoragePoolWrapper wrapper ) throws IOException, PoolReadOnlyException, PathResolveException {
        return entry.getApi().createWebDavServer(wrapper);
    }

    @Override
    public int createWebDavSearchServer( SearchWrapper wrapper ) throws IOException, PoolReadOnlyException, PathResolveException {
        return entry.getApi().createWebDavSearchServer(wrapper);
    }

    @Override
    public String checkRestoreErrFSElem( IWrapper wrapper, RemoteFSElem path ) {
        return entry.getApi().checkRestoreErrFSElem(wrapper, path);
    }

    @Override
    public boolean fixDoubleDir( IWrapper wrapper, RemoteFSElem path ) throws SQLException, IOException {
        return entry.getApi().fixDoubleDir(wrapper, path);
    }

    
    


}
