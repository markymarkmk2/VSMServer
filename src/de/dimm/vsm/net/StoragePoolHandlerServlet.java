/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import com.caucho.hessian.server.HessianServlet;
import de.dimm.vsm.Exceptions.DBConnException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.auth.UserManager;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.net.interfaces.StoragePoolHandlerInterface;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerServlet extends HessianServlet implements StoragePoolHandlerInterface
{
    public static final String version = "1.0.0";
    

    SearchContextManager searchContextManager;
    StoragePoolHandlerContextManager poolContextManager;


    public StoragePoolHandlerServlet(SearchContextManager searchContextManager, StoragePoolHandlerContextManager contextManager) throws Exception
    {

        this.poolContextManager = contextManager;
        this.searchContextManager = searchContextManager;
    }

    public StoragePoolHandlerContextManager getContextManager()
    {
        return poolContextManager;
    }
    public SearchContextManager getSearchContextManager()
    {
        return searchContextManager;
    }

    public static RemoteFSElem genRemoteFSElemfromNode( FileSystemElemNode node, FileSystemElemAttributes attr )
    {
        if (node == null)
            return null;

        RemoteFSElem ret = new RemoteFSElem(node, attr);
        return ret;
    }

  

    @Override
    public RemoteFSElem resolve_node( StoragePoolWrapper pool, String path ) throws SQLException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler.isInsideMappingDir( path))
        {
            RemoteFSElem elem = RemoteFSElem.createDir(path);
            return elem;
        }
        path = handler.resolveMappingDir(  path);
            
        FileSystemElemNode e = handler.resolve_node(path);
        if (e == null)
            return null;
        
        FileSystemElemAttributes attr = handler.getActualFSAttributes(e, pool.getQry() );
        return genRemoteFSElemfromNode(e, attr);
    }

    @Override
    public long getTotalBlocks(StoragePoolWrapper pool)
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            return handler.getTotalBlocks();
        return 0;
    }

    @Override
    public long getUsedBlocks(StoragePoolWrapper pool)
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            return handler.getUsedBlocks();
        return 0;
    }

    @Override
    public int getBlockSize(StoragePoolWrapper pool)
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            return handler.getBlockSize();
        return 0;
    }

    private static void checkCommit(  StoragePoolHandler handler ) throws IOException
    {
        try
        {
            handler.commit_transaction();
        }
        catch (SQLException sQLException)
        {
            throw new IOException("Das Anlegen des Verzeichnisses schlug fehl", sQLException);
        }
    }
    @Override
    public void mkdir( StoragePoolWrapper pool, String pathName ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        handler.mkdir(pathName);
        checkCommit( handler );
    }
/*
    @Override
    public FileHandle open_file_handle( StoragePoolWrapper pool, String path, boolean create ) throws IOException
    {
        StoragePoolHandler handler = getHandler(pool);
        return handler.open_file_handle(path, create);
    }*/

    @Override
    public String getName(StoragePoolWrapper pool)
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            return handler.getName();
        return null;
    }

    @Override
    public boolean delete_fse_node( StoragePoolWrapper pool, String path ) throws PoolReadOnlyException, SQLException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        boolean ret = handler.delete_fse_node(path);
        handler.commit_transaction();

        return ret;
    }
/*
    @Override
    public List<FileSystemElemNode> get_child_nodes( StoragePoolWrapper pool, FSENodeInterface fse )
    {
        StoragePoolHandler handler = getHandler(pool);
        return handler.get_child_nodes(fse);
    }*/

    @Override
    public void move_fse_node( StoragePoolWrapper pool, String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        handler.move_fse_node(from, to);
        handler.commit_transaction();
    }

    @Override
    public String getVersion()
    {
        return version;
    }


    public long open_fh( StoragePoolWrapper pool, long nodeIdx ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {        
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);

        long ret = handler.open_fh(nodeIdx, false);
        checkCommit( handler );
        return ret;
    }
    
    public long open_stream( StoragePoolWrapper pool, long nodeIdx, int streamInfo, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);

        return handler.open_stream(nodeIdx, streamInfo, create);
    }

    private FileSystemElemNode create_fs_elem_node( StoragePoolHandler handler, String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        FileSystemElemNode e = handler.create_fse_node_complete(fileName, type);
        return e;
    }


    @Override
    public RemoteFSElem create_fse_node( StoragePoolWrapper pool, String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        String path = pool.resolveRelPath(fileName);
        FileSystemElemNode e = create_fs_elem_node(handler, path, type);
        return genRemoteFSElemfromNode(e, e.getAttributes());
    }
    
    public long create_fh( StoragePoolWrapper pool, String vsmPath, String type) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {        
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        String path = pool.resolveRelPath(vsmPath);
        FileSystemElemNode e = create_fs_elem_node(handler, path, type);
        long ret = handler.open_fh(e, true);
        checkCommit( handler );
        return ret;        
    }


    
    public long create_stream( StoragePoolWrapper pool,  String vsmPath, String type, int streamInfo) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {        
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        FileSystemElemNode e = handler.create_fse_node_complete ( vsmPath, type);

        long ret = handler.open_stream(e, streamInfo, true);
        checkCommit( handler );
        return ret;

    }
    
    public List<RemoteFSElem> mappedUserDir(StoragePoolHandler handler, RemoteFSElem node) {
        String path = node.getPath();
        List<User.VsmFsEntry> mapList = handler.getPoolQry().getUser().getFsMapper().getVsmList();
        List<RemoteFSElem> ret = new ArrayList<>();
        try {

            for (int i = 0; i < mapList.size(); i++) {
                User.VsmFsEntry vsmFsEntry = mapList.get(i);
                // INSIDE THIS MAPPING ENTRY?
                if (!vsmFsEntry.getuPath().startsWith(path) && !path.startsWith(vsmFsEntry.getuPath()) )
                    continue;

                if (path.length() <= vsmFsEntry.getuPath().length())
                {
                    String restPath = vsmFsEntry.getuPath().substring(path.length());
                    if (restPath.startsWith("/") && restPath.length() > 1)
                        restPath = restPath.substring(1);
                    String[] paths = restPath.split("/");

                    if (paths.length == 0 || (paths.length == 1 && paths[0].isEmpty()))
                    {
                        FileSystemElemNode fseNode = handler.resolve_elem_by_path(  vsmFsEntry.getvPath() );
                        if (fseNode == null)
                            continue;

                        // THIS IS THE NEWEST ENTRY FOR THIS FILE
                        FileSystemElemAttributes attr = handler.getActualFSAttributes(fseNode, handler.getPoolQry() );
                        RemoteFSElem remoteNode = genRemoteFSElemfromNode(fseNode, attr);
                        return get_unmapped_child_nodes(handler, remoteNode);
                    }
                    else
                    {
                        String dirName = paths[0];
                        if (dirName.isEmpty() && paths.length > 1)
                            dirName = paths[1];

                        String newPath = path;
                        if (!newPath.endsWith("/"))
                            newPath += "/";
                        newPath += dirName;

                        if (containsDir(ret, dirName))
                            continue;

                        RemoteFSElem remoteNode = RemoteFSElem.createDir(dirName);
                        ret.add(remoteNode);
    //                    remoteNode = RemoteFSElem.createDir(newPath);
                    }
                }
                else
                {
                    String restPath = path.substring( vsmFsEntry.getuPath().length() );
                    String newFullPath =  vsmFsEntry.getvPath() + restPath;
                    FileSystemElemNode fseNode = handler.resolve_elem_by_path( newFullPath );
                    if (fseNode == null)
                        continue;

                    // THIS IS THE NEWEST ENTRY FOR THIS FILE
                    FileSystemElemAttributes attr = handler.getActualFSAttributes(fseNode, handler.getPoolQry() );
                    RemoteFSElem remoteNode = genRemoteFSElemfromNode(fseNode, attr);
                    return get_unmapped_child_nodes(handler, remoteNode);
                }
            }
            return ret;
        } catch (SQLException sQLException) {
            LogManager.err_db("Kann Mapping node nicht auflösen", sQLException);
        }
        return ret;
    }
   



    @Override
    public List<RemoteFSElem> get_child_nodes( StoragePoolWrapper pool, RemoteFSElem node ) throws SQLException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);

        List<RemoteFSElem> ret = get_child_nodes(handler, node);
        
        return ret;
    }

  
    public List<RemoteFSElem> get_child_nodes( StoragePoolHandler handler, RemoteFSElem node ) throws SQLException
    {        
        StoragePoolQry qry = handler.getPoolQry();
        
        if (qry.isUseMappingFilter()) 
        {
            if (handler.isInsideMappingDir( node.getPath()))
            {
                return mappedUserDir( handler, node );
            }
        }
        return get_unmapped_child_nodes(handler, node);
     }
    
     private List<RemoteFSElem> get_unmapped_child_nodes( StoragePoolHandler handler, RemoteFSElem node ) throws SQLException
     {
        StoragePoolQry qry = handler.getPoolQry();
        List<RemoteFSElem> ret = new ArrayList<>();
         FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem(  node );
        if (fseNode == null)
            return ret;

        try
        {
            // IF WE HAVE ACTUAL FILESYSTEM THEN RELOAD NODE EVERY TIME
            if (qry.getSnapShotTs() <= 0)
            {
                handler.em_refresh(fseNode);
            }
        }
        catch (Exception exception)
        {
            Log.err("Objekt kann nicht refreshed werden", fseNode.toString(), exception);
        }

        UserManager umgr = Main.get_control().getUsermanager();

        Map<String,FileSystemElemNode>blockedNodes = new HashMap<>();
        Map<String,RemoteFSElem>unBlockedNodes = new HashMap<>();

        if (fseNode != null)
        {
            List<FileSystemElemNode> list = fseNode.getChildren(handler.getEm());

            for (int i = 0; i < list.size(); i++)
            {
                FileSystemElemNode fileSystemElemNode = list.get(i);

                // THIS IS THE NEWEST ENTRY FOR THIS FILE
                FileSystemElemAttributes attr = handler.getActualFSAttributes(fileSystemElemNode, qry );

                // OBVIOUSLY THE FILE WAS CREATED AFTER TS -> INVISIBLE
                if (attr == null)
                    continue;

                // ACLS STARTUNDER SYSTEMROOT
                if (!node.getPath().equals("/"))
                {
                    if (!qry.matchesUser(fileSystemElemNode, attr, umgr))
                    {
                        blockedNodes.put(fileSystemElemNode.getName(), fseNode);
                        continue;
                    }
                }

                // FILE WAS DELETED AT TS
                if (attr.isDeleted() && !qry.isShowDeleted())
                    continue;

                ret.add( genRemoteFSElemfromNode( fileSystemElemNode, attr) );
//                unBlockedNodes.put(fileSystemElemNode.getName(), genRemoteFSElemfromNode( fileSystemElemNode, attr));
            }
        }

        // NOW REMOVE ALL BLOCKED NODES FROM UNBLOCKED LIST
        for (int i = 0; i < ret.size(); i++)
        {
            RemoteFSElem elem = ret.get(i);
            if (blockedNodes.containsKey(elem.getName()))
            {
                ret.remove(i);
                i--;
            }
        }
//        for (String blockedNodePath  : blockedNodes.keySet())
//        {
//            unBlockedNodes.remove(blockedNodePath);
//        }

        // AND ADD THE REST TO THE RETURN LIST
//        for (String unBlockedNodePath  : unBlockedNodes.keySet())
//        {
//            ret.add( unBlockedNodes.get(unBlockedNodePath) );
//        }
        Collections.sort(ret, new Comparator<RemoteFSElem>() {

            @Override
            public int compare( RemoteFSElem o1, RemoteFSElem o2 )
            {
                return o1.getPath().compareTo(o2.getPath());
            }
        });

        return ret;
    }

    @Override
    public void set_ms_times( StoragePoolWrapper pool, long fileNo, long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws SQLException, DBConnException, PoolReadOnlyException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        handler.set_ms_times(fileNo, toJavaTime, toJavaTime0, toJavaTime1);
    }

    @Override
    public boolean exists( StoragePoolWrapper pool, RemoteFSElem fseNode )
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        return handler.exists(fseNode.getIdx());
    }

    @Override
    public void force( StoragePoolWrapper pool, long fileNo, boolean b ) throws IOException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        handler.force(fileNo, b);
    }

    @Override
    public byte[] read( StoragePoolWrapper pool, long fileNo, int length, long offset ) throws IOException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        byte b[] = new byte[length];
        int rlen =  handler.read(fileNo, b, length, offset);
        if (rlen < 0)
        {
            return null;
        }
        
        if (rlen < b.length)
        {
            byte bb[] = new byte[rlen];
            System.arraycopy(b, 0, bb, 0, rlen);
            b = bb;
        }
        return b;
    }

    public int read( StoragePoolWrapper pool, long fileNo, byte[] data, long offset ) throws IOException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        int length = data.length;
        int rlen = handler.read(fileNo, data, length, offset);
        if (rlen < 0)
        {
            return -1;
        }

        return rlen;
    }

    @Override
    public void create( StoragePoolWrapper pool, long fileNo ) throws IOException, PoolReadOnlyException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            handler.create(fileNo);
        else
            Log.err("Ungültiger Handler in Aufruf", "create");
    }
    @Override
    public long length( StoragePoolWrapper pool, long fileNo )
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
            return handler.getLength(fileNo);
        else
            Log.err("Ungültiger Handler in Aufruf", "create");
        return -1;
    }

    @Override
    public void truncateFile( StoragePoolWrapper pool, long fileNo, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
        {
            handler.truncateFile(fileNo, size);
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "truncateFile");

    }

    @Override
    public void close_fh( StoragePoolWrapper pool, long fileNo ) throws IOException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
        {
            handler.close_fh(fileNo);
            checkCommit( handler );
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "close_fh");

    }

    @Override
    public void writeFile( StoragePoolWrapper pool, long fileNo, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (handler != null)
        {
            handler.writeFile(fileNo, b, length, offset);
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "writeFile");
        
        
    }
/*
    @Override
    public void create( StoragePoolWrapper pool, FileSystemElemNode fseNode )
    {
        StoragePoolHandler handler = getHandler(pool);
        handler.create();
    }
*/
    @Override
    public void set_attribute( StoragePoolWrapper pool, RemoteFSElem fseNode, String string, Integer valueOf )
    {
        Log.err("Noch nicht implementiert", "set_attribute");
    }

    @Override
    public String read_symlink( StoragePoolWrapper pool, RemoteFSElem fseNode )
    {
        Log.err("Noch nicht implementiert", "read_symlink");
        return null;
    }

    @Override
    public void create_symlink( StoragePoolWrapper pool, RemoteFSElem fseNode, String to )
    {
        Log.err("Noch nicht implementiert", "create_symlink");
    }

    @Override
    public void truncate( StoragePoolWrapper pool, RemoteFSElem fseNode, long size )
    {
        Log.err("Noch nicht implementiert", "truncate");
    }

    @Override
    public void set_last_modified( StoragePoolWrapper pool, RemoteFSElem fseNode, long l )
    {
        Log.err("Noch nicht implementiert", "set_last_modified");
    }

    @Override
    public String get_xattribute( StoragePoolWrapper pool, RemoteFSElem fseNode, String name ) throws SQLException
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        if (fseNode.getIdx() > 0)
        {
            FileSystemElemNode elem = handler.resolve_fse_node_from_db( fseNode.getIdx() );
            if (elem != null)
                return elem.getAttributes().getAclInfoData();
        }
        return null;
    }

    @Override
    public void set_last_accessed( StoragePoolWrapper pool, RemoteFSElem fseNode, long l )
    {
        Log.err("Noch nicht implementiert", "set_last_accessed");
    }

    @Override
    public List<String> list_xattributes( StoragePoolWrapper pool, RemoteFSElem fseNode )
    {
        Log.err("Noch nicht implementiert", "list_xattributes");
        return null;
    }

    @Override
    public void add_xattribute( StoragePoolWrapper pool, RemoteFSElem fseNode, String name, String valStr )
    {
        Log.err("Noch nicht implementiert", "add_xattribute");
    }

    @Override
    public void set_mode( StoragePoolWrapper pool, RemoteFSElem fseNode, int mode )
    {
        Log.err("Noch nicht implementiert", "set_mode");
    }

    @Override
    public void set_owner_id( StoragePoolWrapper pool, RemoteFSElem fseNode, int uid )
    {
        Log.err("Noch nicht implementiert", "set_owner_id");
    }

    @Override
    public void set_group_id( StoragePoolWrapper pool, RemoteFSElem fseNode, int gid )
    {
        Log.err("Noch nicht implementiert", "set_group_id");
    }

    @Override
    public boolean isReadOnly(StoragePoolWrapper pool)
    {
        StoragePoolHandler handler = poolContextManager.getHandlerbyWrapper(pool);
        return handler.isReadOnly();
    }

   
    boolean removeFSElem( IWrapper wrapper, RemoteFSElem node ) throws PoolReadOnlyException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            return false;
        
        boolean ret = handler.remove_fse_node(fseNode, true);
        if (!ret)
            return false;

        try
        {
            // THIS IS COMMITTED IMMEDIATELY
            handler.close_transaction();
            return true;

        }
        catch (Exception e)
        {
            Log.err("Abbruch bei", "removeFSElem", e);
        }

        return false;

    }
    
    boolean undeleteFSElem( IWrapper wrapper, RemoteFSElem node ) throws PoolReadOnlyException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            return false;

        
        try
        {
            handler.setDeleted(fseNode, false, System.currentTimeMillis());
            // THIS IS COMMITTED IMMEDIATELY
            handler.close_transaction();
            return true;

        }
        catch (Exception e)
        {
            Log.err("Abbruch bei", "undeleteFSElem", e);
        }

        return false;
    }
    
    boolean deleteFSElem( IWrapper wrapper, RemoteFSElem node ) throws PoolReadOnlyException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            return false;


        try
        {
            handler.setDeleted(fseNode, true, System.currentTimeMillis());
            // THIS IS COMMITTED IMMEDIATELY
            handler.close_transaction();
            return true;

        }
        catch (Exception e)
        {
            Log.err("Abbruch bei", "deleteFSElem", e);
        }

        return false;
    }

    void closeTransactions( IWrapper wrapper ) throws SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        if (handler != null)
            handler.close_transaction();
    }
    
    public boolean restoreFSElem( IWrapper wrapper, List<RemoteFSElem> nodes, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        if (handler != null)
            return restoreFSElem(handler, nodes, targetIP, targetPort, targetPath, flags, user);
        else
            Log.err("Ungültiger Handler in Aufruf", "restoreFSElem");
        return false;
    }

    public boolean restoreFSElem( StoragePoolHandler handler, List<RemoteFSElem> nodes, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, IOException
    {
        List<FileSystemElemNode> fseNodes = handler.resolve_node_by_remote_elem( nodes);

        boolean ret = false;

        StoragePoolQry qry = handler.getPoolQry();
        
        try
        {
            InetAddress adr = InetAddress.getByName(targetIP);
            RemoteFSElem target = RemoteFSElem.createDir(targetPath);
            Restore rest = new Restore(handler, fseNodes, flags, qry, adr, targetPort, target);


            JobManager jm = Main.get_control().getJobManager();
            JobInterface ji =  rest.createRestoreJob(user);
            if (ji != null)
            {
                jm.addJobEntry( ji );
                ret = true;
            }
        }
        catch (UnknownHostException unknownHostException)
        {
            Log.warn( "Unbekannter Host", targetIP + ":" + targetPort + " " + targetPath, unknownHostException );
            ret = false;
        }
        
        return ret;
    }
    boolean restoreJob( StoragePoolHandler handler, ArchiveJob job, String targetIP, int targetPort, String targetPath, int rflags, User user )
    {
        boolean ret = false;

        StoragePoolQry qry = handler.getPoolQry();

        try
        {
            InetAddress adr = InetAddress.getByName(targetIP);
            RemoteFSElem target = RemoteFSElem.createDir(targetPath);
            Restore rest = new Restore(handler, job, rflags, qry, adr, targetPort, target);


            JobManager jm = Main.get_control().getJobManager();
            JobInterface ji =  rest.createRestoreJob(user);
            if (ji != null)
            {
                jm.addJobEntry( ji );
                ret = true;
            }
        }
        catch (UnknownHostException unknownHostException)
        {
            Log.err( "Unbekannter Host", targetIP + ":" + targetPort + " " + targetPath, unknownHostException );
            ret = false;
        }
        catch (IOException e)
        {
            Log.err( "Fehler beim Start von Restore",  targetPath, e );
            ret = false;
        }
        return ret;
    }

    boolean removeJob( StoragePoolHandler handler, ArchiveJob job ) throws  SQLException, PoolReadOnlyException
    {
        return handler.remove_job(job);
    }

    StoragePoolHandler getPoolHandlerByWrapper( IWrapper _wrapper )
    {
        if (_wrapper instanceof SearchWrapper)
        {
            SearchWrapper wrapper = (SearchWrapper)_wrapper;
            SearchContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getSearchContextManager();
            StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(wrapper);
            return sp_handler;
        }
        if (_wrapper instanceof StoragePoolWrapper)
        {
            StoragePoolWrapper wrapper = (StoragePoolWrapper)_wrapper;
            StoragePoolHandlerContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getContextManager();
            StoragePoolHandler sp_handler = contextMgr.getHandlerbyWrapper(wrapper);
            return sp_handler;
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    private boolean containsDir(List<RemoteFSElem> ret, String dirName) {
         for (int i = 0; i < ret.size(); i++) {
            RemoteFSElem remoteFSElem = ret.get(i);
            if (remoteFSElem.getPath().equals(dirName))
                return true;
        }
         return false;
    }

}
