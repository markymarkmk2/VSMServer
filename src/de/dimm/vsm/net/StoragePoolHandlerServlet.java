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
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.hash.StringUtils;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.net.interfaces.StoragePoolHandlerInterface;
import de.dimm.vsm.net.mapping.UserDirMapper;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerServlet extends HessianServlet implements StoragePoolHandlerInterface
{
    public static final String version = "1.0.0";
    Logger LOG = Logger.getLogger(StoragePoolHandlerServlet.class);
    

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
    public RemoteFSElem resolve_node( IWrapper pool, String path ) throws SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler.isInsideMappingDir( path))
        {
            String vsmPath = handler.resolveMappingDir(  path);
            
            FileSystemElemNode e = handler.resolve_node(vsmPath);
            if (e == null)
                return RemoteFSElem.createDir(path);
        
            FileSystemElemAttributes attr = handler.getActualFSAttributes(e, pool.getQry() );
            if (attr.isDeleted() && !pool.getQry().isShowDeleted())
                return null;
            
            // Reales Objekt aus DB nehm n und mit virtuellem Pafd setzen
            RemoteFSElem elem = genRemoteFSElemfromNode(e, attr);
            elem.setPath(path);
            return elem;
        }
        path = handler.resolveMappingDir(  path);
            
        FileSystemElemNode e = handler.resolve_node(path);
        if (e == null)
            return null;
        
        FileSystemElemAttributes attr = handler.getActualFSAttributes(e, pool.getQry() );
        if (attr.isDeleted() && !pool.getQry().isShowDeleted())
            return null;
        
        return genRemoteFSElemfromNode(e, attr);
    }

    @Override
    public long getTotalBlocks(IWrapper pool)
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
            return handler.getTotalBlocks();
        return 0;
    }

    @Override
    public long getUsedBlocks(IWrapper pool)
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
            return handler.getUsedBlocks();
        return 0;
    }

    @Override
    public int getBlockSize(IWrapper pool)
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
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
    public void mkdir( IWrapper pool, String pathName ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        handler.mkdir(pathName);
        checkCommit( handler );
    }

    @Override
    public String getName(IWrapper pool)
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
            return handler.getName();
        return null;
    }

    @Override
    public boolean delete_fse_node_path( IWrapper pool, String path ) throws PoolReadOnlyException, SQLException, IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        boolean ret = handler.delete_fse_node(path);
        handler.commit_transaction();

        return ret;
    }
    @Override
    public boolean delete_fse_node_idx( IWrapper pool, long idx ) throws PoolReadOnlyException, SQLException, IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        boolean ret = handler.delete_fse_node(idx);
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
    public void move_fse_node( IWrapper pool, String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        handler.move_fse_node(from, to);
        handler.commit_transaction();
    }

    @Override
    public void move_fse_node_idx( IWrapper pool,long idx, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        handler.move_fse_node_idx(idx, to);
        handler.commit_transaction();
    }

    @Override
    public String getVersion()
    {
        return version;
    }


    @Override
    public long open_fh( IWrapper pool, long nodeIdx, boolean forWrite  ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);

        long ret = handler.open_fh(nodeIdx, forWrite);
        checkCommit( handler );
        return ret;
    }

    @Override
    public long open_stream( IWrapper pool, long nodeIdx, boolean forWrite   ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);

        return handler.open_stream(nodeIdx, 0, forWrite);
    }

    private FileSystemElemNode create_fs_elem_node( StoragePoolHandler handler, String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        FileSystemElemNode e = handler.create_fse_node_complete(fileName, type);
        return e;
    }


    @Override
    public RemoteFSElem create_fse_node( IWrapper pool, String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        String path = fileName; //pool.resolveRelPath(fileName);
        FileSystemElemNode e = create_fs_elem_node(handler, path, type);
        LOG.debug("Created node " + e);
        return genRemoteFSElemfromNode(e, e.getAttributes());
    }
    
    @Override
    public long create_fh( StoragePoolWrapper pool, String vsmPath, String type) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {        
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        String path = pool.resolveRelPath(vsmPath);
        FileSystemElemNode e = create_fs_elem_node(handler, path, type);
        LOG.debug("Created node " + e);
        long ret = handler.open_fh(e, true);
        if (ret == -1) {
            String errText = handler.buildCheckOpenNodeErrText(e);
            if (StringUtils.isEmpty(errText)) {
                errText = "Node kann nicht geöffnet werden";
            }
            throw new IOException(errText);
        }
        checkCommit( handler );
        return ret;        
    }


    
    @Override
    public long create_stream( StoragePoolWrapper pool,  String vsmPath, String type, int streamInfo) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {        
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        String path = pool.resolveRelPath(vsmPath);
        FileSystemElemNode e = handler.create_fse_node_complete ( vsmPath, type);

        LOG.debug("Created stream " + e);
        long ret = handler.open_stream(e, streamInfo, true);
        checkCommit( handler );
        return ret;
    }

    @Override
    public List<RemoteFSElem> get_child_nodes( IWrapper pool, RemoteFSElem node ) throws SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
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
                return UserDirMapper.mappedUserDir( handler, node );
            }
        }
        LOG.debug("get_child_nodes " + node.toString());
        return UserDirMapper.get_unmapped_child_nodes(handler, node);
     }
    
     
    @Override
    public void set_ms_times( IWrapper pool, long fileNo, long cTime, long aTime, long mTime ) throws SQLException, DBConnException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        handler.set_ms_times(fileNo, cTime, aTime, mTime);
    }

    @Override
    public boolean exists( IWrapper pool, RemoteFSElem fseNode )
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        LOG.debug("exists " + fseNode.toString());
        return handler.exists(fseNode.getIdx());
    }

    @Override
    public boolean isReadOnly( IWrapper pool, long fileNo  ) throws IOException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        LOG.debug("isReadOnly " + fileNo);
        return handler.isReadOnly(fileNo);
    }

    @Override
    public void force( IWrapper pool, long fileNo, boolean b ) throws IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        handler.force(fileNo, b);
    }

    @Override
    public byte[] read( IWrapper pool, long fileNo, int length, long offset ) throws IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        Log.debug("read len " + length + " offs " + offset, Long.toString(fileNo));
        byte b[] = new byte[length];
        int rlen =  handler.read(fileNo, b, length, offset);
        if (rlen < 0)
        {
            Log.debug("read got null");
            return null;
        }
        
        if (rlen < b.length)
        {
            byte bb[] = new byte[rlen];
            System.arraycopy(b, 0, bb, 0, rlen);
            b = bb;
        }
        Log.debug("read got " + b.length);
        return b;
    }

    public int read( IWrapper pool, long fileNo, byte[] data, long offset ) throws IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        int length = data.length;
        Log.debug("read len " + length + " offs " + offset, Long.toString(fileNo));
        int rlen = handler.read(fileNo, data, length, offset);
        Log.debug("read got " + rlen);
        if (rlen < 0)
        {
            return -1;
        }

        return rlen;
    }

    @Override
    public void create( IWrapper pool, long fileNo ) throws IOException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        Log.debug("create", Long.toString(fileNo));
        if (handler != null)
            handler.create(fileNo);
        else
            Log.err("Ungültiger Handler in Aufruf", "create");
    }
    @Override
    public long length( IWrapper pool, long fileNo )
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        Log.debug("length", Long.toString(fileNo));
        if (handler != null)
            return handler.getLength(fileNo);
        else
            Log.err("Ungültiger Handler in Aufruf", "create");
        return -1;
    }

    @Override
    public void truncateFile( IWrapper pool, long fileNo, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            Log.debug("truncateFile to " + size, Long.toString(fileNo));
            handler.truncateFile(fileNo, size);
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "truncateFile");

    }
    
    @Override
    public void updateAttributes( IWrapper pool, long fileNo, RemoteFSElem elem ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            Log.debug("updateAttributes for " + elem.getPath());
            handler.updateAttributes( fileNo, System.currentTimeMillis(), elem );
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "truncateFile");

    }

    @Override
    public void close_fh( IWrapper pool, long fileNo ) throws IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            handler.close_fh(fileNo);
            checkCommit( handler );
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "close_fh");

    }

    @Override
    public void writeFile( IWrapper pool, long fileNo, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            Log.debug("write len " + length + " offs " + offset, Long.toString(fileNo));
            handler.writeFile(fileNo, b, length, offset);
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "writeFile");
        
        
    }
    @Override
    public void writeBlock( IWrapper pool, long fileNo, String hash, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            Log.debug("write len " + length + " offs " + offset, Long.toString(fileNo));
            handler.writeBlock(fileNo, hash, b, length, offset);
            handler.commit_transaction();
        }
        else
            Log.err("Ungültiger Handler in Aufruf", "writeFile");
        
        
    }
    @Override
    public boolean checkBlock( IWrapper pool, String hash) throws IOException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        if (handler != null)
        {
            Log.debug("CheckBlock " + hash + " offs ");
            return handler.checkBlock(hash);
        }
        else
            throw new IOException("Ungültiger Handler in Aufruf" +  "writeFile");
        
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
    public void set_attribute( IWrapper pool, RemoteFSElem fseNode, String string, Integer valueOf )
    {
        Log.err("Noch nicht implementiert", "set_attribute");
    }

    @Override
    public String read_symlink( IWrapper pool, RemoteFSElem fseNode )
    {
        Log.err("Noch nicht implementiert", "read_symlink");
        return null;
    }

    @Override
    public void create_symlink( IWrapper pool, RemoteFSElem fseNode, String to )
    {
        Log.err("Noch nicht implementiert", "create_symlink");
    }

    @Override
    public void truncate( IWrapper pool, RemoteFSElem fseNode, long size )
    {
        Log.err("Noch nicht implementiert", "truncate");
    }

    @Override
    public void set_last_modified( IWrapper pool, RemoteFSElem fseNode, long l )
    {
        Log.err("Noch nicht implementiert", "set_last_modified");
    }

    @Override
    public String get_xattribute( IWrapper pool, RemoteFSElem fseNode, String name ) throws SQLException
    {
        Log.err("Noch nicht implementiert", "set_last_modified");

//        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
//        if (fseNode.getIdx() > 0)
//        {
//            FileSystemElemNode elem = handler.resolve_fse_node_from_db( fseNode.getIdx() );
//            if (elem != null)
//                return elem.listXattributes()elem.getAttributes().getAclInfoData();
//        }
        return null;
    }

    @Override
    public void set_last_accessed( IWrapper pool, RemoteFSElem fseNode, long l )
    {
        Log.err("Noch nicht implementiert", "set_last_accessed");
    }

    @Override
    public List<String> list_xattributes( IWrapper pool, RemoteFSElem fseNode )
    {
        Log.err("Noch nicht implementiert", "list_xattributes");
        return null;
    }

    @Override
    public void add_xattribute( IWrapper pool, RemoteFSElem fseNode, String name, String valStr )
    {
        Log.err("Noch nicht implementiert", "add_xattribute");
    }

    @Override
    public void set_mode( IWrapper pool, RemoteFSElem fseNode, int mode )
    {
        Log.err("Noch nicht implementiert", "set_mode");
    }

    @Override
    public void set_owner_id( IWrapper pool, RemoteFSElem fseNode, int uid )
    {
        Log.err("Noch nicht implementiert", "set_owner_id");
    }

    @Override
    public void set_group_id( IWrapper pool, RemoteFSElem fseNode, int gid )
    {
        Log.err("Noch nicht implementiert", "set_group_id");
    }

    @Override
    public boolean isReadOnly(IWrapper pool)
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper(pool);
        return handler.isReadOnly();
    }

   
    boolean removeFSElem( IWrapper wrapper, RemoteFSElem node ) throws PoolReadOnlyException, SQLException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            return false;
        
        Log.debug("removeFSElem", fseNode.toString());
        
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

        Log.debug("undeleteFSElem", fseNode.toString());
        
        try
        {
            handler.setDeleted(fseNode, false, System.currentTimeMillis());
            // THIS IS COMMITTED IMMEDIATELY
            handler.close_transaction();
            return true;

        }
        catch (SQLException | DBConnException | PoolReadOnlyException e)
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

        Log.debug("deleteFSElem", fseNode.toString());

        try
        {
            handler.setDeleted(fseNode, true, System.currentTimeMillis());
            // THIS IS COMMITTED IMMEDIATELY
            handler.close_transaction();
            return true;

        }
        catch (SQLException | DBConnException | PoolReadOnlyException e)
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

    public static StoragePoolHandler getPoolHandlerByWrapper( IWrapper _wrapper )
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

    @Override
    public void set_ms_filetimes( IWrapper wrapper, RemoteFSElem node, long ctime, long atime, long mtime ) throws IOException, SQLException, PoolReadOnlyException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            throw new IOException("Node not found");

        handler.check_open_transaction();
        handler.set_ms_times(fseNode, ctime, atime, mtime);
        handler.commit_transaction();

    }

    public List<RemoteFSElem> get_versions( StoragePoolHandler handler, RemoteFSElem node ) throws SQLException, IOException
    {        
        
        LOG.debug("get_versions " + node.toString());
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem( node);
        if (fseNode == null)
            throw new IOException("Node not found");
        boolean wasRealized = fseNode.getHistory().isRealized();
        
        List<FileSystemElemAttributes> attrs = fseNode.getHistory(handler.getEm());
        
        List<RemoteFSElem> ret = new ArrayList<>();
                 
        for (int i = 0; i < attrs.size(); i++)
        {
            FileSystemElemAttributes fileSystemElemAttributes = attrs.get(i);
            RemoteFSElem elem = StoragePoolHandlerServlet.genRemoteFSElemfromNode(fseNode, fileSystemElemAttributes);
            ret.add(elem);            
        }   
        if (!wasRealized)
            fseNode.getHistory().unRealize();
        
        return ret;
     }

    
    public boolean restoreVersionedFSElem( IWrapper wrapper, List<RemoteFSElem> nodes, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, IOException
    {
        StoragePoolHandler handler = getPoolHandlerByWrapper( wrapper );
        if (handler != null)
            return restoreVersionedFSElem(handler, nodes, targetIP, targetPort, targetPath, flags, user);
        else
            Log.err("Ungültiger Handler in Aufruf", "restoreFSElem");
        return false;
    }
    public boolean restoreVersionedFSElem( StoragePoolHandler handler, List<RemoteFSElem> nodes, String targetIP, int targetPort, String targetPath, int flags, User user ) throws SQLException, IOException
    {
        List<FileSystemElemNode> fseNodes = handler.resolve_node_by_remote_elem( nodes);
        List<FileSystemElemAttributes> fseAttrs = handler.resolve_attrs_by_remote_elem( nodes);

        boolean ret = false;

        StoragePoolQry qry = handler.getPoolQry();
        
        try
        {
            InetAddress adr = InetAddress.getByName(targetIP);
            RemoteFSElem target = RemoteFSElem.createDir(targetPath);
            Restore rest = new Restore(handler, fseNodes, fseAttrs, flags, qry, adr, targetPort, target);

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

    String checkRestoreErrFSElem( IWrapper wrapper, RemoteFSElem path ) {
        
        StoragePoolHandler sp = getPoolHandlerByWrapper( wrapper );
        FileSystemElemNode fseNode = null;
        try {
            fseNode = sp.resolve_node_by_remote_elem(path);
            if (fseNode == null) {
                return "Kein Eintrag in VSM-DB für: " + path.getName();
            }
        }
        catch (SQLException sQLException) {
            return "Abbruch beim Lesen von Eintrag in VSM-DB für: " + path.getName() + ": " + sQLException.getMessage();
        }
        
        return sp.buildCheckOpenNodeErrText(fseNode);
    }
 

}
