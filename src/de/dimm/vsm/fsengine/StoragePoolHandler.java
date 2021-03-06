/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.fsengine.hashcache.HashCache;
import de.dimm.vsm.net.SearchEntry;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.Exceptions.DBConnException;
import de.dimm.vsm.net.interfaces.BootstrapHandle;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.auth.UserManager;
import de.dimm.vsm.backup.HandleWriteRunner;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.SearchContext;
import de.dimm.vsm.net.SearchPathResolver;
import de.dimm.vsm.preview.IPreviewData;
import de.dimm.vsm.preview.IPreviewReader;
import de.dimm.vsm.preview.PreviewReader;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.ArchiveJobFileLink;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;



/**
 *
 * @author mw
 */
public abstract class StoragePoolHandler /*implements RemoteFSApi*/
{
    protected StoragePool pool;
    
    private static long nodeMinFreeSpace = 1000*1024*1024; // 1GB
    //
    protected static final String BS_REGEXP_SEPARATOR = "\\\\";
    protected static final String SLASH_REGEXP_SEPARATOR = "/";
    
    // FIlesystemchanges < 120 s werden nicht historisiert:
    // Attribute werden updated nicht neu inserted
    // Datein werden gelöscht, nicht DeleteFLags gesetzt
    public static int minFilechangeThresholdS = 120;    



    // THIS LIST CONTAINS ALL HANDLERS OF S-NODES FROM THE POOL
    protected ArrayList<StorageNodeHandler> storage_node_handlers;
    protected HashMap<String,FileSystemElemNode> dirHashMap = new HashMap<>();

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DEBUG, DANGEROUS !!!!!!!!!!!!!!!!!!!!
    public static final boolean immediateCommit = false;
    protected boolean skip_instantiate_in_fs = false;;
    protected boolean skip_bootstrap = false;


    protected StoragePoolQry poolQry;
    protected FSEMapEntryHandler fseMapHandler;
    SearchContext searchContext;
    PathResolver pathResolver;
    
    HandleWriteRunner writeRunner;
    
    private IPreviewReader previewReader;    
    

    public StoragePoolHandler( StoragePool pool, StoragePoolQry qry )
    {
        this.pool = pool;
        poolQry = qry;
        storage_node_handlers = new ArrayList<>();

        fseMapHandler = new FSEMapEntryHandler(this);

        searchContext = null;
        pathResolver = new PathResolver(this);
        writeRunner = new HandleWriteRunner();                
    }

    public IPreviewReader getPreviewReader() {
        // Lazy creation
        if (previewReader == null) {
            previewReader = new PreviewReader(this);
        }
        return previewReader;
    }
    
        

    public static int getMinFilechangeThresholdS()
    {
        return minFilechangeThresholdS;
    }
    /**
     * Ist Node old genug zum persitieren von Änderungen
     * @param node
     * @return 
     */
    public boolean isFileChangePersitent(FileSystemElemNode node)
    {
        long actTs = System.currentTimeMillis();
                // Nur bei Änderungen, die länger als MIN_FILECHANGE_THRESHOLD_S existieren, wird ein neues Attribut vergeben
        long diffSinceLastUpdate = actTs - node.getAttributes().getTs();
        return (diffSinceLastUpdate/1000 > getMinFilechangeThresholdS());                
    }
    
    

    public static void setNodeMinFreeSpace( long _nodeMinFreeSpace )
    {
        nodeMinFreeSpace = _nodeMinFreeSpace;
    }

    public long getNodeMinFreeSpace()
    {
        return nodeMinFreeSpace;
    }
    


    public void setSearchContext( SearchContext searchContext )
    {
        this.searchContext = searchContext;
    }

    public void setPathResolver( PathResolver pathResolver )
    {
        this.pathResolver = pathResolver;
    }

    public abstract GenericEntityManager getEm();



    public abstract void em_persist( Object o, boolean noCache ) throws SQLException;
    public abstract void em_persist( Object o ) throws SQLException;
    public abstract void raw_persist( Object o, long idx ) throws SQLException;
    public abstract void em_remove( Object o ) throws SQLException;
    public abstract void em_detach( Object o ) throws SQLException;
    public abstract <T> T em_merge( T t ) throws SQLException;
    public abstract <T> T em_find( Class<T> t, long idx ) throws SQLException;
    public abstract void tx_commit();
    public abstract  void check_open_transaction();
    public abstract  void commit_transaction() throws SQLException;
    public abstract  boolean  is_transaction_active();
    public abstract  void check_commit_transaction() throws SQLException;
    public abstract  void rollback_transaction();
    public abstract  void close_transaction() throws SQLException;
    public abstract void close_entitymanager();
    public abstract <T> List<T> createQuery( String string, Class<T> aClass ) throws SQLException;
    public abstract <T> List<T> createQuery( String string, Class<T> aClass, int maxResults ) throws SQLException;
    public abstract <T> List<T> createQuery( String string, Class<T> aClass, int maxResults, boolean distinct ) throws SQLException;
    public abstract List<Object[]> createNativeQuery( String string, int maxResults ) throws SQLException;
    public abstract boolean nativeCall( String string );
    public abstract int nativeUpdate( String string );
    public abstract <T> T createSingleResultQuery( String string, Class<T> aClass ) throws SQLException;
    public abstract void em_refresh( Object fseNode );
    public abstract DedupHashBlock findHashBlock( String remote_hash );

//    public abstract void addDedupBlock2Cache(DedupHashBlock blk );
//    public abstract DedupHashBlock getDedupBlockFromCache( String remote_hash );



    public void setPoolQry( StoragePoolQry poolQry )
    {
        this.poolQry = poolQry;
    }

//    public abstract void updateLazyListsHandler(  List l );
//    public abstract void updateLazyListsHandler(  FileSystemElemNode node );

    public boolean realizeInFs() throws SQLException
    {
        // GET ROOT NODE
        FileSystemElemNode node = resolve_elem_by_path( "/" );

        // RELOAD FROM DB
        node = em_find(FileSystemElemNode.class, node.getIdx());


        // IF THIS ROOT WASNT INSTATIATED IN FS, WE DO IT ON THE FLY
        if (node.getLinks(getEm()).isEmpty())
        {
            List<AbstractStorageNode> s_nodes = get_primary_storage_nodes(/*forWrite*/ true);
            if (s_nodes.isEmpty())
                throw new SQLException("No Storagenodes online" );
            try
            {

                for (int i = 0; i < s_nodes.size(); i++)
                {
                    AbstractStorageNode abstractStorageNode = s_nodes.get(i);
                    PoolNodeFileLink l = new PoolNodeFileLink();
                    l.setFileNode(node);
                    l.setStorageNode(abstractStorageNode);
                    em_persist(l);
                    node.addLink( getEm(), l );
//                    node = em_merge(node);
                }
                instantiate_in_fs(s_nodes, node);
                write_bootstrap_data(node);
            }
            catch (SQLException | PathResolveException | IOException | PoolReadOnlyException exc)
            {
                Log.err("Wurzelverzeichnis kann nicht erzeugt werden", pool.toString(), exc);
                return false;
            }
        }
        return true;
    }
    
    void checkAvailable()
    {
        List<AbstractStorageNode> node_list = resolve_storage_nodes(pool);
        for (int i = 0; i < node_list.size(); i++)
        {
            AbstractStorageNode abstractStorageNode = node_list.get(i);
            boolean statusChanged = false;

            if (StorageNodeHandler.isAvailable(abstractStorageNode))
            {
                // DETECT ONLINE NODE
                if (abstractStorageNode.isTempOffline())
                {
                    abstractStorageNode.setNodeMode(AbstractStorageNode.NM_ONLINE);
                    statusChanged = true;
                }
            }
            else
            {
                // DETECT MISSING NODE SET TO TEMP-OFFLINE
                if (abstractStorageNode.isOnline())
                {
                    abstractStorageNode.setNodeMode(AbstractStorageNode.NM_TEMP_OFFLINE);
                    statusChanged = true;
                }
            }
            if (statusChanged)
            {
                check_open_transaction();
                try
                {
                    em_merge(abstractStorageNode);
                    if (abstractStorageNode.getCloneNode() != null)
                    {
                        em_merge(abstractStorageNode.getCloneNode());
                    }

                    commit_transaction();
                }
                catch (SQLException exc)
                {
                    Log.err("SpeicherNode kann nicht aktualisiert werden", abstractStorageNode.getName(), exc);
                }
            }
        }
    }

    public long checkStorageNodeSpace(List<AbstractStorageNode> node_list)
    {
        for (int i = 0; i < node_list.size(); i++)
        {
            AbstractStorageNode abstractStorageNode = node_list.get(i);
            
            // OFFLINE
            if (!abstractStorageNode.isOnline())
                continue;

            // FS MISSING ?
            if (!StorageNodeHandler.isAvailable(abstractStorageNode))
                continue;

            long free = StorageNodeHandler.getFreeSpace(abstractStorageNode);
            if (free < nodeMinFreeSpace)
            {
                LogicControl.sleep(1000);
                free = StorageNodeHandler.getFreeSpace(abstractStorageNode);
                if (free < nodeMinFreeSpace)
                {
                    free = StorageNodeHandler.getFreeSpace(abstractStorageNode);
                    Log.warn("SpeicherNode ist voll, Restmenge " + SizeStr.format(free) + ": " +  abstractStorageNode.getName());
                    abstractStorageNode.setNodeMode(AbstractStorageNode.NM_FULL);

                    // CLONE NODE TOO
                    if (abstractStorageNode.getCloneNode() != null)
                    {
                        abstractStorageNode.getCloneNode().setNodeMode(AbstractStorageNode.NM_FULL);
                    }


                    check_open_transaction();
                    try
                    {
                        em_merge(abstractStorageNode);
                        if (abstractStorageNode.getCloneNode() != null)
                        {
                            em_merge(abstractStorageNode.getCloneNode());
                        }
                        
                        commit_transaction();
                    }
                    catch (SQLException exc)
                    {
                        Log.err("SpeicherNode kann nicht aktualisiert werden", abstractStorageNode.getName(), exc);
                    }

                    // THIS NODE IS NOT ONLINE ANYMORE, TRY NEXT
                    continue;
                }
            }
            return free;
        }
        return 0;
    }

    public boolean checkStorageNodeExists()
    {
        // CHECK FOR EXISTENCE
        List<AbstractStorageNode> node_list = get_primary_storage_nodes(/*forWrite*/ true);
        if (node_list.isEmpty())
            return false;
        
        for (int i = 0; i < node_list.size(); i++)
        {
            AbstractStorageNode abstractStorageNode = node_list.get(i);
            File f = new File(abstractStorageNode.getMountPoint());
            if (!f.exists())
                return false;
        }
        return true;

    }


    public long checkStorageNodeSpace()
    {
        // CHECK FOR SPACE ON REG NODES
        checkAvailable();
        List<AbstractStorageNode> node_list = get_primary_storage_nodes(/*forWrite*/ false);
        long freeSpace = checkStorageNodeSpace( node_list );

        // AND DD NODES
        ArrayList<AbstractStorageNode> l = new ArrayList<>();

        AbstractStorageNode ddNode = get_primary_dedup_node_for_write();
        if (ddNode != null)
        {
            l.add(ddNode);
        }

        long freeDDSpace = checkStorageNodeSpace( l );


        //RETURN SMALLES RESTLEN
        return (freeSpace < freeDDSpace)? freeSpace : freeDDSpace;
    }


    public void init_bootstrap()
    {
        List<AbstractStorageNode> node_list = resolve_storage_nodes( pool );
        for (int i = 0; i < node_list.size(); i++)
        {
            AbstractStorageNode abstractStorageNode = node_list.get(i);
            StorageNodeHandler node_handler = get_handler_for_node( abstractStorageNode );

            try
            {
                BootstrapHandle handle = new FS_BootstrapHandle<>(abstractStorageNode, abstractStorageNode);
                Object o = handle.read_object(abstractStorageNode);
                if (o == null)
                {
                    handle.write_object(abstractStorageNode);
                }

                handle = new FS_BootstrapHandle<>(abstractStorageNode, pool);
                o = handle.read_object(pool);
                if (o == null)
                {
                    handle.write_object(pool);
                }
            }
            catch (PathResolveException | IOException exc)
            {
                Log.err("Cannot init_bootstrap", exc);
            }
        }
    }

    public StoragePoolQry getPoolQry()
    {
        return poolQry;
    }



    public static String getSeparator( RemoteFSElem fse )
    {
        if (fse.getSeparatorChar() == '\\')
            return BS_REGEXP_SEPARATOR;
        return SLASH_REGEXP_SEPARATOR;
    }

    public StoragePool getPool()
    {
        return pool;
    }


    public long getTotalBlocks()
    {
        if (searchContext != null)
        {
            return searchContext.getTotalBlocks();
        }
        List<AbstractStorageNode> storageNodes = get_primary_storage_nodes(/*forWrite*/ true);
        if (storageNodes.isEmpty())
            return 0;

        StorageNodeHandler sn_handler = get_handler_for_node(storageNodes.get(0));
        return sn_handler.getTotalBlocks();

    }

    public long getUsedBlocks()
    {
        if (searchContext != null)
        {
            return searchContext.getUsedBlocks();
        }
        List<AbstractStorageNode> storageNodes = get_primary_storage_nodes(/*forWrite*/ true);
        if (storageNodes.isEmpty())
            return 0;
        StorageNodeHandler sn_handler = get_handler_for_node(storageNodes.get(0));

        return sn_handler.getUsedBlocks();

    }
    public int getBlockSize()
    {
        if (searchContext != null)
        {
            return searchContext.getBlockSize();
        }

        List<AbstractStorageNode> storageNodes = get_primary_storage_nodes(/*forWrite*/ true);
        if (storageNodes.isEmpty())
            return 0;

        StorageNodeHandler sn_handler = get_handler_for_node(storageNodes.get(0));

        return sn_handler.getBlockSize();
    }

    // THIS IS THE ENTRY FROM THE MOUNTED DRIVE BEFORE get_child_nodes, AFTER resolve_elem_by_path
    public FileSystemElemNode resolve_node_by_remote_elem(  RemoteFSElem node ) throws SQLException
    {
        return pathResolver.resolve_node_by_remote_elem(node);
    }
    // THIS IS THE ENTRY FROM THE MOUNTED DRIVE BEFORE get_child_nodes, AFTER resolve_elem_by_path
    public List<FileSystemElemNode> resolve_node_by_remote_elem(  List<RemoteFSElem> nodes ) throws SQLException
    {
        List<FileSystemElemNode> retList = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++)
        {
            RemoteFSElem node = nodes.get(i);
            retList.add(resolve_node_by_remote_elem(node));

        }
        return retList;
    }
    
    public List<FileSystemElemAttributes> resolve_attrs_by_remote_elem(  List<RemoteFSElem> nodes ) throws SQLException, IOException
    {
        List<FileSystemElemAttributes> retList = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++)
        {
            RemoteFSElem node = nodes.get(i);
            FileSystemElemNode fseNode = resolve_node_by_remote_elem(node);
            List<FileSystemElemAttributes> history = fseNode.getHistory(getEm());
            FileSystemElemAttributes attr = null;
            for (int j = 0; j < history.size(); j++)
            {
                FileSystemElemAttributes fileSystemElemAttributes = history.get(j);
                if (fileSystemElemAttributes.getIdx() == node.getAttrIdx())
                {
                    attr = fileSystemElemAttributes;
                    break;
                }                
            }
            if (attr == null)
                throw new IOException("Kann Attribut nicht aufloesen " + fseNode);
            retList.add(attr);
        }
        return retList;
    }
   
    

    // THIS IS THE ENTRY FROM THE MOUNTED DRIVE
    public FileSystemElemNode resolve_elem_by_path( String dir_path) throws SQLException
    {
        return pathResolver.resolve_elem_by_path(dir_path);
    }

    // USED BY BACKUP TO READ INCREMENTAL DIRECTORIES
    public FileSystemElemNode resolve_child_elem( FileSystemElemNode node, RemoteFSElem childRemoteFSElem )
    {
        String sep = getSeparator(childRemoteFSElem);
        String[] path_arr = childRemoteFSElem.getPath().split(sep);

        if (path_arr.length == 0)
            return null;

        // GET NEXT CHILD
        return resolve_child_node( node, path_arr[path_arr.length - 1] );
    }

    public static String resolve_real_child_path( RemoteFSElem childRemoteFSElem )
    {
        return resolve_real_child_path(childRemoteFSElem.getPath(), childRemoteFSElem.getSeparatorChar());
    }
    public static String resolve_real_child_path( String path, char seperator )
    {

        if (path.length() == 0)
            return path;

        // GET RID OF : IN PATH
        if (path.length() > 2 && path.charAt(1) == ':')
        {
            StringBuilder sb = new StringBuilder();
            sb.append( path.charAt(0));
            sb.append(path.substring(2));
            path = sb.toString();
        }

        if (seperator != '/')
        {
            path = path.replace( seperator, '/' );
        }
        if (path.charAt(0) == '/')
            path = path.substring(1);

        return path;
    }

    public FileSystemElemNode getRootDir()
    {
        return pathResolver.getRootDir();
    }


    // PROBABLY INEFFICIENT
    public FileSystemElemNode resolve_parent_dir_node( String rel_dir_path )
    {
        return pathResolver.resolve_parent_dir_node( rel_dir_path);
    }

    // CREATES A DIRECTORY-NODE INCLUDING ALL NECESSARY PARENTDIRECTORIES FOR A PATH
    public FileSystemElemNode create_parent_dir_node( String dir_path ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        return pathResolver.create_parent_dir_node(dir_path);
    }


    boolean useDirHmap = false;

    FileSystemElemNode resolve_child_node( FileSystemElemNode act_dir, String string  )
    {
        FileSystemElemNode node = null;
        if (useDirHmap)
        {
         node = dirHashMap.get(Long.toHexString(act_dir.getIdx()) + string);
         if (node != null)
             return node;
        }
        boolean showDeleted = getPoolQry().isShowDeleted();

        List<FileSystemElemNode> childNodes = act_dir.getChildren(getEm());
        

        if (childNodes != null)
        {
            for (int i = 0; i < childNodes.size(); i++)
            {
                FileSystemElemNode fileSystemElemNode = childNodes.get(i);
                if (fileSystemElemNode == null )
                {
                    Log.err("Found empty Childnode in " , act_dir.toString()  );
                    continue;
                }
                if (fileSystemElemNode.getAttributes() == null )
                {
                    Log.err("Found empty ChildAttribute in " , act_dir.toString() + ": " + fileSystemElemNode.toString()  );
                    continue;
                }
                if (!showDeleted && fileSystemElemNode.getAttributes().isDeleted())
                    continue;
                
                if (fileSystemElemNode.getName().equals(string))
                {
                    node = fileSystemElemNode;
                    break;
                }
            }
        }
        else
        {
            Log.err("No childs found for node" , act_dir.toString() + ": " + string );
        }

        if (useDirHmap && node != null)
        {
            if (dirHashMap.size() > 10000)
                dirHashMap.clear();

            dirHashMap.put(Long.toHexString(act_dir.getIdx()) + string, node);
        }

        return node;
    }
    public static void build_relative_virtual_path( FileSystemElemNode file_node, StringBuilder sb )
    {
        sb.setLength(0);
        sb.insert(0, file_node.getName());
        sb.insert(0, "/");

        int max_depth = 1024;

        while( file_node.getParent() != null)
        {
            file_node = file_node.getParent();
            if (!file_node.getName().equals("/"))
            {
                sb.insert(0, file_node.getName());
                sb.insert(0, "/");
            }

            if (max_depth-- <= 0)
                throw new RuntimeException("Path_is_too_deep");
        }
    }

    public void build_virtual_path( FileSystemElemNode file_node, StringBuilder sb )
    {
        build_relative_virtual_path( file_node, sb);

        sb.insert(0, pool.getName() );
        sb.insert(0, "/" );
    }
    public String resolve_node_path(FileSystemElemNode file_node )
    {
        StringBuilder sb = new StringBuilder();

        build_virtual_path(file_node, sb);

        return sb.toString();
    }

    public List<PoolNodeFileLink> get_pool_node_file_links( FileSystemElemNode dnode )
    {
        List<PoolNodeFileLink> list = dnode.getLinks(getEm());
        return list;
    }

    // RECURSIVE REMOVE!
    boolean _remove_fse_node( FileSystemElemNode node ) throws PoolReadOnlyException, SQLException, IOException, PathResolveException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);

        Log.debug("Removing node ", node.toString());

        // REMOVE CHILDREN -> RECURSIVE DOWN FIRST!!!, NO CASCADE DELETE IN ER-MAPPING
        List<FileSystemElemNode> children = get_child_nodes(node);
        while(children.size() > 0)
        {
            FileSystemElemNode child = children.remove(0);
            _remove_fse_node( child);
        }
        check_open_transaction();

        FileSystemElemAttributes attr = node.getAttributes();

        try
        {
            // IF DB REMOVAL IS OKAY, REMOVE FS DATA
            if (remove_fs_entries(node))
            {
                // REMOVE ARCHIVEJOB ENTRIES, TODO: IMPROVE PERFORMANCE
                nativeCall("delete from ArchiveJobFileLink where ArchiveJobFileLink.fileNode_idx=" + node.getIdx());
                commit_transaction();

                // Löschen mit allen Cascade-Deletes (Siehe Record Spec)
                em_remove(node);
                commit_transaction();

                return true;
            }
        }
        catch (java.sql.SQLIntegrityConstraintViolationException e )
        {
            Log.err("Constraint Fehler", node.toString(), e);
        }
        finally
        {
            node.setAttributes(attr);
        }
        return false;
    }

    boolean remove_fs_entries( FileSystemElemNode node )
    {

        // REMOVE FS ENTITIES, IF THIS FAILS, WE ARE LOST, THIS LEAVES SOME ORPHANS
        try
        {
            FileHandle[] fh = resolve_all_file_handles(node);

            for (int i = 0; i < fh.length; i++)
            {
                if (!fh[i].delete())
                {
                    Log.err( "Dateiverknüpfungen können nicht gelöscht werden", fh[i].toString());
                }
            }
            FileHandle[] afh = resolve_all_attribute_handles(node);

            for (int i = 0; i < afh.length; i++)
            {
                if (!afh[i].delete())
                {
                    Log.err( "Dateiattribute können nicht gelöscht werden",  afh[i].toString());
                }
            }
            BootstrapHandle[] bfh = resolve_all_bootstrap_handles(node);

            for (int i = 0; i < bfh.length; i++)
            {
                if (!bfh[i].delete())
                {
                    Log.err( "Bootstrapdaten können nicht gelöscht werden",  bfh[i].toString());
                }
            }
            return true;
        }
        catch (PathResolveException | PoolReadOnlyException | UnsupportedEncodingException exc)
        {
            Log.err( "Löschen schlug fehl", node.toString(), exc);
        }
        
        // Entfernen der Previews
        try {
            List<FileSystemElemAttributes> attrs = node.getHistory(getEm());
            for (FileSystemElemAttributes attr : attrs) {
                File f = PreviewReader.getExistingPreviewFile(node, this, attr);
                if (f != null && f.exists()) {
                    f.delete();
                }
            }
        }
        catch (Exception exc) {
            Log.err( "Entfernen der  Previews schlug fehl", node.toString(), exc);
        }
        return false;
    }

    public boolean remove_job( ArchiveJob job ) throws  SQLException, PoolReadOnlyException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FSEIndexer indexer = LogicControl.getStorageNubHandler().getIndexer(pool);
        indexer.open();

        em_refresh(job);
        check_open_transaction();

        FileSystemElemNode jobDir = job.getDirectory();

        job.setDirectory(null);
        em_merge(job);
        commit_transaction();

        // REMOVE ALL LINKED FILENODES BACKWARDS
        List<ArchiveJobFileLink> linkList = job.getLinks().getList(getEm());

        for (int i = linkList.size() - 1; i >= 0; i--)
        {
            ArchiveJobFileLink archiveJobFileLink = linkList.get(i);
            remove_fse_node(archiveJobFileLink.getFileNode(), true);
            indexer.removeNodes(archiveJobFileLink.getFileNode());
        }

        em_remove(job);

        if (jobDir != null)
            remove_fse_node(jobDir, true);
        
        commit_transaction();


        indexer.removeJob(job);

        indexer.flushSync();


        return true;
    }

    public boolean remove_fse_node( FileSystemElemNode node, boolean withCommit ) throws PoolReadOnlyException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);


        try
        {
            // W commit, maybe someone has created the node a second ago
            commit_transaction();
            // FIRST REMOVE DB-ENTRY, IF THIS FAILS, WE HAVE ROLLBACK AND NOTHING IS CHANGED
            check_open_transaction();
            
            _remove_fse_node( node);

            // OK, SEEMS SO THAT WE DELETED EVERYTHING IN CORRECT ORDER, REST IS DONE BY CASCADE DELETE (ATTR, NODELINKS)
            // UNLINK FROM PARENT
            if (node.getParent() != null)
            {
                if (!node.getParent().getChildren().removeIfRealized(node))
                {
                    node.getParent().getChildren().unRealize();
                }
            }


            if (withCommit)
            {
                commit_transaction();
            }

        }
        catch (SQLException | PoolReadOnlyException | IOException | PathResolveException exc)
        {
            Log.err("Cannot remove_fse_node", node.toString(), exc);
            return false;
        }

        return true;
    }

    public List<AbstractStorageNode> get_primary_storage_nodes( boolean forWrite )
    {
        List<AbstractStorageNode> list = resolve_storage_nodes( pool );

        List<AbstractStorageNode> ret = new ArrayList<>();

        // TODO: HANDLE MULTIPLE NODES
        // WE HAVE NO CLONING YET
        for (int i = 0; i < list.size(); i++)
        {
            AbstractStorageNode s_node = list.get(i);
            if (forWrite)
            {
                // FOR WRITE WE NEED THE FIRST WRITABLE NODE
                if (s_node.isOnline())
                {
                    ret.add( s_node );
                    break;
                }
            }
            else
            {
                // FOR READ WE NEED ALL READABLE
                if (s_node.isFullOrOnline())
                {
                    ret.add( s_node );
                }
            }
        }
        
        if (ret.size() > 0)
            return ret;

        return ret;
    }

    public AbstractStorageNode get_primary_dedup_node_for_write()
    {
        // TODO: MERKMAL HAS DEDUP BLOCKS
        // WE HAVE NO CLONING YET
        List<AbstractStorageNode> list = resolve_storage_nodes( pool );
        for (int i = 0; i < list.size(); i++)
        {
            AbstractStorageNode s_node = list.get(i);
            if (s_node.isOnline())
            {
                return s_node;
            }
        }
        return null;
    }

    boolean retryConnectOnException = false;
    public List<AbstractStorageNode> register_fse_node_to_db( FileSystemElemNode node) throws IOException, PoolReadOnlyException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);



        PoolNodeFileLink pnfl;
        try
        {
            check_open_transaction();

            List<AbstractStorageNode> s_nodes = get_primary_storage_nodes(/*forWrite*/ true);
            if (s_nodes.isEmpty())
                throw new IOException("Cannot find primary storage");


            // STORE ELEM, WE HAVE TO RESET ATTR, BECAUSE IF CONSTRAINT INTEGRITY
            // THIS SHOULD BE MADE BETTER
            FileSystemElemAttributes attr = node.getAttributes();
            em_persist(node);
            em_persist(attr);
            node.setAttributes(attr);
            em_merge(node);
                        


            // UPDATE LINK
            for (int i = 0; i < s_nodes.size(); i++)
            {
                AbstractStorageNode s_node = s_nodes.get(i);

                pnfl = new PoolNodeFileLink();
                pnfl.setCreation(new Date());

                pnfl.setStorageNode(s_node);
                pnfl.setFileNode(node);

                // STORE STORAGENODE LINK
                em_persist(pnfl);

                node.addLink(getEm(), pnfl);
            }

            check_commit_transaction();

            return s_nodes;
        }
        catch (IOException | SQLException e)
        {
            // ON ERR RETRY BROKEN CONNECTION
            if (!retryConnectOnException)
            {
                retryConnectOnException = true;
                reinitConnection();
                List<AbstractStorageNode> l =  register_fse_node_to_db( node);
                retryConnectOnException = false;
                return l;
            }
            Log.err("Cannot register node", node.toString(), e);


            throw new IOException("Cannot register node: " + e.getMessage(),  e);
        }
    }

    private static AbstractStorageNode get_preferred_storage_node( List<PoolNodeFileLink> link_list )
    {
        for (int i = 0; i < link_list.size(); i++)
        {
            PoolNodeFileLink poolNodeFileLink = link_list.get(i);

            AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();
            if (s_node.isOnline())
            {
                return s_node;
            }
        }

        return null;
    }

    public FileSystemElemNode mkdir( String pathName ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        return create_fse_node_complete( pathName, FileSystemElemNode.FT_DIR );
    }


    private static String get_name_from_path( String fileName ) throws PathResolveException
    {
        if (fileName.isEmpty())
            return fileName;

        if (fileName.charAt(0) != '/')
            return fileName;

        // ROOT ?
        if (fileName.length() == 1)
            return fileName;

        int idx = fileName.lastIndexOf('/');

        if (idx == fileName.length() - 1)
            throw new PathResolveException("Path has trailing slash:" + fileName);

        return fileName.substring(idx + 1);

    }
    private static  String get_parent_from_path( String fileName ) throws PathResolveException
    {
        if (fileName.isEmpty())
            return null;

        if (fileName.charAt(0) != '/')
            throw new PathResolveException("Path is relative:" + fileName);

        // ROOT ?
        if (fileName.length() == 1)
            return null;

        int idx = fileName.lastIndexOf('/');

        if (idx == fileName.length()-1)
            throw new PathResolveException("Path has trailing slash:" + fileName);

        return fileName.substring(0, idx);

    }


    public FileSystemElemNode create_fse_node_complete( String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileSystemElemNode node = null;

        FileSystemElemNode parent = resolve_parent_dir_node(fileName);

        if (parent != null)
        {
            if (isReadOnly(parent))
                throw new PoolReadOnlyException(pool);
            
            // Check for existing same FileType, then just add a new Attribute
            try
            {
                node = resolve_node(fileName);
                // Same Type ? 
                if (node != null && node.getTyp().equals(type))
                {
                    // SHould we undelete ?
                    if (node.getAttributes().isDeleted())
                    {
                        undelete_attributes( node, System.currentTimeMillis());                        
                    }
                    return node;
                }
            }
            catch (SQLException exc)
            {
                 Log.err("Cannot create_fse_node_complete", exc);
            }
        }

        String name = get_name_from_path( fileName );

        Date now = new Date();

        int posixMode = 0666;
        // CREATE AND FILL NODE
        if (FileSystemElemNode.isFile(type))
        {
            node = FileSystemElemNode.createFileNode();
            posixMode |= 0100000;
        }
        if (FileSystemElemNode.isDirectory(type))
        {
            node = FileSystemElemNode.createDirNode();
            posixMode |= 0040000;
        }
        if (node == null)
        {
            Log.err("Cannot create_fse_node_complete for type " + type);
            return null;
        }
        node.getAttributes().setName(name);
        node.getAttributes().setAccessDateMs( now.getTime());
        node.getAttributes().setCreationDateMs( now.getTime());
        node.getAttributes().setModificationDateMs( now.getTime());
        node.getAttributes().setTs(  System.currentTimeMillis());
        node.getAttributes().setPosixMode( posixMode );


        node.setTyp(type);
        node.setParent(parent);
        if (parent == null) // THIS IS ROOT
        {
            node.setPool(getPool());
        }
        else
        {
            node.setPool(parent.getPool());
            // Add new Childs to parent, maybe we have live filesystem
//            parent.getChildren(getEm()).add(node);
        }


        // CREATE IN REAL FS
        List<AbstractStorageNode> s_nodes = register_fse_node_to_db( node );
        
        // Reload from DB
        try
        {
            node = resolve_fse_node_from_db(node.getIdx());
        }
        catch (SQLException sQLException)
        {
            throw new IOException("Relead von DB schlug fehl", sQLException);
        }
        if (parent != null)
        {
            parent.getChildren().addIfRealized(node);
        }

        if (!skip_instantiate_in_fs)
            instantiate_in_fs( s_nodes, node );

        // WRITE BOOTSTRAPDATA
        if (!skip_bootstrap)
            write_bootstrap_data( node );

        
        return node;
    }

    public void instantiate_in_fs( List<AbstractStorageNode> s_nodes, FileSystemElemNode node ) throws PathResolveException, IOException, PoolReadOnlyException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);

        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);
            StorageNodeHandler sn_handler = get_handler_for_node(s_node);

            FileHandle ret = sn_handler.create_file_handle(node, true);
            ret.create();
        }
    }

    public FileSystemElemNode create_fse_node( String abs_path, RemoteFSElem elem, long actTimestamp ) throws IOException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);


        FileSystemElemNode node;

        FileSystemElemNode parent = resolve_parent_dir_node( abs_path);
        if (parent == null)
        {
            parent = create_parent_dir_node(abs_path);
        }
        else
        {
             if (isReadOnly(parent))
                throw new PoolReadOnlyException(pool);    
        }

        String name = get_name_from_path( abs_path );


        // CREATE AND FILL NODE
        if (elem.isFile())
        {
            node = FileSystemElemNode.createFileNode();
            node.getAttributes().setAclInfoData( elem.getAclinfoData() );
        }
        else if (elem.isDirectory())
        {
            node = FileSystemElemNode.createDirNode();
            node.getAttributes().setAclInfoData( elem.getAclinfoData() );
        }
        else if (elem.isSymbolicLink())
        {
            node = FileSystemElemNode.createSymLinkNode();

            // A REAL HACK: SYMLINK DONT NEED EXTENDED ATTRIBUTES SO WE USE THIS FIELD FOR LINKPATH
            node.getAttributes().setAclInfoData( elem.getLinkPath() );
        }
        else
        {
            node = FileSystemElemNode.createOtherNode();
            node.getAttributes().setAclInfoData( elem.getAclinfoData() );
        }
        node.getAttributes().setName(name);

        node.getAttributes().setTs( actTimestamp );
        node.getAttributes().setAccessDateMs( elem.getAtimeMs());
        node.getAttributes().setCreationDateMs( elem.getCtimeMs());
        node.getAttributes().setModificationDateMs( elem.getMtimeMs());
        node.getAttributes().setFsize( elem.getDataSize() );
        node.getAttributes().setStreamSize( elem.getStreamSize() );
        node.getAttributes().setPosixMode(elem.getPosixMode());
        node.getAttributes().setUid(elem.getUid());
        node.getAttributes().setGid(elem.getGid());
        node.getAttributes().setAclinfo( elem.getAclinfo());

        node.setParent(parent);

        if (parent == null) // THIS IS ROOT DIR
        {
            node.setPool(getPool());
        }
        else
        {
            parent.getChildren().addIfRealized(node);
            node.setPool(parent.getPool());
        }

        return node;
    }

    public void move_fse_node( FileSystemElemNode fsenode, String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly(fsenode))
            throw new PoolReadOnlyException(pool);

        String to_name = get_name_from_path( to );
        String from_parent = get_parent_from_path(from );
        String to_parent = get_parent_from_path( to );

        // Move inside same directory (Rename)
        if (from_parent.equals(to_parent))
        {
            boolean needNewAttributes = isFileChangePersitent(fsenode);

            // CREATE NEW ATTRIBUTES OR UPDATE OLD ONE
            FileSystemElemAttributes attr = fsenode.getAttributes();
            if (needNewAttributes)
            {
                attr = new FileSystemElemAttributes(fsenode.getAttributes());
            }

            attr.setName(to_name);           

            mergeOrPersistAttribute(fsenode, attr, needNewAttributes, System.currentTimeMillis());             
            commit_transaction();            
            write_bootstrap_data( fsenode );
            return;
        }
        throw new IOException("Unsupported Move Operation");
    }

    public boolean isReadOnly()
    {
        return poolQry.isReadOnly();
    }

    public boolean isReadOnly(FileSystemElemNode node)
    {
        return poolQry.isReadOnly(node);
    }


    public List<FileSystemElemNode> get_child_nodes( FileSystemElemNode node )
    {
        List<FileSystemElemNode> childNodes = node.getChildren(getEm());
//        updateLazyListsHandler(childNodes);
        return childNodes;
    }


    public long open_fh( String path, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        FileSystemElemNode node = resolve_elem_by_path(path);
        if (node == null)
            return -1;


        return open_fh( node, create);
    }

    public long open_fh( long idx, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        // FILE HANDLE IDX IS NOT NODE-IDX

        // TODO: CACHE
        FileSystemElemNode node = resolve_fse_node_from_db(idx);
        if (node == null)
            throw new PathResolveException("Kein Node zu DB-Idx " + idx);

        long fileNo = open_fh( node, create );

        return fileNo;
    }


    public long open_stream( long idx, int streamInfo, boolean create ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
    {
        // FILE HANDLE IDX IS NOT NODE-IDX

        FileSystemElemNode node = resolve_fse_node_from_db(idx);
        if (node == null)
            throw new PathResolveException("Kein Node zu DB-Idx " + idx);

        long fileNo = open_stream( node, streamInfo, create );

        return fileNo;
    }


    public long open_fh( FileSystemElemNode node, boolean create ) throws IOException, PoolReadOnlyException, PathResolveException, SQLException
    {
        if (create && isReadOnly(node))
            throw new PoolReadOnlyException(pool);

        return fseMapHandler.open_fh(node, create);
    }

    public long open_versioned_fh( FileSystemElemNode node, FileSystemElemAttributes attrs ) throws IOException, PoolReadOnlyException, PathResolveException, SQLException
    {
        return fseMapHandler.open_versioned_fh(node, attrs);
    }

    public long open_stream( FileSystemElemNode node, int streamInfo, boolean create ) throws IOException, PoolReadOnlyException, PathResolveException, UnsupportedEncodingException, SQLException
    {
        if (create && isReadOnly(node))
            throw new PoolReadOnlyException(pool);

        return fseMapHandler.open_stream(node, streamInfo, create);
    }

    public void close_fh( long idx ) throws IOException
    {
        fseMapHandler.close_fh(idx);
    }

    public FileHandle getFhByFileNo( long fileNo )
    {
        return fseMapHandler.getFhByFileNo(fileNo);
    }
    public FileSystemElemNode getNodeByFileNo( long fileNo )
    {
        return fseMapHandler.getNodeByFileNo(fileNo);
    }
    public void removeByFileNo( long fileNo )
    {
        fseMapHandler.removeByFileNo(fileNo);
    }
    
    public String buildCheckOpenNodeErrText(FileSystemElemNode node) {
        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( node );
        
        if (s_nodes.isEmpty()) {
            List<PoolNodeFileLink> links = node.getLinks(getEm());
            StringBuilder sb = new StringBuilder("Online Speichernode für " + node.getName() + " nicht gefunden oder nicht valide:\n");

            for (PoolNodeFileLink poolNodeFileLink : links) {
                sb.append(poolNodeFileLink.getStorageNode().getName()).
                        append(" -> ").
                        append(poolNodeFileLink.getStorageNode().getNodeMode()).append("\n");
            } 
            return sb.toString();
        }
        return "";
    }
 
    public FileHandle open_versioned_file_handle(FileSystemElemNode node, FileSystemElemAttributes attrs ) throws IOException, PathResolveException, SQLException
    {
        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( node );
        FileHandle ret = null;
        if (s_nodes.isEmpty()) {            
            throw new IOException(buildCheckOpenNodeErrText(node));             
        }

        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);

            if (s_node.isFS())
            {
                StorageNodeHandler snHandler = get_handler_for_node(s_node);

                FileHandle fs_ret;
                fs_ret = snHandler.create_versioned_DDFS_handle( this, node, attrs );
                
                // IF WE HAVE MORE THAN ONE HANDLE
                if (ret != null)
                {
                    // IS RETURN ALREADY A MULTIHANDLE, THEN ADD THIS ONE
                    if (ret instanceof MultiFileHandle)
                    {
                        MultiFileHandle mfh = (MultiFileHandle)ret;
                        mfh.add(fs_ret);
                    }
                    else
                    {
                        // CREATE A MULTIHANDLE AND ADD THE CREATED HANDLES
                        MultiFileHandle mfh = new MultiFileHandle();
                        mfh.add(ret);
                        mfh.add(fs_ret);
                        ret = mfh;
                    }
                }
                else
                {
                    ret = fs_ret;
                }
            }
        }
        return ret;
    }    
    public FileHandle open_file_handle(FileSystemElemNode node, boolean create ) throws IOException, PathResolveException, SQLException
    {
        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( node );
        FileHandle ret = null;

        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);

            if (s_node.isFS())
            {
                StorageNodeHandler snHandler = get_handler_for_node(s_node);

                FileHandle fs_ret;
                if (getPool().isLandingZone())
                    fs_ret = snHandler.create_file_handle(node, create );
                else
                    fs_ret = snHandler.create_DDFS_handle( this, node, create );
                
                // IF WE HAVE MORE THAN ONE HANDLE
                if (ret != null)
                {
                    // IS RETURN ALREADY A MULTIHANDLE, THEN ADD THIS ONE
                    if (ret instanceof MultiFileHandle)
                    {
                        MultiFileHandle mfh = (MultiFileHandle)ret;
                        mfh.add(fs_ret);
                    }
                    else
                    {
                        // CREATE A MULTIHANDLE AND ADD THE CREATED HANDLES
                        MultiFileHandle mfh = new MultiFileHandle();
                        mfh.add(ret);
                        mfh.add(fs_ret);
                        ret = mfh;
                    }
                }
                else
                {
                    ret = fs_ret;
                }
            }
        }
        return ret;
    }

    public BootstrapHandle open_bootstrap_handle(FileSystemElemNode node ) throws IOException, PathResolveException
    {
        BootstrapHandle ret = null;

        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( node );
        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);

            if (s_node.isFS())
            {
                StorageNodeHandler sh = get_handler_for_node(s_node);
                BootstrapHandle fs_ret = sh.create_bootstrap_handle(node);

                // IF WE HAVE MORE THAN ONE HANDLE
                if (ret != null)
                {
                    // IS RETURN ALREADY A MULTIHANDLE, THEN ADD THIS ONE
                    if (ret instanceof MultiBootstrapHandle)
                    {
                        MultiBootstrapHandle mfh = (MultiBootstrapHandle)ret;
                        mfh.add(fs_ret);
                    }
                    else
                    {
                        // CREATE A MULTIHANDLE AND ADD THE CREATED HANDLES
                        MultiBootstrapHandle mfh = new MultiBootstrapHandle();
                        mfh.add(ret);
                        mfh.add(fs_ret);
                        ret = mfh;
                    }
                }
                else
                {
                    ret = fs_ret;
                }
            }
        }
        return ret;
    }

    public BootstrapHandle open_bootstrap_handle(FileSystemElemAttributes attr ) throws IOException, PathResolveException
    {
        BootstrapHandle ret = null;

        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( attr.getFile() );
        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);

            if (s_node.isFS())
            {
                StorageNodeHandler sh = get_handler_for_node(s_node);
                BootstrapHandle fs_ret = sh.create_bootstrap_handle(attr);

                // IF WE HAVE MORE THAN ONE HANDLE
                if (ret != null)
                {
                    // IS RETURN ALREADY A MULTIHANDLE, THEN ADD THIS ONE
                    if (ret instanceof MultiBootstrapHandle)
                    {
                        MultiBootstrapHandle mfh = (MultiBootstrapHandle)ret;
                        mfh.add(fs_ret);
                    }
                    else
                    {
                        // CREATE A MULTIHANDLE AND ADD THE CREATED HANDLES
                        MultiBootstrapHandle mfh = new MultiBootstrapHandle();
                        mfh.add(ret);
                        mfh.add(fs_ret);
                        ret = mfh;
                    }
                }
                else
                {
                    ret = fs_ret;
                }
            }
        }
        return ret;
    }
    
    public BootstrapHandle open_bootstrap_handle(PoolNodeFileLink attr ) throws IOException, PathResolveException
    {
        BootstrapHandle ret = null;
        AbstractStorageNode s_node = attr.getStorageNode();

        if (s_node.isFS())
        {
            StorageNodeHandler sh = get_handler_for_node(s_node);
            ret = sh.create_bootstrap_handle(attr);
        }
        return ret;
    }
        
    public BootstrapHandle open_bootstrap_handle(DedupHashBlock block, HashBlock node ) throws IOException, PathResolveException
    {
        AbstractStorageNode s_node = get_primary_dedup_node_for_write();

        if (s_node.isFS())
        {
            StorageNodeHandler sh = get_handler_for_node(s_node);
            BootstrapHandle ret = sh.create_bootstrap_handle(block, node);

            return ret;
        }
        throw new IOException("Unsupported StorageNode " + s_node.getName());

    }

    public BootstrapHandle open_bootstrap_handle(DedupHashBlock block, XANode node ) throws IOException, PathResolveException
    {
        AbstractStorageNode s_node = get_primary_dedup_node_for_write();

        if (s_node.isFS())
        {
            StorageNodeHandler sh = get_handler_for_node(s_node);
            BootstrapHandle ret = sh.create_bootstrap_handle(block, node);

            return ret;
        }
        throw new IOException("Unsupported StorageNode " + s_node.getName());

    }

    private List<AbstractStorageNode> resolve_storage_nodes( FileSystemElemNode node )
    {

//        return get_primary_storage_nodes(false);
        List<PoolNodeFileLink> list = get_pool_node_file_links(node);
        if (list.isEmpty())
        {
            Log.err("Fehlende Links für Dateinode", node.toString());
        }

        List<AbstractStorageNode> s_nodes = new ArrayList<>();


        for (int i = 0; i < list.size(); i++)
        {
            PoolNodeFileLink poolNodeFileLink = list.get(i);

            AbstractStorageNode tmp_s_node = poolNodeFileLink.getStorageNode();
            if (tmp_s_node.isFullOrOnline())
            {
                s_nodes.add(tmp_s_node);
            }
        }
        return s_nodes;
    }

    public final void add_storage_node_handlers()
    {
        if (pool == null)
            return;

        for (int i = 0; i < pool.getStorageNodes().getList(getEm()).size(); i++)
        {
            AbstractStorageNode node = pool.getStorageNodes().getList(getEm()).get(i);

            add_storage_node_handler( node );
        }
    }

    public void add_storage_node_handler( AbstractStorageNode fs_node )
    {
        StorageNodeHandler sn_handler = StorageNodeHandler.createStorageNodeHandler(fs_node, this);

        storage_node_handlers.add( sn_handler );
    }

    public StorageNodeHandler get_handler_for_node( AbstractStorageNode fs_node )
    {
        for (int i = 0; i < storage_node_handlers.size(); i++)
        {
            StorageNodeHandler storageNodeHandler = storage_node_handlers.get(i);
            if (storageNodeHandler.get_node().getIdx() == fs_node.getIdx())
                return storageNodeHandler;
        }
        throw new RuntimeException("Missing StorageNodeHandler for storage node " + fs_node.getName());
    }


    private FileHandle[] resolve_all_file_handles( FileSystemElemNode node ) throws PathResolveException
    {
        ArrayList<FileHandle> list = new ArrayList<>();

        List<PoolNodeFileLink> link_list = get_pool_node_file_links( node );
        if (link_list == null)
            return new FileHandle[0];

        for (int i = 0; i < link_list.size(); i++)
        {
            PoolNodeFileLink poolNodeFileLink = link_list.get(i);

            AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();

            StorageNodeHandler sn_handler = new StorageNodeHandler(s_node, this);
            FileHandle fh = sn_handler.create_file_handle(node, /*cretee*/false);

            list.add(fh);
        }

        return list.toArray(new FileHandle[0]);
    }

    private BootstrapHandle[] resolve_all_bootstrap_handles( FileSystemElemNode node ) throws PathResolveException
    {
        ArrayList<BootstrapHandle> list = new ArrayList<>();

        List<PoolNodeFileLink> link_list = get_pool_node_file_links( node );
        if (link_list == null)
            return new BootstrapHandle[0];

        for (int i = 0; i < link_list.size(); i++)
        {
            PoolNodeFileLink poolNodeFileLink = link_list.get(i);

            AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();

            StorageNodeHandler sn_handler = new StorageNodeHandler(s_node, this);
            BootstrapHandle fh = sn_handler.create_bootstrap_handle(node);

            list.add(fh);

            // ADD ALL ATTRIBUTE NODES
            List<FileSystemElemAttributes> attrList = node.getHistory(getEm());
            for (FileSystemElemAttributes attr : attrList)
            {
                fh = sn_handler.create_bootstrap_handle(attr);
                list.add(fh);
            }
        }

        return list.toArray(new BootstrapHandle[0]);
    }
    private FileHandle[] resolve_all_attribute_handles( FileSystemElemNode node ) throws PathResolveException, UnsupportedEncodingException
    {
        ArrayList<FileHandle> list = new ArrayList<>();

        List<PoolNodeFileLink> link_list = get_pool_node_file_links( node );
        if (link_list == null)
            return new FileHandle[0];

        for (int i = 0; i < link_list.size(); i++)
        {
            PoolNodeFileLink poolNodeFileLink = link_list.get(i);

            AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();

            StorageNodeHandler sn_handler = new StorageNodeHandler(s_node, this);

            FileHandle fh = sn_handler.create_xa_node_handle(node, /*cretee*/false);

            list.add(fh);
        }

        return list.toArray(new FileHandle[0]);
    }


    public <T> T resolve_node_from_db( Class<T> cl, long idx ) throws SQLException
    {
        T node = em_find(cl, idx);
        return node;
    }
    public  FileSystemElemNode resolve_fse_node_from_db( long idx ) throws SQLException
    {
        FileSystemElemNode node = em_find(FileSystemElemNode.class, idx);
        return node;
    }

    public  AbstractStorageNode resolve_storage_node_from_db( long idx ) throws SQLException
    {
        AbstractStorageNode node = em_find(AbstractStorageNode.class, idx);
        return node;
    }


    public List<AbstractStorageNode> resolve_storage_nodes( StoragePool pool )
    {
        List<AbstractStorageNode> nodes=  pool.getStorageNodes().getList(getEm());
        //updateLazyListsHandler(nodes);
        return nodes;
    }

    public HashBlock create_hashentry( FileSystemElemNode node, String hash_value, DedupHashBlock block, long offset, int len, boolean r, long ts  ) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);


        HashBlock he = new HashBlock();
        he.setTs(ts);
        he.setFileNode(node);
        he.setBlockOffset(offset);
        he.setBlockLen(len);
        he.setDedupBlock(block);
        he.setReorganize(r);
        he.setHashvalue(hash_value);

        check_open_transaction();

        em_persist(he, /*noCache*/true);

        check_commit_transaction();

        return he;

    }

    public String get_single_hash_block( FileSystemElemNode node, long offset, int read_len )
    {
        // TODO SPEED UP WITH HASHMAP OR DIRECT ACCESS
        List<HashBlock> list = node.getHashBlocks().getList(getEm());
        for (int i =list.size() - 1; i >= 0 ; i--)
        {
            HashBlock hb = list.get(i);
            if (hb.getBlockOffset() == offset && hb.getBlockLen() == read_len)
                return hb.getHashvalue();

        }

        return null;
    }

    public DedupHashBlock create_dedup_hashblock( FileSystemElemNode node, String remote_hash,int read_len ) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);


        DedupHashBlock he = new DedupHashBlock();

        he.setHashvalue(remote_hash);
        he.setBlockLen(read_len);

        check_open_transaction();

        em_persist(he, /*noCache*/true);

        check_commit_transaction();

        //addDedupBlock2Cache( he );

        return he;
    }

    public FileHandle check_exist_dedupblock_handle( DedupHashBlock dhb ) throws PathResolveException, UnsupportedEncodingException, IOException
    {
        List<AbstractStorageNode> s_nodes = get_primary_storage_nodes(/*forWrite*/ false);
        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);
            if (s_node.isFS())
            {
                StorageNodeHandler snHandler = get_handler_for_node(s_node);
                FileHandle ret = snHandler.create_file_handle(dhb, false);
                if (ret != null && ret.exists())
                {
                    return ret;
                }
            }
        }
        return null;
    }
 


    public FileHandle open_dedupblock_handle( DedupHashBlock dhb, boolean create ) throws PathResolveException, UnsupportedEncodingException, IOException
    {
        boolean mustExist = true;
        FileHandle ret = null;
        if (create)
        {
            AbstractStorageNode s_node = get_primary_dedup_node_for_write();
            if (s_node.isFS())
            {
                StorageNodeHandler snHandler = get_handler_for_node(s_node);
                ret = snHandler.create_file_handle(dhb, true);
                return ret;
            }
            throw new IOException("Unsupported StorageNode type " + s_node.getName());
        }
        else
        {
            // LOOK FOR FIRST EXISTING NODE
            List<AbstractStorageNode> s_nodes = get_primary_storage_nodes(/*forWrite*/ false);
            for (int i = 0; i < s_nodes.size(); i++)
            {
                AbstractStorageNode s_node = s_nodes.get(i);
                if (s_node.isFS())
                {
                    StorageNodeHandler snHandler = get_handler_for_node(s_node);
                    ret = snHandler.create_file_handle(dhb, false);
                    if (ret.exists())
                    {
                        break;
                    }
                    else
                    {
                        ret = null;
                    }
                }
            }

            if (ret != null && ret.exists())
            {
                return ret;
            }
            throw new IOException("Cannot find DedupBlock " + dhb.toString() + " in any StorageNode" );
        }
    }

    public void remove_dedup_hash_block( DedupHashBlock dhb ) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);


        check_open_transaction();


        em_remove(dhb);

        check_commit_transaction();

        HashCache hashCache = LogicControl.getStorageNubHandler().getHashCache(getPool());

        hashCache.removeDhb( dhb );

    }

    public HandleWriteRunner getWriteRunner() {
        return writeRunner;
    }

    public void setPool( StoragePool pool )
    {
        this.pool = pool;
    }

    public void write_bootstrap_data( FileSystemElemNode node ) throws IOException, PathResolveException
    {        
        BootstrapHandle handle = open_bootstrap_handle(node);
        handle.write_bootstrap( node );  // THIS INCLUDES ATTRIBUTES        
        node.getLinks().realize(getEm());
        for (PoolNodeFileLink pnfl : node.getLinks())
        {
            handle = open_bootstrap_handle(pnfl);
            handle.write_bootstrap( pnfl );  // THIS INCLUDES ATTRIBUTES
        }
    }
    public void write_bootstrap_data( FileSystemElemAttributes attr ) throws IOException, PathResolveException
    {
        BootstrapHandle handle = open_bootstrap_handle(attr);
        handle.write_bootstrap(attr );
    }

    public void write_bootstrap_data( DedupHashBlock block, HashBlock node ) throws IOException, PathResolveException
    {
        BootstrapHandle handle = open_bootstrap_handle(block, node);
        handle.write_bootstrap( node );
    }

    public void write_bootstrap_data( DedupHashBlock block, XANode node ) throws IOException, PathResolveException
    {
        BootstrapHandle handle = open_bootstrap_handle(block, node);
        handle.write_bootstrap( node );
    }

    public static String resolve_bootstrap_path( AbstractStorageNode s_node, FileSystemElemNode node ) throws IOException, PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(node, sb);
        return s_node.getMountPoint() + "/" + sb.toString();
    }

    public static String resolve_bootstrap_path( AbstractStorageNode s_node, DedupHashBlock block, HashBlock node ) throws IOException, PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(block, node, sb);
        return s_node.getMountPoint() + "/" + sb.toString();
    }
    public static String resolve_bootstrap_path( AbstractStorageNode s_node, DedupHashBlock block, XANode node ) throws IOException, PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(block, node, sb);
        return s_node.getMountPoint() + "/" + sb.toString();
    }



    public String getName()
    {
        return pool.getName();
    }
    public boolean delete_fse_node( FileSystemElemNode node ) throws PoolReadOnlyException, SQLException, IOException
    {
        if (node == null)
            throw new IOException("delete_fse_node: Node ist null");
        
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);
        try
        {
            if (isFileChangePersitent(node))
            {
                setDeleted(node, true, System.currentTimeMillis());
            }
            else
            {
                Log.debug("Delete Node wird nicht historisiert", node.toString());
                remove_fse_node(node, immediateCommit);
            }
        }
        catch (DBConnException exception)
        {
            Log.err("Cannot delete_fse_node", node.toString(), exception);
            return false;
        }
        return true;              
    }
    
    public boolean delete_fse_node( long fileNo ) throws PoolReadOnlyException, SQLException, IOException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);
        
        FileSystemElemNode node = getNodeByFileNo( fileNo );
        return delete_fse_node( node );
    }

    public boolean delete_fse_node( String path ) throws PoolReadOnlyException, SQLException, IOException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileSystemElemNode node = resolve_node(path);        
        return delete_fse_node( node );
    }

    public void move_fse_node_idx( long fileNo, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);
        
        FileSystemElemNode node = getNodeByFileNo( fileNo );
        StringBuilder sb = new StringBuilder();
        build_relative_virtual_path(node, sb);
        String from = sb.toString();
        move_fse_node(node, from, to);
    }
    


    public void move_fse_node( String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);


        FileSystemElemNode _f = resolve_node(from);
        move_fse_node(_f, from, to);
    }

    public FileSystemElemNode resolve_node( String path ) throws SQLException
    {
        FileSystemElemNode elem = resolve_elem_by_path(path);
        if (elem == null)
            return null;

//        long idx = add_file_handle_no( elem );

        return elem;
    }

    public void set_ms_times( FileSystemElemNode fseNode, long ctoJavaTime, long atoJavaTime, long mtoJavaTime ) throws SQLException, DBConnException, PoolReadOnlyException
    {
        if (isReadOnly(fseNode))
            throw new PoolReadOnlyException(pool);

        if (ctoJavaTime != 0)
            fseNode.getAttributes().setCreationDateMs(ctoJavaTime);
        if (atoJavaTime != 0)
            fseNode.getAttributes().setAccessDateMs(atoJavaTime);
        if (mtoJavaTime != 0)
            fseNode.getAttributes().setModificationDateMs(mtoJavaTime);

        // Timestamps von Verzeichnissen werden nicht historisiert
        // TODO: ist das korrekt so ??
        if (fseNode.isDirectory())
        {
            check_open_transaction();
            fseNode.setAttributes(em_merge(fseNode.getAttributes()));
        }
        else
        {
            mergeOrPersistAttribute(fseNode, fseNode.getAttributes(), isFileChangePersitent(fseNode), System.currentTimeMillis());  
        }
        
        commit_transaction();
    }


    public boolean exists( FileSystemElemNode fseNode )
    {
        return fseNode.getAttributes().isDeleted();
    }

    public boolean exists( long fileNo )
    {
        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        return fseNode != null ? exists(fseNode) : false;
    }

    public boolean isReadOnly( long fileNo ) throws IOException, SQLException
    {
        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        
        if (fseNode == null)
            throw new IOException("Fileno " + fileNo + " nicht gefunden");
        return isReadOnly(fseNode);        
    }


    public void set_ms_times( long fileNo, long toJavaTime, long toJavaTime0, long toJavaTime1 ) throws SQLException, DBConnException, PoolReadOnlyException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);


        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        if (fseNode != null)
        {
            set_ms_times(fseNode, toJavaTime, toJavaTime0, toJavaTime1);
        }
    }

    public long getLength(long fileNo)
    {
        FileHandle fh = getFhByFileNo( fileNo );
        if (fh != null)
        {
            return fh.length();
        }
        return -1;
    }

    public void force( long fileNo, boolean b ) throws IOException
    {
        FileHandle fh = getFhByFileNo( fileNo );
        if (fh != null)
        {
            fh.force(b);
        }
    }

    public int read( long fileNo, byte[] b, int length, long offset ) throws IOException
    {
        int rlen = -1;
        FileHandle fh = getFhByFileNo( fileNo );
        if (fh != null)
        {
            rlen = fh.read(b, length, offset);
        }
        return rlen;
    }


    public void create( long fileNo ) throws IOException, PoolReadOnlyException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileHandle fh = getFhByFileNo( fileNo );
        if (fh != null)
        {
            fh.create();
        }
    }

    public void truncateFile( long fileNo, long size ) throws IOException, SQLException, PoolReadOnlyException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        
        if (isReadOnly(fseNode))
            throw new PoolReadOnlyException(pool);
        
        FileHandle fh = getFhByFileNo( fileNo );
        if (fh != null)
        {
            fh.truncateFile(size);
        }

        fseNode.getAttributes().setFsize(size);
        fseNode.setAttributes(em_merge( fseNode.getAttributes()));
    }


    public void writeFile( long fileNo, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        Log.debug("writeFile len " +  length + " Node:" + fseNode.getIdx() + " Attr:" + fseNode.getAttributes().getIdx() + " " +  fseNode.getAttributes());
        
        if (isReadOnly(fseNode))
            throw new PoolReadOnlyException(pool);
        
        FileHandle fh = getFhByFileNo( fileNo );
        
        
        if (fh != null)
        {
            fh.writeFile(b, length, offset);
        }
        else
            throw new IOException("Cannot retrieve FileHandle for fileNo " + fileNo);
//        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );

        // TODO: DETECT NEW SIZE AFTER WRITE
//        long newSize = offset + length;
//        if (newSize > fseNode.getAttributes().getFsize())
//        {
//            fseNode.getAttributes().setFsize(newSize);
//            //fseNode.setAttributes(em_merge(fseNode.getAttributes()));
//        }

    }
    public boolean checkBlock( String hash ) throws IOException, SQLException
    {
        // READ FROM CACHE
        HashCache hashCache = LogicControl.getStorageNubHandler().getHashCache(getPool());
        DedupHashBlock dhb;
        
        if (hashCache.isInited())
        {
            long idx = hashCache.getDhbIdx(hash);
            return (idx > 0);
        }
        else
        {
            dhb = findHashBlock(hash );
        } 
        return dhb != null;        
    }
    
    public void writeBlock( long fileNo, String hash, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        FileSystemElemNode fseNode = getNodeByFileNo( fileNo );
        if (fseNode == null) {
            throw new IOException("Cannot retrieve FileNode for fileNo " + fileNo);
        }
        Log.debug("writeFile len " +  length + " Node:" + fseNode.getIdx() + " Attr:" + fseNode.getAttributes().getIdx() + " " +  fseNode.getAttributes());
        
        if (isReadOnly(fseNode))
            throw new PoolReadOnlyException(pool);
        
        FileHandle fh = getFhByFileNo( fileNo );
        
        
        if (fh != null)
        {
            fh.writeBlock(hash, b, length, offset);
        }
        else
        {
            throw new IOException("Cannot retrieve FileHandle for fileNo " + fileNo);
        }
    }

    public void setDeleted( FileSystemElemNode fsenode, boolean b, long actTimestamp ) throws SQLException, DBConnException, PoolReadOnlyException
    {
        if (isReadOnly(fsenode))
            throw new PoolReadOnlyException(pool);

        boolean needNewAttributes = isFileChangePersitent(fsenode);
        
        // CREATE NEW ATTRIBUTES OR UPDATE OLD ONE
        FileSystemElemAttributes attr = fsenode.getAttributes();
        if (needNewAttributes)
        {
            attr = new FileSystemElemAttributes(fsenode.getAttributes());
        }
                
        attr.setDeleted(b);
        
        mergeOrPersistAttribute(fsenode, attr, needNewAttributes, actTimestamp); 
        Log.debug((b ? Main.Txt("Setze"):Main.Txt("Entferne")) + " " + Main.Txt("Löschflag für"), fsenode.toString());
        
    }
    
   
    public void updateAttributes( long fileNo,  long actTimestamp, RemoteFSElem elem ) throws SQLException, DBConnException, PoolReadOnlyException, IOException
    {
        FileSystemElemNode fsenode;
    
        if (fileNo >= 0)
        {
            fsenode = getNodeByFileNo( fileNo );
        }
        else
        {
            fsenode = resolve_fse_node_from_db(elem.getIdx());            
        }
        if (fsenode == null)
            throw new IOException("Node konnte nicht aufgeloset werden: " + fileNo + "/" + elem.getIdx());
        
        if (isReadOnly(fsenode))
            throw new PoolReadOnlyException(pool);

        
        add_new_fse_attributes( fsenode, elem, actTimestamp );
        Log.debug(Main.Txt("Setze neue attribute  für"), fsenode.toString()); 
    }


    // CAUTION, THIS MODIFIES THE ACTUAL FILE WITHOUT HISTORY MANAGEMENT!!!!
    void update_filesize( FileSystemElemNode node, long size ) throws PoolReadOnlyException, SQLException, DBConnException
    {
        if (isReadOnly())
            throw new PoolReadOnlyException(pool);

        node.getAttributes().setFsize(size);
        em_merge( node.getAttributes());

    }

    public FileSystemElemAttributes getActualFSAttributes( FileSystemElemNode fsenode, StoragePoolQry qry )
    {
        // TODO: MORE SOPHISTICATED QRY OPTIIONS
        long actTimestamp = qry.getSnapShotTs();

        // THIS IS THE NEWEST ENTRY FOR THIS FILE
        FileSystemElemAttributes attr = fsenode.getAttributes();
        
        if (qry.hasSearchList())
        {
            if (!qry.matchesSearchListTimestamp( attr ))
            {
                attr = null;
                // LOOK IN COMPLETE HISTORY
                List<FileSystemElemAttributes> attrList = fsenode.getHistory(getEm());
                for (int j = 0; j < attrList.size(); j++)
                {
                    FileSystemElemAttributes fileSystemElemAttributes = attrList.get(j);

                    if (qry.matchesSearchListTimestamp( fileSystemElemAttributes ))
                    {
                        attr = fileSystemElemAttributes;
                        break;
                    }
                }
            }
            return attr;
        }

        // CHECK IW WE LOOK FOR DATA OLDER THAN TS
        if (actTimestamp > 0 && actTimestamp < attr.getTs())
        {
            // DEFAULT IS NOT FOUND (WE ARE TOO YOUNG)
            attr = null;
            long diff = System.currentTimeMillis();

            // LOOK IN COMPLETE HISTORY
            List<FileSystemElemAttributes> attrList = fsenode.getHistory(getEm());
            for (int j = 0; j < attrList.size(); j++)
            {
                FileSystemElemAttributes fileSystemElemAttributes = attrList.get(j);

                // IS OLDER ?
                if (fileSystemElemAttributes.getTs() < actTimestamp)
                {
                    // CHECK IF IT IS NEAREST TO TS
                    long ddiff = actTimestamp - fileSystemElemAttributes.getTs();
                    if (diff > ddiff)
                    {
                        diff = ddiff;
                        attr = fileSystemElemAttributes;
                    }
                }
            }
        }
        return attr;
    }

    public void add_new_fse_attributes( FileSystemElemNode fsenode, RemoteFSElem elem, long actTimestamp) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly(fsenode))
            throw new PoolReadOnlyException(pool);

        // IS FILE OLD ENOUGH FÜR NEW ATTRIBUTES?
        boolean needNewAttributes = isFileChangePersitent(fsenode);

        // CREATE NEW ATTRIBUTES OR UPDATE OLD ONE
        FileSystemElemAttributes attr = fsenode.getAttributes();
        if (needNewAttributes)
        {
            attr = new FileSystemElemAttributes(fsenode.getAttributes());
        }

        attr.setAccessDateMs( elem.getAtimeMs());
        attr.setCreationDateMs( elem.getCtimeMs());
        attr.setModificationDateMs( elem.getMtimeMs());
        attr.setFsize( elem.getDataSize() );
        attr.setStreamSize( elem.getStreamSize() );
        
        attr.setDeleted(false);

        if (elem.isSymbolicLink())
        {
            // A REAL HACK: SYMLINK DONT NEED EXTENDED ATTRIBUTES SO WE USE THIS FIELD FOR LINKPATH
            attr.setAclInfoData( elem.getLinkPath() );
        }
        else
        {
            attr.setAclInfoData( elem.getAclinfoData() );
        }
        
        mergeOrPersistAttribute(fsenode, attr, needNewAttributes, actTimestamp);        
    }
    
    void mergeOrPersistAttribute(FileSystemElemNode fsenode, FileSystemElemAttributes attr, boolean needNewAttributes, long actTimestamp) throws SQLException
    {
        check_open_transaction();
        attr.setTs( actTimestamp );
        if (needNewAttributes)
        {           
            // ADD TO HISTORY BUT AVOID LOADING AN UNLOADED LAZY LIST
            fsenode.getHistory().addIfRealized(attr);

            // SET AS NEW ACTUAL ATTR
            fsenode.setAttributes(attr);

            // Store new
            em_persist(attr);
            em_merge(fsenode);            
        }
        else
        {
            // Update to DB
            em_merge(attr);
        }
        check_commit_transaction();        
    }
    
    public void undelete_attributes( FileSystemElemNode fsenode, long actTimestamp) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly(fsenode))
            throw new PoolReadOnlyException(pool);

        boolean needNewAttributes = isFileChangePersitent(fsenode);
        
        // CREATE NEW ATTRIBUTES OR UPDATE OLD ONE
        FileSystemElemAttributes attr = fsenode.getAttributes();
        if (needNewAttributes)
        {
            attr = new FileSystemElemAttributes(fsenode.getAttributes());
        }
        
        attr.setDeleted(false);
        
        mergeOrPersistAttribute(fsenode, attr, needNewAttributes, actTimestamp); 
    }


    public FileHandle open_xa_handle( FileSystemElemNode node, int streamInfo, boolean create ) throws PathResolveException, UnsupportedEncodingException, IOException, SQLException
    {
        List<AbstractStorageNode> s_nodes = resolve_storage_nodes( node );
        FileHandle ret = null;

        for (int i = 0; i < s_nodes.size(); i++)
        {
            AbstractStorageNode s_node = s_nodes.get(i);

            if (s_node.isFS())
            {
                StorageNodeHandler sn_handler = get_handler_for_node(s_node);
                FileHandle fs_ret;

                if (getPool().isLandingZone())
                    fs_ret = sn_handler.create_xa_node_handle( node, create );
                else
                    fs_ret = sn_handler.create_DDFS_StreamHandle( this, node, streamInfo,  create );


                // IF WE HAVE MORE THAN ONE HANDLE
                if (ret != null)
                {
                    // IS RETURN ALREADY A MULTIHANDLE, THEN ADD THIS ONE
                    if (ret instanceof MultiFileHandle)
                    {
                        MultiFileHandle mfh = (MultiFileHandle)ret;
                        mfh.add(fs_ret);
                    }
                    else
                    {
                        // CREATE A MULTIHANDLE AND ADD THE CREATED HANDLES
                        MultiFileHandle mfh = new MultiFileHandle();
                        mfh.add(ret);
                        mfh.add(fs_ret);
                        ret = mfh;
                    }
                }
                else
                {
                    ret = fs_ret;
                }
            }
        }
        return ret;
    }

    public XANode create_xa_hashentry( FileSystemElemNode node, String hash_value, int streamInfo, DedupHashBlock block, long offset, int len, boolean reorganize, long ts ) throws PoolReadOnlyException, SQLException
    {
        if (isReadOnly(node))
            throw new PoolReadOnlyException(pool);

        XANode he = new XANode();
        he.setTs( ts);
        he.setFileNode(node);
        he.setBlockOffset(offset);
        he.setBlockLen(len);
        he.setDedupBlock(block);
        he.setReorganize(reorganize);
        he.setHashvalue(hash_value);
        he.setStreamInfo(streamInfo);

        check_open_transaction();

        em_persist(he, /*noCache*/true);

        check_commit_transaction();

        return he;
    }

    
//
//    public List<FileSystemElemNode> search( ArrayList<SearchEntry> slist) throws SQLException
//    {
//        return search(slist, Integer.MAX_VALUE);
//    }

    public List<IndexResult> search( ArrayList<SearchEntry> slist, int limit ) throws SQLException
    {

        FSEIndexer fse = LogicControl.getStorageNubHandler().getIndexer(pool);

        
        List<IndexResult> list = fse.searchNodes(this, slist, null, limit);


        // TODO: ADD ACLS TO DB SO THAT WE CAN FILTER REULTS THROUGH DB
        UserManager umgr = Main.get_control().getUsermanager();
        
        if (pathResolver instanceof SearchPathResolver) {
            pathResolver.getRootDir().getChildren().clear();
        }

        // FILTER OUT BASED ON ACL
        StoragePoolQry qry = getPoolQry();
        for (int i = 0; i < list.size(); i++)
        {
            FileSystemElemNode fileSystemElemNode = list.get(i).getNode();
            FileSystemElemAttributes attributes = list.get(i).getAttributes( getEm());
            if (!qry.matchesUser(fileSystemElemNode, attributes, umgr))
            {
                list.remove(i);
                i--;
            }
            if (pathResolver instanceof SearchPathResolver) {
                pathResolver.getRootDir().getChildren().add(fileSystemElemNode);
            }
        }
        return list;
    }

    public List<ArchiveJob> searchJob( ArrayList<SearchEntry> slist, int limit ) throws SQLException
    {

        FSEIndexer fse = LogicControl.getStorageNubHandler().getIndexer(pool);

        
        List<ArchiveJob> list = fse.searchJobs(this, slist,  limit);


        return list;
    }

    String getRealFieldname( String arg, String fTablename, String aTablename , String jTablename )
    {
        if (arg.startsWith("_A_."))
        {
            return aTablename + arg.substring(3);
        }
        if (arg.startsWith("_F_."))
        {
            return fTablename + arg.substring(3);
        }
        if (arg.startsWith("_J_."))
        {
            return jTablename + arg.substring(3);
        }
        return arg;
    }

     private void buildWhereString( SearchEntry entry, StringBuilder sb, String fTablename, String aTablename, String jTablename  )
    {
        if (sb.length() > 0)
        {
            if (entry.isPrevious_or())
            {
                sb.append(" or ");
            }
            else
            {
                sb.append(" and ");
            }
        }
        if (entry.isPrevious_neg())
        {
            sb.append(" not ");
        }
        if (!entry.getChildren().isEmpty())
        {

            StringBuilder child_where = new StringBuilder();
            for (int i = 0; i < entry.getChildren().size(); i++)
            {
                SearchEntry child_r = entry.getChildren().get(i);

                buildWhereString(child_r, child_where, fTablename, aTablename, jTablename);
            }
            if (child_where.length() > 0)
            {
                sb.append("(");
                sb.append(child_where);
                sb.append(")");
            }
        }
        else
        {
            String fieldname = getRealFieldname( entry.getFnameforArgtype(), fTablename, aTablename, jTablename );

            if (!entry.isStringArgType())
            {
                sb.append(fieldname);
                sb.append(entry.getOpString());
                sb.append(entry.getArgValue());
            }
            else
            {
                boolean caseInsensitive = entry.isCaseInsensitive() && entry.getOpString().contains("like");

                if (caseInsensitive)
                    sb.append("upper(");
                sb.append(fieldname);
                if (caseInsensitive)
                sb.append(")");
                sb.append(entry.getOpString());

                sb.append("'");
                if (entry.getArgOp().equals(SearchEntry.OP_ENDS) || entry.getArgOp().equals(SearchEntry.OP_CONTAINS))
                {
                    sb.append("%");
                }

                if (caseInsensitive)
                    sb.append(entry.getArgValue().toUpperCase());
                else
                    sb.append(entry.getArgValue());

                if (entry.getArgOp().equals(SearchEntry.OP_BEGINS) || entry.getArgOp().equals(SearchEntry.OP_CONTAINS))
                {
                    sb.append("%");
                }

                sb.append("'");
            }
        }
    }
     
    public void removeDedupBlock( DedupHashBlock dhb, FileSystemElemNode node ) throws SQLException, PathResolveException, UnsupportedEncodingException, PoolReadOnlyException, IOException
    {
        em_remove(dhb);
        
        List<PoolNodeFileLink> link_list = null;
        if (node != null)
        {            
            get_pool_node_file_links(node);
        }
        
        if (link_list == null)
        {
            List<AbstractStorageNode> snodes = get_primary_storage_nodes(/*forWrite*/ false);
            for (int i = 0; i < snodes.size(); i++)
            {
                AbstractStorageNode s_node = snodes.get(i);
                StorageNodeHandler sn_handler = get_handler_for_node(s_node);
                FileHandle fh = sn_handler.create_file_handle( dhb, /*create*/ false);
                if (fh != null && fh.exists())
                {                    
                    fh.delete();
                    BootstrapHandle bfh = sn_handler.create_bootstrap_handle(dhb);
                    bfh.delete();
                }
            }
        }

        else
        {

            // REMOVE FROM ALL CONNECTED NODES
            for (int i = 0; i < link_list.size(); i++)
            {
                PoolNodeFileLink poolNodeFileLink = link_list.get(i);

                AbstractStorageNode s_node = poolNodeFileLink.getStorageNode();

                StorageNodeHandler sn_handler = get_handler_for_node(s_node);
                FileHandle fh = sn_handler.create_file_handle( dhb, /*create*/ false);

                fh.delete();

                BootstrapHandle bfh = sn_handler.create_bootstrap_handle(dhb);
                bfh.delete();
            }
        }

    }

//    public void removeHashBlock( HashBlock hashBlock ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
//    {
//        // REMOVE BLOCK FROM DATABASE
//        em_remove(hashBlock);
//
//        // DO WE HAVE DEDUP DATABLOCK CONNECTED ?
//        if (hashBlock.getDedupBlock() != null)
//        {
//            // NOT IN USE OTHERWISE?
//            if (checkDedupBlockNotUsed(hashBlock.getDedupBlock()))
//            {
//                removeDedupBlock( hashBlock.getDedupBlock(), hashBlock.getFileNode() );
//            }
//        }
//    }

//
//    public void removeXANode( XANode xaBlock ) throws IOException, PoolReadOnlyException, SQLException, PathResolveException
//    {
//        // REMOVE BLOCK FROM DATABASE
//        em_remove(xaBlock);
//
//        // DO WE HAVE DEDUP DATABLOCK CONNECTED ?
//        if (xaBlock.getDedupBlock() != null)
//        {
//            // NOT IN USE OTHERWISE?
//            if (checkDedupBlockNotUsed(xaBlock.getDedupBlock()))
//            {
//                removeDedupBlock( xaBlock.getDedupBlock(), xaBlock.getFileNode() );
//            }
//        }
//    }

//    private boolean checkDedupBlockNotUsed(DedupHashBlock dhb) throws SQLException
//    {
//        // TODO LOCKING!!!
//
//        List<Object[]> list = createNativeQuery("select idx from HashBlock where dedupBlock_idx=" + dhb.getIdx(), 2);
//        if (!list.isEmpty())
//            return false;
//
//        List<Object[]> xa_list = createNativeQuery("select idx from XANode where dedupBlock_idx=" + dhb.getIdx(), 2);
//        if (!xa_list.isEmpty())
//            return false;
//
//        return true;
//    }

    public void moveHashBlock( HashBlock hashBlock, StoragePool targetPool )
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    protected void reinitConnection() throws IOException
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public ArchiveJobFileLink addArchiveLink( ArchiveJob archiveJob, FileSystemElemNode node ) throws SQLException
    {
        ArchiveJobFileLink ajfl = new ArchiveJobFileLink();
        ajfl.setArchiveJob(archiveJob);
        ajfl.setFileNode(node);
        em_persist(ajfl, /*noCache*/true);
        archiveJob.getLinks().addIfRealized( ajfl);
        return ajfl;
    }

    public boolean isInsideMappingDir(String path)
    {

        List<User.VsmFsEntry> mapList = getPoolQry().getUser().getFsMapper().getVsmList();

            for (int i = 0; i < mapList.size(); i++) {
                User.VsmFsEntry vsmFsEntry = mapList.get(i);
                // Mapping auf Pool begrenzt?
                if (!vsmFsEntry.isPool(pool))
                    continue;
                
                if (vsmFsEntry.getuPath().startsWith(path) || path.startsWith( vsmFsEntry.getuPath()))
                    return true;
            }
            return false;
        }


   public String resolveMappingDir( String path) {
        if (!getPoolQry().isUseMappingFilter())
            return path;

        List<User.VsmFsEntry> mapList = getPoolQry().getUser().getFsMapper().getVsmList();

            for (int i = 0; i < mapList.size(); i++) {
                User.VsmFsEntry vsmFsEntry = mapList.get(i);
                // Mapping auf Pool begrenzt?
                if (!vsmFsEntry.isPool(pool))
                    continue;
                
                if (path.startsWith( vsmFsEntry.getuPath())) {
                    String restpath = path.substring(vsmFsEntry.getuPath().length());
                    return vsmFsEntry.getvPath() + restpath;
                }                
            }
            return path;
        }

    public List<IPreviewData> getPreviewData( List<RemoteFSElem> path, Properties props ) throws IOException, SQLException {
        return getPreviewReader().getPreviews(path, props);
    }      

}
