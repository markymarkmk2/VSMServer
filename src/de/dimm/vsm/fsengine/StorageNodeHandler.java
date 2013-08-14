/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.BootstrapHandle;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Utilities.Hex;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 *
 * @author mw
 *
 *
 * From <Path/content/> on we have the database-IDS for the directories / files as Folder / Filenames
 */
public class StorageNodeHandler
{
    public static final int DEDUPBLOCK_IDX_CHARACTERS_PER_DIRLEVEL = 3;

    public static final String PATH_FSNODES_PREFIX = "/fs";
    public static final String PATH_DEDUPNODES_PREFIX = "/dd";
    
    public static final String DEDUP_PREFIX = "dd_";
    public static final String FSEA_PREFIX = "at_";
    public static final String FSEN_PREFIX = "fs_";
    public static final String HASHBLOCK_PREFIX = "hb_";
    public static final String PNFL_PREFIX = "pn_";
    public static final String XATTR_PREFIX = "xa_";
    public static final String BOOTSTRAP_SUFFIX = ".xml";

    public static final String BOOTSTRAP_PATH = "bootstrap";
    public static final String XA_PATH = "xa";


    AbstractStorageNode storageNode;

    StoragePoolHandler storage_pool_handler;

    public static final String NODECACHE = "NodeCache";
    

    protected StorageNodeHandler( AbstractStorageNode storageNode, StoragePoolHandler fsh_handler )
    {
        this.storageNode = storageNode;
        this.storage_pool_handler = fsh_handler;
    }

    public static StorageNodeHandler createStorageNodeHandler( AbstractStorageNode fs_node, StoragePoolHandler aThis )
    {
        if (fs_node.getCloneNode() != null && fs_node.getCloneNode() != fs_node)
        {
            return new CloneStorageNodeHandler(fs_node, aThis);
        }
        return new StorageNodeHandler(fs_node, aThis);
    }

    public static boolean initNode(AbstractStorageNode node)
    {
        if (node.isVirgin())
        {
            if (node.isFS() && node.getMountPoint() != null)
            {
                File f = new File( node.getMountPoint() );
                f.mkdirs();
                if (!f.exists())
                    return false;

                f = new File( node.getMountPoint() + PATH_FSNODES_PREFIX  );
                f.mkdir();
                f = new File( node.getMountPoint() + PATH_DEDUPNODES_PREFIX );
                f.mkdir();

                node.setNodeMode( AbstractStorageNode.NM_ONLINE);
                return true;
            }
        }
        return false;

    }
    public boolean initNode()
    {
        return initNode(storageNode);
    }

    public static Cache getCache()
    {
        CacheManager.create();
        if (!CacheManager.getInstance().cacheExists(NODECACHE))
        {
            Cache memoryOnlyCache = new Cache(NODECACHE, 50000, false, false, 50, 50);
            CacheManager.getInstance().addCache(memoryOnlyCache);
            memoryOnlyCache.setStatisticsEnabled(true);
        }
        return CacheManager.getInstance().getCache(NODECACHE);
    }

    public static String getFullParentPath( FileSystemElemNode file_node ) throws PathResolveException
    {
        // TYPE IS ALLOWED TO CHANGE OVER TIME FROM FILE TO DIR, WE NEED DIFFERENT FS INSTANTIATIONS
        Cache c = getCache();
        Element el = c.get(Long.toString(file_node.getIdx()) + "NodePath");
        if (el != null)
        {
            return (String)el.getValue();
        }

        StringBuilder sb = new StringBuilder();

        int max_depth = 1024;

        while (file_node.getParent() != null)
        {
            file_node = file_node.getParent();
            sb.insert(0, Hex.fromLong(file_node.getIdx()) );
            sb.insert(0, "/");
            if (max_depth-- <= 0)
                throw new PathResolveException("Path_is_too_deep");
        }
        String path = sb.toString();
        c.put( new Element( Long.toString(file_node.getIdx()) + "NodePath", path) );

        return path;
    }

    private static void build_block_path( long idx, StringBuilder sb ) throws PathResolveException
    {
        String val = Hex.fromLong(idx, true);

        sb.append( "/");


        // PATH IS HASHVAL SPLITTED INTO 4-CHAR LENGTH SUBDIRS
        int max_len = val.length();
        int pos = 0;
        while (pos < max_len)
        {
            int end = pos + 2;
            if (end < max_len)
            {
                sb.append(val.substring(pos, end) );
                sb.append( "/");
            }

            pos += 2;
        }
    }

    
    public static void build_node_path( FileSystemElemNode file_node, StringBuilder sb ) throws PathResolveException
    {

        if (!file_node.isDirectory())
        {
            sb.insert(0,'.');
            sb.append(file_node.getTyp());
        }
        sb.insert(0, Hex.fromLong(file_node.getIdx()));
        sb.insert(0, "/");

        sb.insert(0, getFullParentPath( file_node ) );

        sb.insert(0, PATH_FSNODES_PREFIX);
    }

    public static void build_node_path( DedupHashBlock dhb, StringBuilder sb ) throws PathResolveException, UnsupportedEncodingException
    {
        String val = Hex.fromLong(dhb.getIdx(), true);

        sb.append(PATH_DEDUPNODES_PREFIX);
        build_block_path( dhb.getIdx(), sb );

        sb.append( dhb.getHashvalue());
    }
    
    public static void build_xa_node_path( FileSystemElemNode file_node, StringBuilder sb ) throws PathResolveException, UnsupportedEncodingException
    {
        sb.insert(0, Hex.fromLong(file_node.getIdx()));
        sb.insert(0, "/" + XA_PATH + "/");

        sb.insert(0, getFullParentPath( file_node ) );

        sb.insert(0, PATH_FSNODES_PREFIX);

    }

    static void build_bootstrap_path( FileSystemElemNode file_node, StringBuilder sb ) throws PathResolveException
    {
        sb.insert(0, BOOTSTRAP_SUFFIX );
        sb.insert(0, Hex.fromLong(file_node.getIdx()));
        sb.insert(0, "/" + BOOTSTRAP_PATH + "/" + FSEN_PREFIX);

        sb.insert(0, getFullParentPath( file_node ) );

        sb.insert(0, PATH_FSNODES_PREFIX);
    }

    static void build_bootstrap_path( FileSystemElemAttributes attr, StringBuilder sb ) throws PathResolveException
    {
        FileSystemElemNode file_node = attr.getFile();
        sb.insert(0, BOOTSTRAP_SUFFIX );
        sb.insert(0, Hex.fromLong(attr.getIdx()));
        sb.insert(0, "/" + BOOTSTRAP_PATH + "/" + FSEA_PREFIX);

        sb.insert(0, getFullParentPath( file_node ) );

        sb.insert(0, PATH_FSNODES_PREFIX);
    }

//    static void build_bootstrap_path( XANode node, StringBuilder sb ) throws PathResolveException
//    {
//        sb.insert(0, BOOTSTRAP_SUFFIX );
//        sb.insert(0, Hex.fromLong(node.getIdx()));
//        sb.insert(0, "/" + BOOTSTRAP_PATH + "/" + XATTR_PREFIX);
//
//        sb.insert(0, getFullParentPath( node.getFileNode() ) );
//
//        sb.insert(0, PATH_FSNODES_PREFIX);
//    }
    static void build_bootstrap_path( PoolNodeFileLink node, StringBuilder sb ) throws PathResolveException
    {
        sb.insert(0, BOOTSTRAP_SUFFIX );
        sb.insert(0, Hex.fromLong(node.getIdx()));
        sb.insert(0, "/" + BOOTSTRAP_PATH + "/" + PNFL_PREFIX);
        sb.insert(0, getFullParentPath( node.getFileNode() ) );
        sb.insert(0, PATH_FSNODES_PREFIX);
    }

    // Root Dir für all DD Bootstrap entries
    static void build_bootstrap_path( DedupHashBlock dhb, StringBuilder sb ) throws PathResolveException, UnsupportedEncodingException
    {
        sb.append(PATH_DEDUPNODES_PREFIX);
        build_block_path( dhb.getIdx(), sb );

        sb.append( BOOTSTRAP_PATH + "/" );
        sb.append(dhb.getHashvalue());
    }
    static void build_bootstrap_path( DedupHashBlock dhb, HashBlock node, StringBuilder sb ) throws PathResolveException, UnsupportedEncodingException
    {
        sb.append(PATH_DEDUPNODES_PREFIX);
        build_block_path( dhb.getIdx(), sb );

        sb.append( BOOTSTRAP_PATH + "/" );
        sb.append(dhb.getHashvalue()).append( "/");
        sb.append(HASHBLOCK_PREFIX + "_").append( node.getIdx());
        sb.append(BOOTSTRAP_SUFFIX );
    }
    static void build_bootstrap_path( DedupHashBlock dhb, XANode node, StringBuilder sb ) throws PathResolveException, UnsupportedEncodingException
    {
        sb.append(PATH_DEDUPNODES_PREFIX);
        build_block_path( dhb.getIdx(), sb );

        sb.append( BOOTSTRAP_PATH + "/" );
        sb.append(dhb.getHashvalue()).append( "/");
        sb.append(XATTR_PREFIX + "_").append( node.getIdx());
        sb.append(BOOTSTRAP_SUFFIX );
    }

    
    static <T> void build_bootstrap_path( StringBuilder sb, T object ) throws PathResolveException
    {
        sb.insert(0, BOOTSTRAP_SUFFIX );
        sb.insert(0, object.getClass().getSimpleName());
        sb.insert(0, "/" + BOOTSTRAP_PATH + "/");
        sb.insert(0, PATH_FSNODES_PREFIX);
    }



/*
    public boolean remove_fse_node( FileSystemElemNodeHandler f )
    {
        return storage_pool_handler.remove_fse_node( f.getNode() );
        
    }*/

    public long getTotalBlocks()
    {
        if (storageNode.isFS() && storageNode.getMountPoint() != null)
        {
            File f = new File( storageNode.getMountPoint() );
            return f.getTotalSpace() / getBlockSize();
        }
        return 1024;
    }

    public long getUsedBlocks()
    {
        if (storageNode.isFS() && storageNode.getMountPoint() != null)
        {
            File f = new File( storageNode.getMountPoint() );

            return (f.getTotalSpace()- f.getUsableSpace() )/ getBlockSize();
        }
        return 0;
    }
    public int getBlockSize()
    {
        return 1024;
    }
    public static boolean isAvailable(AbstractStorageNode n)
    {
        if (n.isFS() && n.getMountPoint() != null)
        {
            File f = new File( n.getMountPoint() );
            return f.exists();
        }
        return true;
    }
    public static long getUsedSpace(AbstractStorageNode n)
    {
        if (n.isFS() && n.getMountPoint() != null)
        {
            File f = new File( n.getMountPoint() );
            return f.getTotalSpace() - f.getUsableSpace();
        }
        return 0;
    }
    // TAKES LONG!
    public static long getRealUsedSpace(AbstractStorageNode n)
    {
        if (n.isFS() && n.getMountPoint() != null)
        {
            File f = new File( n.getMountPoint() );
            long len = calcRecursiveLen( f );
            return len;
        }
        return 0;
    }
    public static long getFreeSpace(AbstractStorageNode n)
    {
        if (n.isFS() && n.getMountPoint() != null)
        {
            File f = new File( n.getMountPoint() );
            return f.getUsableSpace();
        }
        return 0;
    }
    public static long calcRecursiveLen( File f )
    {
        long len = 0;
        if (f.isDirectory())
        {
            File[] children = f.listFiles();
            for (int i = 0; i < children.length; i++)
            {
                File file = children[i];
                len += calcRecursiveLen(file);
            }
            return len;
        }
        else
        {
            return f.length();
        }
    }
    public static boolean isRoot(AbstractStorageNode n)
    {
        if (n.isFS())
        {
            File[] roots = File.listRoots();
            for (int i = 0; i < roots.length; i++)
            {
                File file = roots[i];
                if (file.getAbsolutePath().equals( new File(n.getMountPoint()).getAbsolutePath() ))
                    return true;
            }
        }
        return false;
    }

    public AbstractStorageNode get_node()
    {
        return storageNode;
    }



    public FileHandle create_file_handle(FileSystemElemNode node, boolean create) throws PathResolveException
    {
        if (storageNode.isFS())
        {
            FileHandle ret = FS_FileHandle.create_fs_handle(storageNode, node, create );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public FileHandle create_xa_node_handle(FileSystemElemNode node, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            FileHandle ret = FS_FileHandle.create_xa_handle(storageNode, node, create );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public FileHandle create_file_handle(DedupHashBlock block, boolean create) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            FileHandle ret = FS_FileHandle.create_dedup_handle(storageNode, block, create );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(FileSystemElemNode node) throws PathResolveException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, node );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(PoolNodeFileLink node) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, node );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(DedupHashBlock block) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new DDEntriesDir_BootstrapHandle(this.storageNode, block );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(FileSystemElemAttributes attr) throws PathResolveException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, attr );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(DedupHashBlock block, HashBlock node) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, block, node );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(DedupHashBlock block, XANode node) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, block, node );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
    public BootstrapHandle create_bootstrap_handle(AbstractStorageNode node) throws PathResolveException, UnsupportedEncodingException
    {
        if (storageNode.isFS())
        {
            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, node );
            return ret;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }
//    public BootstrapHandle create_bootstrap_handle(XANode node) throws PathResolveException, UnsupportedEncodingException
//    {
//        if (storageNode.isFS())
//        {
//            BootstrapHandle ret = new FS_BootstrapHandle(this.storageNode, node );
//            return ret;
//        }
//        throw new UnsupportedOperationException("Not yet implemented");
//    }
//  
    public FileHandle create_DDFS_handle( StoragePoolHandler aThis, FileSystemElemNode node, boolean create ) throws PathResolveException, IOException, SQLException
    {
        return DDFS_WR_FileHandle.create_fs_handle(this.storageNode, aThis, node, create );
    }

    public FileHandle create_DDFS_StreamHandle( StoragePoolHandler aThis, FileSystemElemNode node, int streamInfo, boolean create ) throws PathResolveException, IOException, SQLException
    {
        return DDFS_WR_FileHandle.create_fs_stream_handle(this.storageNode, aThis, node, streamInfo, create );
    }

/*
    void update_filesize( FileSystemElemNode node, long size ) throws PoolReadOnlyException, SQLException, DBConnException
    {
        storage_pool_handler.update_filesize( node, size );
    }*/


}
