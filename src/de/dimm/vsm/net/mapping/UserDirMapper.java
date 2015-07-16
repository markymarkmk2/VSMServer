/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.mapping;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.auth.UserManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolHandlerServlet;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Administrator
 */
public class UserDirMapper
{

    public static List<RemoteFSElem> mappedUserDir( StoragePoolHandler handler, RemoteFSElem node )
    {
        String path = node.getPath();
        List<User.VsmFsEntry> mapList = handler.getPoolQry().getUser().getFsMapper().getVsmList();
        List<RemoteFSElem> ret = new ArrayList<>();
        try
        {
            for (int i = 0; i < mapList.size(); i++)
            {
                User.VsmFsEntry vsmFsEntry = mapList.get(i);
                // Mapping auf Pool begrenzt?
                if (!vsmFsEntry.isPool(handler.getPool()))
                    continue;
                
                // INSIDE THIS MAPPING ENTRY?
                if (!vsmFsEntry.getuPath().startsWith(path) && !path.startsWith(vsmFsEntry.getuPath()))
                {
                    continue;
                }

                boolean matchesSubPath = path.equals(vsmFsEntry.getuPath());
                matchesSubPath |= path.length() < vsmFsEntry.getuPath().length() && vsmFsEntry.getuPath().charAt(path.length()) == '/';
                boolean isRoot = path.equals("/");

                if (matchesSubPath || isRoot)
                {
                    String restPath = vsmFsEntry.getuPath().substring(path.length());
                    if (restPath.startsWith("/") && restPath.length() > 1)
                    {
                        restPath = restPath.substring(1);
                    }
                    String[] paths = restPath.split("/");

                    if (paths.length == 0 || (paths.length == 1 && paths[0].isEmpty()))
                    {
                        FileSystemElemNode fseNode = handler.resolve_elem_by_path(vsmFsEntry.getvPath());
                        if (fseNode == null)
                        {
                            continue;
                        }

                        // THIS IS THE NEWEST ENTRY FOR THIS FILE
                        FileSystemElemAttributes attr = handler.getActualFSAttributes(fseNode, handler.getPoolQry());
                        RemoteFSElem remoteNode = StoragePoolHandlerServlet.genRemoteFSElemfromNode(fseNode, attr);
                        ret.addAll( get_unmapped_child_nodes(handler, remoteNode) );
                    }
                    else
                    {
                        String dirName = paths[0];
                        if (dirName.isEmpty() && paths.length > 1)
                        {
                            dirName = paths[1];
                        }

                        String newPath = path;
                        if (!newPath.endsWith("/"))
                        {
                            newPath += "/";
                        }
                        newPath += dirName;

                        // Je nachdem ob wir abs-Path haben oder Nicht die Pfade abs oder rel eintragen
                        String addDirName = dirName;
                        
                        if (containsDir(ret, addDirName))
                        {
                            continue;
                        }

                        RemoteFSElem remoteNode = RemoteFSElem.createDir(addDirName);
                        ret.add(remoteNode);
                        //                    remoteNode = RemoteFSElem.createDir(newPath);
                    }
                }
                else
                {
                    String restPath = path.substring(vsmFsEntry.getuPath().length());
                    String newFullPath = vsmFsEntry.getvPath() + restPath;
                    FileSystemElemNode fseNode = handler.resolve_elem_by_path(newFullPath);
                    if (fseNode == null)
                    {
                        continue;
                    }

                    // THIS IS THE NEWEST ENTRY FOR THIS FILE
                    FileSystemElemAttributes attr = handler.getActualFSAttributes(fseNode, handler.getPoolQry());
                    RemoteFSElem remoteNode = StoragePoolHandlerServlet.genRemoteFSElemfromNode(fseNode, attr);
                    ret.addAll( get_unmapped_child_nodes(handler, remoteNode) );
                }
            }
            return ret;
        }
        catch (SQLException  sQLException)
        {
            LogManager.err_db("Kann Mapping node nicht auflösen", sQLException);
        }
        return ret;
    }

    private static boolean containsDir( List<RemoteFSElem> ret, String dirName )
    {
        for (int i = 0; i < ret.size(); i++)
        {
            RemoteFSElem remoteFSElem = ret.get(i);
            if (remoteFSElem.getPath().equals(dirName))
            {
                return true;
            }
        }
        return false;
    }
    
    public static List<FileSystemElemNode> filter_child_nodes( StoragePoolHandler handler, FileSystemElemNode fseNode ) throws SQLException {
        StoragePoolQry qry = handler.getPoolQry();
        try
        {
            // IF WE HAVE ACTUAL FILESYSTEM THEN RELOAD NODE EVERY TIME
            if (qry.getSnapShotTs() <= 0 && !Main.get_bool_prop(GeneralPreferences.CACHE_ON_WRITE_FS, false))
            {
                handler.em_refresh(fseNode);
            }
        }
        catch (Exception exception)
        {
            Log.err("Objekt kann nicht refreshed werden", fseNode.toString(), exception);
        }
        UserManager umgr = Main.get_control().getUsermanager();

        Map<String, FileSystemElemNode> blockedNodes = new HashMap<>();
        Map<String, FileSystemElemNode> foundNodes = new HashMap<>();

        boolean blockDuplDirs = Main.get_bool_prop(GeneralPreferences.BLOCK_DUPL_DIRS);
        List<FileSystemElemNode> ret = new ArrayList<>();

        if (fseNode != null)
        {
            List<FileSystemElemNode> list = fseNode.getChildren(handler.getEm());

            for (int i = 0; i < list.size(); i++)
            {
                FileSystemElemNode fileSystemElemNode = list.get(i);

                // THIS IS THE NEWEST ENTRY FOR THIS FILE
                FileSystemElemAttributes attr = handler.getActualFSAttributes(fileSystemElemNode, qry);

                // OBVIOUSLY THE FILE WAS CREATED AFTER TS -> INVISIBLE
                if (attr == null)
                {
                    continue;
                }

                // ACLS STARTUNDER SYSTEMROOT
                if (!fseNode.getName().equals("/"))
                {
                    if (!qry.matchesUser(fileSystemElemNode, attr, umgr))
                    {
                        blockedNodes.put(fileSystemElemNode.getName(), fseNode);
                        continue;
                    }
                }

                // FILE WAS DELETED AT TS
                if (attr.isDeleted() && !qry.isShowDeleted())
                {
                    continue;
                }

                // Already in Answer ?
                FileSystemElemNode existElem = foundNodes.get(fileSystemElemNode.getName());

                // Check Dupl
                if (blockDuplDirs && existElem != null)
                {
                    // Found older dupl -> skip
                    if (fileSystemElemNode.getAttributes().getIdx() < existElem.getAttributes().getIdx())
                    {
                        continue;
                    }
                    else
                    {
                        // Found newer node -> replace older node
                        ret.remove(existElem);
                    }
                }
                // Add to result
                ret.add(fileSystemElemNode);
                foundNodes.put(fileSystemElemNode.getName(), fileSystemElemNode);
            }
        }

        // NOW REMOVE ALL BLOCKED NODES FROM UNBLOCKED LIST
        for (int i = 0; i < ret.size(); i++)
        {
            FileSystemElemNode elem = ret.get(i);
            if (blockedNodes.containsKey(elem.getName()))
            {
                ret.remove(i);
                i--;
            }
        } 
        // Sort
        Collections.sort(ret, new Comparator<FileSystemElemNode>()
        {
            @Override
            public int compare( FileSystemElemNode o1, FileSystemElemNode o2 )
            {
                return o1.getName().compareTo(o2.getName());
            }
        });        
        return ret;
    }

    public static List<RemoteFSElem> get_unmapped_child_nodes( StoragePoolHandler handler, RemoteFSElem node ) throws SQLException
    {
        StoragePoolQry qry = handler.getPoolQry();
        List<RemoteFSElem> ret = new ArrayList<>();
        FileSystemElemNode fseNode = handler.resolve_node_by_remote_elem(node);
        if (fseNode == null)
        {
            return ret;
        }

        try
        {
            // IF WE HAVE ACTUAL FILESYSTEM THEN RELOAD NODE EVERY TIME
            if (qry.getSnapShotTs() <= 0 && !Main.get_bool_prop(GeneralPreferences.CACHE_ON_WRITE_FS, false))
            {
                handler.em_refresh(fseNode);
            }
        }
        catch (Exception exception)
        {
            Log.err("Objekt kann nicht refreshed werden", fseNode.toString(), exception);
        }
        
        List<FileSystemElemNode> childNodes = filter_child_nodes(handler, fseNode);
        for (int i = 0; i < childNodes.size(); i++)
        {
            FileSystemElemNode fileSystemElemNode = childNodes.get(i);

            // THIS IS THE NEWEST ENTRY FOR THIS FILE
            FileSystemElemAttributes attr = handler.getActualFSAttributes(fileSystemElemNode, qry);
            RemoteFSElem elem = StoragePoolHandlerServlet.genRemoteFSElemfromNode(fileSystemElemNode, attr);
                        
            if (qry.isShowVersions()) {
                List<FileSystemElemAttributes> attrList = fileSystemElemNode.getHistory(handler.getEm());
                elem.setMultVersions( attrList.size() > 1);    
            }

            ret.add(elem);            
        }
        
//        Collections.sort(ret, new Comparator<RemoteFSElem>()
//        {
//            @Override
//            public int compare( RemoteFSElem o1, RemoteFSElem o2 )
//            {
//                return o1.getPath().compareTo(o2.getPath());
//            }
//        });

        return ret;
    }
}
