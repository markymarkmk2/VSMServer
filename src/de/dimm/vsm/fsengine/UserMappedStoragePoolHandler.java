/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.auth.User;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.StoragePool;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mw
 */
public class UserMappedStoragePoolHandler extends JDBCStoragePoolHandler {

    boolean insideMapping = false;
    public UserMappedStoragePoolHandler(JDBCEntityManager em, StoragePool pool, StoragePoolQry qry) throws SQLException {
        super(em, pool, qry);
    }

    @Override
    public List<FileSystemElemNode> get_child_nodes(FileSystemElemNode node) {
        
        StoragePoolQry qry = getPoolQry();
        List<User.VsmFsEntry> mapList = qry.getUser().getFsMapper().getVsmList();
        if (node.getName().equals("/") && !insideMapping) 
        {
            // Rekussion vermeiden, wir müssen nur einmal Mappen
            insideMapping = true;
            try {
                List<FileSystemElemNode> children = new ArrayList<FileSystemElemNode>(); // childrennode.getChildren(em);
                //children.clear();

                for (int i = 0; i < mapList.size(); i++) {
                    User.VsmFsEntry vsmFsEntry = mapList.get(i);
                    FileSystemElemNode mapNode = resolve_elem_by_path(vsmFsEntry.getvPath());
                    if( mapNode != null)
                    {
                        mapNode.getAttributes().setName(vsmFsEntry.getuPath());
                        children.add(mapNode);
                    }
                }
                return children;
            } catch (SQLException sQLException) {
                LogManager.err_db("Kann Mapping node nicht auflösen", sQLException);
            }
            finally
            {
                insideMapping = false;
            }
        }
        return super.get_child_nodes(node);
    }
   
}
