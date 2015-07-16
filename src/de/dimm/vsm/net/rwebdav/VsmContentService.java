/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMFSInputStream;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolHandlerServlet;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class VsmContentService implements IVsmContentService {

    GuiServerApi api;
    IWrapper wrapper;

    public VsmContentService( GuiServerApi api, IWrapper wrapper ) {
        this.api = api;
        this.wrapper = wrapper;
    }

    
    
    @Override
    public void setFileContent( RemoteFSElem file, InputStream in ) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getFileContent( RemoteFSElem file ) throws FileNotFoundException, SQLException {
        StoragePoolHandler sp = StoragePoolHandlerServlet.getPoolHandlerByWrapper(wrapper);
        FileSystemElemNode node = sp.resolve_node_by_remote_elem(file);
        VSMFSInputStream is = new VSMFSInputStream(sp, node);
        return is;                
    }
    
}
