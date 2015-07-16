/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.webdav;

import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMFSInputStream;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Administrator
 */
public class VsmContentService implements IVsmContentService {

    StoragePoolHandler sp;

    public VsmContentService( StoragePoolHandler sp ) {
        this.sp = sp;
    }
    
    
    @Override
    public void setFileContent( FileSystemElemNode file, InputStream in ) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getFileContent( FileSystemElemNode file ) throws FileNotFoundException {
        
        VSMFSInputStream is = new VSMFSInputStream(sp, file);
        return is;
    }
    
}
