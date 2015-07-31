/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class TestPreviewReader implements IPreviewReader {

    @Override
    public IPreviewData getPreviewDataFile( FileSystemElemNode node, FileSystemElemAttributes attr ) {
        File f = new File("D:\\Bilder\\Terrassentür.png");
        IMetaData metaData = new MetaData();
        IPreviewData data = new PreviewData( -1, f, metaData, "Terrassentür.png");
        return data;
    }

    @Override
    public List<IPreviewData> getPreviewDataDir(  FileSystemElemNode node ) {
        List<IPreviewData> result = new ArrayList<>();
        for (FileSystemElemNode child: node.getChildren()) {
            FileSystemElemAttributes attr = child.getAttributes();
            result.add( getPreviewDataFile(child, attr));
        }
        return result;
    }

    @Override
    public List<IPreviewData> getPreviews( List<RemoteFSElem> path, Properties props ) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public IPreviewData getPreviewDataFileAsync( FileSystemElemNode node, FileSystemElemAttributes attr, Properties props ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<IPreviewData> getPreviewDataDirAsync( FileSystemElemNode node, Properties props ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<IPreviewData> getPreviewStatus( List<IPreviewData> list ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void abortPreview( List<IPreviewData> list ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deletePreviewDataFile( FileSystemElemNode node, FileSystemElemAttributes attr ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
}
