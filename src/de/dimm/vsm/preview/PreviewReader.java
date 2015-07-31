/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.mapping.UserDirMapper;
import de.dimm.vsm.preview.imagemagick.PreviewRenderer;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.PoolNodeFileLink;
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
public class PreviewReader implements IPreviewReader {

    StoragePoolHandler handler;
    IPreviewRenderer renderer;
    static boolean async = true;
    boolean abortCurrentDir;
    
    
    public PreviewReader( StoragePoolHandler handler ) {
        this.handler = handler;
        renderer = new PreviewRenderer(handler);             
    }
    
    IPreviewData readPreviewData(  FileSystemElemNode node, FileSystemElemAttributes attr  ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }            
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        try {
            StorageNodeHandler.build_preview_path(node, attr, fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        String previewRoot = getPreviewRoot(handler);
        if (previewRoot != null) {
            f = new File(previewRoot, fsPath.toString());
        }
        if (f == null || !f.exists()) {
            for (PoolNodeFileLink pnfl: node.getLinks(handler.getEm())) {            
                f = new File(pnfl.getStorageNode().getMountPoint(), fsPath.toString());
                if (f.exists() ) {
                    break;
                }
            }
        }
        if (f == null || !f.exists()) {
            f = renderer.renderPreviewFile( node);   
            if (f == null || !f.exists()) {
                f = new File("norender.png");
            }
        }
        
        IMetaData metaData = new MetaData();        
        IPreviewData data = new PreviewData(node.getAttributes().getIdx(), f, metaData, node.getName());
        return data;    
    }
    public File getExistingPreviewFile(FileSystemElemNode node, FileSystemElemAttributes attr) throws IOException {
        return getExistingPreviewFile(node, handler, attr);
    }
    
    public static String getPreviewRoot(StoragePoolHandler handler) throws IOException {
        String previewRoot = Main.get_prop(GeneralPreferences.PREVIEW_ROOT);
        if (previewRoot != null) {
            previewRoot +="/" + handler.getPool().getIdx();            
        } 
        return previewRoot;
    }
    
    
    public static File getExistingPreviewFile(FileSystemElemNode node, StoragePoolHandler handler, FileSystemElemAttributes attr) throws IOException {
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        
        try {
            StorageNodeHandler.build_preview_path(node, attr, fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        String previewRoot = getPreviewRoot(handler);
        if (previewRoot != null) {            
            f = new File(previewRoot, fsPath.toString());
        }
        if (f == null || !f.exists()) {
            for (PoolNodeFileLink pnfl: node.getLinks(handler.getEm())) {            
                f = new File(pnfl.getStorageNode().getMountPoint(), fsPath.toString());
                if (f.exists() ) {
                    break;
                }
            }
        }
        return f;        
    }
    
    @Override
    public IPreviewData getPreviewDataFile( FileSystemElemNode node, FileSystemElemAttributes attr ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }  
                
        File f = getExistingPreviewFile(node, attr);
        if (f == null || !f.exists()) {
            f = renderer.renderPreviewFile( node);   
            if (f == null || !f.exists()) {
                f = new File("norender.png");
            }
        }
        
        IMetaData metaData = new MetaData();
        
        IPreviewData data = new PreviewData(node.getAttributes().getIdx(), f, metaData, node.getName());
        return data;
    }
    

    @Override
    public IPreviewData getPreviewDataFileAsync( FileSystemElemNode node, FileSystemElemAttributes attr, Properties props ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }  
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        try {
            StorageNodeHandler.build_preview_path(node, attr, fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        String previewRoot = getPreviewRoot(handler);        
        if (previewRoot != null) {
            f = new File(previewRoot, fsPath.toString());
        }
        if (f == null || !f.exists()) {
            for (PoolNodeFileLink pnfl: node.getLinks(handler.getEm())) {            
                f = new File(pnfl.getStorageNode().getMountPoint(), fsPath.toString());
                if (f.exists() ) {
                    break;
                }
            }
        }
        if (f == null || !f.exists()) {
            f = renderer.getOutFile(node);
        }
        // Delete? preview file entfernen
        if (isPropTrue(props, IPreviewData.DELETE)) {
            if (f != null && f.exists()) {
                f.delete();
            }
            return null;
        }        
        // Ohne Caching? preview file entfernen
        if (isPropTrue(props, IPreviewData.NOT_CACHED)) {
            if (f != null && f.exists()) {
                f.delete();
            }
        }
        // Nur Cached files?
        if (isPropTrue(props, IPreviewData.ONLY_CACHED)) {
            if (f == null || !f.exists()) {
                return null;
            }
        }
        
        MetaData metaData = new MetaData();
        IPreviewData data = new PreviewData(node.getAttributes().getIdx(), f, metaData, node.getName());
        
        if (f == null || !f.exists()) {
            renderer.startRenderPreviewFile( node, data);   
        }
        else {
            metaData.setDone();
        }
        
        return data;
    }

    @Override
    public List<IPreviewData> getPreviewDataDir(  FileSystemElemNode node ) throws IOException, SQLException {
        List<IPreviewData> result = new ArrayList<>();
        List<FileSystemElemNode> children = UserDirMapper.filter_child_nodes(handler, node);
        for (FileSystemElemNode child: children) {
            FileSystemElemAttributes attr = handler.getActualFSAttributes(child, handler.getPoolQry());     
            IPreviewData data =  getPreviewDataFile(child, attr);
            if (data != null) {
                result.add(data);
            }
        }
        return result;
    }

    @Override
    public List<IPreviewData> getPreviewDataDirAsync(  FileSystemElemNode node, Properties props ) throws IOException, SQLException {
        List<IPreviewData> result = new ArrayList<>();
        List<FileSystemElemNode> children = UserDirMapper.filter_child_nodes(handler, node);
        for (FileSystemElemNode child: children) {
            
            // THIS IS THE NEWEST ENTRY FOR THIS FILE
            FileSystemElemAttributes attr = handler.getActualFSAttributes(child, handler.getPoolQry());            
            IPreviewData data =  getPreviewDataFileAsync(child, attr,  props);
            if (data != null) {
                result.add(data);
            }
        }
        return result;
    }
    

    @Override
    public List<IPreviewData> getPreviews( List<RemoteFSElem> path, Properties props ) throws SQLException, IOException {
        List<IPreviewData> result = new ArrayList<>();
        if (isPropTrue(props, IPreviewData.RECURSIVE)) {
            createRecursivePreviewJob( path, props);
            return result;
        }
        
        for (RemoteFSElem remoteFSElem : path) {
            FileSystemElemNode node = handler.resolve_node_by_remote_elem(remoteFSElem);            
            if (node.isDirectory()) {
                result.addAll(async ? getPreviewDataDirAsync(node, props) : getPreviewDataDir(node) );
            }
            else {
                FileSystemElemAttributes attr = handler.getActualFSAttributes(node, handler.getPoolQry());                 
                IPreviewData data =  async ? getPreviewDataFileAsync(node, attr, props) : getPreviewDataFile(node, attr);
                if (data != null) {
                    result.add(data);
                }                
            }
        }        
        return result;
    }   

    @Override
    public List<IPreviewData> getPreviewStatus( List<IPreviewData> list ) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void abortPreview( List<IPreviewData> list ) throws IOException {
        for (IPreviewData iPreviewData : list) {
            if (iPreviewData.getMetaData().isBusy()) {
                iPreviewData.getMetaData().setError("Aborted");            
            }
        }
    }

    private boolean isPropTrue( Properties props, String key ) {
        if (props == null)
            return false;
        
        String val = props.getProperty(key, IPreviewData.FALSE );
        return val.equals( IPreviewData.TRUE);
    }

    private void createRecursivePreviewJob( List<RemoteFSElem> path, Properties props ) throws SQLException {
        for (RemoteFSElem remoteFSElem : path) {
            FileSystemElemNode node = handler.resolve_node_by_remote_elem(remoteFSElem);
            RenderEngineManager renderEngine = Main.get_control().getRenderEngineManager();
            renderEngine.addRecursiveJob(handler, node, props);            
        }
    }

    @Override
    public void deletePreviewDataFile( FileSystemElemNode node, FileSystemElemAttributes attr ) throws IOException {
        File f = getExistingPreviewFile(node, attr);
        if (f != null && f.exists()) {
            f.delete();
        }        
    }
}
