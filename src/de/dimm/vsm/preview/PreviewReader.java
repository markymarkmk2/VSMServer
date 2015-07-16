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
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.PoolNodeFileLink;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class PreviewReader implements IPreviewReader {

    StoragePoolHandler handler;
    IPreviewRenderer renderer;
    
    
    public PreviewReader( StoragePoolHandler handler ) {
        this.handler = handler;
        renderer = new PreviewRenderer(handler);             
    }
    
    IPreviewData readPreviewData(  FileSystemElemNode node ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }            
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        String previewRoot = Main.get_prop(GeneralPreferences.PREVIEW_ROOT);
        try {
            StorageNodeHandler.build_preview_path(node, node.getAttributes(), fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        if (previewRoot != null) {
            previewRoot += File.separatorChar + handler.getPool().getIdx();
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
    
    @Override
    public IPreviewData getPreviewDataFile( FileSystemElemNode node ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }  
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        String previewRoot = Main.get_prop(GeneralPreferences.PREVIEW_ROOT);
        try {
            StorageNodeHandler.build_preview_path(node, node.getAttributes(), fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        if (previewRoot != null) {
            previewRoot += File.separatorChar + handler.getPool().getIdx();
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
    
    @Override
    public IPreviewData getPreviewDataFileAsync( FileSystemElemNode node ) throws IOException {
        if (!renderer.canRenderFile(node.getName())) {
            return null;
        }  
        
        StringBuilder fsPath = new StringBuilder();
        File f = null;
        String previewRoot = Main.get_prop(GeneralPreferences.PREVIEW_ROOT);
        try {
            StorageNodeHandler.build_preview_path(node, node.getAttributes(), fsPath);
        }
        catch (PathResolveException ex) {
            throw new IOException("build_preview_path", ex);
        }
        
        if (previewRoot != null) {
            previewRoot += File.separatorChar + handler.getPool().getIdx();
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
            IPreviewData data =  getPreviewDataFile(child);
            if (data != null) {
                result.add(data);
            }
        }
        return result;
    }

    @Override
    public List<IPreviewData> getPreviewDataDirAsync(  FileSystemElemNode node ) throws IOException, SQLException {
        List<IPreviewData> result = new ArrayList<>();
        List<FileSystemElemNode> children = UserDirMapper.filter_child_nodes(handler, node);
        for (FileSystemElemNode child: children) {
            IPreviewData data =  getPreviewDataFileAsync(child);
            if (data != null) {
                result.add(data);
            }
        }
        return result;
    }
    
    static boolean async = true;

    @Override
    public List<IPreviewData> getPreviews( List<RemoteFSElem> path ) throws SQLException, IOException {
        List<IPreviewData> result = new ArrayList<>();
        for (RemoteFSElem remoteFSElem : path) {
            FileSystemElemNode node = handler.resolve_node_by_remote_elem(remoteFSElem);
            if (node.isDirectory()) {
                result.addAll(async ? getPreviewDataDirAsync(node) : getPreviewDataDir(node) );
            }
            else {
                IPreviewData data =  async ? getPreviewDataFileAsync(node) : getPreviewDataFile(node);
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
   
}
