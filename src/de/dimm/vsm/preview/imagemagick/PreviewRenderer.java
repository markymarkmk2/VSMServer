/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMInputStream;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.preview.IPreviewData;
import de.dimm.vsm.preview.IPreviewRenderer;
import de.dimm.vsm.preview.IRenderer;
import static de.dimm.vsm.preview.PreviewReader.getPreviewRoot;
import de.dimm.vsm.preview.RenderEngineManager;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;


/**
 *
 * @author Administrator
 */
public class PreviewRenderer implements IPreviewRenderer {

    StoragePoolHandler sp_handler;
    // Cache für Installationen ohne PreviewPath und geschlossene StorageNodes
    Map<Long,File> tempPreviewMap = new HashMap<>();
    
    
    public PreviewRenderer(StoragePoolHandler sp_handler ) {
        this.sp_handler = sp_handler;        
    }
    
    @Override
    public boolean canRenderFile( String name ) {
        IRenderer renderer = RenderFactory.getRenderer( getSuffix(name ) );
        return (renderer != null);
    }

    public static String getSuffix( String name) {
        int idx = name.lastIndexOf('.');
        if (idx > 0 && idx < name.length() - 1) {
            return name.substring( idx + 1);
        }
        return "";        
    }
    
    @Override
    public File renderPreviewFile( FileSystemElemNode node ) throws IOException {
              
        if (tempPreviewMap.containsKey(node.getAttributes().getIdx())) {
            return tempPreviewMap.get(node.getAttributes().getIdx());
        }
        
        String suffix =  getSuffix(node.getName() );
        IRenderer renderer = RenderFactory.getRenderer(suffix );
        File outFile = getOutFile(node);
           
        try {
             try (InputStream fis = new VSMInputStream(sp_handler, node)) {
                 
                 renderer.render(suffix, fis, outFile);
                 return outFile;
             }               
        }
        catch (IOException exception) {
            Log.err(Main.Txt( "IO-Fehler beim Preview generieren von Datei " + node.getName()),  exception);
            return null;
        }
        catch (Exception exception) {
            Log.err(Main.Txt( "Fehler beim Preview generieren von Datei " + node.getName()),  exception);
            return null;
        }        
    }

    @Override
    public void startRenderPreviewFile( FileSystemElemNode node, IPreviewData data ) throws IOException {
              
        RenderEngineManager renderEngine = Main.get_control().getRenderEngineManager();
        
        if (tempPreviewMap.containsKey(node.getAttributes().getIdx())) {
            File outFile = tempPreviewMap.get(node.getAttributes().getIdx());
            if (outFile.exists()) {
                data.setPreviewImageFile(outFile);
                data.getMetaData().setDone();
                return;
            }
        }
        
        if (data.getPreviewImageFile().exists()) {
            data.getMetaData().setDone();
            return;
        }
        data.getMetaData().setBusy();
        renderEngine.addJob(sp_handler, data, node);
    }

    @Override
    public void renderPreviewDir( FileSystemElemNode node ) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clearPreviewFile( FileSystemElemNode node ) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clearPreviewDir( FileSystemElemNode node ) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public File getOutFile( FileSystemElemNode node ) throws IOException {
        StringBuilder sb = new StringBuilder();
        String previewRoot = getPreviewRoot(sp_handler);

        if (previewRoot == null) {
             AbstractStorageNode snode = sp_handler.get_primary_dedup_node_for_write();
             if (snode != null) {
                previewRoot = snode.getMountPoint();
             }
        }
        File outFile = null;
        
        try {
             
            if (previewRoot != null) {
                StorageNodeHandler.build_preview_path(node, node.getAttributes(), sb);
                outFile = new File (previewRoot, sb.toString()); 
            } else {
                outFile = File.createTempFile("vsmpreview", ".png");
                outFile.deleteOnExit();
                tempPreviewMap.put(node.getAttributes().getIdx(), outFile);
            }   
            
            if (!outFile.getParentFile().exists()) {
                outFile.getParentFile().mkdirs();
            }
        }
        catch (PathResolveException | IOException exc ) {
            throw new IOException("Fehler beim Erzeugen des Previewpfades" , exc);
        }
        return outFile;
    }
    

}
