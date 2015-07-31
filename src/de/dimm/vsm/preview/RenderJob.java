/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMInputStream;
import de.dimm.vsm.hash.StringUtils;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.preview.imagemagick.PreviewRenderer;
import de.dimm.vsm.preview.imagemagick.RenderFactory;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.io.InputStream;
import org.im4java.core.CommandException;

/**
 *
 * @author Administrator
 */
public class RenderJob implements IRenderJob {

    StoragePoolHandler sp_handler;
    FileSystemElemNode node;
    IPreviewData data;
    boolean abort;

    public RenderJob( StoragePoolHandler sp_handler, IPreviewData data, FileSystemElemNode node ) {
        this.sp_handler = sp_handler;
        this.data = data;
        this.node = node;
    }
    
@Override
    public void setAbort( boolean abort ) {
        this.abort = abort;
    }

    @Override
    public boolean isAbort() {
        return abort;
    }    

    @Override
    public void render() {
        String suffix = PreviewRenderer.getSuffix(node.getName());
        IRenderer renderer = RenderFactory.getRenderer(suffix);

        if (data.getPreviewImageFile().exists()) {
            return;
        }
        
        if(data.getMetaData().isError()) {
            return;
        }
        
        try (InputStream fis = new VSMInputStream(sp_handler, node)) {
            renderer.render(suffix, fis, data.getPreviewImageFile());
            data.getMetaData().setDone();
        }
        catch (IOException exception) {
            String errText = Main.Txt("IO-Fehler beim Preview generieren von Datei ") + node.getName() + " " + exception.getMessage();
            data.getMetaData().setError(errText);
            Log.err(errText, exception);
        }
        catch (CommandException exception) {
            String errText = sp_handler.buildCheckOpenNodeErrText(node);
            if (StringUtils.isEmpty(errText)) {
                errText = node.getName() + " " + exception.getMessage();
            }
            
            data.getMetaData().setError(Main.Txt("Preview erzeugen fehgeschlagen:\n") + errText);
            Log.warn(errText);
        }
        catch (Exception exception) {
            String errText = Main.Txt("Fehler beim Erzeugen der Preview von Datei ") + node.getName() + " " + exception.getMessage();
            data.getMetaData().setError(errText);
            Log.err(errText, exception);
        }
    }
}