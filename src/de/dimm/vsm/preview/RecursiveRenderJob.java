/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMInputStream;
import de.dimm.vsm.hash.StringUtils;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import static de.dimm.vsm.preview.PreviewReader.getPreviewRoot;
import de.dimm.vsm.preview.imagemagick.PreviewRenderer;
import de.dimm.vsm.preview.imagemagick.RenderFactory;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.im4java.core.CommandException;

/**
 *
 * @author Administrator
 */
public class RecursiveRenderJob implements IRenderJob, JobInterface{

    StoragePoolHandler sp_handler;
    FileSystemElemNode rootNode;
    Properties props;
    String actStatus;
    long filesDone = 0;
    long filesRendered = 0;
        
    boolean abort;

    public RecursiveRenderJob( StoragePoolHandler sp_handler, FileSystemElemNode node, Properties props ) {
        this.sp_handler = sp_handler;
        this.rootNode = node;
        this.props = props;
    }

    @Override
    public void setAbort( boolean abort ) {
        this.abort = abort;
        state = JOBSTATE.ABORTING;
    }

    @Override
    public boolean isAbort() {
        return abort;
    }
    
    
    @Override
    public void render() {
        state = JOBSTATE.RUNNING;
        try {
            render(rootNode);            
            state = isAbort() ? JOBSTATE.ABORTED : JOBSTATE.FINISHED_OK_REMOVE;
        }
        catch (IOException exception) {
            state = JOBSTATE.ABORTED;
            actStatus = "Abbruch in recursive render:" + exception.getMessage();
            Log.err(actStatus, exception);
        }
    }
    
    public void render(FileSystemElemNode node) throws IOException {
        if (node.isDirectory()) {
            renderDir(node);
        }
        else {
            renderFile(node);
        }
    }
    
    void renderDir(FileSystemElemNode node ) throws IOException {
        List<FileSystemElemNode> children = node.getChildren(sp_handler.getEm());
        
        for (FileSystemElemNode child : children) {
            if (isAbort()) {
                break;
            }
            render(child);            
        }
    }
    private boolean isPropTrue(  String key ) {
        if (props == null)
            return false;
        
        String val = props.getProperty(key, IPreviewData.FALSE );
        return val.equals( IPreviewData.TRUE);
    }

    void renderFile(FileSystemElemNode node) throws IOException {
        String suffix = PreviewRenderer.getSuffix(node.getName());
        IRenderer renderer = RenderFactory.getRenderer(suffix);

        filesDone++;
        File imageFile = getOutFile(node);
        
        // Delete Only
        if (isPropTrue(IPreviewData.DELETE)) {
            if (imageFile.exists()) {            
                imageFile.delete();
            }
            return;
        }
        
        if (imageFile.exists()) {            
            return;
        }
        actStatus = "Rendere " + node.getName() + "...";
        try (InputStream fis = new VSMInputStream(sp_handler, node)) {
            renderer.render(suffix, fis, imageFile);
            filesRendered++;            
        }
        catch (SQLException exception) {
            actStatus = Main.Txt("DB-Fehler beim Generieren der Preview von Datei ") + node.getName() + " " + exception.getMessage();            
            Log.err(actStatus, exception);
            throw new IOException(actStatus);
        }
        catch (IOException exception) {
            actStatus = Main.Txt("IO-Fehler beim Generieren der Preview von Datei ") + node.getName() + " " + exception.getMessage();            
            Log.err(actStatus, exception);
            throw new IOException(actStatus);
        }
        catch (CommandException exception) {
            actStatus = sp_handler.buildCheckOpenNodeErrText(node);
            if (StringUtils.isEmpty(actStatus)) {
                actStatus = node.getName() + " " + exception.getMessage();
            }                        
            Log.warn(actStatus);
        }
        catch (Exception exception) {
            actStatus = Main.Txt("Fehler beim Generieren der Preview von Datei ") + node.getName() + " " + exception.getMessage();            
            Log.err(actStatus, exception);
            throw new IOException(actStatus);
        }
    }
    

    private File getOutFile( FileSystemElemNode node ) throws IOException {
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
            } 
            else {
                // Kein persistenter PreviewRoot?
                // Dann kann auch nichts dauerhaft gespeichert werden
                throw new IOException("Kein PreviewRoot eingerichtet");                
            }
            
            if (!outFile.getParentFile().exists()) {
                outFile.getParentFile().mkdirs();
            }
        }
        catch (PathResolveException exc ) {
            actStatus = "Fehler beim Erzeugen des Previewpfades";
            throw new IOException(actStatus , exc);
        }
        return outFile;
    }    
    
    JOBSTATE state;
    Date startTime = new Date();

    @Override
    public JOBSTATE getJobState() {
        return state;
    }

    @Override
    public void setJobState( JOBSTATE jOBSTATE ) {
        state = jOBSTATE;        
    }

    @Override
    public InteractionEntry getInteractionEntry() {
        return null;
    }

    @Override
    public String getStatusStr() {
        return actStatus;
    }

    @Override
    public String getStatisticStr() {
        return "Files done: " + filesDone + " files rendered: " + filesRendered;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public Object getResultData() {
        return null;
    }

    @Override
    public String getProcessPercent() {
        return "";
    }

    @Override
    public String getProcessPercentDimension() {
        return "";
    }

    @Override
    public void abortJob() {
        setAbort(true);
    }

    @Override
    public void run() {
        render();
    }

    @Override
    public User getUser() {
        return null;
    }

    @Override
    public void close() {        
    }
}