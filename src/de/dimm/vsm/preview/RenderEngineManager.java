/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.VSMFSInputStream;
import de.dimm.vsm.hash.StringUtils;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.preview.imagemagick.PreviewRenderer;
import de.dimm.vsm.preview.imagemagick.RenderFactory;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.im4java.core.CommandException;

class RenderJob {

    StoragePoolHandler sp_handler;
    FileSystemElemNode node;
    IPreviewData data;

    public RenderJob( StoragePoolHandler sp_handler, IPreviewData data, FileSystemElemNode node ) {
        this.sp_handler = sp_handler;
        this.data = data;
        this.node = node;
    }

    void render() {
        String suffix = PreviewRenderer.getSuffix(node.getName());
        IRenderer renderer = RenderFactory.getRenderer(suffix);

        if (data.getPreviewImageFile().exists()) {
            return;
        }
        try (InputStream fis = new VSMFSInputStream(sp_handler, node)) {
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

/**
 *
 * @author Administrator
 */
public class RenderEngineManager extends WorkerParent {

    public static final int MAX_RENDER_JOBS = 1000;
    ArrayBlockingQueue<RenderJob> renderList;
    Thread runner;
    ExecutorService service;

    public RenderEngineManager() {
        super("RenderEngine");
    }

    void workRenderJobs() {

        while (!isShutdown()) {

            try {
                RenderJob job = renderList.poll(1000, TimeUnit.MILLISECONDS);
                if (job != null) {
                    job.render();
                }
            }
            catch (InterruptedException interruptedException) {
            }
        }
    }

    @Override
    public boolean initialize() {
        renderList = new ArrayBlockingQueue<>(MAX_RENDER_JOBS);
        service = Executors.newSingleThreadExecutor();
        runner = new Thread(new Runnable() {
            @Override
            public void run() {
                workRenderJobs();
            }
        }, "renderEngineThread");
        runner.start();
        return true;
    }

    @Override
    public boolean isPersistentState() {
        return true;
    }

    @Override
    public void run() {
        is_started = true;
        int last_minute_checked = -1;
        GregorianCalendar cal = new GregorianCalendar();

        setStatusTxt("");
        while (!isShutdown()) {
            LogicControl.sleep(1000);
            if (isPaused()) {
                continue;
            }

            cal.setTime(new Date());
            int minute = cal.get(GregorianCalendar.MINUTE);
            if (minute == last_minute_checked) {
                continue;
            }
        }
        finished = true;
    }

    @Override
    public boolean check_requirements( StringBuffer sb ) {
        return true;
    }

    @Override
    public String get_task_status() {
        if (renderList.isEmpty()) {
            return " Idle ";
        }
        return " " + Main.Txt("Aktive Jobs: ") + renderList.size();
    }

    public boolean addJob( StoragePoolHandler sp_handler, IPreviewData data, FileSystemElemNode node ) {
        if (renderList.remainingCapacity() == 0) {
            return false;
        }
        VSMFSLogger.getLog().debug("Neuer Renderjob für " + node.getName());
        RenderJob job = new RenderJob(sp_handler, data, node);
        renderList.add(job);
        return true;
    }
}
