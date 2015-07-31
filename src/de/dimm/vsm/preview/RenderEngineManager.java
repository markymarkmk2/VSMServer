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
import de.dimm.vsm.records.FileSystemElemNode;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



/**
 *
 * @author Administrator
 */
public class RenderEngineManager extends WorkerParent {

    public static final int MAX_RENDER_JOBS = 1000;
    ArrayBlockingQueue<IRenderJob> renderList;
    Thread runner;
    ExecutorService service;

    public RenderEngineManager() {
        super("RenderEngine");
    }

    void workRenderJobs() {

        this.setTaskState(TASKSTATE.RUNNING);
        while (!isShutdown()) {

            try {
                IRenderJob job = renderList.poll(1000, TimeUnit.MILLISECONDS);                
                if (job != null) {
                    if (job instanceof RecursiveRenderJob) {
                        Main.get_control().getJobManager().addJobEntry((RecursiveRenderJob)job);
                    }
                    else {
                        job.render();
                    }
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
    public boolean addRecursiveJob( StoragePoolHandler sp_handler, FileSystemElemNode node, Properties props ) {
        if (renderList.remainingCapacity() == 0) {
            return false;
        }
        VSMFSLogger.getLog().debug("Neuer Renderjob für " + node.getName());
        RecursiveRenderJob job = new RecursiveRenderJob(sp_handler, node, props);
        renderList.add(job);
        
        return true;
    }
}
