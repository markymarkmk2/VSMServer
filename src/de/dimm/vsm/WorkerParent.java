/*
 * WorkerParent.java
 *
 * Created on 10. Oktober 2007, 11:36
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.tasks.InteractionEntry;
import de.dimm.vsm.tasks.TaskInterface;


/**
 *
 * @author Administrator
 */
public abstract class WorkerParent implements TaskInterface
{
    private String name;
    private boolean shutdown;
    private String statusTxt = "Startup";
    private boolean goodState;
    String last_status = "";
    protected boolean finished = false;
    protected boolean is_started = false;

    

    public static final String ST_STARTUP = "Startup";
    public static final String ST_RUN = "Running";
    public static final String ST_IDLE = "Idle";
    public static final String ST_ERROR = "Error";
    public static final String ST_BUSY = "Busy";
    public static final String ST_SHUTDOWN = "Shutdown";

    private TASKSTATE taskState;
    Thread workerThr;
    
    /** Creates a new instance of WorkerParent */
    public WorkerParent(String _name)
    {
        name = _name;
        setGoodState(true);
    }

    @Override
    public long getIdx()
    {
        return Main.get_control().getTaskIdx( this );
    }

    void loadTaskState()
    {
        if (!isPersistentState())
            return;

    }
    void saveTaskState()
    {
        if (!isPersistentState())
            return;
    }





 
    @Override
    public String getName()
    {
        return name;
    }
    abstract public boolean initialize();
    abstract public void run();

    public boolean start_run_loop()
    {
        final WorkerParent me = this;
        workerThr = new Thread( new Runnable() {

            @Override
            public void run()
            {
                Log.debug("Starting Worker", name);
                me.run();
                Log.debug("Finished Worker", name);
            }
        }, getName());

        workerThr.start();
        return true;
    }

    public boolean isVisible()
    {
        return true;
    }

    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    public boolean isShutdown()
    {
        return shutdown;
    }

    public void setShutdown(boolean shutdown)
    {
        this.shutdown = shutdown;
    }
    public boolean isFinished()
    {

        return finished;
    }
    public void close()
    {
        try
        {
            workerThr.join(5000);
        }
        catch (InterruptedException interruptedException)
        {
        }
    }
    public boolean isStarted()
    {
        return is_started;
    }

    public String getStatusTxt()
    {
        return statusTxt;
    }

    public void setStatusTxt(String statusTxt)
    {
        this.statusTxt = statusTxt;
        if (statusTxt.length() > 0 && statusTxt.compareTo(last_status) != 0)
        {
            String classname = this.getName();
            LogManager.msg_system( LogManager.LVL_VERBOSE, classname + ": " + statusTxt );
        }
        last_status = statusTxt;
    }

    public void clrStatusTxt(String statusTxt)
    {
        if (statusTxt.compareTo(this.statusTxt) == 0)
            this.statusTxt = ST_IDLE;
        last_status = statusTxt;
    }

    public boolean isGoodState()
    {
        return goodState;
    }

    public final void setGoodState(boolean goodState)
    {
        this.goodState = goodState;
    }

    public String get_task_status()
    {
        if (shutdown)
            return Main.Txt("Wird_heruntergefahren");
        if (is_started)
            return Main.Txt("Ist_gestartet");
        if (finished)
            return Main.Txt("Ist_beendet");
        return "?";
    }

    public void setPaused( boolean paused )
    {
        this.taskState = (paused) ? TASKSTATE.PAUSED : TASKSTATE.RUNNING;
    }

    public boolean isPaused()
    {
        return taskState == TASKSTATE.PAUSED;
    }

    @Override
    public int getProcessPercent()
    {
        return 0;
    }

    @Override
    public String getProcessPercentDimension()
    {
        return "";
    }

    @Override
    public String getStatisticStr()
    {
        return "";
    }

    @Override
    public TASKSTATE getTaskState()
    {
        return taskState;
    }

    @Override
    public void setTaskState( TASKSTATE jOBSTATE )
    {
        taskState = jOBSTATE;
        if (isPersistentState())
        {
            if (Main.get_control() != null)
                 Main.get_control().writeTaskPreferences();
        }
    }


    @Override
    public String getStatusStr()
    {
        return statusTxt;
    }

    @Override
    public InteractionEntry getInteractionEntry()
    {
        return null;
    }

    public boolean isPersistentState()
    {
        return false;
    }
  
    
}
