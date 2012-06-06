/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.Utilities.StatusEntry;
import de.dimm.vsm.Utilities.StatusHandler;
import org.apache.commons.lang.builder.EqualsBuilder;

/**
 *
 * @author mw
 */
public abstract class WorkerParentChild  implements StatusHandler
{
    StatusEntry status = new StatusEntry();
    
    protected boolean do_finish;
    protected boolean started;
    protected boolean finished;

    public abstract void idle_check();
    public abstract void run_loop();
    public abstract Object get_db_object();
    public abstract String get_task_status_txt();
    public abstract String get_name();

    
    public void finish()
    {
        do_finish = true;
    }
    public boolean is_started()
    {
        return started;
    }
    public void set_started( boolean b)
    {
        this.started = b;
    }

    public void sleep_seconds( int seconds )
    {
        // CHECK FINISHED EVERY 100ms
        seconds *= 10;
        while (!do_finish && seconds > 0)
        {
            LogicControl.sleep(100);
            seconds--;
        }
    }

    public boolean is_finished()
    {
        return finished;
    }
    
    @Override
    public String get_status_txt()
    {
        return status.get_status_txt();
    }

    @Override
    public int get_status_code()
    {
        return status.get_status_code();
    }
    @Override
    public void set_status( int code, String st)
    {
        status.set_status(code, st);
    }

    public boolean is_same_db_object( Object db_object )
    {
        return EqualsBuilder.reflectionEquals( get_db_object(), db_object);
    }

    public int get_mandant_id()
    {
        return 0;
    }

}
