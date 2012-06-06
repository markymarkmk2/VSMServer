/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.Utilities.Preferences;
import de.dimm.vsm.tasks.TaskInterface.TASKSTATE;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class TaskPreferences extends Preferences
{

    @Override
    public String getFilename()
    {
        return "TaskPrefs.dat";
    }

    public TaskPreferences(List<WorkerParent> list)
    {
        super( Main.PREFS_PATH );
        
        for (int i = 0; i < list.size(); i++)
        {
            WorkerParent workerParent = list.get(i);
            if (workerParent.isPersistentState())
            {
                prop_names.add( "Status" + workerParent.getName() );
            }
        }
        read_props();
    }

    boolean isInRead = false;
    void readTaskState(List<WorkerParent> list)
    {
        try
        {
            for (int i = 0; i < list.size(); i++)
            {
                WorkerParent workerParent = list.get(i);

                if (!workerParent.isPersistentState())
                    continue;

                // READ WITH DEFAULT RUNNING
                String prop = get_prop("Status" + workerParent.getName(), TASKSTATE.RUNNING.name() );
                
                TASKSTATE ts = TASKSTATE.valueOf(prop);
                isInRead = true;
                if (workerParent.getTaskState() != ts)
                    workerParent.setTaskState( TASKSTATE.valueOf(prop) );
                isInRead = false;
            }
        }
        catch (Exception e)
        {
            Log.warn("Taskpreferences können nicht geladen werden", e);
        }

    }
    
    void writeTaskState(List<WorkerParent> list)
    {
        if (isInRead)
            return;

        try
        {
            for (int i = 0; i < list.size(); i++)
            {
                WorkerParent workerParent = list.get(i);

                if (!workerParent.isPersistentState())
                {
                    continue;
                }
                String prop = workerParent.getTaskState().name();
                set_prop("Status" + workerParent.getName(), prop);
            }
            store_props();
        }
        catch (Exception e)
        {
            Log.err("Taskpreferences können nicht geschrieben werden", e);
        }
    }

    

}
