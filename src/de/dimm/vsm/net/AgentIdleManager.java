/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import java.util.ArrayList;
import java.util.List;
import org.catacombae.jfuse.util.Log;



/**
 *
 * @author Administrator
 */
public class AgentIdleManager extends WorkerParent
{  

    List<IAgentIdleManager> idleList;

    public AgentIdleManager(DerbyStoragePoolNubHandler nubHandler)
    {
        super("AgentIdleManager");
        idleList = new ArrayList<IAgentIdleManager>();
        idleList.add(Main.get_control().getCdpManager());
        idleList.add(Main.get_control().getHfManager());
        idleList.add(Main.get_control().getAutoMountManager());
    }

    @Override
    public boolean isVisible() {
        return false;
    }

    @Override
    public boolean initialize()
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        return "";
    }

    @Override
    public boolean isPersistentState()
    {
        return false;
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }


    @Override
    public void run()
    {
        int cnt = 0;
        for (IAgentIdleManager iAgentIdleManager : idleList) 
        {            
            iAgentIdleManager.startIdle();
        }
        
        is_started = true;
        while (!isShutdown())
        {
            LogicControl.sleep(1000);

            if (isPaused())
                continue;

            cnt++;
            for (IAgentIdleManager iAgentIdleManager : idleList) 
            {
                if (cnt % iAgentIdleManager.getCycleSecs() == 0) 
                {
                    iAgentIdleManager.doIdle();
                }
            }
        }
        
        
        for (IAgentIdleManager iAgentIdleManager : idleList) 
        {
            iAgentIdleManager.stopIdle();
        }
        Log.debug("AgentIdleManager is finished");
        
        finished = true;
    }

}
