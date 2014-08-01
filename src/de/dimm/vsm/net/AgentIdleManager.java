/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.IStoragePoolNubHandler;
import java.util.ArrayList;
import java.util.List;
import org.catacombae.jfuse.util.Log;



/**
 *
 * @author Administrator
 */
public class AgentIdleManager extends WorkerParent
{  

    List<IAgentIdleManagerEntry> idleList;

    public AgentIdleManager(IStoragePoolNubHandler nubHandler)
    {
        super("AgentIdleManager");
        idleList = new ArrayList<>();
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
        for (IAgentIdleManagerEntry iAgentIdleManager : idleList) 
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
            for (IAgentIdleManagerEntry iAgentIdleManager : idleList) 
            {
                if (cnt % iAgentIdleManager.getCycleSecs() == 0) 
                {
                    if (!LogicControl.getStorageNubHandler().isCacheLoading())
                    {
                        iAgentIdleManager.doIdle();
                    }
                    else
                    {
                        iAgentIdleManager.setStatusTxt("Caches werden geladen");
                    }
                }
            }
        }
        
        
        for (IAgentIdleManagerEntry iAgentIdleManager : idleList) 
        {
            iAgentIdleManager.stopIdle();
        }
        Log.debug("AgentIdleManager is finished");
        
        finished = true;
    }

}
