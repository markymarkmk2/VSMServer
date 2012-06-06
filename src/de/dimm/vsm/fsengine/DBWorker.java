/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.WorkerParent;

/**
 *
 * @author Administrator
 */
public class DBWorker extends WorkerParent
{
    public static final int NODE_MINFREE_SPACE = 1000*1024*1024; // 1GB

    public DBWorker()
    {
        super("DBWorker");
    }

    @Override
    public boolean isVisible()
    {
        return false;
    }


    @Override
    public boolean initialize()
    {
        return true;
    }

   

    @Override
    public void run()
    {
        while (!isShutdown())
        {
            LogicControl.sleep(60*1000);
            
            checkStorageNodes();
            
        }

    }

    private void checkStorageNodes()
    {

    }


}
