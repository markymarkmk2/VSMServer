package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import java.util.ArrayList;
import java.util.List;


public class ShutdownHook extends Thread
{
    List<Runnable> children;
    boolean shutdownFinished = false;

    public ShutdownHook()
    {
        children = new ArrayList<Runnable>();
    }
    public void addHook( Runnable t )
    {
        children.add(t);
    }

    @Override
    public void run()
    {
        // DONE ALREADY MANUALLY ? 
        if (shutdownFinished)
            return;

        Log.info("VSM wird heruntergefahren...");
        for (int i = children.size() - 1; i >= 0; i--)
        {
            Runnable r = children.get(i);
            r.run();            
        }
        Main.get_control().shutdown();
        Log.info("VSM wird beendet");

        shutdownFinished = true;
    }

}
