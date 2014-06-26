/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.derbyserver;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import org.apache.derby.drda.NetworkServerControl;

/**
 *
 * @author Administrator
 */
public class StandaloneDerbyCmdDBServer  implements DerbyDBServer
{
    String url;
    String server;
    int port;
    
    NetworkServerControl control;
   
    boolean started;
   
   
    File startFile;
    File stopFile;

    public StandaloneDerbyCmdDBServer( StoragePoolNub nub, String _url ) throws Exception {
        this.url = _url;
        this.server = DerbyStoragePoolNubHandler.getServerByConnectString(nub);
        this.port = DerbyStoragePoolNubHandler.getPortByConnectString(nub);
        InetAddress adr = Inet4Address.getByName(server);
        control = new NetworkServerControl(adr, port);
        
        File startDir = new File("derbystart");
        if (!startDir.exists())
            startDir.mkdir();
        File stopDir = new File("derbystop");
        if (!stopDir.exists())
            stopDir.mkdir();
        
        startFile = new File(startDir, Integer.toString(port));
        
        stopFile = new File(stopDir, Integer.toString(port));

        if (startFile.exists())
            startFile.delete();
        if (stopFile.exists())
            stopFile.delete();
        
       
    }
    
    
    
    @Override
    public void start()
    {        
        try {

            boolean started = false;
            try {
                control.ping();
                Log.debug("Network-DB Server läuft bereits", url);
                started = true;
            }
            catch (Exception exception) {
            }            
            if (!started)
            {            
                Log.debug("Warte auf Start von Network-DB Server ", url);
                if (stopFile.exists())
                    stopFile.delete();
                startFile.createNewFile();

                int retries = 60;
                while (retries > 0)
                {

                    try {
                        control.ping();
                        break;
                    }
                    catch (Exception exception) {
                    }
                    LogicControl.sleep(1000);
                    retries--;
                }
                if (retries <= 0)
                    Log.err("Timeout beim Start von Network-DB Server ", url);
            }
        }
        catch (Exception ex) {
             Log.err("Fehler beim Start von Network-DB Server ", url, ex);
        }
    }
    
    @Override
    public void stop()
    {
        try {
            boolean stopped = true;
            try {
                control.ping();
                Log.debug("Network-DB Server läuft noch", url);
                stopped = false;
            }
            catch (Exception exception) {
            }            
            
            if (!stopped)
            {        
                Log.debug("Warte auf Stop von Network-DB Server ", url);
                
                if (startFile.exists())
                    startFile.delete();
                stopFile.createNewFile();

                int retries = 10;
                while (retries > 0)
                {

                    try {
                        control.ping();                        
                    }
                    catch (Exception exception) {
                        break;
                    }
                    LogicControl.sleep(1000);
                    retries--;
                }
                if (retries <= 0)
                    Log.err("Timeout beim Stop von Network-DB Server ", url);
            }     
        }
        catch (IOException ex) {
            Log.err("Fehler beim Stop von Network-DB Server ", "", ex);
        }        
    }

    @Override
    public boolean isSame( String server, int port ) {
        return this.port == port && this.server.equals(server);
    }
}
