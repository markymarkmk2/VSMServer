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
public class StandaloneDerbyDBServer  implements DerbyDBServer
{
    String url;
    String server;
    int port;
    
    NetworkServerControl control;
   
    boolean started;
    Thread thread;
    
    String javaCmd =  "\"" + System.getProperty("java.home") + "/bin/java\"" + " -cp=" + System.getProperty("java.class.path")+ " -jar derbyrun.jar ";
    
    String jarFile;
    String threadName;

    public StandaloneDerbyDBServer( StoragePoolNub nub, String _url ) throws Exception {
        this.url = _url;
        this.server = DerbyStoragePoolNubHandler.getServerByConnectString(nub);
        this.port = DerbyStoragePoolNubHandler.getPortByConnectString(nub);
        InetAddress adr = Inet4Address.getByName(server);
        control = new NetworkServerControl(adr, port);
        
        // TODO: CLASS PATH DOESNT WORK (-cp=VSM.jar)
        File f = new File("dist/lib/derbyrun.jar");
        if (!f.exists())
            f = new File("lib/derbyrun.jar");
        if (!f.exists())
            f = new File("derbyrun.jar");
        
        jarFile = f.getPath();
        threadName = "DerbyServer " + server + ":" + port;
    }
    
    
    
    @Override
    public void start()
    {        
        try {
            
            final ProcessBuilder pb = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", 
                    "-cp", System.getProperty("java.class.path"), "-jar", jarFile, 
                    "server", "start", 
                    "-h", server, "-p", Integer.toString(port));
            
            pb.redirectError( new File(Main.LOG_PATH, "DerbyServerErr_" + server + ":" + port + ".log"));
            pb.redirectOutput(new File(Main.LOG_PATH, "DerbyServerOut_" + server + ":" + port + ".log"));
                        
            thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        Log.debug("Network-DB Server start  : " + threadName);
                        Process p = pb.start(); 
                        int exitVal = p.waitFor();
                        Log.debug("Network-DB Server beendet: " + threadName + " p.exitValue(): " +exitVal);
                    }
                    catch (IOException iOException) {
                        Log.err("Fehler beim ThreadStart von Network-DB Server ", threadName, iOException);
                    }
                    catch (InterruptedException ex) {
                        Log.err("Abbruch beim ThreadStart von Network-DB Server ", threadName, ex);
                    }
                }
            }, threadName);

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
                thread.start();

                int retries = 10;
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
            final ProcessBuilder pb = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", 
                    "-cp", System.getProperty("java.class.path"), "-jar", jarFile, 
                    "server", "shutdown", 
                    "-h", server, "-p", Integer.toString(port));
            
            //Map<String, String> env = pb.environment();
            //env.put("CLASSPATH", System.getProperty("java.class.path"));            
            
            Log.debug("Network-DB Server ende  : " + threadName);
            Process p = pb.start();    
            p.waitFor();
            Log.debug("Network-DB Server beendet: " + threadName + " p.exitValue(): " + p.exitValue());            
            //String cmd = javaCmd + "server shutdown -h " + server + " -p " + port;            
            //Runtime.getRuntime().exec(cmd);
            //control.shutdown();
        }
        catch (IOException | InterruptedException ex) {
            Log.err("Fehler beim Stop von Network-DB Server ", threadName, ex);
        }
        if (thread != null)
        {
            try {
                thread.join(10000);
            }
            catch (InterruptedException interruptedException) {
                Log.err("Fehler beim Join von Network-DB Server ", threadName, interruptedException);
            }
        }
    }

    @Override
    public boolean isSame( String server, int port ) {
        return this.port == port && this.server.equals(server);
    }
}
