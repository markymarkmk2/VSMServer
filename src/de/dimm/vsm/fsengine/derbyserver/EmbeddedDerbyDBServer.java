/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.derbyserver;

import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import org.apache.derby.drda.NetworkServerControl;

/**
 *
 * @author Administrator
 */
public class EmbeddedDerbyDBServer implements DerbyDBServer
{
    String url;
    String server;
    int port;
    
    NetworkServerControl control;
   

    public EmbeddedDerbyDBServer( StoragePoolNub nub, String _url ) throws Exception {
        this.url = _url;
        this.server = DerbyStoragePoolNubHandler.getServerByConnectString(nub);
        this.port = DerbyStoragePoolNubHandler.getPortByConnectString(nub);
        InetAddress adr = Inet4Address.getByName(server);
        control = new NetworkServerControl(adr, port);
    }
    
    @Override
    public void start()
    {        
        try {
            PrintWriter pw = new PrintWriter("logs/EmbeddedDerby_" + port + ".log");
            control.start(pw);
        }
        catch (Exception ex) {
             Log.err("Fehler beim Start von Network-DB Server ", url, ex);
        }
    }
    
    @Override
    public void stop()
    {
        try {           
            control.shutdown();
        }
        catch (Exception ex) {
            Log.err("Fehler beim Stop von Network-DB Server ", url, ex);
        }
    }

    @Override
    public boolean isSame( String server, int port ) {
        return this.port == port && this.server.equals(server);
    }
}