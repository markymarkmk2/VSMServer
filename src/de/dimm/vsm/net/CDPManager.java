/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.net.interfaces.AgentApi;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Excludes;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Objects;


class CDPTicketEntry
{
    CdpTicket ticket;
    ClientVolume volume;

    public CDPTicketEntry( CdpTicket ticket, ClientVolume volume )
    {
        this.ticket = ticket;
        this.volume = volume;
    }

    @Override
    public boolean equals( Object obj )
    {
        if (obj instanceof CDPTicketEntry)
        {
            CDPTicketEntry t = (CDPTicketEntry)obj;
            if (t.volume.getClinfo() != null && volume.getClinfo() != null)
            {
                if (t.volume.getClinfo().getSched().getPool().getIdx() != volume.getClinfo().getSched().getPool().getIdx())
                    return false;
            }
                
            return (t.volume.getIdx() == volume.getIdx());
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.volume);
        return hash;
    }


}
/**
 *
 * @author Administrator
 */
public class CDPManager extends WorkerParent implements IAgentIdleManagerEntry
{
    
    final List<CDPTicketEntry> cdpEntries;
    Thread runner;

    public CDPManager()
    {
        super("CDPManager");
        cdpEntries = new ArrayList<CDPTicketEntry>();
    }

    @Override
    public boolean isPersistentState()
    {
        return true;
    }

    @Override
    public boolean initialize()
    {
        try
        {
            fillClientVolList();
        }
        catch (SQLException sQLException)
        {
            Log.err(Main.Txt("ClientvolumeList nicht lesbar"), sQLException);
            return false;
        }
        
        return true;
    }

    void fillClientVolList() throws SQLException
    {

        List<StoragePool> pools = Main.get_control().getStoragePoolList();

        List<CDPTicketEntry> newEntries = new ArrayList<CDPTicketEntry>();

        for (int i = 0; i < pools.size(); i++)
        {
            StoragePool storagePool = pools.get(i);
            GenericEntityManager em = Main.get_control().get_util_em(storagePool);
            List<ClientVolume> cv = em.createQuery("select T1 from ClientVolume", ClientVolume.class);
            for (int j = 0; j < cv.size(); j++)
            {
                ClientVolume clientVolume = cv.get(j);
                if (clientVolume.getCdp() && !clientVolume.getDisabled() && !clientVolume.getClinfo().getDisabled())
                {
                    newEntries.add(new CDPTicketEntry(null, clientVolume));
                }
            }
        }

        // SYNCHRONIZE LISTS
        synchronized(cdpEntries)
        {
            // REMOVE ALL MISSING ENTRIES
            for (int i = 0; i < cdpEntries.size(); i++)
            {
                CDPTicketEntry cDPTicketEntry = cdpEntries.get(i);
                if (!newEntries.contains(cDPTicketEntry))
                {
                    cdpEntries.remove(i);
                    i--;
                    stopCDP(cDPTicketEntry);
                    continue;
                }
            }

            // SKIP ALL EXSISTING ENTRIES
            for (int i = 0; i < newEntries.size(); i++)
            {
                CDPTicketEntry cDPTicketEntry = newEntries.get(i);
                if (cdpEntries.contains(cDPTicketEntry))
                {
                    newEntries.remove(i);
                    i--;
                    continue;
                }
            }
            // ADD ALL NEW ENTRIES
            for (int i = 0; i < newEntries.size(); i++)
            {
                CDPTicketEntry cDPTicketEntry = newEntries.get(i);
                cdpEntries.add(cDPTicketEntry);                
            }
        }
    }

   



    void stopAllStarted()
    {
        for (int i = 0; i < cdpEntries.size(); i++)
        {
            CDPTicketEntry t = cdpEntries.get(i);
            stopCDP(t);
        }
    }
    
    void startAllStopped()
    {
        for (int i = 0; i < cdpEntries.size(); i++)
        {
            CDPTicketEntry t = cdpEntries.get(i);
            startCDP(t);
        }
        setStatusTxt("");
    }

    boolean isInPause;
    int last_minute_checked = -1;
    GregorianCalendar cal;

    @Override
    public int getCycleSecs() {
        return 10;
    }
    
    
    @Override
    public void startIdle()
    {
        isInPause = isPaused();  
        cal = new GregorianCalendar();
        last_minute_checked = -1;     
    }
    /**
     *
     */
    @Override
    public void stopIdle()
    {
        // STOP ON SHUTDOWN IS DIABLED, CDP RUNS ON
        if (Main.get_bool_prop(GeneralPreferences.STOP_CDP_ON_SHUTDOWN))
        {
            Log.debug("CDPManager is stopping");
            stopAllStarted(); 
            Log.debug("CDPManager is stopped"); 
            Log.debug("CDPManager is stopped, with stopping Agents (StopCdpOnShutdown=1)"); 
        }
        else
        {
            Log.debug("CDPManager is stopped, *without* stopping Agents (StopCdpOnShutdown=0)"); 
        }
        
    }

    // IS HANDLED INSIDE AgentIdleManager
    @Override
    public void run()
    {
        is_started = true;
        while (!isShutdown())
        {
            LogicControl.sleep(1*1000);
                       
        }     
        finished = true;
    }
    
    @Override
    public void doIdle()
    {  
        if (isInPause != isPaused())
        {
            if (isPaused())
            {
                setStatusTxt(Main.Txt("Stoppe Client-Dienste"));
                stopAllStarted();
            }
            isInPause = isPaused();
        }
        if (isPaused())
        {
            return;
        }
        if (LogicControl.getStorageNubHandler().isCacheLoading()) {
            return;
        }

        // START ALL PENDING ENTRIES
        startAllStopped();


        cal.setTime(new Date());
        int minute = cal.get(GregorianCalendar.MINUTE);
        if (minute == last_minute_checked)
        {
            return;
        }
        last_minute_checked = minute;

        // ONCE A MINUTE REREAD CLIENTVOL LIST
        try
        {
            setStatusTxt(Main.Txt("Lese Clientvolumes"));
            fillClientVolList();
            setStatusTxt("");
        }
        catch (SQLException sQLException)
        {
            Log.err(Main.Txt("ClientvolumeList nicht lesbar"), sQLException);
        }        
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        return " " + (isPaused() ? Main.Txt("Pause") : Main.Txt("bereit")) + " ";
    }

    private void stopCDP( CDPTicketEntry t )
    {
        if (t.ticket == null)
            return;

        String name = t.volume.getClinfo().getIp() + ":" + t.volume.getClinfo().getPort() + "->" + t.volume.getVolumePath().getPath();

        try
        {
            AgentApiEntry api = LogicControl.getApiEntry(t.volume.getClinfo());
            if (api == null || !api.isOnline())
                throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + t.volume.getClinfo().toString() );

            api.getApi().stop_cdp(t.ticket);
            t.ticket = null;
            api.close();
            Log.debug(Main.Txt("CDP wurde gestoppt"), name );
        }
        catch (Exception exc)
        {
            Log.err(Main.Txt("CDP kann nicht gestoppt werden"), name, exc);
        }
    }

    boolean offlineCheck = false;
    private void startCDP( CDPTicketEntry t )
    {
        String name = t.volume.getClinfo().getIp() + ":" + t.volume.getClinfo().getPort() + "->" + t.volume.getVolumePath().getPath();
        ClientVolume clientVolume = t.volume;
        AgentApiEntry api = null;

        try
        {
            try
            {
                api = LogicControl.getApiEntry(clientVolume.getClinfo().getIp(), clientVolume.getClinfo().getPort(), /*msg*/ false);
                startCDP(name, api, t);
            }
            catch (UnknownHostException exc)
            {
                 Log.err(Main.Txt("CDP kann nicht gestartet werden"), name, exc);
            }
        }
        finally
        {
            if (api != null)
            {
                try
                {
                    api.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }
    private void startCDP( String name, AgentApiEntry api, CDPTicketEntry t )
    {
        ClientVolume clientVolume = t.volume;
        if (t.ticket != null)
        {
            try
            {
                boolean online = api.check_online(false);

                if (!online)
                {
                    if (!offlineCheck)
                        Log.debug(Main.Txt("CDP Agent ist nicht erreichbar"), name );
                    offlineCheck = true;
                    t.ticket = null;
                }
                return;
            }
            catch (Exception ex)
            {
                if (!offlineCheck)
                    Log.debug(Main.Txt("CDP Agent ist nicht erreichbar"), name, ex );
                offlineCheck = true;
                t.ticket = null;
                return;
            }
        }

        
        try
        {
            if (!api.check_online(false))
            {
                if (!offlineCheck)
                {
                    Log.debug(Main.Txt("CDP kann nicht gestartet werden") + ", " + Main.Txt("CDP Agent ist nicht erreichbar"), name );
                    offlineCheck = true;
                }
                return;
            }
            offlineCheck = false;
            setStatusTxt(Main.Txt("Starte Client-Dienst") + " " + clientVolume.toString());


            // ( InetAddress addr, int port, boolean ssl, boolean tcp, RemoteFSElem file, long schedIdx, long clientInfoIdx, long clientVolumeIdx ) throws IOException;
            CdpTicket ticket = api.getApi().init_cdp(Main.getServerAddress(), Main.getServerPort(), false, false,
                    clientVolume.getVolumePath(),
                    clientVolume.getClinfo().getSched().getPool().getIdx(),
                    clientVolume.getClinfo().getSched().getIdx(),
                    clientVolume.getClinfo().getIdx(),
                    clientVolume.getIdx());

            if (ticket.isOk())
            {
                t.ticket = ticket;
                if (!clientVolume.getClinfo().getExclList().isEmpty() && api.hasBooleanOption(AgentApi.OP_CDP_EXCLUDES))
                {
                    // CREATE DUPLICATES W/O DB LINKS
                    ArrayList<Excludes> clientExcList = new ArrayList<Excludes>();
                    for (int i = 0; i < clientVolume.getClinfo().getExclList().size(); i++)
                    {
                        Excludes excludes = clientVolume.getClinfo().getExclList().get(i);
                        Excludes cloneexcludes = new Excludes();
                        cloneexcludes.clone(excludes);
                        cloneexcludes.setClinfo(null);
                        clientExcList.add(cloneexcludes);
                    }
                    api.getApi().set_cdp_excludes( t.ticket, clientExcList );
                }
                Log.debug(Main.Txt("CDP wurde gestartet"), name );

            }
            else
            {
                Log.err(Main.Txt("CDP konnte nicht initialisiert werden"), name + ": " + ticket.getErrorText());
            }

        }
        catch (Exception exc)
        {
            Log.err(Main.Txt("CDP kann nicht gestartet werden"), name, exc);
        }
    }

    

}
