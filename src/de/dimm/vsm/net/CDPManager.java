/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.backup.AgentApiEntry;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.records.ClientVolume;
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
public class CDPManager extends WorkerParent
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
            Log.err("ClientvolumeList nicht lesbar", sQLException);
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

    @Override
    public void run()
    {
        int last_minute_checked = -1;
        GregorianCalendar cal = new GregorianCalendar();
        boolean isInPause = isPaused();

        while (!isShutdown())
        {
            LogicControl.sleep(10*1000);

            if (isInPause != isPaused())
            {
                if (isPaused())
                {
                    setStatusTxt(Main.Txt("Stoppe Client-Dienste"));
                    stopAllStarted();
                }
            }
            if (isPaused())
            {
                continue;
            }

            // START ALL PENDING ENTRIES
            startAllStopped();

           
            cal.setTime(new Date());
            int minute = cal.get(GregorianCalendar.MINUTE);
            if (minute == last_minute_checked)
            {

                continue;
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
                Log.err("ClientvolumeList nicht lesbar", sQLException);
            }            
        }

        // STOP ON SHUTDOWN
        stopAllStarted();
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        return isPaused() ? " Pause " : " bereit ";
    }

    private void stopCDP( CDPTicketEntry t )
    {
        if (t.ticket == null)
            return;

        String name = t.volume.getClinfo().getIp() + ":" + t.volume.getClinfo().getPort() + "->" + t.volume.getVolumePath().getPath();

        try
        {
            AgentApiEntry api = LogicControl.getApiEntry(t.volume.getClinfo());
            api.getApi().stop_cdp(t.ticket);
            t.ticket = null;
            api.close();
            Log.debug("CDP wurde gestoppt", name );
        }
        catch (Exception exc)
        {
            Log.err("CDP kann nicht gestoppt werden", name, exc);
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
                api = LogicControl.getApiEntry(clientVolume.getClinfo().getIp(), clientVolume.getClinfo().getPort());
                startCDP(name, api, t);
            }
            catch (UnknownHostException unknownHostException)
            {
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
                boolean online = api.check_online();

                if (!online)
                {
                    Log.debug("CDP Agent ist nicht erreichbar", name );
                    t.ticket = null;
                }
                return;
            }
            catch (Exception ex)
            {
                Log.debug("CDP Agent ist nicht erreichbar", name, ex );
                t.ticket = null;
                return;
            }
        }

        
        try
        {
            if (!api.check_online())
            {
                if (!offlineCheck)
                {
                    Log.debug("CDP konnte nicht gestartet werden, Agent ist nicht erreichbar", name );
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
                Log.debug("CDP wurde gestartet", name );
            }
            else
            {
                Log.err("CDP konnte nicht initialisiert werden", name + ": " + ticket.getErrorText());
            }

        }
        catch (Exception exc)
        {
            Log.err("CDP kann nicht gestartet werden", name, exc);
        }
    }

    

}
