/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.backup.AgentApiEntry;
import de.dimm.vsm.backup.BackupManager;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.net.interfaces.ServerApi;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class ServerApiImpl implements ServerApi
{

    static long idx = 0;
    @Override
    public boolean alert( String reason, String msg )
    {
        Log.debug("Meldung", idx++ + ": " + reason + ": " + msg);
        return true;
    }

    @Override
    public boolean alert_list( List<String> reason, String msg )
    {
        for (int i = 0; i < reason.size(); i++)
        {
            String string = reason.get(i);
            Log.debug("Meldung", idx++ + ": " + string + ": " + msg);
        }
        return true;
    }

    @Override
    public Properties get_properties()
    {
        Properties p = new Properties();
        p.setProperty(ServerApi.SOP_SV_VER, Main.get_version_str());
        p.setProperty(ServerApi.SOP_OS_NAME, System.getProperty("os.name"));
        p.setProperty(ServerApi.SOP_OS_VER, System.getProperty("os.version"));
        p.setProperty(ServerApi.SOP_OS_ARCH, System.getProperty("os.arch"));
        
        return p;
    }

    @Override
    public boolean cdp_call( CdpEvent file, CdpTicket ticket )
    {
        BackupManager bm = Main.get_control().getBackupManager();
        JobManager jm = Main.get_control().getJobManager();
        AgentApiEntry api = null;
        JobInterface job = null;
        try
        {
            Log.debug("CDP-Call", file.toString());
            if (file.elem == null || file.elem.getPath() == null || file.elem.getPath().isEmpty())
            {
                Log.warn("Ignoriere leeren CDP-Call",  file.toString());
                return false;
            }
            if (jm.isPoolBusy(ticket.getPoolIdx()))
            {
                Log.debug("Stoppe CDP-Call da Pool aktiv ist");
                return false;
            }
            final StoragePool pool = Main.get_control().getStoragePool(ticket.getPoolIdx());
            GenericEntityManager em = Main.get_control().get_util_em(pool);
            Schedule sched = em.em_find(Schedule.class, ticket.getSchedIdx());
            ClientInfo info = em.em_find(ClientInfo.class, ticket.getClientInfoIdx());
            ClientVolume vol = em.em_find(ClientVolume.class, ticket.getClientVolumeIdx());

            // DO NOT ALLOW MULTPLE PARALLEL CDP JOBS FOR ONE POOL
            synchronized(pool)
            {
                api = LogicControl.getApiEntry(info);
                if (api == null || !api.isOnline())
                    throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + info.toString() );

                job = bm.createCDPJob(api, sched, info, vol, file);

                job.setJobState(JobInterface.JOBSTATE.MANUAL_START);

                // CREATE AND RUN JOB MANUALLY
                Main.get_control().getJobManager().addJobEntry(job);

                job.run();
            }
        }
        catch (Exception exception)
        {
            Log.err("Fehler bei CDP job", file.toString(), exception);
        }
        finally
        {
            // REMOVE JOB AND CLOSE API
            Main.get_control().getJobManager().removeJobEntry(job);

            if (api != null)
            {
                try
                {
                    api.close();
                }
                catch (IOException ex)
                {

                }
            }
        }
        return true;
    }

    @Override
    public boolean cdp_call_list( List<CdpEvent> fileList, CdpTicket ticket )
    {
        BackupManager bm = Main.get_control().getBackupManager();
        JobManager jm = Main.get_control().getJobManager();
        AgentApiEntry api = null;
        JobInterface job = null;
        try
        {
            if (ticket == null)
            {
                Log.warn("Ignoriere ungültigen CDP-Call, Ticket fehlt");
                return false;
            }
            if (jm.isPoolBusy(ticket.getPoolIdx()))
            {
                Log.debug("Stoppe CDP-Call da Pool aktiv ist");
                return false;
            }
            for (int i = 0; i < fileList.size(); i++)
            {
                CdpEvent file = fileList.get(i);
                Log.debug("CDP-Call", file.toString());
                if (file.elem == null || file.elem.getPath() == null || file.elem.getPath().isEmpty())
                {
                    Log.warn("Ignoriere leeren CDP-Call",  file.toString());
                    return false;
                }
            }
            StoragePool pool = Main.get_control().getStoragePool(ticket.getPoolIdx());
            GenericEntityManager em = Main.get_control().get_util_em(pool);
            Schedule sched = em.em_find(Schedule.class, ticket.getSchedIdx());
            ClientInfo info = em.em_find(ClientInfo.class, ticket.getClientInfoIdx());
            ClientVolume vol = em.em_find(ClientVolume.class, ticket.getClientVolumeIdx());

            // DO NOT ALLOW MULTPLE PARALLEL CDP JOBS FOR ONE POOL
            synchronized(pool)
            {
                api = LogicControl.getApiEntry(info);
                if (api == null || !api.isOnline())
                    throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + info.toString() );

                job = bm.createCDPJob(api, sched, info, vol, fileList);

                job.setJobState(JobInterface.JOBSTATE.MANUAL_START);
                Main.get_control().getJobManager().addJobEntry(job);

                job.run();            
            }

        }
        catch (Exception exception)
        {
            Log.err("Fehler bei CDP List-Job", exception);
        }
        finally
        {
            // REMOVE JOB AND CLOSE API
            Main.get_control().getJobManager().removeJobEntry(job);

            if (api != null)
            {
                try
                {
                    api.close();
                }
                catch (IOException ex)
                {

                }
            }
        }
        return true;
    }

}
