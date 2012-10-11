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
import de.dimm.vsm.net.interfaces.ServerApi;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    public boolean alert( List<String> reason, String msg )
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
            final StoragePool pool = Main.get_control().getStoragePool(ticket.getPoolIdx());
            GenericEntityManager em = Main.get_control().get_util_em(pool);
            Schedule sched = em.em_find(Schedule.class, ticket.getSchedIdx());
            ClientInfo info = em.em_find(ClientInfo.class, ticket.getClientInfoIdx());
            ClientVolume vol = em.em_find(ClientVolume.class, ticket.getClientVolumeIdx());

            api = LogicControl.getApiEntry(info);
            if (api == null || !api.isOnline())
                throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + info.toString() );

            // DO NOT ALLOW MULTPLE PARALLEL CDP JOBS FOR ONE POOL
            synchronized(pool)
            {
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
    public boolean cdp_call( List<CdpEvent> fileList, CdpTicket ticket )
    {
        BackupManager bm = Main.get_control().getBackupManager();
        try
        {
            if (ticket == null)
            {
                Log.warn("Ignoriere ungültigen CDP-Call, Ticket fehlt");
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

            AgentApiEntry api = LogicControl.getApiEntry(info);
            if (api == null || !api.isOnline())
                throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + info.toString() );

            JobInterface job = bm.createCDPJob(api, sched, info, vol, fileList);

            job.setJobState(JobInterface.JOBSTATE.MANUAL_START);
            Main.get_control().getJobManager().addJobEntry(job);

            job.run();

            Main.get_control().getJobManager().removeJobEntry(job);

            api.close();

        }
        catch (Exception exception)
        {
            Log.err("Fehler bei CDP List-Job", exception);
        }
        return true;
    }

}
