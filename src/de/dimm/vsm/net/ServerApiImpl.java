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
        try
        {
            Log.debug("CDP-Call", file.toString());
            if (file.elem == null || file.elem.getPath() == null || file.elem.getPath().isEmpty())
            {
                Log.warn("Ignoriere leeren CDP-Call",  file.toString());
                return false;
            }
            StoragePool pool = Main.get_control().getStoragePool(ticket.getPoolIdx());
            GenericEntityManager em = Main.get_control().get_util_em(pool);
            Schedule sched = em.em_find(Schedule.class, ticket.getSchedIdx());
            ClientInfo info = em.em_find(ClientInfo.class, ticket.getClientInfoIdx());
            ClientVolume vol = em.em_find(ClientVolume.class, ticket.getClientVolumeIdx());

            AgentApiEntry api = LogicControl.getApiEntry(info);
            if (api == null || !api.isOnline())
                throw new IOException(Main.Txt("Agent kann nicht kontaktiert werden") + ": " + info.toString() );

            JobInterface job = bm.createCDPJob(api, sched, info, vol, file);

            job.setJobState(JobInterface.JOBSTATE.MANUAL_START);
            Main.get_control().getJobManager().addJobEntry(job);

            job.run();
            
            Main.get_control().getJobManager().removeJobEntry(job);

            api.close();

        }
        catch (Exception exception)
        {
            Log.err("Fehler bei CDP job", file.toString(), exception);
        }
        return true;
    }

}
