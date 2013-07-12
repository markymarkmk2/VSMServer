/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.backup.jobinterface;

import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.AgentApiEntry;
import de.dimm.vsm.backup.BackupContext;
import de.dimm.vsm.backup.BackupManager;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.CdpEvent;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Schedule;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class CDPJobInterface implements JobInterface
{

    BackupManager mgr;
    BackupContext actualContext;
    AgentApiEntry api;
    Schedule sched;
    ClientInfo info;
    ClientVolume volume;
    List<CdpEvent> evList;
    Date start = new Date();
    JobInterface.JOBSTATE js;

    public CDPJobInterface( BackupManager mgr, AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, CdpEvent ev )
    {
        this.mgr = mgr;
        this.actualContext = null;
        this.api = api;
        this.sched = sched;
        this.info = info;
        this.volume = volume;
        this.evList = new ArrayList<>();
        this.evList.add(ev);
        js = JobInterface.JOBSTATE.MANUAL_START;
    }

    public CDPJobInterface( BackupManager mgr, AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, List<CdpEvent> evList )
    {
        this.mgr = mgr;
        this.actualContext = null;
        this.api = api;
        this.sched = sched;
        this.info = info;
        this.volume = volume;
        this.evList = new ArrayList<>();
        this.evList.addAll(evList);
        js = JobInterface.JOBSTATE.MANUAL_START;
    }

    @Override
    public JobInterface.JOBSTATE getJobState()
    {
        if (actualContext != null)
        {
            return actualContext.getJobState();
        }
        return js;

    }

    @Override
    public void setJobState( JobInterface.JOBSTATE jOBSTATE )
    {
        if (actualContext != null)
        {
            actualContext.setJobState(jOBSTATE);
        }
        else
        {
            js = jOBSTATE;
        }
    }

    @Override
    public InteractionEntry getInteractionEntry()
    {
        return null;
    }

    @Override
    public String getStatusStr()
    {
        if (actualContext != null)
        {
            return actualContext.getStatus();
        }
        return "";
    }

    @Override
    public String getStatisticStr()
    {
        if (actualContext != null)
        {
            return actualContext.getStat().toString();
        }
        return "";
    }

    @Override
    public Date getStartTime()
    {
        return start;
    }

    @Override
    public Object getResultData()
    {
        return null;
    }

    @Override
    public String getProcessPercent()
    {
        if (actualContext != null)
        {
            return  actualContext.getStat().getSpeedPerSec();
        }
        return "";
    }

    @Override
    public String getProcessPercentDimension()
    {
        if (actualContext != null)
                return  actualContext.getStat().getSpeedDim();
        return "";
    }

    @Override
    public void abortJob()
    {
        if (actualContext != null)
        {
            actualContext.setAbort(true);
        }
    }

    public Schedule getSched()
    {
        return sched;
    }

    @Override
    public void run()
    {
        try
        {
            actualContext = mgr.initCDPbackup(api, sched, info, volume);
            mgr.handleCDPbackup(actualContext, api, evList, sched);
        }
        catch (Throwable ex)
        {
            if (actualContext != null)
            {
                actualContext.setJobState(JobInterface.JOBSTATE.ABORTED);
            }
        }
        finally
        {
            if (actualContext != null)
            {
                try
                {

                    mgr.closeCDPbackup(actualContext);
                }
                catch (Exception exception)
                {
                    Log.err("Fehler beim Schließen von CDP", exception);
                }
            }
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public User getUser()
    {
        return null;
    }
}