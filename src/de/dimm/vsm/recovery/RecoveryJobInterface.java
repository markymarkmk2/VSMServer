/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.recovery;

import de.dimm.vsm.auth.User;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import java.util.Date;

/**
 *
 * @author Administrator
 */
public class RecoveryJobInterface implements JobInterface
{

    RecoveryManager mgr;
    private Date startDate;
    JobInterface.JOBSTATE jobState;
    User user;

    public RecoveryJobInterface( User user, RecoveryManager mgr )
    {
        this.mgr = mgr;
        this.user = user;
    }

    @Override
    public JobInterface.JOBSTATE getJobState()
    {
        if (mgr.isAbort())
            jobState = JOBSTATE.ABORTED;
        return jobState;
    }

    @Override
    public void setJobState( JobInterface.JOBSTATE jOBSTATE )
    {
        jobState = jOBSTATE;
        if (jobState == JOBSTATE.ABORTED)
            mgr.abort();
    }

    @Override
    public InteractionEntry getInteractionEntry()
    {
        return null;
    }

    @Override
    public String getStatusStr()
    {
        return mgr.getStatusStr();
    }

    @Override
    public String getStatisticStr()
    {
        return mgr.getStatisticStr();
    }

    @Override
    public Date getStartTime()
    {
        return startDate;
    }

    @Override
    public Object getResultData()
    {
        return "";
    }

    @Override
    public String getProcessPercent()
    {
        return mgr.getProcessPercent();
    }

    @Override
    public String getProcessPercentDimension()
    {
        return "%";
    }

    @Override
    public void abortJob()
    {
        mgr.abort();
    }

    @Override
    public void run()
    {
        jobState = JOBSTATE.RUNNING;
        mgr.scan();
        if (jobState == JOBSTATE.RUNNING)
            jobState = JOBSTATE.FINISHED_OK;
    }

    @Override
    public User getUser()
    {
        return user;
    }

    @Override
    public void close()
    {
        mgr.close();
    }
}