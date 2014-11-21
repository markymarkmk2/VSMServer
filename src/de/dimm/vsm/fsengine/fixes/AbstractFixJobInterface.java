/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.fixes;

import de.dimm.vsm.auth.User;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import java.sql.SQLException;
import java.util.Date;

/**
 *
 * @author Administrator
 */
public class AbstractFixJobInterface implements JobInterface 
{

    JOBSTATE state;
    IFix fix;
    Date start;
    User user;
    

    public AbstractFixJobInterface( User _user, IFix fix )
    {
        user = _user;
        this.fix = fix;
        start = new Date();
    }
    
    
    @Override
    public JOBSTATE getJobState()
    {
        return state;
    }

    @Override
    public void setJobState( JOBSTATE jOBSTATE )
    {
        state = jOBSTATE;
    }

    @Override
    public InteractionEntry getInteractionEntry()
    {
        return null;
    }

    @Override
    public String getStatusStr()
    {
        return fix.getStatusStr();
    }

    @Override
    public String getStatisticStr()
    {
        return fix.getStatisticStr();
    }

    @Override
    public Date getStartTime()
    {
        return start;
    }

    @Override
    public Object getResultData()
    {
        return fix.getResultData();
    }

    @Override
    public String getProcessPercent()
    {
        return fix.getProcessPercent();
    }

    @Override
    public String getProcessPercentDimension()
    {
        return fix.getProcessPercentDimension();
    }

    @Override
    public void abortJob()
    {
        fix.abortJob();
        state = JOBSTATE.ABORTED;
    }

    @Override
    public void run()
    {
        state = JOBSTATE.RUNNING;
        try
        {
            boolean result = fix.runFix();
            state = fix.isAborted() ? JOBSTATE.ABORTED : (result ? JOBSTATE.FINISHED_OK : JOBSTATE.FINISHED_ERROR);
        }
        catch (Exception sQLException)
        {
            state = JOBSTATE.ABORTED;
        }
    }

    @Override
    public User getUser()
    {
        return user;
    }

    @Override
    public void close()
    {
        fix.close();
    }
    
}
