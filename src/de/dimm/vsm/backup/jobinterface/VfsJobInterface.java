/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.backup.jobinterface;

import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.BackupContext;
import de.dimm.vsm.backup.BackupManager;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.MountEntry;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class VfsJobInterface implements JobInterface
{

    BackupManager mgr;
    BackupContext actualContext;
    AgentApiEntry api;
    MountEntry mountEntry;
    List<RemoteFSElem> elems;
    Date start = new Date();
    JobInterface.JOBSTATE js;
    User user;

    public VfsJobInterface( BackupManager mgr, AgentApiEntry api, MountEntry mountEntry, List<RemoteFSElem> elem )
    {
        this.mgr = mgr;
        this.actualContext = null;
        this.api = api;
        this.mountEntry = mountEntry;

        this.elems = elem;
        js = JobInterface.JOBSTATE.MANUAL_START;
        
        if (mountEntry.getUsername() != null && Main.get_control().getUsermanager().existsUser(mountEntry.getUsername()))
        {
            user = Main.get_control().getUsermanager().getUser(mountEntry.getUsername());
        }
    }

    public MountEntry getMountEntry()
    {
        return mountEntry;
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

    @Override
    public void run()
    {
        try
        {
            actualContext = mgr.initVfsbackup(api, mountEntry);
            mgr.handleVfsbackup(actualContext, api, elems, mountEntry);            
            actualContext.setJobState(actualContext.getResult() ? JobInterface.JOBSTATE.FINISHED_OK : JobInterface.JOBSTATE.FINISHED_ERROR);            
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

                    mgr.closeVfsbackup(actualContext);
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
        return user;
    }
}
