/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.jobs;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Backup.BackupJobInterface;
import de.dimm.vsm.backup.hotfolder.MMImportManager.MMImportJobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.records.HotFolder;
import de.dimm.vsm.records.Schedule;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.swing.Timer;

class TestJob implements JobInterface, ActionListener
{

    JOBSTATE jobState;
    InteractionEntry ie;
    String statusStr;
    int processPercent;
    Timer timer;
    Date jobStartTime;
    User user;

    @Override
    public User getUser()
    {
        return user;
    }



    static int d = 500;
    public TestJob( JOBSTATE jobState, InteractionEntry ie, String statusStr, int processPercent )
    {
        this.jobState = jobState;
        this.ie = ie;
        this.statusStr = statusStr;
        this.processPercent = processPercent;
        timer = new Timer(d, this);
        timer.start();
        d+= 500;
        jobStartTime = new Date();
        user = User.createSystemInternal();
    }

    @Override
    public InteractionEntry getInteractionEntry()
    {
        return ie;
    }

    @Override
    public Date getStartTime()
    {
        return jobStartTime;
    }

    @Override
    public JOBSTATE getJobState()
    {
        return jobState;
    }

    @Override
    public Object getResultData()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getProcessPercent()
    {
        return processPercent;
    }

    @Override
    public String getStatusStr()
    {
        return statusStr;
    }


    @Override
    public void abortJob()
    {
        jobState = jobState == JOBSTATE.FINISHED_ERROR ? JOBSTATE.RUNNING : JOBSTATE.FINISHED_ERROR;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        processPercent++;
        if (processPercent >= 100)
            processPercent = 0;
    }

   

    @Override
    public void setJobState( JOBSTATE jOBSTATE )
    {
        jobState = jOBSTATE;
    }

    

    @Override
    public void run()
    {
        while(jobState == JOBSTATE.RUNNING)
        {
            LogicControl.sleep(100);
        }
    }

    @Override
    public String getProcessPercentDimension()
    {
        return "%";
    }

    @Override
    public String getStatisticStr()
    {
        return "";
    }

   



}
/**
 *
 * @author Administrator
 */
public class JobManager extends WorkerParent
{
    final List<JobEntry> list;
    long actIdx = 0;
    private static final boolean test = false;

    public JobManager()
    {
        super("JobManager");
        
        list = new ArrayList<JobEntry>();

        if (test)
        {
            addJobEntry( new TestJob(JobInterface.JOBSTATE.RUNNING, null, "test 1", 10) );
            addJobEntry( new TestJob(JobInterface.JOBSTATE.FINISHED_OK, null, "test 2", 100) );
            InteractionEntry ie = new InteractionEntry(InteractionEntry.INTERACTION_TYPE.OK,
                    InteractionEntry.SEVERITY.INFO, "Textlang", "Textkurz", 
                    new Date(), 15, InteractionEntry.INTERACTION_ANSWER.OK);

            addJobEntry( new TestJob(JobInterface.JOBSTATE.NEEDS_INTERACTION, ie, "test 3", 50) );
        }

    }

    @Override
    public boolean isVisible()
    {
        return false;
    }


    public final void addJobEntry( final JobInterface job )
    {
        synchronized( list)
        {
            final JobEntry entry = new JobEntry(job);
            entry.setIdx(actIdx++);
            list.add(entry);
            if (job.getJobState() != JOBSTATE.MANUAL_START)
            {
                Thread thr = new Thread(new Runnable() {

                    @Override
                    public void run()
                    {
                        job.run();
                        if (job.getJobState() == JOBSTATE.FINISHED_OK_REMOVE)
                        {
                            list.remove(entry);
                        }
                    }
                }, "JobEntry");
                entry.setThread(thr);
                thr.start();
            }
        }
    }
    public boolean removeJobEntry( JobInterface job )
    {
        synchronized( list)
        {
            for (int i = 0; i < list.size(); i++)
            {
                JobEntry jobEntry = list.get(i);
                if (jobEntry.job == job)
                {
                    return list.remove(jobEntry);
                }
            }
        }
        return false;
    }


    void removeJob(int idx)
    {
        synchronized( list)
        {
            JobEntry entry = list.get(idx);
            if (entry.getJob().getJobState() == JobInterface.JOBSTATE.FINISHED_ERROR || entry.getJob().getJobState() == JobInterface.JOBSTATE.FINISHED_OK)
            {
                list.remove(idx);
            }
        }
    }

    @Override
    public void run()
    {
        while (!isShutdown())
        {
            LogicControl.sleep(500);

            long now = System.currentTimeMillis();

            synchronized( list)
            {
                for (int i = 0; i < list.size(); i++)
                {
                    JobEntry jobEntry = list.get(i);
                    if (jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.NEEDS_INTERACTION)
                    {
                        InteractionEntry ientry = jobEntry.getJob().getInteractionEntry();

                        if (ientry.getTimeout_s() > 0)
                        {
                            if (ientry.getCreated().getTime() + ientry.getTimeout_s()*1000 > now)
                            {
                                ientry.setAnswer( ientry.getDefaultAnswer() );
                                jobEntry.getJob().setJobState(JobInterface.JOBSTATE.RUNNING);
                            }
                        }
                    }
                    else if (jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.ABORTED)
                    {
                        list.remove(jobEntry);
                    }
                }
            }
        }
        finished = true;
    }

    @Override
    public boolean initialize()
    {
        return true;
    }



    List<JobEntry> getJobList()
    {
        return list;
    }

    public JobEntry[] getJobArray(User user)
    {
        synchronized(list)
        {
            if (user == null || user.isAdmin())
            {
                return list.toArray( new JobEntry[0]);
            }
        }
        List<JobEntry> userList = new ArrayList<JobEntry>();
        for (int i = 0; i < list.size(); i++)
        {
            JobEntry jobEntry = list.get(i);
            if (jobEntry.getUser().equals(user))
                userList.add(jobEntry);
        }
        return userList.toArray( new JobEntry[0]);
    }



    public boolean isBackupRunning(Schedule sched)
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof BackupJobInterface)
            {
                BackupJobInterface bi = (BackupJobInterface) jobEntry.getJob();
                if (bi.getActSchedule() != null && bi.getActSchedule().getIdx() == sched.getIdx() &&
                        bi.getActSchedule().getPool().getIdx() == sched.getPool().getIdx())
                {
                    if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                        jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isImportRunning( HotFolder node )
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof MMImportJobInterface)
            {
                MMImportJobInterface bi = (MMImportJobInterface) jobEntry.getJob();
                if (bi.getHotFolder() != null && bi.getHotFolder().getIdx() == node.getIdx())
                {
                    if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                        jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

 

}
