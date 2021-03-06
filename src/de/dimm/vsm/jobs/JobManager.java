/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.jobs;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Backup.BackupJobInterface;
import de.dimm.vsm.backup.hotfolder.MMImportManager.MMImportJobInterface;
import de.dimm.vsm.backup.jobinterface.CDPJobInterface;
import de.dimm.vsm.backup.jobinterface.VfsJobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.lifecycle.NodeMigrationManager.MigrationJob;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.CdpTicket;
import de.dimm.vsm.records.AbstractStorageNode;
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
    public String getProcessPercent()
    {
        return Integer.toString(processPercent);
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

    @Override
    public void close()
    {
        
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
        
        list = new ArrayList<>();

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
                Thread workerThr = new Thread(new Runnable()
                {

                    @Override
                    public void run()
                    {
                        job.run();
                    }
                }, job.getClass().getSimpleName());
                entry.setThread(workerThr);
                workerThr.start();
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
                    jobEntry.close();
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
                entry.close();
                list.remove(idx);
            }
        }
    }

    @Override
    public void run()
    {
        is_started = true;
        while (!isShutdown())
        {
            LogicControl.sleep(500);

            long now = System.currentTimeMillis();

            synchronized( list)
            {
                for (int i = 0; i < list.size(); i++)
                {
                    JobEntry jobEntry = list.get(i);
                    try {
                        if (jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.NEEDS_INTERACTION) {
                            InteractionEntry ientry = jobEntry.getJob().getInteractionEntry();
                            
                            if (ientry.getTimeout_s() > 0) {
                                if (ientry.getCreated().getTime() + ientry.getTimeout_s() * 1000 > now) {
                                    ientry.setAnswer(ientry.getDefaultAnswer());
                                    jobEntry.getJob().setJobState(JobInterface.JOBSTATE.RUNNING);
                                }
                            }
                        } else if (jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.ABORTED || jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.FINISHED_OK_REMOVE ) {
                            // Automatic Remove
                            jobEntry.close();
                            list.remove(jobEntry);
                            i--;
                        } else if (jobEntry.getJob().getJobState() == JobInterface.JOBSTATE.FINISHED_OK ) {
                            // Automatic Remove after 1d
                            long kickTimeout = Main.get_int_prop(GeneralPreferences.KICK_JOB_TIMEOUT_S, 86400);
                            if (kickTimeout > 0 && jobEntry.getStarted() != null && (now - jobEntry.getStarted().getTime()) > kickTimeout*1000) {
                                jobEntry.close();
                                list.remove(jobEntry);
                                i--;
                            }
                        }
                        
                    } catch (Exception e) {
                        Log.err("Abbruch in Run", e);
                    }
                }
            }
        }
        Log.debug("JobManager is shutting down...");
        
        // We are in Shutdown -> Stop all pending jobs
        for (JobEntry entry: list)
        {
            Log.debug("Aborting Job on shutdown", entry.toString());
            entry.getJob().abortJob();            
        }
        
        boolean allStopped;
        int maxWaitS=10;
        while (maxWaitS > 0)
        {
            allStopped = true;
            for (JobEntry entry: list)
            {
                if (entry.getJob().getJobState() == JobInterface.JOBSTATE.ABORTED) {
                    entry.close();
                    list.remove(entry);
                    allStopped = false;
                    break;
                }
                if (entry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                    entry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                {
                    allStopped = false;
                    break;
                }
            }
            if (allStopped)
                break;
            
            maxWaitS--;
            LogicControl.sleep(1000);
        }
        Log.debug("JobManager is shutdown");
        finished = true;
    }

    @Override
    public boolean initialize()
    {
        return true;
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
        List<JobEntry> userList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++)
        {
            JobEntry jobEntry = list.get(i);

            if (jobEntry.getUser() != null && jobEntry.getUser().equals(user))
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

    public boolean isMigrationJobBusy( AbstractStorageNode node )
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof MigrationJob)
            {
                MigrationJob mi = (MigrationJob) jobEntry.getJob();

                if (mi.getSrc() != null && mi.getSrc().getIdx() == node.getIdx())
                {
                    if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                        jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                    {
                        return true;
                    }
                }
                if (mi.getTargets() != null)
                {
                    for (int j = 0; j < mi.getTargets().size(); j++)
                    {
                        if (mi.getTargets().get(j).getIdx() == node.getIdx())
                        {
                            if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                                jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                            {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
    public boolean isPoolBusyBackup(long poolIdx)
    {
        BackupJobInterface bi = getPoolBusyBackup(poolIdx);
        return (bi != null);
    }
    public boolean isPoolBusyBackup(CdpTicket ticket)
    {
        BackupJobInterface bi = getPoolBusyBackup(ticket);
        return (bi != null);
    }
    
    public BackupJobInterface getPoolBusyBackup(long poolIdx)
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof BackupJobInterface)
            {
                BackupJobInterface bi = (BackupJobInterface) jobEntry.getJob();
                if (bi.getActSchedule() != null && bi.getActSchedule().getPool().getIdx() == poolIdx)
                {
                    if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                        jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                    {
                        return bi;
                    }
                }
            }
        }
        return null;
    }

    public BackupJobInterface getPoolBusyBackup(CdpTicket ticket)
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof BackupJobInterface)
            {
                BackupJobInterface bi = (BackupJobInterface) jobEntry.getJob();
                if (bi.getActSchedule() != null && bi.getActSchedule().getPool().getIdx() == ticket.getPoolIdx())
                {
                    if (bi.getActClientInfo() != null && bi.getActClientInfo().getIdx() == ticket.getClientInfoIdx())
                    {
                        if (jobEntry.getJobStatus() != JOBSTATE.FINISHED_OK &&
                            jobEntry.getJobStatus() != JOBSTATE.FINISHED_ERROR)
                        {
                            return bi;
                        }
                    }
                }
            }
        }
        return null;
    }
    
    public boolean isPoolBusyCDP(long poolIdx)
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof CDPJobInterface)
            {
                CDPJobInterface bi = (CDPJobInterface) jobEntry.getJob();
                if (bi.getSched() != null && bi.getSched().getPool().getIdx() == poolIdx)
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
    public boolean isPoolBusyVfs(long poolIdx)
    {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof VfsJobInterface)
            {
                VfsJobInterface bi = (VfsJobInterface) jobEntry.getJob();
                if (bi.getMountEntry().getPool().getIdx() == poolIdx)
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
    
    public <T> List<T> getJobList(Class<T> clazz) {
        
        List<T> foundJobs = new ArrayList<>();
                
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob().getClass().getSimpleName().equals(clazz.getSimpleName())) {               
               foundJobs.add((T)jobEntry.getJob());                       
            }
        }
        return foundJobs;
    }

    public void abortOlderTasks( Schedule sched, BackupJobInterface job) {
        JobEntry[] jobs = getJobArray(null);
        for (int i = 0; i < jobs.length; i++)
        {
            JobEntry jobEntry = jobs[i];
            if (jobEntry.getJob() instanceof BackupJobInterface)
            {
                // Stop all other ready Jobs 
                BackupJobInterface bi = (BackupJobInterface) jobEntry.getJob();
                if (bi != job && 
                        bi.getActSchedule() != null && 
                        bi.getActSchedule().getIdx() == sched.getIdx() &&
                        bi.getActSchedule().getPool().getIdx() == sched.getPool().getIdx())
                {
                    if (jobEntry.getJobStatus() == JOBSTATE.FINISHED_OK || jobEntry.getJobStatus() == JOBSTATE.FINISHED_ERROR)
                    {
                        bi.abortJob();
                    }
                }
            }
        }    
    }
    public int getActJobsRunnung() {
        int cnt = 0;
        JobEntry[] jobs = getJobArray(null);
        for (JobEntry job: jobs) {
            if (job.getJobStatus() == JobInterface.JOBSTATE.RUNNING)
                cnt++;
        }
        return cnt;
    }

}
