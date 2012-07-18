/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.MMapi.JobStatus;
import de.dimm.vsm.MMapi.MMAnswer;
import de.dimm.vsm.MMapi.MMapi;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.ParseToken;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.AgentApiEntry;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.HotFolder;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

//  sprintf( buff, "NA:\'%.*s\' KB:%.0lf TI:%ld ID:%ld DD:%ld DI:\'%s\'\n",

/**
 *
 * @author Administrator
 */
public class MMArchivManager
{
    final MMapi api;
    
    boolean abort = false;
    JobStatus actState = null;
        
   
    HotFolder hotFolder;
    RemoteFSElem elem;
    JOBSTATE jobState;
    String status;
    boolean result;

    public void setJobState( JOBSTATE jobState )
    {
        this.jobState = jobState;
    }

    public void setResult( boolean result )
    {
        this.result = result;
    }

    public boolean isResult()
    {
        return result;
    }


    public String getStatus()
    {
        return status;
    }

    public void setStatus( String status )
    {
        this.status = status;
    }

    
    public MMArchivManager( HotFolder hotFolder, RemoteFSElem elem )
    {
        String ip = hotFolder.getIp();
        if (hotFolder.getMmIP() != null && !hotFolder.getMmIP().isEmpty())
            ip = hotFolder.getMmIP();

        this.api = new MMapi(ip, 11112);
        
        this.hotFolder = hotFolder;
        this.elem = elem;
        jobState = JOBSTATE.SLEEPING;

    }

    public static JobInterface createArchivJob( HotFolder hotFolder, RemoteFSElem elem, User user )
    {
        MMArchivManager mm = new MMArchivManager(hotFolder, elem);
        JobInterface job = mm.createJob(user);
        return job;
    }

    void open() throws IOException, SQLException, Exception
    {
        status = Main.Txt("MediaManager wird kontaktiert")  + ": " + api.getHost() + ":" + api.getPort();
        api.connect();
        if (!api.isConnected())
            throw new IOException(Main.Txt("MediaManager kann nicht angesprochen werden") + ": " +  api.getHost() + ":" + api.getPort() );

        status = Main.Txt("VSM-Agent wird kontaktiert")  + ": " + hotFolder.getIp() + ":" + hotFolder.getPort();
       
    }

    void close() throws SQLException
    {
        if (api != null)
            api.disconnect();

    }

    void archiveJob()
    {
        String jobName = elem.getName();

        try
        {
            if (archiveJob(jobName))
            {
                setJobState(JobInterface.JOBSTATE.FINISHED_OK);
                setResult(true);
            }
            else
            {
                setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);
                setResult(false);
            }
        }
        catch (Throwable throwable)
        {
            setStatus( "Abbruch" + ": " + throwable.getMessage());
            setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);
            setResult(false);
        }
    }

    
    
    void debug( String s )
    {
        LogManager.msg_comm( LogManager.LVL_DEBUG, "MMApi: " + s);
    }

    long startTS;

    // CONVERT NAMESPACE TO MM-JOBNAMES
    public static String getMMJobName( String jobName )
    {
        String s = jobName.replace(' ', '_');
        if (s.length() >= 40)
            s = s.substring(0, 40);
        
        return s;
    }

    private String getArchivePath( String jobName )
    {
        String path = hotFolder.getMountPath().getPath();
        if (hotFolder.getMmMountPath() != null && !hotFolder.getMmMountPath().isEmpty())
        {
            path = hotFolder.getMmMountPath();
        }
        return path + "/" + jobName;
    }
    
    private boolean archiveJob(String jobName) throws IOException, Exception, Throwable
    {
        jobState = JOBSTATE.RUNNING;
        
        startTS = System.currentTimeMillis();
        
        Log.debug("Archiviere Job", jobName );

        String mediaType = hotFolder.getMmMediaType();
        String verify = hotFolder.isMmVerify() ? "1" :"0";
        String cmd = "archive_job " + getMMJobName(jobName) + " MT:" + mediaType + " VY:" + verify  + " \"" + getArchivePath(jobName) + "\" ";

        debug( cmd );
        MMAnswer ma = api.sendMM(cmd);

        if (ma.getCode() != 0)
        {
            Log.err( "MMApi Archivierung schlug fehl", ma.getTxt());
            return false;
        }
        
        ParseToken pt = new ParseToken(ma.getTxt());
        boolean hasJobId = true;
        long jobId = pt.GetLongValue("ID:");
        if (jobId <= 0)
            hasJobId = false;

        JobStatus js = waitForJobReady( hasJobId, jobId );

        if (js == null)
        {
            throw new IOException( Main.Txt("Unerwartetes Ende in Job"));
        }

        api.sendMM("abort_task " + js.getTaskId());

        // BUGFIX FOR PREMATURLY ABORTED JOBS
        if (js.getStatus().contains("abgebrochen"))
        {
            js.setState( JobStatus.JOB_ERROR );
        }

        if (js.getState() == JobStatus.JOB_USER_READY || js.getState() == JobStatus.JOB_READY)
        {
            return true;
        }
        if (js.getState() == JobStatus.JOB_ERROR)
        {
            Log.debug("Archivierung schlug fehl", jobName +  ": " + js.getStatus() );
        }

        return false;

    }

    JobStatus waitForJobReady( boolean hasJobId, long jobId ) throws IOException
    {
        while(!abort)
        {
            sleep(1000);

            JobStatus js = api.getJobState( hasJobId, jobId );
            if (js == null)
            {
                return null;
            }
            synchronized(api)
            {
                actState = js;
            }
            if (js.getState() == JobStatus.JOB_BUSY)
                continue;
            if (js.getState() == JobStatus.JOB_SLEEPING)
                continue;
            if (js.getState() == JobStatus.JOB_WAITING)
                continue;

            if (js.getState() == JobStatus.JOB_USER_READY || js.getState() == JobStatus.JOB_READY ||js.getState() == JobStatus.JOB_ERROR)
                return js;
        }
        
        return null;
    }

    void sleep( int ms )
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (InterruptedException interruptedException)
        {
        }
    }
//
//    AgentApiEntry openAgentApi(HotFolder hotFolder)
//    {
//        AgentApiEntry a = null;
//        try
//        {
//            a = Main.get_control().getApiEntry(hotFolder.getIp(), hotFolder.getPort());
//            if (!a.check_online())
//            {
//                return null;
//            }
//        }
//        catch (Exception unknownHostException)
//        {
//            return null;
//        }
//        return a;
//    }

    JobInterface createJob(User user)
    {

        return new MMArchivJobInterface(user);
    }

    

    public class MMArchivJobInterface implements JobInterface
    {

        boolean finished = false;
        Date startTime;
        User user;

        public MMArchivJobInterface(User user)
        {
            this.user = user;
            startTime = new Date();
        }

        @Override
        public User getUser()
        {
            return user;
        }


        @Override
        public Date getStartTime()
        {
            return startTime;
        }

        @Override
        public JOBSTATE getJobState()
        {
            if (abort)
                return JOBSTATE.ABORTED;

            return jobState;

        }

        @Override
        public Object getResultData()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }


        @Override
        public InteractionEntry getInteractionEntry()
        {
            return null;
        }

        @Override
        public String getStatusStr()
        {
            return getStatus();          
        }

        @Override
        public int getProcessPercent()
        {
            if (getJobState() == JOBSTATE.FINISHED_OK)
                return 100;
            if (getJobState() == JOBSTATE.FINISHED_ERROR)
                return 0;

            int seconds = (int)((System.currentTimeMillis() - startTS) / 1000);
            return seconds;
        }

        @Override
        public void abortJob()
        {
            abort = true;
            setJobState( JOBSTATE.ABORTED );
        }


        @Override
        public String getProcessPercentDimension()
        {
            if (getJobState() == JOBSTATE.FINISHED_OK || getJobState() == JOBSTATE.FINISHED_ERROR)
                return "%";
            
            return "s";
        }

        @Override
        public void run()
        {
            try
            {
                open();
                archiveJob();

            }
            catch (Exception exception)
            {
                setJobState( JOBSTATE.FINISHED_ERROR );
                if (status != null)
                {                    
                    setStatus(Main.Txt("Archivierung wurde abgebrochen") + ":" + status + ": " + exception.getMessage());
                }
                else
                {
                    status = Main.Txt("Archivierung wurde abgebrochen") + ":" + exception.getMessage();
                }
                Log.err(Main.Txt("Archivierung wurde abgebrochen"), exception );
            }
            finally
            {
                if (abort)
                {
                    setJobState(JOBSTATE.ABORTED);
                }
                else
                {
                    if (isResult() && getJobState() != JOBSTATE.FINISHED_ERROR)
                    {
                        setJobState(JOBSTATE.FINISHED_OK);
                    }
                    else
                    {
                        setJobState(JOBSTATE.FINISHED_ERROR);
                    }
                }
                
                try
                {
                    close();
                }
                catch (Exception sQLException)
                {
                    Log.err(Main.Txt("Fehler beim Schließen des Contextes"), sQLException);
                }
            }
            finished = true;
        }

        @Override
        public String getStatisticStr()
        {
            if (actState != null)
            {
                String ret = actState.getStatus();
                return ret;
            }
            return "";
        }

        public HotFolder getHotFolder()
        {
            return hotFolder;
        }

        @Override
        public void setJobState( JOBSTATE jOBSTATE )
        {
            jobState = jOBSTATE;
        }
        @Override
        public void close()
        {

        }

       
    }

}
