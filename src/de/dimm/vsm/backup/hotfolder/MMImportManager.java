/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.MMapi.JobError;
import de.dimm.vsm.MMapi.JobInfo;
import de.dimm.vsm.MMapi.JobStatus;
import de.dimm.vsm.MMapi.MMAnswer;
import de.dimm.vsm.MMapi.MMapi;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.ParseToken;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.fsengine.JDBCStoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.fsengine.StoragePoolNubHandler;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HotFolder;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

//  sprintf( buff, "NA:\'%.*s\' KB:%.0lf TI:%ld ID:%ld DD:%ld DI:\'%s\'\n",


/**
 *
 * @author Administrator
 */
public class MMImportManager
{
    final MMapi api;
    
    boolean abort = false;
    JobStatus actState = null;
    List<JobError> nokList;
    AgentApiEntry agentApi;
    User user;
    StoragePoolHandler sp_handler;
    HotFolder hotFolder;
    ArchiveJobContext actualContext;
    long fromIdx;
    long tillIdx;
    boolean withOldJobs;

    public MMImportManager( HotFolder hotFolder, long fromIdx, long tillIdx,  boolean withOldJobs )
    {
        this.api = new MMapi(hotFolder.getIp(), 11112);
        
        nokList = new ArrayList<JobError>();
        this.fromIdx = fromIdx;
        this.tillIdx = tillIdx;
        this.hotFolder = hotFolder;
        this.withOldJobs = withOldJobs;

    }

    public static JobInterface createImportJob( HotFolder hotFolder, long fromIdx, long tillIdx, boolean withOldJobs, User user  )
    {
        MMImportManager backup = new MMImportManager(hotFolder, fromIdx, tillIdx, withOldJobs);
        JobInterface job = backup.createJob(user);
        return job;
    }

    void open() throws IOException, SQLException, Exception
    {
        StoragePoolNubHandler nubHandler = Main.get_control().getStorageNubHandler();

        StoragePool pool = nubHandler.getStoragePool( hotFolder.getPoolIdx());

        user = User.createSystemInternal();
        sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( nubHandler, pool, user, /*rdonly*/false);
        if (pool.getStorageNodes().getList( sp_handler.getEm()).isEmpty())
            throw new Exception(Main.Txt("No Storage for pool defined"));


        sp_handler.realizeInFs();

        sp_handler.check_open_transaction();

        preStartStatus = Main.Txt("MediaManager wird kontaktiert")  + ": " + api.getHost() + ":" + api.getPort();
        api.connect();
        if (!api.isConnected())
            throw new IOException(Main.Txt("MediaManager kann nicht angesprochen werden") + ": " +  api.getHost() + ":" + api.getPort() );

        preStartStatus = Main.Txt("VSM-Agent wird kontaktiert")  + ": " + hotFolder.getIp() + ":" + hotFolder.getPort();

        agentApi = openAgentApi(hotFolder);
        if (agentApi == null)
            throw new IOException(Main.Txt("Agent kann nicht angesprochen werden") + ": " + hotFolder.getIp() + ":" + hotFolder.getPort() );

        actualContext = new ArchiveJobContext( null, (ArchiveJobNameGenerator)null, agentApi, sp_handler);
        actualContext.setAbortOnError(true);

    }

    void close() throws SQLException
    {
        if (api != null)
            api.disconnect();

        try
        {
            if (agentApi != null)
                agentApi.close();
        }
        catch (IOException iOException)
        {
        }
        
        sp_handler.commit_transaction();
        sp_handler.close_transaction();
        sp_handler.close_entitymanager();

        if (actualContext != null)
        {
            actualContext.close();
        }
    }


    double jobPercent = 0.0;
    boolean importAllJobs() throws IOException, SQLException
    {
        actualContext.setJobState(JobInterface.JOBSTATE.RUNNING);
        actualContext.setResult(false);

        Log.info("Auslesen der MM-Jobs..." );
        MMAnswer ma = api.sendMM("list_jobs");
        if (ma == null)
            return false;


        List<String> jobs = MMapi.getAnswerList(ma);

        if (withOldJobs)
        {
            Log.info("Auslesen der alten MM-Jobs..." );
            ma = api.sendMM("list_old_jobs __ALLJOBS__");
            if (ma == null)
                return false;

            List<String> old_jobs = MMapi.getAnswerList(ma);
            jobs.addAll(old_jobs);
        }

        Log.info("Import der MM-Jobs beginnt", jobs.size() + " Jobs" );
        long totalSize = 0;
        long actSize = 0;
        List<JobInfo> jobList = new ArrayList<JobInfo>();

        for (int i = 0; i < jobs.size(); i++)
        {
            String job = jobs.get(i);
            JobInfo ji = new JobInfo(job);
            if (ji.getIdx() < fromIdx)
                continue;
            if (tillIdx > 0 && ji.getIdx() > tillIdx )
                continue;

            totalSize += ji.getSize();
            jobList.add(ji);
        }
        if (totalSize == 0)
        {
            Log.err("Keine Daten zum Import gefunden");
            return false;
        }

        // SORT ASCENDING FOR FASTER RESTORE
        Collections.sort(jobList, new Comparator<JobInfo>() {

            @Override
            public int compare( JobInfo o1, JobInfo o2 )
            {
                long diff = o1.getIdx() - o2.getIdx();
                if (diff == 0)
                    return 0;
                return diff < 0 ? -1 : 1;
            }
        });

        int acnt = 0;
        for (int i = 0; i < jobList.size(); i++)
        {
            JobInfo ji = jobList.get(i);

            if (isJobArchived(ji))
            {
                Log.debug("Bereits erfolgreich archiviert", ji.getName() + " (" + ji.getIdx() + ")" );
                continue;
            }

            actualContext.setStatus( ji.getName() + " (" + SizeStr.format(ji.getSize()) + ")");

            try
            {
                importJob(ji);
                acnt++;
            }
            catch (Throwable throwable)
            {
                actualContext.setResult(false);
                actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);
                Log.err("Import wird abgebrochen", throwable);
                actualContext.setStatus(throwable.getMessage());
                return false;
            }
            actSize += ji.getSize();
            jobPercent = actSize * 100.0 / totalSize;
        }

        actualContext.setStatus( "Finished " + acnt + " total ( " + SizeStr.format(actSize) + ") , " + nokList.size() + " error");
        Log.info("Import der MM-Jobs ist abgeschlossen", acnt + " Jobs, " + SizeStr.format(actSize));
        if (!nokList.isEmpty())
        {
            actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);

            Log.err("Beim Import sind Fehler aufgetreten, nicht importiert wurden " + nokList.size() + " Jobs");
            actualContext.setResult(false);
        }
        else
        {
            actualContext.setResult(true);
        }

        return true;

    }
    void debug( String s )
    {
        LogManager.msg_comm( LogManager.LVL_DEBUG, "MMApi: " + s);
    }
    private boolean isJobArchived(JobInfo ji) throws SQLException
    {
        List<ArchiveJob> jobs = sp_handler.createQuery("select T1 from ArchiveJob T1 where T1.sourceType='m' and T1.sourceIdx=" + ji.getIdx(), ArchiveJob.class);

        if (jobs.isEmpty())
            return false;

        for (int i = 0; i < jobs.size(); i++)
        {
            ArchiveJob archiveJob = jobs.get(i);
            if (archiveJob.isOk())
            {
                if (!archiveJob.getStartTime().equals(ji.getCreated()))
                {
                    archiveJob.setStartTime(ji.getCreated());
                    updateJobCreationDate( archiveJob);
                }
                return true;
            }
        }
        return false;
    }
    private void updateJobCreationDate( ArchiveJob job ) throws SQLException
    {
        sp_handler.em_merge(job);
        actualContext.getIndexer().updateJobAsync( job );
        actualContext.getIndexer().flushAsync();
    }

    private String getJobText( JobInfo ji )
    {
        return ji.getName() + " (" + ji.getIdx() + "): " + SizeStr.format(ji.getSize());
    }

    private void importJob(JobInfo ji) throws IOException, Exception, Throwable
    {
        boolean doWait = false;

        String jobPath = hotFolder.getMountPath().getPath() + "/" + ji.getName();

        Log.debug("Restauriere Job", getJobText(ji) );
        actualContext.setStatus( Main.Txt("Restauriere Job") + " " +  getJobText(ji) );

        long now = System.currentTimeMillis();
        agentApi.getApi().create_dir( RemoteFSElem.createDir(jobPath) );

        String cmd = "reload_job " + (doWait?"WAIT":"") + " ID \"" + hotFolder.getMountPath().getPath() + "/" + ji.getName() + "\" KP:1 " + ji.getIdx();

        debug( cmd );
        MMAnswer ma = api.sendMM(cmd);

        if (ma.getCode() != 0)
        {
            Log.err( "MMApi Restore schlug fehl", ma.getTxt());
            return;
        }
        
        ParseToken pt = new ParseToken(ma.getTxt());
        boolean hasJobId = true;
        long jobId = pt.GetLongValue("ID:");
        String idStr = pt.GetString("ID:");

        if (idStr == null || idStr.isEmpty())
            hasJobId = false;

        JobStatus js = waitForJobReady( hasJobId, jobId );

        if (js == null)
        {
            throw new IOException( Main.Txt("Unerwartetes Ende in Job"));
        }
        
        if (js.getState() == JobStatus.JOB_ERROR)
        {
            api.sendMM("abort_task " + js.getTaskId());

            Log.debug("Restore schlug fehl", getJobText(ji) );
            JobError je = new JobError(ji, js);
            nokList.add(je);
            return;
        }
        if (js.getState() == JobStatus.JOB_USER_READY)
        {
            api.sendMM("abort_task " + js.getTaskId());
        }

        

        // OK, JOB IS RESTORED, NOW ARCHIVE IT
        handleImport(hotFolder, ji);

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

    AgentApiEntry openAgentApi(HotFolder hotFolder)
    {
        AgentApiEntry a = null;
        try
        {
            a = LogicControl.getApiEntry(hotFolder.getIp(), hotFolder.getPort());
            if (!a.check_online(/*wthMsg*/true))
            {
                return null;
            }
        }
        catch (Exception unknownHostException)
        {
            return null;
        }
        return a;
    }

    private ArchiveJobContext handleImport(  HotFolder hotFolder, JobInfo ji) throws SQLException, PoolReadOnlyException, PathResolveException, IOException
    {

        Log.debug(Main.Txt("Importiere Job"), getJobText(ji) );
        actualContext.setStatus( Main.Txt("Importiere Job") + " " +  getJobText(ji) );

        String jobPath = hotFolder.getMountPath().getPath() + "/" + ji.getName();
        RemoteFSElem elem = RemoteFSElem.createDir(jobPath);
        
        ArchiveJobNameGenerator nameGen = new DirectoryArchiveJobNameGenerator(hotFolder, elem, agentApi, sp_handler);
        

        // HOTFOLDER CONTEXT RESOLVES THE FILESYSTEM WE ARE USING
        actualContext.setRelPath( jobPath );
        actualContext.setNameGen( nameGen );
        actualContext.setAbortOnError(true);
        actualContext.setResult(true);

        //CREATE ARCHIVEJOB ENTRY
        actualContext.createJob();

        actualContext.getArchiveJob().setName(ji.getName());
        actualContext.getArchiveJob().setSourceType("m");
        actualContext.getArchiveJob().setSourceIdx(ji.getIdx());
        // SET ORIGINAL MM-CREATION DATE
        actualContext.getArchiveJob().setStartTime(ji.getCreated());
        
        long size = actualContext.getStat().getTotalSize();

        try
        {
            Backup.backupRemoteFSElem(actualContext, elem, actualContext.getArchiveJob().getDirectory(), /*recursive*/ true, /*onlyNewer*/ false);
        }
        catch (Throwable throwable)
        {
             Log.err(Main.Txt("Fehler beim Sichern des MM-ArchivJobs"), ji.getName(), throwable );
             actualContext.setStatus(Main.Txt("Fehler beim Sichern des MM-ArchivJobs") + " " + ji.getName() + ": " + throwable.getMessage() );
             actualContext.setResult(false);
        }

        actualContext.getArchiveJob().setEndTime( new Date() );
        actualContext.getArchiveJob().setTotalSize( actualContext.getStat().getTotalSize() - size);

        // SUCCEEDED?
        if (actualContext.getResult() && actualContext.isErrorFree())
        {
            actualContext.getArchiveJob().setOk(true);
            try
            {
                Log.debug(Main.Txt("Importieren beendet, lösche JobDaten"), getJobText(ji) );
                agentApi.getApi().deleteDir(elem, /*recursive*/ true);
            }
            catch (Exception exception)
            {
                 Log.err(Main.Txt("MM-ArchivJob kann nicht gelöscht werden"), exception );
                 HotFolderManager.addHFError(hotFolder, elem, Main.Txt("MM-ArchivJob kann nicht gelöscht werden"));
            }
        }
        else
        {
            throw new IOException(actualContext.getStatus());
        }
        
        // SET JOB RESULT
        actualContext.updateArchiveJob();

        // PUSH JOB TO INDEX
        actualContext.getIndexer().addToIndexAsync( actualContext.getArchiveJob() );

        
        return actualContext;
    }

    JobInterface createJob(User user)
    {
        return new MMImportJobInterface(user);
    }

    String preStartStatus;

    public class MMImportJobInterface implements JobInterface
    {

        boolean finished = false;
        Date startTime;
        User user;

        @Override
        public User getUser()
        {
            return user;
        }



        public MMImportJobInterface(User user)
        {
            this.user = user;
            startTime = new Date();
        }

        @Override
        public Date getStartTime()
        {
            return startTime;
        }

        @Override
        public JOBSTATE getJobState()
        {
            if (actualContext != null)
                return actualContext.getJobState();

            if (abort)
                return JOBSTATE.ABORTED;

            return JOBSTATE.FINISHED_ERROR;
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
                return actualContext.getStatus();

            if (preStartStatus != null)
                return preStartStatus;
            return "?";
        }

        @Override
        public String getProcessPercent()
        {            
            return String.format("%.0f", jobPercent );
        }

        @Override
        public void abortJob()
        {
            abort = true;
            if (actualContext != null)
            {
                actualContext.setAbort( true );
                if (finished)
                {
                    actualContext.setJobState( JOBSTATE.ABORTED );
                }
            }
        }

        @Override
        public void setJobState( JOBSTATE jOBSTATE )
        {
            if (actualContext != null)
            {
                    actualContext.setJobState( jOBSTATE );
            }
        }

        @Override
        public String getProcessPercentDimension()
        {
            return "MB/s";
        }

        @Override
        public void run()
        {
            try
            {
                open();
                boolean ret = importAllJobs();

            }
            catch (Exception exception)
            {
                if (actualContext != null)
                {
                    actualContext.setJobState( JOBSTATE.FINISHED_ERROR );
                    actualContext.setStatus(Main.Txt("Import wurde abgebrochen") + ":" + exception.getMessage());
                }
                else
                {
                    preStartStatus = Main.Txt("Import wurde abgebrochen") + ":" + exception.getMessage();
                }
                Log.err(Main.Txt("Abbruch in Import"), exception );
            }
            finally
            {
                if (actualContext != null)
                {
                    if (abort)
                    {
                        actualContext.setJobState(JOBSTATE.ABORTED);
                    }
                    else
                    {
                        if (actualContext.getResult() && actualContext.getJobState() != JOBSTATE.FINISHED_ERROR)
                        {
                            actualContext.setJobState(JOBSTATE.FINISHED_OK);
                        }
                        else
                        {
                            actualContext.setJobState(JOBSTATE.FINISHED_ERROR);
                        }
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
            if (actualContext != null)
            {
                int qlen = 0;
                if (actualContext.getPoolhandler() instanceof JDBCStoragePoolHandler)
                {
                    JDBCStoragePoolHandler dh = (JDBCStoragePoolHandler)actualContext.getPoolhandler();

                    qlen = dh.getPersistQueueLen();
                }
                String ret = actualContext.getStat().toString();
                if (qlen > 0)
                    ret += " PQ: " + qlen;

                int wqlen = actualContext.getWriteRunner().getQueueLen();
                if (wqlen > 0)
                    ret += " WQ: " + wqlen;

                return ret;
            }
            return "";
        }

        @Override
        public Object getResultData()
        {           
            return nokList;
        }


        public HotFolder getHotFolder()
        {
            return hotFolder;
        }
        @Override
        public void close()
        {

        }


    }

}
