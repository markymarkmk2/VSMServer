/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.VariableResolver;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Backup.BackupJobInterface;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.fsengine.StoragePoolNubHandler;
import de.dimm.vsm.jobs.JobEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobManager;
import de.dimm.vsm.mail.NotificationEntry;
import de.dimm.vsm.net.CdpEvent;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.BackupJobResult;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.Job;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

class ScheduleStart
{
    long nextStart;
    Schedule sched;
    boolean started;

    public ScheduleStart( long nextStart, Schedule sched )
    {
        this.nextStart = nextStart;
        this.sched = sched;
        started = false;
    }

    @Override
    public String toString()
    {
        return sched.getName() + " " + Main.getDateString(nextStart);
    }

    
}

/**
 *
 * @author Administrator
 */
public class BackupManager extends WorkerParent
{

    final List<ScheduleStart> startList;
    Thread runner;

    public static final String BA_AGENT_OFFLINE = "BA_AGENT_OFFLINE";
    public static final String BA_ERROR = "BA_ERROR";
    public static final String BA_ABORT = "BA_ABORT";
    public static final String BA_FILE_ERROR = "BA_FILE_ERROR";
    public static final String BA_SNAPSHOT_FAILED = "BA_SNAPSHOT_FAILED";

   
    public static final String BA_OKAY = "BA_OKAY";
    public static final String BA_NOT_OKAY = "BA_NOT_OKAY";
    public static final String BA_VOLUME_OKAY = "BA_VOLUME_OKAY";
    public static final String BA_CLIENT_OKAY = "BA_CLIENT_OKAY";

    public static final String BA_GROUP_ERROR = "BA_GROUP_ERROR";



    public BackupManager()
    {
        super("BackupManager");

        startList = new ArrayList<ScheduleStart>();


        Main.addNotification( new NotificationEntry(BA_AGENT_OFFLINE,
                "Agent ist offline", "Der Agent $AGENT für Backup $NAME kann nicht kontaktiert werden", NotificationEntry.Level.WARNING, true));


        Main.addNotification( new NotificationEntry(BA_ERROR,
                "Fehler beim Sichern in Backup $NAME", "In Volume $VOLUME bei Agent $AGENT im Backup $NAME traten Fehler auf", NotificationEntry.Level.ERROR, false));
        Main.addNotification( new NotificationEntry(BA_FILE_ERROR,
                "Fehler beim Sichern in Backup $NAME", "In Volume $VOLUME bei Agent $AGENT im Backup $NAME können folgende Einträge nicht gesichert werden", NotificationEntry.Level.WARNING, false));

        Main.addNotification( new NotificationEntry(BA_ABORT,
                "Abbruch beim Sichern in Backup $NAME", "Die Sicherung von Volume $VOLUME bei Agent $AGENT im Backup $NAME wurde abgebrochen", NotificationEntry.Level.ERROR, false));


        Main.addNotification( new NotificationEntry(BA_VOLUME_OKAY,
                "Volume $VOLUME beendet", "Volume $VOLUME auf $AGENT bei Backup $NAME wurde erfolgreich gesichert", NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_CLIENT_OKAY,
                "Client $AGENT beendet", "Client $AGENT bei Backup $NAME wurde erfolgreich gesichert", NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_OKAY,
                "Backup $NAME erfolgreich beendet", "", NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_NOT_OKAY,
                "Backup $NAME nicht erfolgreich beendet", "", NotificationEntry.Level.ERROR, false));

        Main.addNotification( new NotificationEntry(BA_SNAPSHOT_FAILED,
                "Fehler beim Erzeugen des Snapshots in Backup $NAME", "In Volume $VOLUME bei Agent $AGENT im Backup $NAME konnte kein Snapshot erzeugt werden", NotificationEntry.Level.WARNING, false));

        Main.addNotification( new NotificationEntry(BA_GROUP_ERROR,
                "Alle Fehler in Backup", "BA_SNAPSHOT_FAILED,BA_AGENT_OFFLINE,BA_ERROR,BA_FILE_ERROR,BA_ABORT,BA_NOT_OKAY", NotificationEntry.Level.GROUP, false));
    }

    @Override
    public boolean initialize()
    {
        updateStartList();
        return true;
    }

    // THIS HAS TO BE CALLED AT END OF EVERY BACKUP AND AFTER PARAMETER
    public void updateStartList()
    {
        List<StoragePool> pools = LogicControl.getStorageNubHandler().listStoragePools();

        synchronized(startList)
        {
            startList.clear();
            
            for (int p = 0; p < pools.size(); p++)
            {
                StoragePool storagePool = pools.get(p);

                GenericEntityManager em = Main.get_control().get_util_em(storagePool);
                long now = System.currentTimeMillis();



                List<Schedule> list = null;
                try
                {
                    list = em.createQuery("select T1 from Schedule T1", Schedule.class);
                }
                catch (SQLException sQLException)
                {
                    Log.err("fehler beim Ermitteln der Zeitpläne", sQLException);
                    return;
                }
                for (int i = 0; i < list.size(); i++)
                {
                    Schedule schedule = list.get(i);
                    if (schedule.getDisabled())
                        continue;


                    try
                    {
                        ScheduleStart start = calcNextStart(schedule, now);

                        if (start != null)
                        {
                            startList.add(start);

                        }
                    }
                    catch (Exception e)
                    {
                        Log.err("Fehler beim Ermitteln der Startzeit", schedule.toString(), e );
                    }
                }
            }
        }
    }

    static ScheduleStart calcNextStart( Schedule schedule, long now )
    {

        GregorianCalendar baseCal = new GregorianCalendar();

        ScheduleStart start = null;

        // 1. CYCLE MODE
        if (schedule.getIsCycle())
        {

            baseCal.setTime(schedule.getScheduleStart());

            // SPEED UP: IF WE ARE ON DAY BOUNDARY, THEN WE CAN START ON THIS DAY MINUS ONE DAY (OFFSET BAY BE LARGER THAN CYCLE)
            if ( baseCal.getTimeInMillis() < now  && 86400*1000 % schedule.getCycleLengthMs() == 0)
            {
                int h = baseCal.get(GregorianCalendar.HOUR_OF_DAY);
                int m = baseCal.get(GregorianCalendar.MINUTE);
                int s = baseCal.get(GregorianCalendar.SECOND);
                baseCal.setTimeInMillis( System.currentTimeMillis() );
                baseCal.set(GregorianCalendar.HOUR_OF_DAY, h);
                baseCal.set(GregorianCalendar.MINUTE, m);
                baseCal.set(GregorianCalendar.SECOND, s);
                baseCal.set(GregorianCalendar.MILLISECOND, 0);
                baseCal.add(GregorianCalendar.DAY_OF_YEAR, -1);
            }


            long startTime = baseCal.getTimeInMillis();

            int n = 0;

            // FIND THE FIRST CYCLEENTRY IN THE FUTURE
            for (n = 0;;n++)
            {
                long check = startTime +  n * schedule.getCycleLengthMs();

                ScheduleStart actS = new ScheduleStart(check, schedule);

                if (check > now)
                {
                    if (start == null || start.nextStart > check)
                        start = actS;
                    break;
                }
            }
        }
        else
        {
            List<Job> jobList = null;

            if (schedule.getJobs().isRealized())
                jobList = schedule.getJobs().getList();
            else
            {
                GenericEntityManager em = Main.get_control().get_util_em(schedule.getPool());
                jobList = schedule.getJobs().getList(em);
            }

            for (int i = 0; i < jobList.size(); i++)
            {
                Job job = jobList.get(i);
                if (job.getDisabled())
                    continue;

                if (schedule.getScheduleStart() == null)
                {
                    Log.err("Überspringe wegen ungültiger Startzeit", ": " + schedule.toString());
                    continue;
                }
                if (schedule.getCycleLengthMs() > 86400000 && job.getDayNumber() < 0)
                {
                    Log.err("Überspringe wegen ungültigem Starttag", ": " + schedule.toString());
                    continue;
                }
                if (schedule.getCycleLengthMs() <= 0)
                {
                    Log.err("Überspringe wegen ungültiger Zyklusdauer", ": " + schedule.toString());
                    continue;
                }

                baseCal.setTime(schedule.getScheduleStart());
                ScheduleStart baseTimeStart = new ScheduleStart( baseCal.getTimeInMillis(), schedule);


                int n = 0;

                // FIND THE FIRST CYCLEENTRY IN THE FUTURE
                for (n = 0;;n++)
                {
                    int offsetStartS = (int)(job.getOffsetStartMs() / 1000 );
                    int days = job.getDayNumber();
                    if (days < 0)
                        days = 0;

                    // ADD CYCLE
                    long startTime = baseCal.getTimeInMillis();
                    baseCal.setTimeInMillis(startTime +  n * schedule.getCycleLengthMs());

                    baseCal.set(GregorianCalendar.HOUR_OF_DAY, offsetStartS / 3600);
                    baseCal.set(GregorianCalendar.MINUTE, (offsetStartS % 3600) / 60);
                    baseCal.set(GregorianCalendar.SECOND, offsetStartS % 60);
                    baseCal.set(GregorianCalendar.MILLISECOND, 0);
                    baseCal.add(GregorianCalendar.DAY_OF_YEAR, days);


                    long check = baseCal.getTimeInMillis();
                    ScheduleStart actS = new ScheduleStart(check, schedule);

                    if (check > now)
                    {
                        if (start == null || start.nextStart > check)
                            start = actS;
                        break;
                    }
                }
            }
        }

        return start;
    }

    @Override
    public boolean isPersistentState()
    {
        return true;
    }



    @Override
    public void run()
    {
        long lastCheck = System.currentTimeMillis();
        
        long startWindowS = Main.get_int_prop(GeneralPreferences.BA_START_WINDOW_S, Backup.DEFAULT_START_WINDOW_S);

        setStatusTxt("");
        while(!isShutdown())
        {
            LogicControl.sleep(1000);
            setStatusTxt("");

            if (isPaused())
                continue;

            // CHECK FOR NEW START EVERY Ns
            long now = System.currentTimeMillis();
            if (now - lastCheck < 5*1000)
                continue;

            setStatusTxt(Main.Txt("Prüfe Zyklusstarts"));
            lastCheck = now;

            synchronized(startList)
            {
                for (int i = 0; i < startList.size(); i++)
                {
                    ScheduleStart scheduleStart = startList.get(i);

                    // ARE WE INSIDE STARTWINDOW?
                    if (scheduleStart.nextStart > now && scheduleStart.nextStart < (now + startWindowS*1000))
                    {
                        // ASK FOR FLAG
                        if (!scheduleStart.started)
                        {
                            // AND DB RESULT
                            if (!checkDbSchedWasStarted( scheduleStart.sched, startWindowS, now))
                            {
                                scheduleStart.started = true;
                                setStatusTxt(Main.Txt("Starte") + scheduleStart.sched.toString());
                                startSchedule(scheduleStart.sched, User.createSystemInternal());
                            }
                        }
                    }
                    
                    // CATCH STARTED JOB AND RECALC NEW START TIME
                    if ( scheduleStart.started && scheduleStart.nextStart < (now - startWindowS*1000))
                    {
                        // WE RECALC IF JOB WAS STARTED
                        ScheduleStart nextScheduleStart = calcNextStart(scheduleStart.sched, now);
                        scheduleStart.nextStart = nextScheduleStart.nextStart;
                        scheduleStart.started = false;
                    }
                }
            }
            setStatusTxt("");
        }
    }



    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        return " Idle ";
    }
    
    
    private static BackupJobResult getLastResult(Schedule sched)
    {

        JDBCEntityManager em = Main.get_control().get_util_em(sched.getPool());

        long lastBackupJobResultIdx = -1;
        List<Object[]> list = null;
        try
        {
            list = em.createNativeQuery("select max(idx) from BackupJobResult b where b.schedule_idx=" + sched.getIdx(), 1);
        }
        catch (SQLException sQLException)
        {
            return null;
        }
        
        if (list.size() > 0 && list.get(0)[0] != null && list.get(0)[0] instanceof Long)
        {
            lastBackupJobResultIdx = (Long)list.get(0)[0];
        }
        else
            return null;

        BackupJobResult jobr = em.em_find(BackupJobResult.class, lastBackupJobResultIdx);
        return jobr;
    }

    private static boolean checkDbSchedWasStarted( Schedule sched, long startWindowS, long startTime )
    {
        BackupJobResult jobr = getLastResult( sched );
        if (jobr == null)
            return false;

        // CHECK IF WE HAVE A START INSIDE THE LAST MINUTE (startWindowS)
        long diff = startTime - jobr.getStartTime().getTime();
        if (diff >= 0 && diff < startWindowS * 1000)
            return true;

        return false;
    }

    
    private void startSchedule( Schedule schedule, User user )
    {
        JobManager jm = Main.get_control().getJobManager();
        if (jm.isBackupRunning(schedule))
        {
            Log.warn("Schedule ist bereits gestartet", schedule.getName());
            return;
        }

        jm.addJobEntry( Backup.createbackupJob(schedule, user));
    }

    public Date getNextStart( Schedule sched)
    {
        for (int i = 0; i < startList.size(); i++)
        {
            ScheduleStart scheduleStart = startList.get(i);
            if (scheduleStart.sched.getIdx() == sched.getIdx() && scheduleStart.sched.getPool().getIdx() == sched.getPool().getIdx())
            {
                return new Date( scheduleStart.nextStart );
            }
        }
        return null;
    }
    public boolean isSchedRunning(Schedule sched)
    {
        JobManager jm = Main.get_control().getJobManager();

        JobEntry[] jarr = jm.getJobArray(null);
        for (int i = 0; i < jarr.length; i++)
        {
            JobEntry je = jarr[i];
            if (je.getJob() instanceof Backup.BackupJobInterface)
            {
                BackupJobInterface bi = (BackupJobInterface) je.getJob();
                if (bi.getActSchedule().getIdx() == sched.getIdx() && bi.getActSchedule().getPool().getIdx() == sched.getPool().getIdx())
                {
                    return true;
                }
            }
        }
        return false;
    }

    public JobInterface createCDPJob(AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, CdpEvent ev)
    {
        setStatusTxt(Main.Txt("Starte CDP Job") + " " + ev.getPath());
        return new CDPJobInterface(api, sched, info, volume, ev);
    }

    public JobInterface createCDPJob(AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, List<CdpEvent> evList)
    {
        setStatusTxt(Main.Txt("Starte CDP JobList"));
        return new CDPJobInterface(api, sched, info, volume, evList);
    }

    public class CDPJobInterface implements JobInterface
    {

        BackupContext actualContext;
        AgentApiEntry api;
        Schedule sched;
        ClientInfo info;
        ClientVolume volume;
        List<CdpEvent> evList;
        Date start = new Date();
        JOBSTATE js;

        public CDPJobInterface( AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, CdpEvent ev )
        {
            this.actualContext = null;
            this.api = api;
            this.sched = sched;
            this.info = info;
            this.volume = volume;
            this.evList = new ArrayList<CdpEvent>();
            this.evList.add(ev);
            js = JOBSTATE.MANUAL_START;
        }
        public CDPJobInterface( AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, List<CdpEvent> evList )
        {
            this.actualContext = null;
            this.api = api;
            this.sched = sched;
            this.info = info;
            this.volume = volume;
            this.evList = new ArrayList<CdpEvent>();
            this.evList.addAll(evList);
            js = JOBSTATE.MANUAL_START;
        }

        @Override
        public JOBSTATE getJobState()
        {
            if (actualContext != null)
            {
                return actualContext.getJobState();
            }
            return js;
            
        }

        @Override
        public void setJobState( JOBSTATE jOBSTATE )
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
        public int getProcessPercent()
        {
            if (actualContext != null)
                return  (int)(actualContext.stat.Speed() / (1000*1000));
            return 0;
        }

        @Override
        public String getProcessPercentDimension()
        {
            return "MB";
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
                actualContext = initCDPbackup(api, sched, info, volume);
                handleCDPbackup(actualContext, api, evList, sched);
            }
            catch (Throwable ex)
            {
                if (actualContext != null)
                    actualContext.setJobState(JOBSTATE.ABORTED);
            }
            finally
            {
                if (actualContext != null)
                {
                    try
                    {

                        closeCDPbackup(actualContext);
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

    public BackupContext initCDPbackup(  AgentApiEntry api,  Schedule sched, ClientInfo info, ClientVolume volume ) throws IOException, Exception
    {
        StoragePool pool = sched.getPool();

        StoragePoolNubHandler nubHandler = LogicControl.getStorageNubHandler();

        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( nubHandler, pool, user, /*rdonly*/false);
        if (pool.getStorageNodes(sp_handler.getEm()).isEmpty())
            throw new Exception("No Storage for pool defined");

        sp_handler.realizeInFs();

        sp_handler.check_open_transaction();

        // HOTFOLDER CONTEXT RESOLVES THE FILESYSTEM WE ARE USING
        BackupContext actualContext = new BackupContext(api, sp_handler, info, volume );
        actualContext.setAbortOnError(true);

        // OPEN INDEXER
        if (actualContext.getIndexer() != null && !actualContext.getIndexer().isOpen())
            actualContext.getIndexer().open();

        return actualContext;
    }

    public void closeCDPbackup(  BackupContext actualContext ) throws IOException, Exception
    {
        actualContext.close();
        //Log.debug("Closing CDP BackupContext" );
        StoragePoolHandler sp_handler = actualContext.getPoolhandler();       
        sp_handler.close_transaction();
        sp_handler.close_entitymanager();
    }


    public BackupContext handleCDPbackup( BackupContext actualContext, AgentApiEntry api,  List<CdpEvent> evList, Schedule sched ) throws Exception, Throwable
    {

        for (int i = 0; i < evList.size(); i++)
        {
            CdpEvent ev = evList.get(i);
            
            RemoteFSElem elem = ev.getElem();
            boolean recursive = ev.getMode() == CdpEvent.CDP_SYNC_DIR_RECURSIVE;

            setStatusTxt(Main.Txt("Sichere CDP") + ": " + elem.getPath());
            actualContext.setStatus(Main.Txt("CDP ist aktiv mit") + " " + elem.getPath());


            try
            {
                // DETECT PATH IN STORAGE
                String abs_path = actualContext.getRemoteElemAbsPath( elem );

                // RESOLVE STARTPATH IF POSSIBLE
                FileSystemElemNode node = actualContext.poolhandler.resolve_elem_by_path( abs_path );

                // MAP NODE IN THIS CONTEXT
                if (node != null)
                {
                    node = actualContext.poolhandler.em_find(FileSystemElemNode.class, node.getIdx());
                }

                Backup.backupRemoteFSElem(actualContext, elem, node, recursive, /*onlyNewer*/ false);
            }
            catch (Throwable throwable)
            {
                 Log.err("Fehler beim CDP des Elements " + elem.getName(), throwable );
                 actualContext.setStatus("Fehler beim CDP des Elements " + elem.getName() + ": " + throwable.getMessage() );
                 actualContext.setResult(false);
            }
        }
        
        // SUCCEEDED?
        if (actualContext.getResult())
        {
            actualContext.setStatus("");
            setStatusTxt("");
            actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_OK_REMOVE);
        }
        else
        {
            actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);
        }

        // PUSH INDEX
        if (actualContext.getIndexer() != null)
        {
            actualContext.getIndexer().flushAsync();
        }

        return actualContext;
    }

   
}
