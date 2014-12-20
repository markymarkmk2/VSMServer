/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Backup.BackupJobInterface;
import de.dimm.vsm.backup.jobinterface.CDPJobInterface;
import de.dimm.vsm.backup.jobinterface.VfsJobInterface;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.IStoragePoolNubHandler;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
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
import de.dimm.vsm.records.MountEntry;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

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

        startList = new ArrayList<>();

        Main.addNotification( new NotificationEntry(BA_AGENT_OFFLINE,
                Main.Txt("Agent ist offline"), Main.Txt("Der Agent $AGENT für Backup $NAME kann nicht kontaktiert werden"), NotificationEntry.Level.WARNING, true));

        Main.addNotification( new NotificationEntry(BA_ERROR,
                Main.Txt("Fehler beim Sichern in Backup $NAME"), Main.Txt("In Volume $VOLUME bei Agent $AGENT im Backup $NAME traten Fehler auf"), NotificationEntry.Level.ERROR, false));
        Main.addNotification( new NotificationEntry(BA_FILE_ERROR,
                Main.Txt("Fehler beim Sichern in Backup $NAME"), Main.Txt("In Volume $VOLUME bei Agent $AGENT im Backup $NAME können folgende Einträge nicht gesichert werden"), NotificationEntry.Level.WARNING, false));

        Main.addNotification( new NotificationEntry(BA_ABORT,
                Main.Txt("Abbruch beim Sichern in Backup $NAME"), Main.Txt("Die Sicherung von Volume $VOLUME bei Agent $AGENT im Backup $NAME wurde abgebrochen"), NotificationEntry.Level.ERROR, false));

        Main.addNotification( new NotificationEntry(BA_VOLUME_OKAY,
                Main.Txt("Volume $VOLUME beendet"), Main.Txt("Volume $VOLUME auf $AGENT bei Backup $NAME wurde erfolgreich gesichert"), NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_CLIENT_OKAY,
                Main.Txt("Client $AGENT beendet"), Main.Txt("Client $AGENT bei Backup $NAME wurde erfolgreich gesichert"), NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_OKAY,
                Main.Txt("Backup $NAME erfolgreich beendet"), "", NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(BA_NOT_OKAY,
                Main.Txt("Backup $NAME nicht erfolgreich beendet"), "", NotificationEntry.Level.ERROR, false));

        Main.addNotification( new NotificationEntry(BA_SNAPSHOT_FAILED,
                Main.Txt("Fehler beim Erzeugen des Snapshots in Backup $NAME"), Main.Txt("In Volume $VOLUME bei Agent $AGENT im Backup $NAME konnte kein Snapshot erzeugt werden"), NotificationEntry.Level.WARNING, false));

        Main.addNotification( new NotificationEntry(BA_GROUP_ERROR,
                Main.Txt("Alle Fehler in Backup"), "BA_SNAPSHOT_FAILED,BA_AGENT_OFFLINE,BA_ERROR,BA_FILE_ERROR,BA_ABORT,BA_NOT_OKAY", NotificationEntry.Level.GROUP, false));
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
        Log.debug("updateStartList");
        
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
                    Log.err("Fehler beim Ermitteln der Zeitpläne", sQLException);
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
                            Log.debug("Neuer Start " + start.toString());
                            startList.add(start);
                        }
                        else
                        {
                            Log.debug("Kein Start für " + schedule.getName());
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

    private static boolean isScheduleOnDayBoundary(Schedule schedule)
    {
        return ((86400*1000l) % schedule.getCycleLengthMs()) == 0;
    }
    
    static ScheduleStart calcNextStart( Schedule schedule, long now )
    {
        GregorianCalendar baseCal = new GregorianCalendar();
        ScheduleStart start = null;

        // 1. CYCLE MODE
        if (schedule.getIsCycle())
        {

            baseCal.setTime(schedule.getScheduleStart());
                                   
            // SPEED UP: IF WE ARE ON DAY BOUNDARY, THEN WE CAN START ON THIS DAY MINUS ONE DAY (OFFSET MAY BE LARGER THAN CYCLE)
            if ( baseCal.getTimeInMillis() < now && isScheduleOnDayBoundary(schedule))
            {
                int h = baseCal.get(GregorianCalendar.HOUR_OF_DAY);
                int m = baseCal.get(GregorianCalendar.MINUTE);
                int s = baseCal.get(GregorianCalendar.SECOND);
                baseCal.setTimeInMillis( now );
                baseCal.set(GregorianCalendar.HOUR_OF_DAY, h);
                baseCal.set(GregorianCalendar.MINUTE, m);
                baseCal.set(GregorianCalendar.SECOND, s - 1); // WG. DER FEHLENDEN MS SICHERHEITSHALBER EINE SEK ZURÜCK
                baseCal.set(GregorianCalendar.MILLISECOND, 0);
                baseCal.add(GregorianCalendar.DAY_OF_YEAR, -1);
            }

            long startTime = baseCal.getTimeInMillis();
            int n;

            // FIND THE FIRST CYCLEENTRY IN THE FUTURE
            for (n = 0;;n++)
            {
                long check = startTime +  n * schedule.getCycleLengthMs();
                if (check > now)
                {
                    ScheduleStart actS = new ScheduleStart(check, schedule);
                    start = actS;
                    Log.debug("Nächster Start " + actS.toString());
                    break;
                }
            }
        }
        else
        {
            List<Job> jobList;

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

                int n;
                baseCal.setTime(new Date(schedule.getScheduleStart().getTime()));
                long startTime;
                
                // SPEED UP: IF WE ARE ON DAY BOUNDARY, THEN WE CAN START ON THIS DAY MINUS ONE DAY (OFFSET MAY BE LARGER THAN CYCLE)
                if ( baseCal.getTimeInMillis() < now && isScheduleOnDayBoundary(schedule))
                {
                    int h = baseCal.get(GregorianCalendar.HOUR_OF_DAY);
                    int m = baseCal.get(GregorianCalendar.MINUTE);
                    int s = baseCal.get(GregorianCalendar.SECOND);
                    baseCal.setTimeInMillis( now );
                    baseCal.set(GregorianCalendar.HOUR_OF_DAY, h);
                    baseCal.set(GregorianCalendar.MINUTE, m);
                    baseCal.set(GregorianCalendar.SECOND, s - 1);
                    baseCal.set(GregorianCalendar.MILLISECOND, 0);
                    baseCal.add(GregorianCalendar.DAY_OF_YEAR, -1);
                    startTime = baseCal.getTimeInMillis();
                } 
                else
                {                
                    long cycles = (now - schedule.getScheduleStart().getTime()) / schedule.getCycleLengthMs();
                    baseCal.setTime(new Date(schedule.getScheduleStart().getTime() + (cycles-1)*schedule.getCycleLengthMs()));
                    startTime = baseCal.getTimeInMillis();
                }

                // FIND THE FIRST CYCLEENTRY IN THE FUTURE
                for (n = 0;;n++)
                {
                    int offsetStartS = (int)(job.getOffsetStartMs() / 1000 );
                    int days = job.getDayNumber();
                    if (days < 0)
                        days = 0;

                    // ADD CYCLE                    
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
                        {
                            start = actS;
                            Log.debug("Nächster Start " + actS.toString());
                        }
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
    public void setPaused( boolean paused ) {
        if (isPaused() && !paused) {
            // Zeiten neu berechnen wenn Pause ausgeschaltet wird
            doUpdateStartList = true;
        }
        super.setPaused(paused);
    }
    
    

    @Override
    public void run()
    {
        is_started = true;
        long lastCheck = System.currentTimeMillis();
        
        long startWindowS = Main.get_int_prop(GeneralPreferences.BA_START_WINDOW_S, Backup.DEFAULT_START_WINDOW_S);

        setStatusTxt("");
        int lastJobsRunning = 0;
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
            
            int actJobsRunning = getActJobsRunnung();
            if (actJobsRunning == 0 && lastJobsRunning > 0) {
                if (CacheManager.getInstance().cacheExists(JDBCEntityManager.OBJECT_CACHE))
                {
                    Cache ch = CacheManager.getInstance().getCache(JDBCEntityManager.OBJECT_CACHE);
                    Log.debug("ObjectCache wird gelöscht (N=" + ch.getSize() + ")");
                    ch.removeAll();
                }
            }
            lastJobsRunning = actJobsRunning;
            
            // Einmal täglich Startliste neu berechnen
            updateStartListDaily();

            setStatusTxt(Main.Txt("Prüfe Zyklusstarts"));
            lastCheck = now;

            synchronized(startList)
            {
                for (int i = 0; i < startList.size(); i++)
                {
                    ScheduleStart scheduleStart = startList.get(i);
                    
                    long realStartWindow = startWindowS * 1000;
                    // Startwindow muss kleiner als Cycle sein!!
                    if (realStartWindow > scheduleStart.sched.getCycleLengthMs()/2)
                        realStartWindow = scheduleStart.sched.getCycleLengthMs()/2;
                        

                    // ARE WE INSIDE STARTWINDOW BEGINNING AT NOW?
                    if (scheduleStart.nextStart > (now  - realStartWindow) && scheduleStart.nextStart < now)
                    {                        
                        // ASK FOR FLAG
                        if (!scheduleStart.started)
                        {
                            Log.debug("Betrete Startfenster " + scheduleStart.toString());
                            scheduleStart.started = true;
                            
                            // AND DB RESULT
                            if (!checkDbSchedWasStarted( scheduleStart.sched, realStartWindow, now))
                            {                               
                                setStatusTxt(Main.Txt("Starte") + " " + scheduleStart.sched.toString());
                                startSchedule(scheduleStart.sched, User.createSystemInternal());                                
                            }
                            else
                            {
                                Log.debug("Job wurde bereits gestartet " + scheduleStart.toString());
                            }
                        }
                    }
                    // CATCH STARTED JOB AND RECALC NEW START TIME AFTER ELAPS OF STARTWINDOW
                    else if ( scheduleStart.started && scheduleStart.nextStart < (now - realStartWindow))
                    {
                        // WE RECALC IF JOB WAS STARTED
                        ScheduleStart nextScheduleStart = calcNextStart(scheduleStart.sched, System.currentTimeMillis());
                        scheduleStart.nextStart = nextScheduleStart.nextStart;
                        scheduleStart.started = false;
                        Log.debug("Neuer Startzeitpunkt " + scheduleStart.toString());
                    }
                }
            }
            setStatusTxt("");
        }
        finished =true;
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

        long lastBackupJobResultIdx;
        List<Object[]> list;
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

    private static boolean checkDbSchedWasStarted( Schedule sched, long startWindowMs, long startTime )
    {
        BackupJobResult jobr = getLastResult( sched );
        if (jobr == null)
            return false;

        // CHECK IF WE HAVE A START INSIDE THE LAST MINUTE (startWindowS)
        long diff = startTime - jobr.getStartTime().getTime();
        if (diff >= 0 && diff < startWindowMs)
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
        return new CDPJobInterface(this, api, sched, info, volume, ev);
    }

    public JobInterface createCDPJob(AgentApiEntry api, Schedule sched, ClientInfo info, ClientVolume volume, List<CdpEvent> evList)
    {
        setStatusTxt(Main.Txt("Starte CDP JobList"));
        return new CDPJobInterface(this, api, sched, info, volume, evList);
    }

    public JobInterface createVfsJob( AgentApiEntry api, MountEntry mountEntry, List<RemoteFSElem> elem )
    {
        setStatusTxt(Main.Txt("Starte CDP JobList"));
        return new VfsJobInterface(this, api, mountEntry, elem);
    }      
    
    public BackupContext initVfsbackup(  AgentApiEntry api,  MountEntry mountEntry ) throws IOException, Exception
    {
        StoragePool pool = mountEntry.getPool();

        IStoragePoolNubHandler nubHandler = LogicControl.getStorageNubHandler();

        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( nubHandler, pool, user, /*rdonly*/false);
        if (pool.getStorageNodes(sp_handler.getEm()).isEmpty())
            throw new Exception("No Storage for pool defined");

        sp_handler.realizeInFs();

        sp_handler.check_open_transaction();

        // HOTFOLDER CONTEXT RESOLVES THE FILESYSTEM WE ARE USING
        BackupContext actualContext = new BackupContext(api, sp_handler, mountEntry );
        actualContext.setAbortOnError(true);

        // OPEN INDEXER
        if (actualContext.getIndexer() != null && !actualContext.getIndexer().isOpen())
            actualContext.getIndexer().open();

        return actualContext;
    }

    public BackupContext initCDPbackup(  AgentApiEntry api,  Schedule sched, ClientInfo info, ClientVolume volume ) throws IOException, Exception
    {
        StoragePool pool = sched.getPool();

        IStoragePoolNubHandler nubHandler = LogicControl.getStorageNubHandler();

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
    
    public void closeVfsbackup(  BackupContext actualContext ) throws IOException, Exception
    {
        actualContext.close();
        //Log.debug("Closing CDP BackupContext" );
        StoragePoolHandler sp_handler = actualContext.getPoolhandler();       
        sp_handler.close_transaction();
        sp_handler.close_entitymanager();
    }

    public BackupContext handleVfsbackup( BackupContext actualContext, AgentApiEntry api, List<RemoteFSElem> elems, MountEntry mountEntry )
    {
        for (int i = 0; i < elems.size(); i++)
        {
            RemoteFSElem elem = elems.get(i);
            
            boolean recursive = false;

            String txt = Main.Txt("VFS ist aktiv mit") + " " + elem.getPath();
            Log.debug(txt);
            setStatusTxt(txt);
            actualContext.setStatus(txt);

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
                 Log.err("Fehler beim VFS des Elements " + elem.getName(), throwable );
                 actualContext.setStatus("Fehler beim VFS des Elements " + elem.getName() + ": " + throwable.getMessage() );
                 actualContext.setResult(false);
            }
        }
        actualContext.getStat().check_stat(true);
        
        // SUCCEEDED?
        if (actualContext.getResult())
        {
            actualContext.setStatus("");
            setStatusTxt("");
            actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_OK_REMOVE);
            String txt = Main.Txt("VFS ohne Fehler beendet");
            Log.debug(txt);

        }
        else
        {
            actualContext.setJobState(JobInterface.JOBSTATE.FINISHED_ERROR);
            String txt = Main.Txt("VFS mit Fehler beendet");
            Log.err(txt);
        }

        // PUSH INDEX
        if (actualContext.getIndexer() != null)
        {
            actualContext.getIndexer().flushAsync();
        }

        return actualContext;
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
                 actualContext.setStatus(Main.Txt("Fehler beim CDP des Elements") + " " + elem.getName() + ": " + throwable.getMessage() );
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

    private boolean doUpdateStartList = false;
    
    private void updateStartListDaily() {
        
        GregorianCalendar baseCal = new GregorianCalendar();

        int h = baseCal.get(GregorianCalendar.HOUR_OF_DAY);
        int m = baseCal.get(GregorianCalendar.MINUTE);

        // At 00:00 recalc Startlist
        if ( h == 0 && m == 0)
        {
            doUpdateStartList = true;
        }
        else
        {
            if (doUpdateStartList)
            {
                doUpdateStartList = false;
                updateStartList();
            }
        }        
    }

    private int getActJobsRunnung() {
        int cnt = 0;
        JobManager jm = Main.get_control().getJobManager();
        for (JobEntry job: jm.getJobList()) {
            if (job.getJobStatus() == JobInterface.JOBSTATE.RUNNING)
                cnt++;
        }
        return cnt;
    }

   
}
