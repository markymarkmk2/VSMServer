/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;


import de.dimm.vsm.CS_Constants;
import de.dimm.vsm.Exceptions.ClientAccessFileException;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.Utilities.StatCounter;
import de.dimm.vsm.Utilities.VariableResolver;
import de.dimm.vsm.Utilities.ZipUtilities;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.ArrayLazyList;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.HashCache;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.JDBCStoragePoolHandler;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.net.CdpEvent;
import de.dimm.vsm.net.CdpTicket;
import de.dimm.vsm.net.ScheduleStatusEntry;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Schedule;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.net.CompEncDataResult;
import de.dimm.vsm.net.HashDataResult;
import de.dimm.vsm.net.interfaces.AgentApi;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.RemoteFSElemWrapper;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.net.interfaces.SnapshotHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.BackupJobResult;
import de.dimm.vsm.records.BackupVolumeResult;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.Excludes;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.XANode;
import de.dimm.vsm.vaadin.VSMCMain;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;




/**
 *
 * @author Administrator
 */

public class Backup
{
    private static boolean hashOnAgent = true;
    private static boolean checkDHBExistance = true;

    private static boolean testModeFull = false;


    public static final int DEFAULT_START_WINDOW_S = 60;

    public static JobInterface createbackupJob( Schedule sched, User user )
    {        
        Backup backup = new Backup(sched);
        JobInterface job = backup.createJob(user);
        return job;
    }

   // List<Schedule> schedules;
    Schedule sched;
    

    
    public static boolean speed_test_no_db= false;
    public static boolean speed_test_no_write= false;
    
    private boolean abort;
    private boolean finished = false;
    private BackupContext actualContext;

    private static boolean withBootstrap = true;
    
    public Backup(Schedule sched)
    {
        if (sched != null)
        {
            GenericEntityManager em = Main.get_control().get_util_em(sched.getPool());

            this.sched = em.em_find(Schedule.class, sched.getIdx());
        }
        abort = false;

        withBootstrap = Main.get_bool_prop(GeneralPreferences.WITH_BOOTSTRAP, true);

        
    }

    private void baNotify( String key, String extraText, VariableResolver vr )
    {
        Main.fire(key, extraText, vr );
    }
    private void baRelease( String key)
    {
        Main.release(key);
    }

    JobInterface createJob(User user)
    {

        return new BackupJobInterface(user);
    }

    String preStartStatus;

    public static boolean isWithBootstrap()
    {
        return withBootstrap;
    }



    private String createNokResultText( List<BackupVolumeResult> list, int totalVolumesTried )
    {
        StringBuilder sb = new StringBuilder();

        sb.append(Main.Txt("Volumes gestartet"));
        sb.append( ": ");
        sb.append(totalVolumesTried);
        sb.append( "\n");
        int cntOk = 0;
        int cntNok = 0;
        for (int i = 0; i < list.size(); i++)
        {
            BackupVolumeResult backupVolumeResult = list.get(i);
            if (backupVolumeResult.isOk())
                cntOk++;
            else
                cntNok++;
        }
        sb.append(Main.Txt("Volumes okay"));
        sb.append( ": ");
        sb.append(cntOk);
        sb.append( "\n");
        sb.append(Main.Txt("Volumes nicht okay"));
        sb.append( ": ");
        sb.append(cntNok);
        sb.append( "\n\n");
        sb.append(Main.Txt("Volumeliste"));
        sb.append( ":\n");

        for (int i = 0; i < list.size(); i++)
        {
            BackupVolumeResult backupVolumeResult = list.get(i);
            sb.append( backupVolumeResult.getVolume().getClinfo().getIp() );
            sb.append( " -> ");
            sb.append( backupVolumeResult.getVolume().getVolumePath() );
            sb.append( ": ");
            sb.append( backupVolumeResult.isOk()? Main.Txt("OK"):Main.Txt("NOK"));
            sb.append( "\n");
        }

        return sb.toString();
    }

    public class BackupCDPTicket
    {
        List<CdpEvent> fileList;
        CdpTicket ticket;

        public BackupCDPTicket( List<CdpEvent> fileList, CdpTicket ticket )
        {
            this.fileList = fileList;
            this.ticket = ticket;
        }
        public BackupCDPTicket( CdpEvent file, CdpTicket ticket )
        {
            this.fileList = new ArrayList<>();
            fileList.add(file);
            this.ticket = ticket;
        }

        public List<CdpEvent> getFileList()
        {
            return fileList;
        }

        public CdpTicket getTicket()
        {
            return ticket;
        }
    }
    public class BackupVfsTicket
    {
        List<RemoteFSElem> elems;
        StoragePoolWrapper ticket;

        public BackupVfsTicket( List<RemoteFSElem> elem, StoragePoolWrapper ticket )
        {
            this.elems = elem;
            this.ticket = ticket;
        }

        public List<RemoteFSElem> getElem()
        {
            return elems;
        }

        public StoragePoolWrapper getTicket()
        {
            return ticket;
        }
    }
    

    public class BackupJobInterface implements JobInterface
    {

        boolean finished = false;
        Date startTime;
        User user;

        public BackupJobInterface(User user)
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
        public void close()
        {

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
            if (actualContext != null)
                return actualContext.getStatus();

            if (preStartStatus != null)
                return preStartStatus;
            return "?";
        }

        @Override
        public String getProcessPercent()
        {
            if (actualContext != null)
                return  actualContext.stat.getSpeedPerSec();

            return "";
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

      /*  @Override
        public void setStatusStr( String s )
        {
            if (actualContext != null)
            {
                actualContext.setStatus(s);
            }
        }

        @Override
        public void setProcessPercent( int p )
        {
        }
*/
        @Override
        public String getProcessPercentDimension()
        {
            if (actualContext != null)
                return  actualContext.stat.getSpeedDim();

            return "";
        }

       


        @Override
        public void run()
        {
            try
            {
                run_schedule();
            }
            catch (Exception exception)
            {
                if (actualContext != null)
                {
                    Log.err("Abbruch mit context in Sicherung", exception );

                    actualContext.setJobState( JOBSTATE.FINISHED_ERROR );
                    actualContext.setStatus(Main.Txt("Das Backup wurde abgebrochen") + ": " + exception.getMessage());


                    baNotify( BackupManager.BA_ABORT, actualContext.getStatus(), actualContext );
                                        
                }
                else
                {
                     Log.err("Abbruch in Sicherung", exception );
                     baNotify( BackupManager.BA_ABORT, exception.getMessage(), actualContext );
                }
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
                    actualContext.close();
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
                if (actualContext.poolhandler instanceof JDBCStoragePoolHandler)
                {
                    JDBCStoragePoolHandler dh = (JDBCStoragePoolHandler)actualContext.poolhandler;

                    qlen = dh.getPersistQueueLen();
                }
                String ret = actualContext.stat.toString();
                if (qlen > 0)
                    ret += " PQ: " + qlen;

                int wqlen = actualContext.writeRunner.getQueueLen();
                if (wqlen > 0)
                    ret += " WQ: " + wqlen;

                return ret;
            }
            return "";
        }
        public Schedule getActSchedule()
        {
            if (actualContext != null)
            {
                return actualContext.getActClientInfo().getSched();
            }
            if (sched != null)
            {
                return sched;
            }
            return null;
        }
        public ClientInfo getActClientInfo()
        {
            if (actualContext != null)
            {
                return actualContext.getActClientInfo();
            }
            return null;
        }
        public ClientVolume getActVolume()
        {
            if (actualContext != null)
            {
                return actualContext.getActVolume();
            }
            return null;
        }
        public void writeCacheStatistics()
        {
            if (actualContext != null)
            {
                if (actualContext.poolhandler.getEm() instanceof JDBCEntityManager)
                {
                    JDBCEntityManager jem = (JDBCEntityManager)actualContext.poolhandler.getEm();
                    jem.writeCacheStatistics(JDBCEntityManager.OBJECT_CACHE);
                    //jem.writeCacheStatistics(JDBCEntityManager.DEDUPBLOCK_CACHE);
                }
            }
        }

        public boolean addCDPEvent( List<CdpEvent> fileList, CdpTicket ticket )
        {
            if (actualContext != null)
            {
                BackupCDPTicket cdpTicket = new BackupCDPTicket(fileList, ticket);
                actualContext.addCDPTicket(cdpTicket);
                return true;
            }
            return false;
        }
        public boolean addCDPEvent( CdpEvent file, CdpTicket ticket )
        {
            if (actualContext != null)
            {
                BackupCDPTicket cdpTicket = new BackupCDPTicket(file, ticket);
                actualContext.addCDPTicket(cdpTicket);
                return true;
            }
            return false;
        }

        public boolean addVfsEvent( List<RemoteFSElem> elems, StoragePoolWrapper ticket )
        {
            if (actualContext != null)
            {
                BackupVfsTicket vfsTicket = new BackupVfsTicket(elems, ticket);
                actualContext.addVfsTicket(vfsTicket);
                return true;
            }
            return false;
        }
    }


    public BackupContext init_context( AgentApiEntry apiEntry, StoragePoolHandler poolhandler, ClientInfo info, ClientVolume vol)
    {        
        return new BackupContext( apiEntry, poolhandler, info, vol);
    }

    public boolean run_schedule(  ) throws Exception
    {
        StoragePool pool = sched.getPool();
        // CREATE RW POOLHANDLER
        User user = User.createSystemInternal();

        StoragePoolHandler sp_handler = null;
        try
        {
            sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( pool, user, /*rdonly*/ false );

            if (pool.getStorageNodes().getList( sp_handler.getEm()).isEmpty())
            {
                throw new Exception("No Storage for pool defined");
            }

            if (sp_handler.get_primary_storage_nodes(/*forWrite*/ true).isEmpty())
            {
                throw new Exception(Main.Txt("Keine beschreibbaren StorageNodes für Pool") + "  " + pool.getName());
            }
            if (sp_handler.get_primary_dedup_node_for_write() == null)
            {
                throw new Exception(Main.Txt("Keine beschreibbarer StorageNodes für Dedup bei Pool") + "  " + pool.getName());
            }

            try
            {
                return run_schedule( sched, sp_handler);
            }
            catch (PoolReadOnlyException poolReadOnlyException)
            {
                // CANNOT HAPPEN, POOL IS NOT RDONLY
                throw new Exception(Main.Txt("StoragePool ist schreibgeschützt"), poolReadOnlyException );
            }
        }
        finally
        {
            if (sp_handler != null)
            {
                sp_handler.commit_transaction();
                sp_handler.close_transaction();
                sp_handler.close_entitymanager();
            }
        }
        
    }

    static void buildStatusText( GenericContext context, String text)
    {
        if (context != null && context instanceof BackupContext)
        {
            BackupContext bc = (BackupContext) context;
            StringBuilder sb = new StringBuilder();

            if (bc.getActClientInfo() != null)
            {
                sb.append(bc.getActClientInfo().getIp());
                sb.append(": ");
            }
            
            if (text != null)
            {
                /*if (text.length() > 80)
                    text =  "..." + text.substring(text.length() - 77);*/
                sb.append(text);
                sb.append(" ");
            }

            context.setStatus(sb.toString());
        }
    }

    private VariableResolver createLocalVr(final ClientInfo clientInfo, final ClientVolume clientVolume)
    {
        VariableResolver vr = new VariableResolver()
        {

            @Override
            public String resolveVariableText( String s )
            {
                if (s.indexOf("$NAME") >= 0)
                {
                    String f = clientInfo.getSched().getName();
                    s = s.replace("$NAME", f );
                }

                if (s.indexOf("$POOL") >= 0)
                {
                    String f = clientInfo.getSched().getPool().getName();
                    s = s.replace("$POOL", f );
                }
                if (s.indexOf("$VOLUME") >= 0)
                {
                    String f = clientVolume.getVolumePath().getPath();
                    s = s.replace("$VOLUME", f );
                }
                if (s.indexOf("$AGENT") >= 0)
                {
                    String f = clientInfo.getIp();
                    s = s.replace("$AGENT", f );
                }
                return s;
            }
        };
        return vr;
    }
    private VariableResolver createSchedVr(final Schedule sched)
    {
        VariableResolver vr = new VariableResolver()
        {

            @Override
            public String resolveVariableText( String s )
            {
                if (s.indexOf("$NAME") >= 0)
                {
                    String f = sched.getName();
                    s = s.replace("$NAME", f );
                }

                if (s.indexOf("$POOL") >= 0)
                {
                    String f = sched.getPool().getName();
                    s = s.replace("$POOL", f );
                }
                return s;
            }
        };
        return vr;
    }


    private boolean run_schedule( final Schedule sched, StoragePoolHandler hdl ) throws PoolReadOnlyException, SQLException
    {
        StatCounter overallCounter = new StatCounter("Total");
        
        BackupJobResult baJobResult = new BackupJobResult();
        baJobResult.setSchedule(sched);
        baJobResult.setStartTime( new Date());
        baJobResult.setBackupVolumeResults( new ArrayLazyList<BackupVolumeResult>());

        hdl.em_persist(baJobResult, /*noCache*/true);
        boolean globalOk = false;

        try
        {
            // RELOAD FROM DB
            sched.getClientList().unRealize();
            
            // AND HOLD A COPY
            List<ClientInfo> client_list = new ArrayList<>();
            client_list.addAll( sched.getClientList().getList(hdl.getEm()) );

            int totalVolumesTried = 0;

            for (int i = 0; i < client_list.size(); i++)
            {
                final ClientInfo clientInfo = client_list.get(i);
                if (clientInfo.getDisabled())
                {
                    Log.info(Main.Txt("Client ist disabled") + ":" + clientInfo.toString());
                    continue;
                }

                if (abort)
                {
                    Log.warn(Main.Txt("Backup wurde abgebrochen") + ":" + clientInfo.toString());
                    break;
                }

                Log.info(Main.Txt("Starte Sicherung von Client") + " " +  clientInfo.toString() +
                            " (" + Integer.toString(i+1) + "/" + client_list.size()+ ")");
                boolean allVolsOkay = true;

                // RELOAD FROM DB
                clientInfo.getVolumeList().unRealize();

                // AND HOLD A COPY
                List<ClientVolume> volume_list = new ArrayList<>();
                volume_list.addAll( clientInfo.getVolumeList().getList(hdl.getEm()) );
                
                StatCounter clientCounter = new StatCounter("Client " + clientInfo.toString());

                for (int j = 0; j < volume_list.size(); j++)
                {
                    final ClientVolume clientVolume = volume_list.get(j);
                    if (clientVolume.getDisabled())
                    {
                        Log.info(Main.Txt("Volume ist disabled") + ":" + clientVolume.toString());
                        continue;
                    }

                    if (abort)
                    {
                        Log.warn(Main.Txt("Backup wurde abgebrochen") + ":" + clientVolume.toString());
                        break;
                    }

                    Log.info(Main.Txt("Starte Sicherung von Volume") + " " + clientVolume.toString() +
                            "(" + Integer.toString(j+1) + "/" + volume_list.size()+ ")");

                    totalVolumesTried++;

                    VariableResolver vr = createLocalVr(clientInfo, clientVolume);

                    BackupVolumeResult baVolumeResult = new BackupVolumeResult();
                    baVolumeResult.setStartTime( new Date());
                    baVolumeResult.setVolume(clientVolume);
                    baVolumeResult.setJobResult(baJobResult);
                    hdl.em_persist(baVolumeResult, /*noCache*/true);

                    BackupContext context = backupClientVolume( hdl, clientInfo, clientVolume );

                    baVolumeResult.setEndTime( new Date() );
                    if (context != null)
                    {
                        baVolumeResult.setOk( context.getResult() );
                        baVolumeResult.setStatus( context.getStatus());
                        baVolumeResult.setFilesChecked( context.stat.getFilesChecked() );
                        baVolumeResult.setFilesTransfered( context.stat.getFilesTransfered() );
                        baVolumeResult.setDataChecked( context.stat.getDataChecked() );
                        baVolumeResult.setDataTransfered( context.stat.getDataTransfered() );
                    }
                    else
                    {
                        baVolumeResult.setOk( false);
                        baVolumeResult.setStatus( preStartStatus );
                        baNotify(BackupManager.BA_ABORT, preStartStatus, vr);
                    }
                    if (!baVolumeResult.isOk())
                        allVolsOkay = false;

                    Log.info(Main.Txt("Sicherung von Volume") + " " + clientVolume.toString() + " beendet " + (baVolumeResult.isOk()? "(OK)": "(NOK)") );
                    
                    hdl.em_merge(baVolumeResult);

                    baJobResult.getBackupVolumeResults().addIfRealized(baVolumeResult);

                    if (context != null)
                    {
                        clientCounter.add(context.stat);
                        overallCounter.add( context.stat );
                        context.close();
                    }
                }
                clientCounter.check_stat();

                if (allVolsOkay)
                {
                    String summary = clientCounter.buildSummary();
                    baNotify(BackupManager.BA_CLIENT_OKAY, Main.Txt("Zusammenfassung") + ":\n\n" + summary, actualContext);
                }
            }
            globalOk = true;

            if (abort)
            {
                baJobResult.setStatus( Main.Txt("Aborted") + " " + Main.getActDateString());
                globalOk = false;
                baNotify(BackupManager.BA_ABORT, baJobResult.getStatus(), actualContext);
            }

            List<BackupVolumeResult> list = baJobResult.getBackupVolumeResults().getList( hdl.getEm() );
            for (int i = 0; i < list.size(); i++)
            {
                BackupVolumeResult baVolumeResult = list.get(i);
                if (!baVolumeResult.isOk())
                    globalOk = false;
            }

            if (globalOk)
            {
                String summary = overallCounter.buildSummary();
                baNotify(BackupManager.BA_OKAY, Main.Txt("Zusammenfassung") + ":\n\n" + summary, actualContext);
            }
            else
            {
                VariableResolver vr = actualContext != null ? actualContext : createSchedVr(sched);
                String nokResultText = createNokResultText( list, totalVolumesTried );
                String summary = overallCounter.buildSummary();
                baNotify(BackupManager.BA_NOT_OKAY, nokResultText + "\n\n" +  Main.Txt("Zusammenfassung") + ":\n\n" + summary, vr);
            }

        }
        catch (Exception exc)
        {
            Log.err("Abbruch bei Backup " + sched.getName() , exc);
            throw exc;
        }
        finally
        {
            baJobResult.setEndTime( new Date() );
            baJobResult.setOk( globalOk );
            hdl.em_merge(baJobResult);
            finished = true;

            Log.info("Beendet", ":%s", sched.getName() );
            overallCounter.check_stat(true);
            return true;
        }
        
        //return true;
    }

    private BackupContext backupClientVolume(  StoragePoolHandler hdl, final ClientInfo clientInfo, ClientVolume clientVolume ) throws PoolReadOnlyException, SQLException
    {
        Properties p;
        
        AgentApiEntry apiEntry = null;

        VariableResolver connectVr = new VariableResolver() {

            @Override
            public String resolveVariableText( String s )
            {
                if (s.indexOf("$AGENT") >= 0)
                {
                    String f = "";
                    if (clientInfo != null)
                        f = clientInfo.getIp();

                    s = s.replace("$AGENT", f );
                }
                if (s.indexOf("$NAME") >= 0)
                {
                    String f = "";
                    if (clientInfo != null)
                        f = clientInfo.getSched().getName();

                    s = s.replace("$NAME", f );
                }
                return s;
            }
        };

        try
        {
            apiEntry = LogicControl.getApiEntry( clientInfo );
            if (apiEntry == null || !apiEntry.isOnline())
            {
                preStartStatus = connectVr.resolveVariableText(Main.Txt("Der Agent kann nicht kontaktiert werden"));
                Log.err(preStartStatus);

                baNotify(BackupManager.BA_AGENT_OFFLINE, preStartStatus, connectVr);
                return null;
            }
            p = apiEntry.getApi().get_properties();
        }
        catch (Exception e)
        {
            preStartStatus = Main.Txt("Abbruch beim Connect zu Agent") + " " + clientInfo.toString();
            
            baNotify(BackupManager.BA_AGENT_OFFLINE, preStartStatus, connectVr);
            Log.err(preStartStatus, e);
            return null;
        }
        
        baRelease(BackupManager.BA_AGENT_OFFLINE);

        String agent_ver = p.getProperty(AgentApi.OP_AG_VER);
        String agent_os = p.getProperty(AgentApi.OP_OS);
        String agent_os_ver = p.getProperty(AgentApi.OP_OS_VER);
        String agent_os_arch = p.getProperty(AgentApi.OP_OS_VER);

        Log.debug("Verbunden mit Agent", clientInfo.toString() + ", " + agent_ver + ", " + agent_os + " " + agent_os_arch + " " + agent_os_ver);

        ArrayList<RemoteFSElem> start = apiEntry.getApi().list_dir(clientVolume.getVolumePath(),/*lazyacl*/true);
        if (start == null || start.isEmpty())
        {
            try
            {
                apiEntry.close();
            }
            catch (IOException iOException)
            {
            }
            preStartStatus = Main.Txt("Quellpfad ist leer oder existiert nicht") + " " + clientVolume.getVolumePath().getPath();
            baNotify(BackupManager.BA_ERROR, preStartStatus, connectVr);            
            Log.err(preStartStatus);
            return null;
        }

        BackupContext context = init_context( /*em,*/ apiEntry, hdl, clientInfo, clientVolume);
        context.stat.initStat();
        context.stat.setName(clientVolume.getVolumePath().toString());
        actualContext = context;
        
        context.stat.setDhbCacheSize( context.hashCache.size() );



        // IF THIS ROOT WASNT INSTATIATED IN FS, WE DO IT ON THE FLY
        context.poolhandler.realizeInFs();

        // CHECK SPACE AND NODE AVAILABILITY
        long freeSpace = context.checkStorageNodes();
        Log.debug("Freier Speicher auf aktuellem SpeicherNode", SizeStr.format(freeSpace));

        if (context.poolhandler.get_primary_dedup_node_for_write() == null)
        {
            preStartStatus = Main.Txt("Kein StorageNode für Dedup verfügbar") + " " +  context.poolhandler.getPool().getName();
            context.close();
            return null;
        }
        if (context.poolhandler.get_primary_storage_nodes(/*forWrite*/ true).isEmpty())
        {
            preStartStatus = Main.Txt("Kein StorageNodes verfügbar") + " " +  context.poolhandler.getPool().getName();
            context.close();
            return null;
        }
        if (!context.checkStorageNodesExists())
        {
            preStartStatus = Main.Txt("StorageNode existiert nicht") + " " +  context.poolhandler.getPool().getName();
            context.close();
            return null;
        }



        // HANDLE SNAPSHOTS
        SnapshotHandle snapshotHandle = null;
        if (clientVolume.getSnapshot())
        {
            buildStatusText(context, Main.Txt("Erzeuge Snapshot") + "...");
            snapshotHandle = context.apiEntry.getApi().create_snapshot(clientVolume.getVolumePath());
            if (snapshotHandle == null)
                baNotify(BackupManager.BA_SNAPSHOT_FAILED, "", context);
        }

        context.poolhandler.check_open_transaction();

        // DO THE REAL BACKUP HERE
        try
        {
            // DETECT PATH IN STORAGE
            String abs_path = context.getRemoteElemAbsPath(  clientVolume.getVolumePath() );

            // RESOLVE STARTPATH IF POSSIBLE
            FileSystemElemNode node = context.poolhandler.resolve_elem_by_path( abs_path );

            if (testModeFull)
            {
                Log.err("****** TEST MODE FULL ******");
                node = null;
            }

            // MAP NODE IN THIS CONTEXT
            if (node != null)
            {
                node = context.poolhandler.em_find(FileSystemElemNode.class, node.getIdx());
            }


            backupRemoteFSElem(context, clientVolume.getVolumePath(), node, /*recursive*/true, clientInfo.isOnlyNewer());

            if (context.isAbort())
            {
                baNotify(BackupManager.BA_ABORT, context.getStatus(), context);
            }
            else if(context.getResult())
            {
                String summary = actualContext.getStat().buildSummary(  );
                baNotify(BackupManager.BA_VOLUME_OKAY, Main.Txt("Zusammenfassung") + ":\n\n" + summary, context );
            }
            else
            {
                if (context.getErrList().isEmpty())
                {
                    baNotify(BackupManager.BA_ERROR, context.getStatus(), context );
                }
                else
                {
                    baNotify(BackupManager.BA_FILE_ERROR, context.getErrListString(), context );
                }
            }
        }
        catch (Throwable exc)
        {
            context.setJobState(JOBSTATE.FINISHED_ERROR);
            if (exc instanceof java.lang.reflect.UndeclaredThrowableException)
            {
                context.setStatus(Main.Txt("Verbindungsabbruch"));
                Log.err("Verbindungsabbruch", "%s :%d", context.apiEntry.getAddr().toString(), context.apiEntry.getPort());
            }
            else
            {
                context.setStatus(Main.Txt("Backupfehler") + ": " + exc.getMessage());
                Log.err("Backupfehler", exc);
            }
            baNotify(BackupManager.BA_ABORT, context.getStatus(), context);
        }

        finally
        {
            context.poolhandler.close_transaction();

            // REMOVE HANDLE SNAPSHOTS
            if (snapshotHandle != null)
            {
                buildStatusText(context, Main.Txt("Entferne Snapshot") + "...");
                context.apiEntry.getApi().release_snapshot(snapshotHandle);
            }

            context.getIndexer().flushAsync();
            
        }
        buildStatusText(context, Main.Txt("Backup volume beendet") );


        if (context.poolhandler.getPool().isLandingZone())
            Log.warn("TODO: RebuildLandingZone ");

        return context;
    }

    static void checkSpace(final GenericContext context) throws IOException
    {
        context.checkStorageNodes();

        if (context.indexer.noSpaceLeft())
        {
            context.setJobState(JOBSTATE.ABORTED);
            context.setResult( false );
            throw new IOException( "Indexspeicherplatz erschöpft");
        }

        if (context.noStorageSpaceLeft())
        {
            context.setJobState(JOBSTATE.ABORTED);
            context.setResult( false );
            throw new IOException( "Nodespeicherplatz erschöpft");
        }
        if (!context.checkStorageNodesExists())
        {
            context.setJobState(JOBSTATE.ABORTED);
            context.setResult( false );
            throw new IOException( "Nodespeicherplatz nicht gefunden");
        }
    }

    public static void backupRemoteFSElem( final GenericContext context, final RemoteFSElem remoteFSElem, final FileSystemElemNode node, boolean recursive, final boolean onlyNewer ) throws PoolReadOnlyException, SQLException, Throwable
    {
        List<Excludes> exclList = context.getExcludes();
        if (exclList != null)
        {
            for (int i = 0; i < exclList.size(); i++)
            {
                Excludes excludes = exclList.get(i);
                if (Excludes.checkExclude( excludes, remoteFSElem ))
                {
                    //Log.debug(Main.Txt("Excludefilter"), excludes.toString() + ": " + remoteFSElem.getPath());
                    return;
                }
            }
        }

        checkSpace(context);
       
        // HANDLE OPS FOR THIS ENTRY
        try
        {
            boolean ret = handle_single_fs_entry( context, node, remoteFSElem);
            if (!ret)
            {
                context.addError(remoteFSElem);
                Log.err("Fehler beim Sichern der Datei", "%s", remoteFSElem.getPath());
                if (context.isAbortOnError())
                {
                    context.setResult( false );
                    return;
                }
            }
        }
        catch (Exception exc)
        {
            Log.err("Abbruch bei Dateisicherung", remoteFSElem.getPath(), exc);
            context.setJobState(JOBSTATE.ABORTED);
            context.setResult( false );
            return;
        }

        if (context.isAbort())
        {
            context.setJobState(JOBSTATE.ABORTED);
            context.setResult( false );
            return;
        }


        // CHILDREN TOO ?
        if (remoteFSElem.isDirectory())
        {
            // IF NODE EXISTS, WE DO NOT FETCH ACL, PROBABLY WE HAVE INCREMENTAL
            final boolean lazyAclInfo = (node != null);

            buildStatusText(context, remoteFSElem.getPath() );


            FutureTask<List<RemoteFSElem>> fRemote = new FutureTask<List<RemoteFSElem>>( new Callable<List<RemoteFSElem>>()
            {
                @Override
                public List<RemoteFSElem> call() throws Exception
                {
                    // HANDLE BACKUP FOR ALL ELEMENTS ON AGENT
                    ArrayList<RemoteFSElem> fs_list = null;
                    try
                    {
                        fs_list = context.apiEntry.getApi().list_dir(remoteFSElem, lazyAclInfo);
                    }
                    catch (Exception e)
                    {
                        Log.warn(Main.Txt("Fehler beim Lesen von Verzeichnis, wiederhole") , remoteFSElem.getPath());
                        LogicControl.sleep(1000);
                        fs_list = context.apiEntry.getApi().list_dir(remoteFSElem, lazyAclInfo);
                    }
                    return fs_list;
                }
            });

            context.remoteListDirexecutor.execute(fRemote);



            List<FileSystemElemNode> childNodes = null;
            
            if (node != null)
            {
                node.getChildren(context.poolhandler.getEm());
            }

            // BUILD A MAP WITH ALL EXISTING NODES TO DETECT DELETED OBJECTS
            // THIS WORKS ONLY IF WE GET ALL DATA FROM AGENT (!onlyNewer)
            HashMap<Long, FileSystemElemNode> deleteMap = null;
            if (!onlyNewer && childNodes != null && !childNodes.isEmpty())
            {
                deleteMap =  new HashMap<Long, FileSystemElemNode>(childNodes.size());

                try
                {
                    for (int i = 0; i < childNodes.size(); i++)
                    {
                        FileSystemElemNode fileSystemElemNode = childNodes.get(i);
                        deleteMap.put(fileSystemElemNode.getIdx(), fileSystemElemNode);
                    }
                }
                catch (Exception e)
                {
                    Log.err(Main.Txt("Fehler beim Lesen von DelList") , remoteFSElem.getPath(), e);
                }
            }

            List<RemoteFSElem> fs_list = null;
            try
            {
                fs_list = fRemote.get();
            }
            catch (InterruptedException interruptedException)
            {
            }
            catch (ExecutionException exc)
            {
                Log.err(Main.Txt("Fehler beim Lesen von Verzeichnis") , remoteFSElem.getPath(), exc);
                throw exc.getCause();
            }


            for (int i = 0; i < fs_list.size(); i++)
            {
                if (context.isAbort())
                    break;

                RemoteFSElem childRemoteFSElem = fs_list.get(i);
                FileSystemElemNode child_node = null;

                if (node != null)
                {
                    child_node = context.poolhandler.resolve_child_elem(node, childRemoteFSElem);
                    
                    if (deleteMap != null && child_node != null)
                    {
                        // FOUND EXISTING NODE -> REMOVE FROM DELETED LIST
                        deleteMap.remove(child_node.getIdx());
                    }
                }

                // IF WE HAVE A NEW NODE, WE SWITCH TO RECURSIVE TRUE
                if (recursive || child_node == null || !child_node.isDirectory())
                {                    
                    backupRemoteFSElem( context, childRemoteFSElem, child_node, true, onlyNewer);
                }

                if (context.getResult() == false)
                {
                    if (context.isAbortOnError())
                        return;
                }
            }
            if (context.isAbort())
            {
                context.setResult( false );
                return;
            }

            // NOW WE HANDLE ALL NOT FOUND NODES
            if (deleteMap != null)
            {
                Collection<FileSystemElemNode> deletedColl = deleteMap.values();
                for (Iterator<FileSystemElemNode> it1 = deletedColl.iterator(); it1.hasNext();)
                {
                    FileSystemElemNode fileSystemElemNode = it1.next();
                    if (!fileSystemElemNode.getAttributes().isDeleted())
                    {
                        try
                        {
                            // ADD A NEW ATTRIBUTE FOR DELETED ENTRIES
                            context.poolhandler.setDeleted(fileSystemElemNode, true, System.currentTimeMillis());
                            Log.debug(Main.Txt("Setze Löschflag für"), fileSystemElemNode.toString());
                        }
                        catch (Exception exception)
                        {
                            Log.err(Main.Txt("Fehler beim Setzen des Löschflags") , fileSystemElemNode.toString(), exception);
                        }
                    }
                }
                deletedColl.clear();
            }

            if (node != null)
            {
                unrealizeChildren( node );
                context.poolhandler.em_detach(node);
            }

            checkCDPTickets( context );
        }
        context.getIndexer().checkFlushAsync();
    }

    static void checkCDPTickets(  GenericContext context ) throws PathResolveException, SQLException, PoolReadOnlyException, Throwable
    {
        if (context instanceof BackupContext)
        {
            BackupContext bc = (BackupContext)context;
            if (bc.getCDPMutex())
            {
                try
                {
                    BackupCDPTicket ticket = bc.getCDPTicket();
                    while( ticket != null && !context.isAbort())
                    {
                        handleCDPTicketBackup( context, ticket );
                        ticket = bc.getCDPTicket();
                    }
                }
                finally
                {
                    bc.releaseCDPMutex();
                }
            }
        }
    }

    static void handleCDPTicketBackup( GenericContext context, BackupCDPTicket ticket ) throws PathResolveException, SQLException, PoolReadOnlyException, Throwable
    {
        ticket.getFileList();
        for (CdpEvent ev : ticket.getFileList())
        {
            FileSystemElemNode node = null;
            RemoteFSElem remoteFSElem = ev.getElem();

            // DETECT PATH IN STORAGE
            String abs_path = context.getRemoteElemAbsPath(remoteFSElem );

            // RESOLVE PATH TO NODE IF POSSIBLE
            node = context.poolhandler.resolve_elem_by_path( abs_path );
         
            // MAP NODE IN THIS CONTEXT
            if (node != null)
            {
                node = context.poolhandler.em_find(FileSystemElemNode.class, node.getIdx());
            }
            backupRemoteFSElem(context, remoteFSElem, node, /* recursive*/ true, /*onlyNewer*/ false );
        }

    }


    static void unrealizeChildren(FileSystemElemNode node)
    {
        //node.getChildren().unRealize();
        if (node.getChildren().isRealized())
        {
            for (int i = 0; i < node.getChildren().size(); i++)
            {
                FileSystemElemNode child = node.getChildren().get(i);
                child.getHashBlocks().unRealize();
                child.getXaNodes().unRealize();
                child.getLinks().unRealize();


                if (child.getChildren().isRealized())
                {
                    unrealizeChildren(child);
                }
            }
            node.getChildren().unRealize();
        }
    }

    static boolean handle_single_fs_entry( GenericContext context, FileSystemElemNode dbNode, RemoteFSElem remoteFSElem ) throws PoolReadOnlyException, PathResolveException, SQLException
    {
        context.stat.check_stat();
        context.stat.addTotalStat( remoteFSElem  );
        boolean ret = true;

        long ts = System.currentTimeMillis();
        

        // ELEM DOESNT EXIST IN DB?
        if (dbNode == null)
        {
            context.setStatus(Main.Txt("Erzeuge ") + remoteFSElem.getPath() );

            if (remoteFSElem.isLazyAclInfo())
            {
                remoteFSElem.setAclinfoData( context.apiEntry.getApi().readAclInfo(remoteFSElem));
            }
            
            context.stat.addTransferStat(remoteFSElem);

            // ADD NEW
            dbNode = insert_remotefsentry_to_pool( context, remoteFSElem, ts );

            if (dbNode != null && context.getIndexer() != null)
            {
                context.getIndexer().addToIndexAsync(dbNode.getAttributes(), context.getArchiveJob());
            }

            // ADD ARCHIVE LINK
            if (context.getArchiveJob() != null)
            {
                context.poolhandler.addArchiveLink( context.getArchiveJob(), dbNode );
            }


            return (dbNode != null) ? true : false;
        }

        boolean do_update = false;

        // CHECK FOR UPADTE
        if (check_timestamps_differ(dbNode, remoteFSElem))
            do_update = true;

        if (!do_update && check_sizes_differ(dbNode, remoteFSElem))
            do_update = true;

        if (!do_update && dbNode.getAttributes().isDeleted())
            do_update = true;


        if (do_update)
        {
            context.setStatus(Main.Txt("Aktualisiere ") + remoteFSElem.getPath() );

            // RELOAD MISSING LAZY ACL
            if (remoteFSElem.isLazyAclInfo())
            {
                remoteFSElem.setAclinfoData( context.apiEntry.getApi().readAclInfo(remoteFSElem));
            }
            
            // UPDATE EXISTING
            ret = update_remotefsentry_to_pool( context, dbNode, remoteFSElem, ts );
            if (ret)
            {
                // READ EXTENDED ATTRIBUTE IF AVAILABLE
//                String adata = read_attribute_data(context, remoteFSElem, dbNode);

                // IF WE REACH THIS, THE FILE WAS UPDATED, ADD NEW ATTRIBUTES TO DB
                context.poolhandler.add_new_fse_attributes(dbNode, remoteFSElem, ts);

                try
                {
                    write_bootstrap_data(context, dbNode.getAttributes());
                }
                catch (Exception exc)
                {
                    Log.err(Main.Txt("Fehler beim Schreiben der Bootstrapdaten für neues Attribut") , dbNode.toString(), exc);
                }
                if (context.getIndexer() != null)
                {
                    context.getIndexer().addToIndexAsync(dbNode.getAttributes(), context.getArchiveJob());
                }

            }
            
            // ADD ARCHIVE LINK
            if (context.getArchiveJob() != null)
            {
                context.poolhandler.addArchiveLink( context.getArchiveJob(), dbNode );
            }
        }

        return ret;
    }

    static boolean check_timestamps_differ( FileSystemElemNode fsenode, RemoteFSElem remoteFSElem)
    {
        if (fsenode == null)
            return true;

        FileSystemElemAttributes node = fsenode.getAttributes();
        if (node == null)
        {
            Log.err(Main.Txt("Node " + fsenode.getIdx() + " hat kein Attribut"));
            return true;
        }
        if (remoteFSElem.getMtimeMs() != node.getModificationDateMs())
            return true;

        if (remoteFSElem.getCtimeMs() != node.getCreationDateMs())
            return true;

        return false;

    }
    static boolean check_sizes_differ( FileSystemElemNode fsenode, RemoteFSElem remoteFSElem)
    {
        if (fsenode == null)
            return true;

        if (fsenode.isDirectory())
            return false;

        FileSystemElemAttributes node = fsenode.getAttributes();
        if (node == null)
        {
            Log.err(Main.Txt("Node " + fsenode.getIdx() + " hat kein Attribut"));
            return true;
        }

        if (remoteFSElem.getDataSize() != node.getFsize())
            return true;
        if (remoteFSElem.getStreamSize() != node.getStreamSize())
            return true;

        return false;

    }
    private static boolean write_complete_node( GenericContext context, RemoteFSElem remoteFSElem, FileSystemElemNode node, long ts) throws PoolReadOnlyException
    {
        FileHandle handle = null;
        RemoteFSElemWrapper remote_handle = null;
        try
        {
            // OPEN LOCAL FILE HANDLE
            handle = context.poolhandler.open_file_handle(node, /*create*/ true);
            
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote file handle " + remoteFSElem.toString());


            // COPY DATA
            long len = remoteFSElem.getDataSize();

            // IF SIZE IS BIGGER THAN A HASHBLOC, WE USE HASHBLOCKS
//            if (len < context.hash_block_size)
            {
                long offset = 0;

                // NEW BLOCKS BELONG TO FILE -> NO REORGANIZATION NEEDED
                boolean reorganize = false;

                int blockCnt = 0;
                while (len > 0)
                {
                    int rlen = context.hash_block_size;
                    if (rlen > len)
                        rlen = (int)len;

                    // READ DATA FROM CLIENT
                    int realLen = 0;
                    byte[] data = null;
                    String hashValue = null;

                    if (hashOnAgent)
                    {                        
                        if (context.isCompressed() || context.isEncrypted())
                        {
                            CompEncDataResult res = context.apiEntry.getApi().read_and_hash_encrypted_compressed(remote_handle,
                                    offset, rlen, context.isEncrypted(), context.isCompressed());

                            if (res != null)
                            {
                                data = res.getData();
                                hashValue = res.getHashValue();
                                // FIRST DECRYPT, THEN DECOPMPRESS, OPPOSITE TO ENCODING AND ENCRYPTING IN AGENTAPI
                                if (context.isEncrypted())
                                {
                                    // DECRYPT DATA TO LENGTH AFTER DECOMPRESSION BEFORE ENCRYPTION
                                    data = CryptTools.decryptXTEA8(data, res.getCompLen());
                                }
                                if (context.isCompressed())
                                {
                                    data = ZipUtilities.lzf_decompressblock(data);
                                }
                                if (data.length != rlen)
                                {
                                    Log.err("Cannot decompress read_and_hash remote file handle " + remoteFSElem.toString());
                                    // FALLBACK
                                    HashDataResult _res = null;
                                    _res = context.apiEntry.getApi().read_and_hash(remote_handle, offset, rlen);
                                    if (_res != null)
                                    {
                                        hashValue = _res.getHashValue();
                                        data = _res.getData();
                                    }
                                }
                            }
                        }
                        else
                        {
                            HashDataResult _res = null;
                            _res = context.apiEntry.getApi().read_and_hash(remote_handle, offset, rlen);
                            if (_res != null)
                            {
                                hashValue = _res.getHashValue();
                                data = _res.getData();
                            }

                        }

                        if (data == null || data.length != rlen)
                        {
                            throw new IOException("Cannot read_and_hash remote file handle " + remoteFSElem.toString());
                        }

                        realLen = data.length;
                    }

                    if (!hashOnAgent)
                    {
                        data = context.apiEntry.getApi().read(remote_handle, offset, rlen);
                        if (data == null || data.length == 0)
                        {
                            throw new IOException("Cannot read_and_hash remote file handle " + remoteFSElem.toString());
                        }

                        byte[] hash = ((BackupContext)context).digest.digest(data);
                        hashValue = CryptTools.encodeUrlsafe( hash );
                        realLen = data.length;
                    }

                    // WRITE FILE TO FILEBUFF
                    if (!speed_test_no_write)
                    {
                        if (len - realLen == 0)
                            context.getWriteRunner().addAndCloseElem(handle, data, rlen, offset);
                        else
                            context.getWriteRunner().addElem(handle, data, realLen, offset );
                        //handle.writeFile(data, realLen, offset);
                    }
                    
                    if (context.getWriteRunner().isWriteError())
                    {                        
                        break;
                    }

                    

                    // ADD HASHENTRY TO DB
                    HashBlock hb = context.poolhandler.create_hashentry(node, hashValue, null, offset, realLen, reorganize, ts);

                    node.getHashBlocks().addIfRealized(hb);
                    
                    context.stat.addTransferLen( realLen );
                    blockCnt++;
                    if (blockCnt % 50 == 0)
                    {
                        context.stat.check_stat();
                    }

                    offset += realLen;
                    len -= realLen;
                }
            }
            if (context.getWriteRunner().isWriteError())
            {
                throw new IOException("Write error in StorageNode");
            }

            return true;
        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Datei wurde nicht gesichert", e.getMessage());
            context.setStatus(VSMCMain.Txt("Entferne") + " " + remoteFSElem.getPath() );
            context.poolhandler.remove_fse_node(node, true);
            context.apiEntry.getApi().get_properties();
        }
        catch (Exception e)
        {
            Log.err( "Fehler beim Schreiben von Node", node.toString(), e);
            context.poolhandler.remove_fse_node(node, true);
        }
        finally
        {
//            // CLOSE LOCAL AND REMOTE HANDLE
//            if (handle != null)
//            {
//                try
//                {
//                    handle.close();
//                }
//                catch (IOException iOException)
//                {
//                    iOException.printStackTrace();
//                }
//            }
            if (context.getWriteRunner().isWriteError())
            {
                byte[] b = new byte[0];
                context.getWriteRunner().addAndCloseElem(handle, b, b.length, 0);
            }
            if (remote_handle != null)
            {
                try
                {
                    context.apiEntry.getApi().close_data(remote_handle);
                }
                catch (IOException e)
                {
                    Log.err( "Fehler beim Schließen von Node", node.toString(), e);
                }
            }
        }
        return false;
    }
    private static boolean write_complete_node_dedup( GenericContext context, RemoteFSElem remoteFSElem, FileSystemElemNode node, long ts) throws PoolReadOnlyException
    {

        RemoteFSElemWrapper remote_handle = null;
        try
        {            
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote file handle " + remoteFSElem.toString());


            // COPY DATA
            long len = remoteFSElem.getDataSize();
            long offset = 0;
            int blockCnt = 0;

            while (len > 0)
            {
                int read_len = context.hash_block_size;
                if (read_len > len)
                    read_len = (int)len;

                // READ DATA FROM CLIENT

                blockCnt++;
                if (blockCnt % 50 == 0)
                {
                    context.stat.check_stat();
                }
                if (blockCnt % 500 == 0)
                {
                    checkSpace( context );
                }
               
                String remote_hash;
                remote_hash = context.apiEntry.getApi().read_hash(remote_handle, offset, read_len, CS_Constants.HASH_ALGORITHM);

                if (remote_hash == null)
                    throw new IOException("Cannot retrieve hash from offset " + offset + " len " + read_len + " of remote file " + remoteFSElem.toString());

                // CHECK FOR EXISTING BLOCK (DEDUP)
                DedupHashBlock block;
                try
                {
                    block = check_for_existing_block( context, remote_hash, checkDHBExistance );
                }
                catch (PathResolveException pathResolveException)
                {
                    // DID WE FIND A BLOCK WITH NO FS ENTRY?
                    Log.err("Verlorener Hashblock wird wiederbelebt", remote_hash, pathResolveException);
                    block = reviveHashBlock(  context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len );
                }


                if (block != null)
                {
                    // OKAY, WE FOUND EARLIER BLOCK, REGISTER HASHBLOCK FOR THIS FILE AND LEAVE
                    HashBlock hb = context.poolhandler.create_hashentry(node, remote_hash, block, offset, read_len, /*reorganize*/ false, ts);
                    node.getHashBlocks().addIfRealized(hb);

                    // UPDATE BOOTSTRAP
                    write_bootstrap_data(context, block, hb);

                    context.stat.addDedupBlock(block);
                }
                else
                {
                    // OKAY, BLOCK DIFFERS OR IS NOT EXISTANT AND THERE WAS NO EXISTING DEDUPBLOCK FOUND
                    transfer_block_and_update_db(context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len, /*xa*/false, ts);

                    context.stat.addTransferBlock();
                    context.stat.addTransferLen( read_len );
                }

                if (context.getWriteRunner().isWriteError())
                {
                    context.getWriteRunner().resetError();
                    throw new IOException("Write error in StorageNode");
                }

                offset += read_len;
                len -= read_len;
            }

            return true;
        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Datei wurde nicht gesichert",e.getMessage());
            context.setStatus(VSMCMain.Txt("Entferne") + " " + remoteFSElem.getPath() );
            context.poolhandler.remove_fse_node(node, true);
            context.apiEntry.getApi().get_properties();
        }
        catch (Exception e)
        {

            Log.err( "Fehler beim Schreiben von Node", node.toString(), e);
            context.setStatus(VSMCMain.Txt("Entferne") + " " + remoteFSElem.getPath() );
            context.poolhandler.remove_fse_node(node, true);
            context.apiEntry.getApi().get_properties();
        }
        finally
        {

            if (remote_handle != null)
            {
                try
                {
                    context.apiEntry.getApi().close_data(remote_handle);
                }
                catch (IOException e)
                {
                    Log.err( "Fehler beim Schließen von Node", node.toString(), e);
                    context.poolhandler.remove_fse_node(node, true);
                }
            }
        }
        return false;
    }
   
//    private static String read_attribute_data( GenericContext context, RemoteFSElem remoteFSElem, FileSystemElemNode node )
//    {
//        if (remoteFSElem.isSymbolicLink())
//            return remoteFSElem.getLinkPath();
//
//        if (remoteFSElem.getAclinfo() != RemoteFSElem.ACLINFO_NONE)
//        {
//            AttributeList alist = context.apiEntry.getApi().get_attributes(remoteFSElem);
//            if (alist != null && !alist.getList().isEmpty())
//            {
//                XStream xs = new XStream();
//                String data = xs.toXML(alist);
//                data = ZipUtilities.deflateString( data );
//                return data;
//            }
//        }
//        return null;
//    }

    private static FileSystemElemNode insert_remotefsentry_to_pool( GenericContext context, RemoteFSElem remoteFSElem, long actTimestamp ) throws PoolReadOnlyException, PathResolveException
    {
        String abs_path = context.getRemoteElemAbsPath( remoteFSElem );

        FileSystemElemNode node = null;

        // FIRST DB STUFF: CREATE DATABASE NODE
        try
        {
            node = context.poolhandler.create_fse_node(abs_path, remoteFSElem, actTimestamp);

        }
        catch (IOException iOException)
        {
            Log.err("Node konnte nicht erzeugt werden", remoteFSElem.toString(), iOException);
            return null;
        }

//        // READ AND SET EXTENDED ATTRIBUTE
//        String adata = read_attribute_data(context, remoteFSElem, node);
//        if (adata != null)
//        {
//            node.getAttributes().setXattribute(adata);
//        }
        

        // NOW UPDATE NEW NODE DATA TO DB
        try
        {
            // CREATE IN REAL FS
            List<AbstractStorageNode> s_nodes = context.poolhandler.register_fse_node_to_db(node);

            context.poolhandler.instantiate_in_fs(s_nodes, node);

        }
        catch (IOException iOException)
        {
            Log.err("Anmeldung bei Datenbank schlug fehl", node.toString(), iOException);
            context.poolhandler.remove_fse_node(node, true);
            return null;
        }
        


        // WRITE FILE DATA
        if (remoteFSElem.isFile())
        {
            if (context.poolhandler.getPool().isLandingZone())
            {
                if (!write_complete_node(context, remoteFSElem, node, actTimestamp))
                {
                    // TODO: LGGING OF FILES WHICH CANNOT BE SAVED
                    return null;
                }
                // READ AND WRITE STREAM DATA ( NTFS STREAMS, MAC RESOURCES, APPLEDOULKE)
                write_stream_data( context, remoteFSElem, node, actTimestamp );
            }
            else
            {
                if (!write_complete_node_dedup(context, remoteFSElem, node, actTimestamp))
                {
                    // TODO: LGGING OF FILES WHICH CANNOT BE SAVED
                    return null;
                }
                // READ AND WRITE STREAM DATA ( NTFS STREAMS, MAC RESOURCES, APPLEDOULKE)
                write_stream_data_dedup( context, remoteFSElem, node, actTimestamp );
            }
        }
        

        // WRITE BOOTSTRAPDATA
        try
        {
            write_bootstrap_data(context, node);
        }
        catch (IOException iOException)
        {
            Log.err("Schreiben der Bootstrapdaten schlug fehl", node.toString(), iOException );
        }


        return node;
    }

    static boolean write_stream_data( GenericContext context, RemoteFSElem remoteFSElem, FileSystemElemNode node, long ts)
    {
        // NOTHING TO DO ?
        if (remoteFSElem.getStreamSize() <= 0)
            return true;

        
        FileHandle handle = null;
        RemoteFSElemWrapper remote_handle = null;
        try
        {
            
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_stream_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote file handle " + remoteFSElem.toString());

            // OPEN LOCAL FILE HANDLE
            handle = context.poolhandler.open_xa_handle(node, remoteFSElem.getStreaminfo(), /*create*/ true);


            // COPY DATA
            long len = remoteFSElem.getStreamSize();

            long offset = 0;

            // NEW BLOCKS BELONG TO FILE -> NO REORGANIZATION NEEDED
            boolean reorganize = false;

            while (len > 0)
            {
                int rlen = context.hash_block_size;
                if (rlen > len)
                    rlen = (int)len;

                // READ DATA FROM CLIENT
                int realLen = 0;

                String hashValue = null;
                byte[] data = null;
                
                if (context.isCompressed() || context.isEncrypted())
                {
                    CompEncDataResult res = context.apiEntry.getApi().read_and_hash_encrypted_compressed(remote_handle,
                            offset, rlen, context.isEncrypted(), context.isCompressed());

                    if (res != null)
                    {
                        data = res.getData();
                        hashValue = res.getHashValue();
                        // FIRST DECRYPT, THEN DECOPMPRESS, OPPOSITE TO ENCODING AND ENCRYPTING IN AGENTAPI
                        if (context.isEncrypted())
                        {
                            // DECRYPT DATA TO LENGTH AFTER DECOMPRESSION BEFORE ENCRYPTION
                            data = CryptTools.decryptXTEA8(data, res.getCompLen());
                        }
                        if (context.isCompressed())
                        {
                            data = ZipUtilities.lzf_decompressblock(data);
                        }
                        if (data.length != rlen)
                        {
                            Log.err("Cannot decompress read_and_hash remote file handle " + remoteFSElem.toString());
                            // FALLBACK
                            HashDataResult _res = null;
                            _res = context.apiEntry.getApi().read_and_hash(remote_handle, offset, rlen);
                            if (_res != null)
                            {
                                hashValue = _res.getHashValue();
                                data = _res.getData();
                            }
                        }
                    }
                }
                else
                {
                    HashDataResult res = null;
                    res = context.apiEntry.getApi().read_and_hash(remote_handle, offset, rlen);
                    if (res != null)
                    {
                        hashValue = res.getHashValue();
                        data = res.getData();
                    }
                }

               if (data == null || data.length != rlen)
                {
                    throw new IOException("Cannot read_and_hash remote file handle " + remoteFSElem.toString());
                }


                realLen = data.length;
                // WRITE FILE TO FILEBUFF
                if (!speed_test_no_write)
                {
                    if (len - realLen == 0)
                    {
                        context.getWriteRunner().addAndCloseElem(handle, data, realLen, offset );
                        handle =  null;  // IS CLOSED IN WRITE RUNNER
                    }
                    else
                        context.getWriteRunner().addElem(handle, data, realLen, offset );
                    //handle.writeFile(res.getData(), realLen, offset);
                }

                
                // ADD HASHENTRY TO DB
                XANode xaNode = context.poolhandler.create_xa_hashentry(node, hashValue, remoteFSElem.getStreaminfo(), null, offset, realLen, reorganize, ts);
                node.getXaNodes().addIfRealized(xaNode);

                context.stat.addTransferLen( realLen );

                offset += realLen;
                len -= realLen;
            }

            if (context.getWriteRunner().isWriteError())
            {
                throw new IOException("Write error in StorageNode");
            }

            return true;

        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Streamdaten wurden nicht gesichert", node.toString() + ": " + e.getMessage());
        }
        catch (Exception e)
        {
            Log.err( "Fehler beim Schreiben von StreamDaten", node.toString(), e);
            
        }
        finally
        {
 
            if (remote_handle != null)
            {
                try
                {
                    context.apiEntry.getApi().close_data(remote_handle);
                }
                catch (IOException e)
                {
                    Log.err( "Fehler beim Schließen von Node", node.toString(), e);
                }
            }
            if (handle != null)
            {
                try
                {
                    handle.close();
                }
                catch (IOException e)
                {
                    Log.err( "Fehler beim Schließen von Node", node.toString(), e);
                }
            }
        }
        return false;
    }

    static boolean write_stream_data_dedup( GenericContext context, RemoteFSElem remoteFSElem, FileSystemElemNode node, long ts)
    {
        // NOTHING TO DO ?
        if (remoteFSElem.getStreamSize() <= 0)
            return true;

        RemoteFSElemWrapper remote_handle = null;

        try
        {
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_stream_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote xa handle " + remoteFSElem.toString());


            long len = remoteFSElem.getStreamSize();
            long offset = 0;




            int block_number = -1;
            while((long)(block_number+1) * context.hash_block_size < len)
            {

                block_number++;

                // TO PREVENT OVERFLOW OF INT MATH DO THIS IN TWO STEPS
                offset = block_number;
                offset *= context.hash_block_size;


                int read_len = context.hash_block_size;
                if (read_len + offset > len)
                    read_len = (int) (len - offset);

                String remote_hash = null;




                // READ REMOTE HASH IF NECESSARY
                remote_hash = context.apiEntry.getApi().read_hash(remote_handle, offset, read_len, CS_Constants.HASH_ALGORITHM);

                if (remote_hash == null)
                    throw new IOException("Cannot retrieve hash from offset " + offset + " len " + read_len + " of remote file " + remoteFSElem.toString());



                // CHECK FOR EXISTING BLOCK (DEDUP)
                DedupHashBlock block = null;

                try
                {
                    block = check_for_existing_block(context, remote_hash, checkDHBExistance);
//                    if (block != null && context.getBugFixHash() != null)
//                    {
//                        remote_hash = context.getBugFixHash();
//                    }
                }
                catch (PathResolveException pathResolveException)
                {
                    // DID WE FIND A BLOCK WITH NO FS ENTRY?
                    Log.err("Verlorener Hashblock wird wiederbelebt", remote_hash, pathResolveException);
                    block = reviveHashBlock(  context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len );
                }

                if (block != null)
                {
                    // OKAY, WE FOUND EARLIER BLOCK, REGISTER HASHBLOCK FOR THIS FILE AND LEAVE
                    XANode xa = context.poolhandler.create_xa_hashentry(node, remote_hash, remoteFSElem.getStreaminfo(), block, offset, read_len, /*reorganize*/ true, ts);

                    // UPDATE BOOTSTRAP
                    write_bootstrap_data(context, block, xa);

                    context.stat.addDedupBlock(block);
                    continue;
                }

                // OKAY, BLOCK DIFFERS OR IS NOT EXISTANT AND THERE WAS NO EXISTING DEDUPBLOCK FOUND
                transfer_block_and_update_db(context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len, /*xa*/ true, ts);

                context.stat.addTransferBlock();
                context.stat.addTransferLen( read_len );

                if (context.getWriteRunner().isWriteError())
                {
                    context.getWriteRunner().resetError();
                    throw new IOException("Write error in StorageNode");
                }

            }
            return true;

        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Fehler beim Schreiben von DedupStreamDaten", node.toString() + ": " + e.getMessage());
            return false;
        }
        catch (Exception e)
        {
            Log.err( "Fehler beim Schreiben von DedupStreamDaten", node.toString(), e);
            return false;
           // context.poolhandler.remove_fse_node(node);
        }
        finally
        {
            try
            {
                context.apiEntry.getApi().close_data(remote_handle);
            }
            catch (IOException e)
            {
                Log.err( "Fehler beim Schließen von Node", node.toString(), e);
            }
        }
    }


    static void write_bootstrap_data( GenericContext context,  FileSystemElemNode node ) throws IOException
    {
        if (withBootstrap)
            context.getWriteRunner().addBootstrap( context.poolhandler, node );
        //context.poolhandler.write_bootstrap_data( node );
    }
    static void write_bootstrap_data( GenericContext context,  FileSystemElemAttributes attr ) throws IOException
    {
        if (withBootstrap)
            context.getWriteRunner().addBootstrap( context.poolhandler, attr );
        //context.poolhandler.write_bootstrap_data( node );
    }

    static private List<HashBlock> remove_older_hashblocks( List<HashBlock> hash_block_list )
    {
        List<HashBlock> ret = new ArrayList<HashBlock>();

        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hash_block_list, new Comparator<HashBlock>() {

            @Override
            public int compare( HashBlock o1, HashBlock o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        HashBlock lastHashBlock = null;
        for (int i = 0; i < hash_block_list.size(); i++)
        {
            HashBlock hashBlock = hash_block_list.get(i);
            if (lastHashBlock != null)
            {
                if (lastHashBlock.getBlockOffset() == hashBlock.getBlockOffset())
                {                    
                    continue;
                }
            }
            ret.add(hashBlock);

            lastHashBlock = hashBlock;
        }
        return ret;
    }

    static private void remove_older_xablocks( List<XANode> xa_block_list )
    {
        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(xa_block_list, new Comparator<XANode>()
        {

            @Override
            public int compare( XANode o1, XANode o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        XANode lastXaBlock = null;
        for (int i = 0; i < xa_block_list.size(); i++)
        {
            XANode hashBlock = xa_block_list.get(i);
            if (lastXaBlock != null)
            {
                if (lastXaBlock.getBlockOffset() == hashBlock.getBlockOffset())
                {
                    xa_block_list.remove(i);
                    i--;
                    continue;
                }
            }

            lastXaBlock = hashBlock;
        }
    }

    static private boolean update_remote_data_entry_to_pool(  GenericContext context, FileSystemElemNode node, RemoteFSElem remoteFSElem, long ts )
    {
        RemoteFSElemWrapper remote_handle = null;
        try
        {
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote file handle " + remoteFSElem.toString());
            

            long len = remoteFSElem.getDataSize();
            long offset = 0;

            // READ HASHBLOCKS
            List<HashBlock> hash_block_list = node.getHashBlocks().getList(context.poolhandler.getEm());

            hash_block_list = remove_older_hashblocks( hash_block_list );

            long blockCnt = 0;

            int block_number = -1;
            while((long)(block_number+1) * context.hash_block_size < len)
            {
                // GO THROUGH ALL BLOCKS
                boolean transfer_block = false;

                block_number++;

                // TO PREVENT OVERFLOW OF INT MATH DO THIS IN TWO STEPS
                offset = block_number;
                offset *= context.hash_block_size;

                int read_len = context.hash_block_size;
                if (read_len + offset > len)
                    read_len = (int) (len - offset);


                blockCnt++;
                if (blockCnt % 50 == 0)
                {
                    context.stat.check_stat();
                }

                String local_hash = null;
                String remote_hash = null;


                // IS THIS A BLOCK NOT INSIDE ORIG ?
                if (block_number >= hash_block_list.size())
                    transfer_block = true;


                if (!transfer_block)
                {
                    // DIRECT ADDRESSING TRY
                    HashBlock hb = hash_block_list.get(block_number);
                    if (hb.getBlockLen() == read_len && hb.getBlockOffset() == offset)
                    {
                        local_hash = hb.getHashvalue();
                    }
                    else
                    {
                        // GET HASH FOR THIS BLOCK IN LIST
                        local_hash = get_actual_hash( hash_block_list, offset, read_len );
                    }

                    // DO WE HAVE A LOCAL HASH FOR THIS BLOCK?
                    if (local_hash == null || local_hash.length() == 0)
                    {
                        // NO, TRANSFER COMPLETE BLO
                        transfer_block = true;
                    }
                }

                // READ REMOTE HASH IF NECESSARY
                remote_hash = context.apiEntry.getApi().read_hash(remote_handle, offset, read_len, CS_Constants.HASH_ALGORITHM);

                if (remote_hash == null)
                    throw new IOException("Cannot retrieve hash from offset " + offset + " len " + read_len + " of remote file " + remoteFSElem.toString());

                if (check_hashes_differ( remote_hash, local_hash ))
                    transfer_block = true;

                // IF WE REACHED THIS WE HAVE FOUND AND CHECKED REMOTE AND LOCAL HASH OF SAME BLOCKPOSITION AND LENGTH
                // BOTH BLOCKS ARE IDENTICAL -> SKIP THIS BLOCK
                if (!transfer_block)
                {
                    context.stat.addCheckBlockSize(read_len);
                    continue;
                }

                // CHECK FOR EXISTING BLOCK (DEDUP)
                DedupHashBlock block = null;

                try
                {
                    block = check_for_existing_block(context, remote_hash, checkDHBExistance);
//                    if (block != null && context.getBugFixHash() != null)
//                    {
//                        remote_hash = context.getBugFixHash();
//                    }
                }
                catch (PathResolveException pathResolveException)
                {
                    // DID WE FIND A BLOCK WITH NO FS ENTRY?
                    Log.err("Verlorener Hashblock wird wiederbelebt", remote_hash, pathResolveException);
                    block = reviveHashBlock(  context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len );
                }

                if (block != null)
                {
                    // OKAY, WE FOUND EARLIER BLOCK, REGISTER HASHBLOCK FOR THIS FILE AND LEAVE
                    HashBlock hb = context.poolhandler.create_hashentry(node, remote_hash, block, offset, read_len, /*reorganize*/ true, ts);
                    node.getHashBlocks().addIfRealized(hb);

                    // UPDATE BOOTSTRAP
                    write_bootstrap_data(context, block, hb);

                    context.stat.addDedupBlock(block);
                    continue;
                }

                // OKAY, BLOCK DIFFERS OR IS NOT EXISTANT AND THERE WAS NO EXISTING DEDUPBLOCK FOUND
                transfer_block_and_update_db(context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len, /*xa*/false, ts);

                context.stat.addTransferBlock();
                context.stat.addTransferLen( read_len );

                if (context.isAbort())
                {
                    throw new Exception("Backup wurde abgebrochen");
                }

            }
            return true;

        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Fehler beim Update von Node", node.toString() + ": " + e.getMessage());
            return false;
        }
        catch (Exception e)
        {
            Log.err( "Fehler beim Update von Node", node.toString(), e);
            return false;
           // context.poolhandler.remove_fse_node(node);
        }
        finally
        {
            try
            {
                if (remote_handle != null)
                {
                    context.apiEntry.getApi().close_data(remote_handle);
                }
            }
            catch (IOException e)
            {
                Log.err( "Fehler beim Schließen von Node", node.toString(), e);
            }
        }
    }

    private static boolean update_remote_xa_entry_to_pool( GenericContext context, FileSystemElemNode node, RemoteFSElem remoteFSElem, long ts )
    {
        RemoteFSElemWrapper remote_handle = null;
        try
        {
            // TODO: WIN + MAC Agent!!!!
            // OPEN REMOTE HANDLE
            remote_handle = context.apiEntry.getApi().open_stream_data(remoteFSElem, AgentApi.FL_RDONLY);

            if (remote_handle == null)
                throw new ClientAccessFileException("Cannot open remote xa handle " + remoteFSElem.toString());
            

            long len = remoteFSElem.getStreamSize();
            long offset = 0;

            // READ XA HASHBLOCKS
            List<XANode> xa_block_list = node.getXaNodes().getList(context.poolhandler.getEm());

            remove_older_xablocks( xa_block_list );


            int block_number = -1;
            int blockCnt = 0;
            
            while((long)(block_number+1) * context.hash_block_size < len)
            {
                // GO THROUGH ALL BLOCKS
                boolean transfer_block = false;

                block_number++;

                // TO PREVENT OVERFLOW OF INT MATH DO THIS IN TWO STEPS
                offset = block_number;
                offset *= context.hash_block_size;


                int read_len = context.hash_block_size;
                if (read_len + offset > len)
                    read_len = (int) (len - offset);


                blockCnt++;
                if (blockCnt % 50 == 0)
                {
                    context.stat.check_stat();
                }

                String local_hash = null;
                String remote_hash = null;


                // IS THIS A BLOCK NOT INSIDE ORIG ?
                if (block_number >= xa_block_list.size())
                    transfer_block = true;


                if (!transfer_block)
                {
                    // GET HASH FOR THIS BLOCK
                    local_hash = get_actual_xa_hash( xa_block_list, offset, read_len );

                    // DO WE HAVE A LOCAL HASH FOR THIS BLOCK?
                    if (local_hash == null || local_hash.length() == 0)
                    {
                        // NO, TRANSFER COMPLETE BLO
                        transfer_block = true;
                    }
                }

                // READ REMOTE HASH IF NECESSARY
                remote_hash = context.apiEntry.getApi().read_hash(remote_handle, offset, read_len, CS_Constants.HASH_ALGORITHM);

                if (remote_hash == null)
                    throw new IOException("Cannot retrieve hash from offset " + offset + " len " + read_len + " of remote file " + remoteFSElem.toString());

                if (check_hashes_differ( remote_hash, local_hash ))
                    transfer_block = true;


                // IF WE REACHED THIS WE HAVE FOUND AND CHECKED REMOTE AND LOCAL HASH OF SAME BLOCKPOSITION AND LENGTH
                // BOTH BLOCKS ARE IDENTICAL -> SKIP THIS BLOCK
                if (!transfer_block)
                {
                    context.stat.addCheckBlockSize(read_len);
                    continue;
                }

                 // CHECK FOR EXISTING BLOCK (DEDUP)
                DedupHashBlock block = null;

                try
                {
                    block = check_for_existing_block(context, remote_hash, checkDHBExistance);
//                    if (block != null && context.getBugFixHash() != null)
//                    {
//                        remote_hash = context.getBugFixHash();
//                    }
                }
                catch (PathResolveException pathResolveException)
                {
                    // DID WE FIND A BLOCK WITH NO FS ENTRY?
                    Log.err("Verlorener Hashblock wird wiederbelebt", remote_hash, pathResolveException);
                    block = reviveHashBlock(  context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len );
                }
                
                if (block != null)
                {
                    // OKAY, WE FOUND EARLIER BLOCK, REGISTER HASHBLOCK FOR THIS FILE AND LEAVE
                    XANode xa = context.poolhandler.create_xa_hashentry(node, remote_hash, remoteFSElem.getStreaminfo(), block, offset, read_len, /*reorganize*/ true, ts);

                    // UPDATE BOOTSTRAP
                    write_bootstrap_data(context, block, xa);

                    context.stat.addDedupBlock(block);
                    continue;
                }

                // OKAY, BLOCK DIFFERS OR IS NOT EXISTANT AND THERE WAS NO EXISTING DEDUPBLOCK FOUND
                transfer_block_and_update_db(context, node, remote_handle, remote_hash, remoteFSElem.getStreaminfo(), offset, read_len, /*xa*/ true, ts);

                context.stat.addTransferBlock();
                context.stat.addTransferLen( read_len );

                if (context.isAbort())
                {
                    throw new Exception("Backup wurde abgebrochen");
                }
            }
            return true;

        }
        catch (ClientAccessFileException e)
        {
            Log.err( "Fehler beim Update von StreamDaten", node.toString() + ": " + e.getMessage());
            return false;
        }
        catch (Exception e)
        {
            // TODO: WIN + MAC Agent
            Log.err( "Fehler beim Update von StreamDaten", node.toString(), e);
            return false;
           // context.poolhandler.remove_fse_node(node);
        }
        finally
        {
            try
            {
                if (remote_handle != null)
                {
                    context.apiEntry.getApi().close_data(remote_handle);
                }
            }
            catch (IOException e)
            {
                Log.err( "Fehler beim Schließen von Node", node.toString(), e);
            }
        }
    }


    private static boolean update_remotefsentry_to_pool(  GenericContext context, FileSystemElemNode node, RemoteFSElem remoteFSElem, long ts )
    {
        boolean ret = true;


        if( remoteFSElem.isFile() )
        {
            ret = update_remote_data_entry_to_pool( context, node, remoteFSElem, ts );
        }

        // DO WE HAVE ADDITIONAL DATA
        if (ret && remoteFSElem.getStreamSize() > 0)
        {
            boolean lret = update_remote_xa_entry_to_pool( context,  node, remoteFSElem, ts );
        }
        return ret;
    }


    static void write_bootstrap_data( GenericContext context, DedupHashBlock block, HashBlock node ) throws IOException
    {
       //context.poolhandler.write_bootstrap_data(  block,  node );
        if (withBootstrap)
            context.getWriteRunner().addBootstrap( context.poolhandler, block, node);
    }
    static void write_bootstrap_data( GenericContext context, DedupHashBlock block, XANode node ) throws IOException
    {
        //context.poolhandler.write_bootstrap_data(  block,  node );
        if (withBootstrap)
            context.getWriteRunner().addBootstrap( context.poolhandler, block, node);
    }

    private static  boolean check_hashes_differ( String remote_hash, String local_hash )
    {        
        return !remote_hash.equals(local_hash);        
    }
    
    static DedupHashBlock check_for_existing_block( GenericContext context, String remote_hash, boolean checkDHBExistance ) throws PathResolveException, IOException
    {
        DedupHashBlock ret = check_for_existing_block(context.poolhandler, context.stat, context.hashCache, remote_hash );
        if (ret == null)
            return null;

        if (checkDHBExistance)
        {
            if (!checkDHBExistance(context, ret ))
                return null;
        }
        return ret;
    }

    public static DedupHashBlock check_for_existing_block( StoragePoolHandler poolhandler, StatCounter stat, HashCache hashCache, String remote_hash ) throws PathResolveException, IOException
    {
        DedupHashBlock dhb = null;

        // READ FROM CACHE
        if (hashCache.isInited())
        {
            long idx = hashCache.getDhbIdx(remote_hash);
            if (idx > 0)
            {
                try
                {
                    dhb = poolhandler.em_find(DedupHashBlock.class, idx);
                }
                catch (SQLException sQLException)
                {
                    Log.err("Kann HashBlock aus Cache nicht auflösen", sQLException);
                }
                if (dhb == null)
                {
                    Log.err("Kann HashBlock aus Cache nicht finden");
                    dhb = poolhandler.findHashBlock(remote_hash );
                }
                stat.addDhbCacheHit();
            }
            else
            {
                stat.addDhbCacheMiss();
            }
        }
        else
        {
            dhb = poolhandler.findHashBlock(remote_hash );
        }

        return dhb;
    }

    static boolean checkDHBExistance(IBackupHelper context,  DedupHashBlock dhb )
    {
        try
        {
            FileHandle fh = context.getPoolHandler().check_exist_dedupblock_handle(dhb);
            if (fh == null || !fh.exists())
            {
                // MABE BLOCK IS CREATED BUT NOT WRITTEN ALREADY
                try
                {
                    context.flushWriteRunner();
                }
                catch (InterruptedException ex)
                {
                }

                fh = context.getPoolHandler().check_exist_dedupblock_handle(dhb);
                if (fh == null || !fh.exists())
                {
                    Log.err("Filesystemblock nicht gefunden für Hash", ": " + dhb.getHashvalue());
                    return false;
                }
            }
        }
        catch (Exception e)
        {
            Log.err("Path exeption during check of DHB existence", ": " + dhb.getHashvalue() , e);
            return false;
        }
        return true;
    }

    public static DedupHashBlock createDedupHashBlock(IBackupHelper context, FileSystemElemNode node, String remote_hash,  int streamInfo, long offset, int read_len, boolean isXa, long ts ) throws PoolReadOnlyException, SQLException, IOException
    {
        final StatCounter stat = context.getStat();
        final HashCache hashCache = context.getHashCache();
        DedupHashBlock dhb = null;
        try
        {
            dhb = context.getPoolHandler().create_dedup_hashblock( node, remote_hash, read_len );
        }
        catch( java.sql.SQLIntegrityConstraintViolationException exc)
        {
            // WE CANNOT INSERT BECAUSE THIS BLOCK EXISTS ALREADY IN DB
            // 2 REASONS: BLOCK IS MISSING IN FS OR CACHE IS NOT UP TO DATE

            dhb = context.getPoolHandler().findHashBlock(remote_hash );
            if (dhb != null)
            {
                if (!checkDHBExistance( context, dhb))
                {
                    Log.err("Fehlender Hashblock wird erneut geschrieben", dhb.toString());
                    // WE DO NOTHING, SINCE WE HAVE TO WRITE THIS NODE NEW
                }
                else
                {
                    // DOUBLE CHECK
                    long idx = hashCache.getDhbIdx(remote_hash);
                    if (idx <= 0)
                        Log.warn("Hashblock fehlt im Cache, Cache wird aktualisiert", dhb.toString());
                    else
                        Log.warn("Hashblock fehlt nicht im Cache, Cache wird dennoch aktualisiert", dhb.toString());


                    // OKAY, WE FOUND EARLIER BLOCK, REGISTER HASHBLOCK FOR THIS FILE AND LEAVE
                    if (!isXa)
                    {
                        HashBlock hb = context.getPoolHandler().create_hashentry(node, remote_hash, dhb, offset, read_len, /*reorganize*/ false, ts);
                        node.getHashBlocks().addIfRealized(hb);
                    }
                    else
                    {
                        XANode hb = context.getPoolHandler().create_xa_hashentry(node, remote_hash, streamInfo, dhb, offset, read_len, /*reorganize*/ false, ts);
                        node.getXaNodes().addIfRealized(hb);
                    }

                    if (hashCache.isInited())
                    {
                        hashCache.addDhb(remote_hash, dhb.getIdx());
                        stat.setDhbCacheSize( hashCache.size() );
                    }
                    return null;
                }
            }
            else
            {
                // CANNOT HANDLE HERE -> SEND UPWARDS
                throw exc;
            }
        }   
        return dhb;
    }

    private static void transfer_block_and_update_db( GenericContext context, FileSystemElemNode node, RemoteFSElemWrapper remote_handle, String remote_hash,  int streamInfo, long offset, int read_len, boolean isXa, long ts ) throws PoolReadOnlyException, SQLException, IOException
    {
        // CREATE NEW DEDUP BLOCK ENTRY
        DedupHashBlock dhb = createDedupHashBlock(context, node, remote_hash, streamInfo, offset, read_len, isXa, ts);
        
        // Wenn festgestellt wird, das neuer Block nicht notwendig ist, dann raus
        if (dhb == null)
        {
            return;
        }

        
        FileHandle handle = null;
        try
        {
            // OPEN LOCAL DEDUP FILE HANDLE
            handle = context.poolhandler.open_dedupblock_handle(dhb, /*create*/ true);


            // READ DATA FROM CLIENT
            byte[] data = null;

            if (context.isCompressed() || context.isEncrypted())
            {
                CompEncDataResult res = context.apiEntry.getApi().readEncryptedCompressed(remote_handle, offset, read_len, context.isEncrypted(), context.isCompressed());

                data = res.getData();

                if (context.isEncrypted())
                {
                    // DECRYPT DATA TO LENGTH AFTER DECOMPRESSION BEFORE ENCRYPTION
                    data = CryptTools.decryptXTEA8(data, res.getCompLen());
                }
                if (context.isCompressed())
                {
                    data = ZipUtilities.lzf_decompressblock(data);
                }
                if (data.length != read_len)
                {
                    Log.err("Cannot uncompress file data");
                    // FALLBACK
                    data = context.apiEntry.getApi().read(remote_handle, offset, read_len);
                }
            }
            else
            {
                data = context.apiEntry.getApi().read(remote_handle, offset, read_len);
            }

            if (data == null || data.length == 0)
            {
                throw new IOException("Cannot read remote file handle ");
            }

            // WRITE DEDUPBLOCK TO BLOCKBUFF
            if (!speed_test_no_write)
            {
                //NOT PARALLEL, WE HAVE ONLY ONE BLOCK PER FILE
                context.getWriteRunner().addAndCloseElem(handle, data, data.length, 0);

                //handle.writeFile(data, data.length, /*offset*/ 0);
            }

            if (isXa)
            {
                // ADD HASHENTRY TO DB
                XANode xa = context.poolhandler.create_xa_hashentry(node, remote_hash, streamInfo, dhb, offset, read_len, /*reorganize*/ true,  ts);

                node.getXaNodes().addIfRealized(xa);

                // UPDATE BOOTSTRAP
                write_bootstrap_data( context, dhb, xa);
            }
            else
            {
                // REGISTER HASHBLOCK TO DB
                HashBlock hb = context.poolhandler.create_hashentry(node, remote_hash, dhb, offset, read_len, /*reorganize*/ true, ts);

                node.getHashBlocks().addIfRealized(hb);

                // UPDATE BOOTSTRAP
                write_bootstrap_data( context, dhb, hb);
            }

            // WRITE TO CACHE
            if (context.hashCache.isInited())
            {
                context.hashCache.addDhb(remote_hash, dhb.getIdx());
                context.stat.setDhbCacheSize( context.hashCache.size() );
            }
        }
        catch (Exception e)
        {
            Log.err( "Fehler beim Schreiben von DedupBlock", node.toString(), e);
            try
            {
                // CLEAN UP
                context.poolhandler.removeDedupBlock(dhb, node);
            }
            catch (Exception ex)
            {
                Log.err("Fehler beim Entfernen von DedupBlock", ex);
            }
            throw new IOException( "Fehler beim Schreiben von DedupBlock " + node.toString(), e);
        }
    }

    static private String get_actual_hash( List<HashBlock> hash_block_list, long offset, int read_len )
    {
        if (hash_block_list == null)
            return null;

        for (int i = 0; i < hash_block_list.size(); i++)
        {
            HashBlock hashBlock = hash_block_list.get(i);
            if (hashBlock.getBlockOffset() == offset)
            {
                if (hashBlock.getBlockLen() == read_len)
                    return hashBlock.getHashvalue();
            }
        }
        return null;
    }
    private static String get_actual_xa_hash( List<XANode> xa_block_list, long offset, int read_len )
    {
        if (xa_block_list == null)
            return null;

        for (int i = 0; i < xa_block_list.size(); i++)
        {
            XANode xaBlock = xa_block_list.get(i);
            if (xaBlock.getBlockOffset() == offset)
            {
                if (xaBlock.getBlockLen() == read_len)
                    return xaBlock.getHashvalue();
            }
        }
        return null;
    }

    public ScheduleStatusEntry getStatusEntry()
    {

        ScheduleStatusEntry entry = new ScheduleStatusEntry(sched);

        if (actualContext != null)
            actualContext.stat.fillStatusEntry(entry);
        

        return entry;
    }

    public Schedule getSched()
    {
        return sched;
    }

    public boolean abort()
    {
        abort = true;
        if (actualContext != null)
        {
            actualContext.setAbort( true );
        }

        int wait_s = 5;

        while (!finished && wait_s-- >= 0)
        {
            LogicControl.sleep( 1000 );
        }
        return finished;
    }

    public void close_entitymanager()
    {
        if (actualContext != null)
        {
            actualContext.poolhandler.close_entitymanager();
        }
    }

    private static DedupHashBlock reviveHashBlock(  GenericContext context, FileSystemElemNode node, RemoteFSElemWrapper remote_handle, String remote_hash, int streaminfo, long offset, int read_len )
    {
        Log.err("Todo: Rebuild lost hash");
        try
        {
            DedupHashBlock block = check_for_existing_block(context, remote_hash, false);

            return null;
        }
        catch (Exception pathResolveException)
        {
        }
        return null;
    }



}
