/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.VariableResolver;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.fsengine.StoragePoolNubHandler;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.mail.NotificationEntry;
import de.dimm.vsm.net.IAgentIdleManager;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.HotFolder;
import de.dimm.vsm.records.HotFolderError;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;



/**
 *
 * @author Administrator
 */
public class HotFolderManager extends WorkerParent implements VariableResolver, IAgentIdleManager
{  
    StoragePoolNubHandler nubHandler;
    ArchiveJobContext actualContext;
    HotFolder actHotfolder;
    private static final String HF_AGENT_OFFLINE = "HF_AGENT_OFFLINE";
    private static final String HF_MM_ARCHIVE_ERROR = "HF_MM_ARCHIVE_ERROR";
    private static final String HF_BACKUP_ERR = "HF_BACKUP_ERR";
    private static final String HF_DELETE_ERR = "HF_DELETE_ERR";
    private static final String HF_OKAY = "HF_OKAY";
    private static final String HF_GROUP_ERROR = "HF_GROUP_ERROR";

    public HotFolderManager(StoragePoolNubHandler nubHandler)
    {
        super("HotFolderManager");
        this.nubHandler = nubHandler;
        actualContext = null;
        actHotfolder = null;

        Main.addNotification( new NotificationEntry(HF_AGENT_OFFLINE, 
                Main.Txt("Agent $AGENT ist offline"), Main.Txt("Der Agent $AGENT für Hotfolder $NAME kann nicht kontaktiert werden"), NotificationEntry.Level.WARNING, true));

        Main.addNotification( new NotificationEntry(HF_MM_ARCHIVE_ERROR,
                Main.Txt("Sicherung über MediaManager schlug fehl"), Main.Txt("Hotfolder $NAME kann Element $PATH nicht bei MM sichern"), NotificationEntry.Level.ERROR, false));
    
        Main.addNotification( new NotificationEntry(HF_BACKUP_ERR,
                Main.Txt("Fehler beim Sichern von Hotfolder"), Main.Txt("Element $PATH bei Agent $AGENT im Hotfolder $NAME kann nicht gesichert werden"), NotificationEntry.Level.ERROR, false));

        Main.addNotification( new NotificationEntry(HF_DELETE_ERR,
                Main.Txt("Fehler beim Löschen von Hotfolder"), Main.Txt("Element $PATH bei Agent $AGENT im Hotfolder $NAME kann nicht gelöscht werden"), NotificationEntry.Level.ERROR, false));

        Main.addNotification( new NotificationEntry(HF_OKAY,
                Main.Txt("Hotfolderauftrag $PATH beendet"), Main.Txt("Element $PATH bei Agent $AGENT im Hotfolder $NAME wurde erfolgreich gesichert"), NotificationEntry.Level.INFO, false));

        Main.addNotification( new NotificationEntry(HF_GROUP_ERROR,
                Main.Txt("Alle Fehler bei Hotfoldersicherung"), "HF_AGENT_OFFLINE,HF_MM_ARCHIVE_ERROR,HF_BACKUP_ERR,HF_DELETE_ERR,BA_GROUP_ERROR", NotificationEntry.Level.GROUP, false));

    }

    public boolean isHotFolderBusy( HotFolder hf )
    {
        if (actualContext != null)
        {
            HotFolder _hf = actualContext.getHotfolder();
            if (_hf != null && _hf.getIdx() == hf.getIdx())
                return true;
        }
        return false;
    }

   

    @Override
    public boolean initialize()
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        if (actualContext != null)
        {
            return actualContext.getStatus();
        }
        return getStatusTxt();
    }

    @Override
    public boolean isPersistentState()
    {
        return true;
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    @Override
    public int getCycleSecs() {
        return 5;
    }

    

    @Override
    public void startIdle()
    {
        
    }
    @Override
    public void stopIdle()
    {
        
    }
    @Override
    public void doIdle() 
    {
        if (isPaused())
            return;         
                   
        try
        {
            checkHotfolders();
        }
        catch (Exception e)
        {
            Log.err("Fehler beim Auswerten des Hotfolders", e);
        } 
    }

    // IS HANDLED INSIDE AgentIdleManager
    @Override
    public void run()
    {
        
        while (!isShutdown())
        {
            LogicControl.sleep(1000);

        }
        
        finished = true;
    }


    void checkHotfolders() throws SQLException
    {
        // OK, WE HAVE A NEW MINUTE, CHECK ALL SCHEDS
        GenericEntityManager em = LogicControl.get_base_util_em();

        List<HotFolder> res = em.createQuery("select r from HotFolder", HotFolder.class);


        if (res.isEmpty())
        {
            return;
        }

        for (int i = 0; i < res.size(); i++)
        {
            HotFolder hotFolder = res.get(i);
            actHotfolder = hotFolder;
            
            setStatusTxt(Main.Txt("Prüfe") + " " + hotFolder.getName());

            // SKIP HF IF WE ARE ALREADY BUSY
            if (Main.get_control().getJobManager().isImportRunning(hotFolder))
                continue;

            if (!hotFolder.isDisabled())
            {
                checkHotfolder(hotFolder);
            }
        }
        actHotfolder = null;
        setStatusTxt("");
    }

    private VariableResolver createSchedVr(final HotFolder hf)
    {
        VariableResolver vr = new VariableResolver()
        {

            @Override
            public String resolveVariableText( String s )
            {
                if (s.indexOf("$NAME") >= 0)
                {
                    String f = "";
                    f = hf.getName();
                    s = s.replace("$NAME", f );
                }
                if (s.indexOf("$AGENT") >= 0)
                {
                    String f = "";
                    f = hf.getIp() + ":" + hf.getPort();
                    s = s.replace("$AGENT", f );
                }

                return s;
            }            
        };
        return vr;
    }

    public ArrayList<String> checkHotfolder(  HotFolder hotFolder )
    {
        ArrayList<String> pathList = new ArrayList<String>();
        AgentApiEntry api = null;
        try
        {
            try
            {
                api = LogicControl.getApiEntry(hotFolder.getIp(), hotFolder.getPort(), /*withMsg*/ false);
                if (!api.isOnline())
                {
                    Main.fire(HF_AGENT_OFFLINE, hotFolder.toString(), createSchedVr(hotFolder));
                    return pathList;
                }
                Main.release(HF_AGENT_OFFLINE);
            }
            catch (Exception unknownHostException)
            {
                Main.fire(HF_AGENT_OFFLINE, unknownHostException.getMessage(), createSchedVr(hotFolder));
                return pathList;
            }

            // OKAY; WE ARE CONNECTED, NOW CHECK

            int folderIdx = 0;
            RemoteFSElem elem = null;
            RemoteFSElem mountPath = hotFolder.getMountPath();
            if (mountPath == null || mountPath.getPath() == null || mountPath.getPath().isEmpty())
            {
                Log.err("Ungültiger Pfad bei Hotfolder" , hotFolder.getName());
                return pathList;
            }
            long getSetttleTime_s = hotFolder.getSettleTime();
            String filter = hotFolder.getFilter();
            boolean onlyFiles = hotFolder.onlyFiles();
            boolean onlyDirs = hotFolder.onlyDirs();

            try
            {
                elem = api.getApi().check_hotfolder(mountPath, getSetttleTime_s, filter, onlyFiles, onlyDirs, folderIdx);
            }
            catch (Exception e)
            {
                Log.err("Hotfolder kann nicht überwacht werden", e );
            }
            if (elem != null)
            {
                try
                {
                    // IF NOT IN ERROR LIST
                    if (!checkHFError( hotFolder, elem ))
                    {
                        ArchiveJobContext context = handleHotfolder( api, hotFolder, elem);
                        if (context != null)
                        {
                            pathList.add(context.getBasePath());
                        }
                        actualContext = null;
                    }


                    // WALK THROUGH ALL ENTRIES SEQUENTIALLY
                    if (!hotFolder.isAtomicEval())
                    {
                        while (elem != null)
                        {
                            folderIdx++;
                            elem = api.getApi().check_hotfolder(mountPath, getSetttleTime_s, filter, onlyFiles, onlyDirs, folderIdx);
                            if (elem != null)
                            {
                                // IF NOT IN ERROR LIST
                                if (!checkHFError( hotFolder, elem ))
                                {
                                    ArchiveJobContext context = handleHotfolder( api, hotFolder, elem);
                                    if (context != null)
                                    {
                                        pathList.add(context.getBasePath());
                                    }
                                    actualContext = null;
                                }
                            }
                        }
                    }
                }
                catch (Throwable e)
                {
                    Log.err("Hotfolder kann nicht abgearbeitet werden", e );
                    Main.fire(HF_BACKUP_ERR, Main.Txt("Hotfolder kann nicht abgearbeitet werden") + ": " +  e.getMessage(), this);
                    addHFError( hotFolder, elem, e.getMessage());
                }
            }
        }
        finally
        {
            if (api != null)
            {
                try
                {
                    api.getFactory().close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        return pathList;
    }

    boolean handleMMArchiv(HotFolder hotFolder, RemoteFSElem elem)
    {
        Log.debug(Main.Txt("MM-Archiv startet") + ": " + elem.getName());
        JobInterface job = MMArchivManager.createArchivJob(hotFolder, elem, User.createSystemInternal());
        Main.get_control().getJobManager().addJobEntry(job);

        while (job.getJobState() != JobInterface.JOBSTATE.FINISHED_ERROR &&
                job.getJobState() != JobInterface.JOBSTATE.FINISHED_OK)
        {
            LogicControl.sleep(100);
        }

        if (job.getJobState() != JobInterface.JOBSTATE.FINISHED_OK )
        {
            Log.err(Main.Txt("MM-Archiv fehlerhaft") + ": " + elem.getName()  + ": " + job.getStatusStr() + ": " + job.getStatisticStr());
            setStatusTxt( Main.Txt("MM-Archiv schlug fehl") + " " + elem.getName() + ": " + job.getStatusStr() + ": " + job.getStatisticStr());
            return false;
        }
        Main.get_control().getJobManager().removeJobEntry(job);
        Log.debug(Main.Txt("MM-Archiv erfolgreich") + ": " + elem.getName());

        return true;
    }

    private ArchiveJobContext handleHotfolder( AgentApiEntry api, HotFolder hotFolder, RemoteFSElem elem ) throws Exception, Throwable
    {

        setStatusTxt(Main.Txt("Archiviere mit VSM") + ": " + elem.getName());

        StoragePool pool = nubHandler.getStoragePool( hotFolder.getPoolIdx());


        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( nubHandler, pool, user, /*rdonly*/false);


        if (pool.getStorageNodes(sp_handler.getEm()).isEmpty())
            throw new Exception("No Storage for pool defined");

        try
        {
            sp_handler.realizeInFs();

            sp_handler.check_open_transaction();

            // HOTFOLDER CONTEXT RESOLVES THE FILESYSTEM WE ARE USING
            actualContext = new ArchiveJobContext( hotFolder, elem, api, sp_handler);
            actualContext.setAbortOnError(true);
            actualContext.checkStorageNodes();

            // PRIOR TO VSM ARCHIVE TO MM?
            if (hotFolder.isMmArchive())
            {
                setStatusTxt(Main.Txt("Archiviere mit MM") + ": " + elem.getName());
                if (!handleMMArchiv( hotFolder, elem))
                {
                    Main.fire(HF_MM_ARCHIVE_ERROR, getStatusTxt(), this );
                    addHFError( hotFolder, elem, getStatusTxt());
                    return actualContext;
                }
            }

            //CREATE ARCHIVEJOB ENTRY
            actualContext.createJob();
            if (actualContext.getIndexer() != null && !actualContext.getIndexer().isOpen())
                actualContext.getIndexer().open();


            actualContext.setStatus(Main.Txt("HotFolder ist aktiv mit") + " " + elem.getName());

            long startSize = actualContext.getStat().getTotalSize();

            try
            {
                Backup.backupRemoteFSElem(actualContext, elem, actualContext.getArchiveJob().getDirectory(), /*recursive*/ true, /*onlyNewer*/ false);
            }
            catch (Throwable throwable)
            {
                 Log.err("Fehler beim Sichern des Hotfolderelements " + elem.getName(), throwable );
                 actualContext.setStatus(Main.Txt("Fehler beim Sichern des Hotfolderelements") + " " + elem.getName() + ": " + throwable.getMessage() );
                 Main.fire(HF_BACKUP_ERR, actualContext.getStatus(), this );

                 actualContext.setResult(false);
            }

            actualContext.getArchiveJob().setEndTime( new Date() );
            actualContext.getArchiveJob().setTotalSize( actualContext.getStat().getTotalSize() - startSize);

            // SUCCEEDED?
            if (actualContext.getResult() && actualContext.isErrorFree())
            {
                actualContext.getArchiveJob().setOk(true);
                try
                {
                    setStatusTxt(Main.Txt("HotFolder wird bereinigt"));
                    actualContext.setStatus(Main.Txt("HotFolder wird bereinigt") + " " + elem.getName());
                    api.getApi().deleteDir(elem, /*recursive*/ true);
                    actualContext.setStatus("");
                    setStatusTxt("");

                }
                catch (Exception exception)
                {
                     Log.err("Hotfolder kann nicht gelöscht werden", exception );
                     addHFError( hotFolder, elem, actualContext.getStatus());

                     Main.fire(HF_DELETE_ERR, exception.getMessage(), this );
                }
            }
            else
            {
                addHFError( hotFolder, elem, actualContext.getStatus());
            }


            // SET JOB RESULT
            actualContext.updateArchiveJob();

            // PUSH JOB TO INDEX
            if (actualContext.getIndexer() != null)
            {
                actualContext.getIndexer().addToIndexAsync( actualContext.getArchiveJob() );
                actualContext.getIndexer().flushAsync();
            }

            String summary = actualContext.getStat().buildSummary(  );
            Main.fire(HF_OKAY, Main.Txt("Zusammenfassung") + ":\n\n" + summary, this );
        }
        finally
        {
            sp_handler.commit_transaction();
            sp_handler.close_transaction();
            sp_handler.close_entitymanager();
        }
        return actualContext;
    }



    public static void addHFError( HotFolder hotFolder, RemoteFSElem elem, String text )
    {
        try
        {
            HotFolderError hferr = new HotFolderError();
            hferr.setHotfolder(hotFolder);
            hferr.setElem(elem);
            hferr.setErrtext(text);
            LogicControl.get_base_util_em().check_open_transaction();
            LogicControl.get_base_util_em().em_persist(hferr);
            LogicControl.get_base_util_em().commit_transaction();

            hotFolder.getErrlist().addIfRealized(hferr);
        }
        catch (Exception sQLException)
        {
            Log.err("Hotfolder-Fehler konnte nicht registriert werden", sQLException );
        }
    }

    boolean checkHFError( HotFolder hotFolder, RemoteFSElem elem )
    {
        String qry = "select T1 from HotFolderError T1 where T1.hotfolder_idx=" + hotFolder.getIdx();
        try
        {
            List<HotFolderError> errList = LogicControl.get_base_util_em().createQuery(qry, HotFolderError.class);
            for (int i = 0; i < errList.size(); i++)
            {
                HotFolderError hotFolderError = errList.get(i);
                if (hotFolderError.getElem().getPath().equals(elem.getPath()))
                {
                    return true;
                }
            }
        }
        catch (SQLException sQLException)
        {
            Log.err("Hotfolder-Fehler konnten nicht ermittelt werden", sQLException );
        }

        return false;
    }

    @Override
    public String resolveVariableText( String s )
    {
        if (s.indexOf("$NAME") >= 0)
        {
            String f = "";
            if (actualContext != null)
                f = actualContext.getHotfolder().getName();

            s = s.replace("$NAME", f );
        }
        if (s.indexOf("$PATH") >= 0)
        {
            String f = "";
            if (actualContext != null)
                f = actualContext.relPath;

            s = s.replace("$PATH", f );
        }
        if (s.indexOf("$POOL") >= 0)
        {
            String f = "";
            if (actualContext != null)
                f = actualContext.getPoolhandler().getPool().getName();

            s = s.replace("$POOL", f );
        }
        if (s.indexOf("$AGENT") >= 0)
        {
            String f = "";
            if (actualContext != null)
                f = actualContext.getHotfolder().getIp();
            else if(actHotfolder != null)
                f = actHotfolder.getIp();

            s = s.replace("$AGENT", f );
        }
        return s;
    }

}
