/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.AgentApiEntry;
import de.dimm.vsm.backup.Backup;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.fsengine.StoragePoolNubHandler;
import de.dimm.vsm.jobs.JobInterface;
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
public class HotFolderManager extends WorkerParent
{  
    StoragePoolNubHandler nubHandler;
    ArchiveJobContext actualContext;

    public HotFolderManager(StoragePoolNubHandler nubHandler)
    {
        super("HotFolderManager");
        this.nubHandler = nubHandler;
        actualContext = null;
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
    public void run()
    {
        int cnt = 0;
        while (!isShutdown())
        {
            LogicControl.sleep(1000);

            if (isPaused())
                continue;

            cnt++;
            // ALLE 5 SEK
            if ((cnt % 5) == 0)
            {
                try
                {
                    checkHotfolders();
                }
                catch (Exception e)
                {
                    Log.err("Fehler beim Auswerten des Hotfolders", e);
                }
            }
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
            setStatusTxt(Main.Txt("Prüfe") + " " + hotFolder.getName());

            // SKIP HF IF WE ARE ALREADY BUSY
            if (Main.get_control().getJobManager().isImportRunning(hotFolder))
                continue;

            if (!hotFolder.isDisabled())
            {
                checkHotfolder(hotFolder);
            }
        }
        setStatusTxt("");
    }

    public ArrayList<String> checkHotfolder(  HotFolder hotFolder )
    {
        ArrayList<String> pathList = new ArrayList<String>();
        AgentApiEntry api = null;
        try
        {
            api = LogicControl.getApiEntry(hotFolder.getIp(), hotFolder.getPort());
            if (!api.check_online())
            {
                return pathList;
            }
        }
        catch (Exception unknownHostException)
        {
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
                addHFError( hotFolder, elem, e.getMessage());
            }
        }
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
        if (hotFolder.isMmArchive())
        {
            setStatusTxt(Main.Txt("Archiviere mit MM") + ": " + elem.getName());
            if (!handleMMArchiv( hotFolder, elem))
            {
                addHFError( hotFolder, elem, getStatusTxt());
                return null;
            }
        }

        setStatusTxt(Main.Txt("Archiviere mit VSM") + ": " + elem.getName());

        StoragePool pool = nubHandler.getStoragePool( hotFolder.getPoolIdx());


        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler( nubHandler, pool, user, /*rdonly*/false);
        if (pool.getStorageNodes(sp_handler.getEm()).isEmpty())
            throw new Exception("No Storage for pool defined");
       

        sp_handler.realizeInFs();

        sp_handler.check_open_transaction();

        // HOTFOLDER CONTEXT RESOLVES THE FILESYSTEM WE ARE USING
        actualContext = new ArchiveJobContext( hotFolder, elem, api, sp_handler);
        actualContext.setAbortOnError(true);

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
             actualContext.setStatus("Fehler beim Sichern des Hotfolderelements " + elem.getName() + ": " + throwable.getMessage() );
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

        sp_handler.commit_transaction();
        sp_handler.close_transaction();
        sp_handler.close_entitymanager();
        
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
}
