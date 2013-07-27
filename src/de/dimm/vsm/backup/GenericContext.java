/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.CS_Constants;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.StatCounter;
import de.dimm.vsm.fsengine.HashCache;
import de.dimm.vsm.fsengine.FSEIndexer;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.Excludes;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Administrator
 */
public abstract class GenericContext implements IBackupHelper
{
    protected AgentApiEntry apiEntry;
    protected StoragePoolHandler poolhandler;
    protected StatCounter stat;
    protected int hash_block_size;
    private boolean result;
    private boolean abort;
    protected String basePath;
    protected boolean abortOnError;
    
    protected long nextCheckByteSize = 0;  // StoargeNodes WERDEN BEI ERRICHEN DIESER GRÖßE GEPR�FT

    protected JOBSTATE state;
    protected String status;
    protected boolean open = false;

    protected HandleWriteRunner writeRunner;

    static int openCounter;

    protected ExecutorService localListDirexecutor = Executors.newFixedThreadPool(1);
    protected ExecutorService remoteListDirexecutor = Executors.newFixedThreadPool(1);


    protected ArchiveJob archiveJob;
    protected FSEIndexer indexer;
    protected HashCache hashCache;

    long lastCheckedSpace = -1;
    List<RemoteFSElem> errList;

    public abstract boolean isCompressed();
    public abstract boolean isEncrypted();

   
     /**
     * Keeps together all data needed for identifing context of ClientCalls
     *
     */

    public GenericContext( AgentApiEntry apiEntry, StoragePoolHandler poolhandler )
    {
        this.apiEntry = apiEntry;
        this.poolhandler = poolhandler;
        this.hash_block_size = Main.get_int_prop(GeneralPreferences.FILE_HASH_BLOCKSIZE, CS_Constants.FILE_HASH_BLOCKSIZE);
        indexer = LogicControl.getStorageNubHandler().getIndexer(poolhandler.getPool());
        stat = new StatCounter("");
        result = false;
        basePath = getClientInfoRootPath( apiEntry.getAddr() ,apiEntry.getPort());
        
        
        result = true;
        abort = false;
        state = JOBSTATE.RUNNING;

         writeRunner = new HandleWriteRunner();

         openCounter++;
         open = true;
         if (indexer != null)
         {
             indexer.open();
         }

         hashCache = LogicControl.getStorageNubHandler().getHashCache(poolhandler.getPool());

         errList = new ArrayList<>();

         if (openCounter > 20) 
         {
             Log.warn("Opened " + openCounter + " Contexts");
         }
    }

    public void setHashCache( HashCache hashCache )
    {
        this.hashCache = hashCache;
    }
 @Override
    public HashCache getHashCache()
    {
        return hashCache;
    }

    @Override
    public void flushWriteRunner() throws InterruptedException
    {
        getWriteRunner().flush();
    }

    @Override
    public StoragePoolHandler getPoolHandler()
    {
        return poolhandler;
    }

    
    public static String getClientInfoRootPath(InetAddress addr, int port )
    {
        return "/" + addr.getHostAddress() + "/" + port;
    }

    public String getBasePath()
    {
        return basePath;
    }

    public abstract String getRemoteElemAbsPath( RemoteFSElem remoteFSElem ) throws PathResolveException;


    boolean isAbortOnError()
    {
        return abortOnError;
    }

    public void setAbortOnError( boolean abortOnError )
    {
        this.abortOnError = abortOnError;
    }

    public void setAbort( boolean abort )
    {
        this.abort = abort;
        this.state = JOBSTATE.ABORTING;
    }

    public boolean isAbort()
    {
        return abort;
    }

    public boolean getResult()
    {
        return result;
    }

    public void setResult( boolean b )
    {
        result = b;
    }

    public StatCounter getStat()
    {
        return stat;
    }

    public List<RemoteFSElem> getErrList()
    {
        return errList;
    }
    public String getErrListString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < errList.size(); i++)
        {
            RemoteFSElem remoteFSElem = errList.get(i);
            if (sb.length() > 0)
                sb.append("\n");
            sb.append(remoteFSElem.getPath());
        }
        return sb.toString();
    }
    
    

    public void close()
    {
        if (!open)
            return;

        open = false;
        
        try
        {
            writeRunner.close();
            apiEntry.close();
            localListDirexecutor.shutdown();
            remoteListDirexecutor.shutdown();
            if (poolhandler.is_transaction_active())
            {
                poolhandler.commit_transaction();
            }
        }
        catch (Exception exception)
        {
            Log.debug("Fehler beim Schließen des Kontexts", exception);
        }
        if (indexer != null)
        {
            indexer.flush();
        }

        openCounter--;
        if (openCounter >= 20) 
        {
            Log.debug("Closed Context");
        }

    }


    void detach( Object o ) throws SQLException
    {
        poolhandler.em_detach(o);
    }

    void addError( RemoteFSElem remoteFSElem )
    {
        errList.add( remoteFSElem );
    }

    public void setJobState( JOBSTATE jOBSTATE )
    {
        state = jOBSTATE;
    }

    public JOBSTATE getJobState()
    {
        return state;
    }

    public void setStatus( String status )
    {
        this.status = status;        
    }

    public String getStatus()
    {
        return status;
    }

    public HandleWriteRunner getWriteRunner()
    {
        return writeRunner;
    }

    public boolean noStorageSpaceLeft()
    {
        if (lastCheckedSpace  != -1)
        {
            return lastCheckedSpace <= poolhandler.getNodeMinFreeSpace();
        }
        return false;
    }
    public boolean checkStorageNodesExists()
    {
        return poolhandler.checkStorageNodeExists();
    }

    public long checkStorageNodes()
    {
        // DO WE HAVE TO CHECK SPACE AGAIN?
        if (stat.getByteTransfered() < nextCheckByteSize && lastCheckedSpace != -1)
            return lastCheckedSpace;

        // HOW MUCH LEFT ON ACTUAL NODE?
        long space = poolhandler.checkStorageNodeSpace();
        lastCheckedSpace = space;

        // CALC NEXT LEN
        // THIS WAY WE CHECK MORE OFTEN WHEN WE GET CLOSER TO THE END OF A STORAGENODE
        long diff = space/2;

        // DONT CHECK MORE OFTEN THAN NECESSARY
        
        if (diff < poolhandler.getNodeMinFreeSpace() / 5)
            diff = poolhandler.getNodeMinFreeSpace() / 5;

        nextCheckByteSize = stat.getByteTransfered() + diff;

        return space;
    }

    public int getHashBlockSize()
    {
        return hash_block_size;
    }

    public StatCounter getStatCounter()
    {
        return stat;
    }

    public ArchiveJob getArchiveJob()
    {
        return archiveJob;
    }

    public void setArchiveJob( ArchiveJob archiveJob )
    {
        this.archiveJob = archiveJob;
    }

    public FSEIndexer getIndexer()
    {
        return indexer;
    }

    public StoragePoolHandler getPoolhandler()
    {
        return poolhandler;
    }

    public List<Excludes> getExcludes()
    {
        return null;
    }
    
   

//    public String getBugFixHash()
//    {
//        return bugFixHash;
//    }
//
//
//    String bugFixHash;
//    DedupHashBlock check_for_existing_block( String remote_hash, boolean checkDHBExistance ) throws PathResolveException
//    {
//        bugFixHash = null;
//        DedupHashBlock block = _check_for_existing_block(remote_hash, checkDHBExistance);
//
//        if (block == null)
//        {
//            if (Main.get_bool_prop(GeneralPreferences.HASH_URL_FORMAT_FIX, false))
//            {
//                // WORKAROUND FOR BUG WITH WRONG (URL-UNSAFE) HASH GENERATION
//                //
//                boolean foundBugHashCandidate = false;
//
//                for (int i = 0; i < remote_hash.length(); i++)
//                {
//                    char ch = remote_hash.charAt(i);
//                    if (ch == '_' || ch == '-')
//                    {
//                        foundBugHashCandidate = true;
//                        break;
//                    }
//                }
//                if (foundBugHashCandidate)
//                {
//                    // HASH CONTAINS URLSAFE CHARS? THEN TRY WITH OLD HASH
//                    String bugHash = remote_hash.replace('_', '/');
//                    bugHash = bugHash.replace('-', '+');
//                    block = check_for_existing_block( bugHash, checkDHBExistance);
//
//                    // FOUND HASH WITH
//                    if (block != null)
//                    {
//                        bugFixHash = bugHash;
//                    }
//                }
//            }
//        }
//        return block;
//    }
//   
}
