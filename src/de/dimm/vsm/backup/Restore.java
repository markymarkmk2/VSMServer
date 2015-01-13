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
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.ZipUtilities;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.LazyList;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.RemoteFSElemWrapper;
import de.dimm.vsm.net.interfaces.AgentApi;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Administrator
 */
public class Restore
{

    RestoreContext actualContext;
    int hash_block_size;
    boolean abort = false;
    List<FileSystemElemNode> nodes;
    List<FileSystemElemAttributes> attrs;
    

    public Restore(StoragePoolHandler poolHandler, FileSystemElemNode node, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        List<FileSystemElemNode> list = new ArrayList<FileSystemElemNode>();
        list.add(node);
            
        setRestoreParam(poolHandler, list, flags, qry, targetIP, targetPort, target);
    }

    public Restore(StoragePoolHandler poolHandler, List<FileSystemElemNode> nodes, List<FileSystemElemAttributes> fseAttrs, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        setRestoreParam(poolHandler, nodes, flags, qry, targetIP, targetPort, target);
        attrs = fseAttrs;
        if (attrs.size() != nodes.size())
            throw new IOException("Ungülithe Attributanzahl");
    }
    public Restore(StoragePoolHandler poolHandler, List<FileSystemElemNode> nodes, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        setRestoreParam(poolHandler, nodes, flags, qry, targetIP, targetPort, target);        
    }
    public Restore(StoragePoolHandler poolHandler, ArchiveJob job, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        RemoteFSElem jobDir = RemoteFSElem.createDir(target.getPath() + "/" + job.getName());

        List<FileSystemElemNode> list = new ArrayList<FileSystemElemNode>();
        list.add(job.getDirectory());
        
        setRestoreParam(poolHandler, list, flags, qry, targetIP, targetPort, jobDir);
        actualContext.apiEntry.getApi().create_dir(jobDir);
    }
    public final void setRestoreParam(StoragePoolHandler poolHandler, FileSystemElemNode node, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        List<FileSystemElemNode> list = new ArrayList<FileSystemElemNode>();
        list.add(node);

        setRestoreParam(poolHandler, list, flags, qry, targetIP, targetPort, target);
    }
    public final void setRestoreParam(StoragePoolHandler poolHandler, List<FileSystemElemNode> nodes, int flags, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target) throws IOException
    {
        this.nodes = nodes;
        if (actualContext != null)
        {
            actualContext.close();
        }
        
        actualContext = init_context(poolHandler, qry, targetIP, targetPort, target, flags);

    }

    public boolean getResult()
    {
        return actualContext.getResult();
    }

    public JobInterface createRestoreJob(User user)
    {
        if (actualContext != null)
        {
            JobInterface job = createJob(user);
            return job;
        }
        return null;
    }

    JobInterface createJob( User user)
    {
        return new RestoreJobInterface( user);
    }

    String preStartStatus;

    class RestoreJobInterface implements JobInterface
    {       
        boolean finished = false;
        Date startTime;
        User user;

        public RestoreJobInterface( User user)
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
        public Object getResultData()
        {
            throw new UnsupportedOperationException("Not supported yet.");
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
            if (actualContext != null)
                return actualContext.stat.getSpeedPerSec();

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


        @Override
        public String getProcessPercentDimension()
        {
            return actualContext.stat.getSpeedDim();
        }




        @Override
        public void run()
        {
            try
            {
                run_restore();
            }
            catch (Exception exception)
            {
                if (actualContext != null)
                {
                    actualContext.setJobState( JOBSTATE.FINISHED_ERROR );
                    actualContext.setStatus(Main.Txt("Restore wurde abgebrochen") + ":" + exception.getMessage());
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
                }
            }
            finished = true;
        }

        @Override
        public String getStatisticStr()
        {
            if (actualContext != null)
            {
                return actualContext.stat.toString();
            }
            return "";
        }
        
        @Override
        public void close()
        {

        }
    }
    
    private void createRecursiveRestorePath(String targetPath) throws IOException
    {
        if (targetPath != null && targetPath.length() > 1)
        {
            RemoteFSElem elem = RemoteFSElem.createDir( targetPath );
            if (!actualContext.apiEntry.getApi().exists( elem ))
            {
                int idx = targetPath.lastIndexOf( '/');
                if (idx < 0)
                    return;
                
                createRecursiveRestorePath( targetPath.substring( 0, idx ));
                
                actualContext.apiEntry.getApi().create_dir( elem );
            }
        }
    }

    public void run_restore()
    {
        String targetPath = actualContext.target.getPath();

        boolean ret = true;
        try
        {
            // RELOAD NODE
            for (int i = 0; i < nodes.size(); i++)
            {
                FileSystemElemNode node = nodes.get(i);

                // RELOAD
                node = actualContext.getPoolhandler().em_find(node.getClass(), node.getIdx());

                FileSystemElemAttributes attr;
                if (attrs == null)
                {
                    // GET CORRECT ATTRIBUTES
                    attr = actualContext.poolhandler.getActualFSAttributes(node, actualContext.qry);
                }
                else
                {
                    attr = attrs.get(i);
                }
                
                
                RemoteFSElem parentElem = RemoteFSElem.createDir( targetPath );
                if (!actualContext.apiEntry.getApi().exists( parentElem ))
                {
                    createRecursiveRestorePath(targetPath);
                }
                

                restore_complete_node(node, attr, targetPath);
            }
            if (actualContext.getResult())
            {
                String txt = nodes.size() + " " +  Main.Txt("Objekten");
                if (nodes.size() == 1)
                    txt = nodes.get(0).getName();
                buildStatusText( Main.Txt("Restore von") + " " + txt + " " + Main.Txt("nach") + " " + targetPath + " "+  Main.Txt("beendet") );
            }
        }
        catch (Exception e)
        {
            actualContext.setStatus(e.getMessage());
            Log.err("Abbruch beim Restore", e);
            ret = false;
        }
        finally
        {
            if (ret == false)
                actualContext.setResult(ret);

            actualContext.close();
        }
        
    }

    final RestoreContext init_context( StoragePoolHandler poolHandler, StoragePoolQry qry, InetAddress targetIP, int targetPort, RemoteFSElem target, int flags ) throws IOException
    {
        Properties p;

        AgentApiEntry apiEntry = null;

        hash_block_size = Main.get_int_prop(GeneralPreferences.FILE_HASH_BLOCKSIZE, CS_Constants.FILE_HASH_BLOCKSIZE);

        try
        {
            apiEntry = LogicControl.getApiEntry(targetIP, targetPort);
            p = apiEntry.getApi().get_properties();
        }
        catch (Exception e)
        {      
            e = AgentApiEntry.getException(e);
            preStartStatus = "Cannot connect to agent " + targetIP.toString() + " " +  e.getMessage();
            VSMFSLogger.getLog().error("Cannot connect to agent " + targetIP.toString(), e);
            throw new IOException(preStartStatus);
        }

        String agent_ver = p.getProperty(AgentApi.OP_AG_VER);
        String agent_os = p.getProperty(AgentApi.OP_OS);
        String agent_os_ver = p.getProperty(AgentApi.OP_OS_VER);
        String agent_os_arch = p.getProperty(AgentApi.OP_OS_VER);

        preStartStatus = "Connected to agent " + targetIP.toString() + ":" + targetPort + ", " + agent_ver + ", " + agent_os + " " + agent_os_arch + " " + agent_os_ver;

        VSMFSLogger.getLog().debug(preStartStatus);
        buildStatusText( preStartStatus );

        return new RestoreContext( poolHandler, qry, apiEntry, target, flags, p);
    }


    static boolean isHotfolderTimestampDir(FileSystemElemNode node)
    {
        return node.hasFlag( FileSystemElemNode.FL_DATEDIR);
    }

    void buildStatusText(  String text)
    {
     
        RestoreContext rc = actualContext;
        if (rc == null)
            return;

        StringBuilder sb = new StringBuilder();

        sb.append(rc.apiEntry.getFactory().getAdress().toString());
        sb.append(": ");

        sb.append(rc.target.getPath());
        sb.append(": ");

        if (text != null)
        {
            if (text.length() > 80)
                text =  "..." + text.substring(text.length() - 77);
            sb.append(text);
            sb.append(" ");
        }

        
        int fps = (int)rc.stat.getFilesTransferedPerSec();
        if (fps > 0)
        {
            sb.append(" ");
            sb.append(fps);
            sb.append(" f/s");
        }
        int dps = (int)rc.stat.getDirsTransferedPerSec();
        if (fps > 0)
        {
            sb.append(" ");
            sb.append(dps);
            sb.append(" d/s");
        }
        

        rc.setStatus(sb.toString());
      
    }



    public void restore_complete_node( FileSystemElemNode node, FileSystemElemAttributes attr, String targetPath/*, RemoteFSElem existingRemoteFSElem*/ ) throws IOException, PathResolveException
    {

        boolean skipDir = false;
        // IF WE HAVE TO SKIP TIMSTAMP DIR ON RESTORE
        // /HotFolder/IP/Port/Name/TimeStampDir
        if (actualContext.skipHotfolderTimestampDir())
        {
            skipDir =  isHotfolderTimestampDir( node );
        }
//        RemoteFSElem remoteFSElem = null;
            
        if (!skipDir)
        {
            try
            {
                restore_node(node, attr, targetPath);
//                remoteFSElem = restore_node(node, attr, targetPath, existingRemoteFSElem);
            }
            catch (Exception exception)
            {
                Log.err( "Restore schlog fehl", node.toString(), exception );
                Log.warn("TODO: Error list restore");
                actualContext.setStatus(exception.getMessage());
                actualContext.setResult(false);
            }
        }

        if (node.isDirectory() && actualContext.isRecursive())
        {
            List<FileSystemElemNode> list = node.getChildren(actualContext.poolhandler.getEm());


            // BILD TARGET PATH W/O HotfolderTimestampDir
            String clientTargetPath;

            if (skipDir)
                clientTargetPath = targetPath;
            else
                clientTargetPath = targetPath + "/" + node.getName();
            

            for (int i = 0; i < list.size(); i++)
            {
                if (abort)
                    break;
                
                FileSystemElemNode childNode = list.get(i);

                // THIS IS THE NEWEST ENTRY FOR THIS FILE
                FileSystemElemAttributes childAttr = actualContext.poolhandler.getActualFSAttributes(childNode, actualContext.qry);

                // OBVIOUSLY THE FILE WAS CREATED AFTER TS
                if (childAttr == null)
                {
                    continue;
                }

                // FILE WAS DELETED AT TS
                if (childAttr.isDeleted() && !actualContext.getPoolHandler().getPoolQry().isShowDeleted())
                {
                    continue;
                }

                restore_complete_node(childNode, childAttr, clientTargetPath);
            }
            // SET DIR TIMES
            RemoteFSElem remoteFSElem = create_target_elem(node, attr, targetPath);
            actualContext.apiEntry.getApi().set_filetimes_named(remoteFSElem);
            node.getChildren().unRealize();
        }
    }

    
    RemoteFSElem create_target_elem( FileSystemElemNode node, FileSystemElemAttributes attr, String targetPath )
    {
        StringBuilder sb = new StringBuilder(targetPath);
        sb.append('/');
        sb.append(attr.getName());

        RemoteFSElem elem = new RemoteFSElem(sb.toString(), node.getTyp(),
                attr.getModificationDateMs(), attr.getCreationDateMs(), attr.getAccessDateMs(),
                attr.getFsize(), attr.getStreamSize());


        elem.setPosixData(attr.getPosixMode(), attr.getUid(), attr.getGid(), attr.getUidName(), attr.getGidName());

        if (elem.isSymbolicLink())
        {
            // A REAL HACK, IN SYMLINKS, THE LONKPATH IS PUT INTO XATTRIBUTES (NOT NEEDED)
            elem.setLinkPath( attr.getAclInfoData() );
        }
        else
        {
            elem.setAclinfoData( attr.getAclInfoData());
            elem.setAclinfo( attr.getAclinfo());
        }

        return elem;
    }
    
    private void close_handle(  FileHandle handle )
    {
        if (handle != null)
        {
            try
            {
                handle.close();
            }
            catch (IOException iOException)
            {
            }
        }
    }
    private void close_handle(  RemoteFSElemWrapper remote_handle ) throws IOException
    {
        if (remote_handle != null)
        {
            actualContext.apiEntry.getApi().close_data(remote_handle);
        }
    }


//    private RemoteFSElem restore_node( FileSystemElemNode node, FileSystemElemAttributes attr, String targetPath, RemoteFSElem existingRemoteFSElem ) throws IOException, PathResolveException
    private void restore_node( FileSystemElemNode node, FileSystemElemAttributes attr, String targetPath) throws IOException, PathResolveException, SQLException
    {
        FileHandle handle = null;
        RemoteFSElemWrapper remote_handle = null;
        RemoteFSElem remoteFSElem = create_target_elem(node, attr, targetPath);

        buildStatusText( remoteFSElem.getPath() );

        
        actualContext.stat.addTotalStat(remoteFSElem);
        actualContext.stat.addTransferStat(remoteFSElem);

        if (node.isDirectory())
        {
            actualContext.apiEntry.getApi().create_dir(remoteFSElem);

            if (remoteFSElem.getStreamSize() > 0)
            {
                restore_stream_data( remoteFSElem,  node,  attr, targetPath );
            }
        }
        else if (node.isFile())
        {
            boolean dataStreamOk = false;
            try
            {
                // OPEN LOCAL FILE HANDLE
                handle = actualContext.poolhandler.open_file_handle(node, /*create*/ false);

                // OPEN REMOTE HANDLE
                // TODO:  WE DO NOT TRY TO RESTORE DIFFERENTIALLY, WE RESTORE COMPLETE
                int targetFlags = AgentApi.FL_CREATE | AgentApi.FL_RDWR | AgentApi.FL_TRUNC;
                remote_handle = actualContext.apiEntry.getApi().open_data(remoteFSElem, targetFlags);
                if (remote_handle == null)
                    throw new IOException( "Cannot open remote handle for write:" + remoteFSElem.getPath());


                List<HashBlock> hbList = node.getHashBlocks(actualContext.poolhandler.getEm());

                // IN CASE WE HAVE WRITTEN THIS PRESENTLY, WE HAVE TO REREAD LIST
                if (hbList instanceof LazyList)
                {
                    LazyList lhb = (LazyList)hbList;
                    lhb.unRealize();
                }

                long len = attr.getFsize();
                long readLen = 0;

                if (hbList.isEmpty())
                {
                    // SINGLE BLOCK w/O HASH?
                    if (len > actualContext.hash_block_size)
                        throw new IOException( "No hashblocks found" );

                    byte[] data = handle.read((int) len, 0);

                    if (actualContext.isEncrypted() || actualContext.isCompressed())
                    {
                        if (actualContext.isCompressed())
                            data = ZipUtilities.lzf_compressblock(data);

                        int encLen = data.length;
                        if (actualContext.isEncrypted())
                            data = CryptTools.encryptXTEA8(data);

                        actualContext.apiEntry.getApi().writeEncryptedCompressed(remote_handle, data, 0, encLen, actualContext.isEncrypted(), actualContext.isCompressed());
                    }
                    else
                        actualContext.apiEntry.getApi().write(remote_handle, data, 0);

                    actualContext.stat.addTransferLen((int)len);
                    readLen = data.length;
                }
                else
                {
                    // BUILD LIST FOR THIS FILE
                    hbList = filter_hashblocks(hbList, attr);

                    for (int i = 0; i < hbList.size(); i++)
                    {
                        HashBlock hashBlock = hbList.get(i);
                        int read_len = hashBlock.getBlockLen();
                        long offset = hashBlock.getBlockOffset();


                        transfer_block(hashBlock, handle, remote_handle, offset, read_len);

                        readLen += read_len;
                    }
                }
                if (readLen != len)
                {
                    throw new IOException("Invalid length while restore: expected " + len + " got " + readLen);
                }


                // TODO WINDOWS STREAMS RESTORE DOES NOT WORK YET
                if (remoteFSElem.getStreamSize() > 0 && !actualContext.isWinAgent())
                {
                    try
                    {
                        restore_stream_data(remoteFSElem, node, attr, targetPath);
                    }
                    catch (Exception e)
                    {
                        e = AgentApiEntry.getException(e);
                        Log.warn("Fehler bei Restore von StreamData", ": " + e.getMessage()/*, iOException*/);
                    }
                }


                // SET TIMES AND ACLS
                actualContext.apiEntry.getApi().set_attributes(remote_handle);

            }
            finally
            {
                close_handle( handle );
                close_handle( remote_handle );
            }
        }
        else if (node.isSymbolicLink())
        {
            actualContext.apiEntry.getApi().create_symlink(remoteFSElem);
        }
        else
        {
            actualContext.apiEntry.getApi().create_other(remoteFSElem);
        }
        actualContext.stat.check_stat();
        //return remoteFSElem;
    }

    void restore_stream_data( RemoteFSElem remoteFSElem,  FileSystemElemNode node, FileSystemElemAttributes attr, String targetPath ) throws IOException, PathResolveException, UnsupportedEncodingException, SQLException
    {                
        RemoteFSElemWrapper remote_handle = null;
        FileHandle handle = null;
        try
        {
                // OPEN REMOTE HANDLE
                // TODO:  WE DO NOT TRY TO RESTORE DIFFERENTIALLY, WE RESTORE COMPLETE
                int targetFlags = AgentApi.FL_CREATE | AgentApi.FL_RDWR | AgentApi.FL_TRUNC;
                remote_handle = actualContext.apiEntry.getApi().open_stream_data(remoteFSElem, targetFlags);
                if (remote_handle == null)
                    throw new IOException( "Cannot open remote stream handle for write:" + remoteFSElem.getPath());

                handle = actualContext.poolhandler.open_xa_handle(node, remoteFSElem.getStreaminfo(), /*create*/ false);

                List<XANode> xablocks = actualContext.poolhandler.createQuery("select T1 from XANode T1 where T1.fileNode_idx=" + node.getIdx(), XANode.class);

                long len = attr.getStreamSize();
                long readLen = 0;

                if (!xablocks.isEmpty())
                {
                    // BUILD LIST FOR THIS FILE
                    xablocks = filter_xanodes(xablocks, attr);

                    for (int i = 0; i < xablocks.size(); i++)
                    {
                        XANode hashBlock = xablocks.get(i);
                        int read_len = hashBlock.getBlockLen();
                        long offset = hashBlock.getBlockOffset();


                        transfer_block(hashBlock, handle, remote_handle, offset, read_len);

                        readLen += read_len;
                    }
                }

                if (readLen != len)
                {
                    throw new IOException("Invalid length while restore: expected " + len + " got " + readLen);
                }
        }
        finally
        {
            if (remote_handle != null)
            {
                close_handle( remote_handle );
            }
            if (handle != null)
            {
                handle.close();
            }
        }
    }


    public static List<HashBlock> filter_hashblocks( List<HashBlock> hbList, FileSystemElemAttributes attrs )
    {
        // BUILD A LIST BASED ON QUERY WITH ASCENDING BLOCK POS
        // IF SOMETHING IS MISSING; WE WILL GET INTO TROUBLE HERE...

        List<HashBlock> ret = new ArrayList<HashBlock>();

        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hbList, new Comparator<HashBlock>() {

            @Override
            public int compare( HashBlock o1, HashBlock o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                if (o1.getTs() != o2.getTs()) 
                    return (o1.getTs() - o2.getTs() > 0) ? -1 : 1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        HashBlock lastHashBlock = null;
        for (int i = 0; i < hbList.size(); i++)
        {
            HashBlock hashBlock = hbList.get(i);

            // IS VALID DATA ENTRY ?
            if (hashBlock.getTs() > attrs.getTs())
                continue; // NO, TOO YOUNG
            
           // IS VALID DATA ENTRY ?
            if (hashBlock.getBlockOffset() > attrs.getFsize())
                continue; // NO, TOO LARGE
            

            if (lastHashBlock != null)
            {                               
                if (lastHashBlock.getBlockOffset() == hashBlock.getBlockOffset())
                {
                    // FIX FOR XA-NODE IN HASHBLOCKS
                    if (lastHashBlock.getTs() == hashBlock.getTs() && 
                            lastHashBlock.getBlockOffset() == 0 && 
                            lastHashBlock.getBlockLen() == attrs.getStreamSize())
                    {
                        LogManager.msg( LogManager.LVL_WARN, LogManager.TYP_DB, "Detected XA-Node in hashList, repaired");
                        ret.remove(lastHashBlock);                        
                    }
                    else
                    {
                        continue;
                    }
                }
            }
            ret.add(hashBlock);

            lastHashBlock = hashBlock;
        }
        
        // NOW WE HAVE A LIST OF THE NEWEST BLOCKS OLDER THAN attrs.Timestamp
        return ret;
    }


    public static List<XANode> filter_xanodes( List<XANode> hbList,FileSystemElemAttributes attrs )
    {
        // BUILD A LIST BASED ON QUERY WITH ASCENDING BLOCK POS
        // IF SOMETHING IS MISSING; WE WILL GET INTO TROUBLE HERE...

        List<XANode> ret = new ArrayList<XANode>();

        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hbList, new Comparator<XANode>() {

            @Override
            public int compare( XANode o1, XANode o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        XANode lastHashBlock = null;
        for (int i = 0; i < hbList.size(); i++)
        {
            XANode hashBlock = hbList.get(i);

            // IS VALID DATA ENTRY ?
            if (hashBlock.getTs() > attrs.getTs())
                continue; // NO, TOO YOUNG
            
            // IS VALID DATA ENTRY ?
            if (hashBlock.getBlockOffset() > attrs.getStreamSize())
                continue; // NO, TOO LARGE
            

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

        // NOW WE HAVE A LIST OF THE NEWEST BLOCKS OLDER THAN qry.Timestamp
        return ret;
    }



    // TRANSFER A SINGLE BLOCK, EITHER FROM ORIGFILE OR FROM DEDUPHASHBLOCK
    private void transfer_block( HashBlock hashBlock, FileHandle local_handle, RemoteFSElemWrapper remote_handle, long offset, int read_len ) throws IOException, PathResolveException
    {        
        FileHandle dedup_handle = null;
        
        FileHandle handle = local_handle;
        long readOffset = offset;
        actualContext.stat.addTransferBlock();
        if (hashBlock.getDedupBlock() != null)
        {
            dedup_handle = actualContext.poolhandler.open_dedupblock_handle(hashBlock.getDedupBlock(), /*create*/ false);
            handle = dedup_handle;
            readOffset = 0;
            actualContext.stat.addDedupBlock(hashBlock.getDedupBlock());
        }

        byte[] data = handle.read(read_len, readOffset);

        if (actualContext.isEncrypted() || actualContext.isCompressed())
        {
            if (actualContext.isCompressed())
                data = ZipUtilities.lzf_compressblock(data);
            int encLen = data.length;
            if (actualContext.isEncrypted())
                data = CryptTools.encryptXTEA8(data);
            actualContext.apiEntry.getApi().writeEncryptedCompressed(remote_handle, data, offset, encLen, actualContext.isEncrypted(), actualContext.isCompressed());
        }
        else
            actualContext.apiEntry.getApi().write(remote_handle, data, offset);

        if (dedup_handle != null)
            dedup_handle.close();

        actualContext.stat.addTransferLen(read_len);
    }
    // TRANSFER A SINGLE BLOCK, EITHER FROM ORIGFILE OR FROM DEDUPHASHBLOCK
    private void transfer_block( XANode xaBlock, FileHandle local_handle, RemoteFSElemWrapper remote_handle, long offset, int read_len ) throws IOException, PathResolveException
    {
        FileHandle dedup_handle = null;

        FileHandle handle = local_handle;
        long readOffset = offset;
        actualContext.stat.addTransferBlock();
        if (xaBlock.getDedupBlock() != null)
        {
            dedup_handle = actualContext.poolhandler.open_dedupblock_handle(xaBlock.getDedupBlock(), /*create*/ false);
            handle = dedup_handle;
            readOffset = 0;
            actualContext.stat.addDedupBlock(xaBlock.getDedupBlock());
        }

        byte[] data = handle.read(read_len, readOffset);


        if (actualContext.isEncrypted() || actualContext.isCompressed())
        {
            if (actualContext.isCompressed())
                data = ZipUtilities.lzf_compressblock(data);
            int encLen = data.length;
            if (actualContext.isEncrypted())
                data = CryptTools.encryptXTEA8(data);

            actualContext.apiEntry.getApi().writeEncryptedCompressed(remote_handle, data, offset, encLen, actualContext.isEncrypted(), actualContext.isCompressed());
        }
        else
            actualContext.apiEntry.getApi().write(remote_handle, data, offset);

        if (dedup_handle != null)
            dedup_handle.close();

        actualContext.stat.addTransferLen(read_len);
    }

//            HashMap<String, RemoteFSElem> remoteDirMap = null;
//
//            if (actualContext.isInkrementalRestore())
//            {
//                // HANDLE BACKUP FOR ALL ELEMENTS ON AGENT
//                ArrayList<RemoteFSElem> fs_list = actualContext.apiEntry.getApi().list_dir(remoteFSElem);
//
//                remoteDirMap = new HashMap<String, RemoteFSElem>();
//                for (int i = 0; i < fs_list.size(); i++)
//                {
//                    RemoteFSElem rfs = fs_list.get(i);
//                    remoteDirMap.put(remoteFSElem.getName(), rfs);
//                }
//            }
//

//    private boolean skipRestore( RemoteFSElem rfs, FileSystemElemAttributes childAttr)
//    {
//        boolean skip = false;
//
//        if (rfs == null)
//            return false;
//
//        // MODTIME DIFFERS
//        if (rfs.getMtimeMs() != childAttr.getModificationDateMs())
//            return false;
//
//        if (rfs.getDataSize() == childAttr.getFsize() && rfs.getStreamSize() == childAttr.getStreamSize())
//        {
//            if (rfs.getAclinfoData() == null && childAttr.getAclInfoData() == null)
//                skip = true;
//
//            if (!skip)
//            {
//                if (rfs.getAclinfoData() != null && childAttr.getAclInfoData() != null &&
//                        rfs.getAclinfoData().equals(childAttr.getAclInfoData()) )
//                {
//                    skip = true;
//                }
//            }
//        }
//        return skip;
//    }
//                // CHECK IF WE CAN SKIP RESTORE
//                RemoteFSElem rfs = null;
//                if (remoteDirMap != null)
//                {
//                    rfs = remoteDirMap.get(childAttr.getName());
//                }
//
//                boolean skip = skipRestore(rfs, childAttr);
//
//                if (skip)
//                    continue;

}
