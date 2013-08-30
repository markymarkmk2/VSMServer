/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.fixes;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.FS_BootstrapHandle;
import de.dimm.vsm.fsengine.JDBCConnectionFactory;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.JDBCStoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Administrator
 */
public class FixBootstrapEntries implements IFix
{
    JDBCEntityManager em;
    StoragePool pool;
    long cnt = 0;
    long actCnt = 0;
    int lastPercent = 0;
    int level = 0;
    boolean abort;
    String status;
    String statistics;
    long startTime;
    
    StoragePoolHandler spHandler;
    ResultData resultData;
    ResultData lastResultData;
    private boolean overwriteExisting;

    
    public static JobInterface createFixJobInterface(User user, StoragePool pool)
    {
        try
        {
            JDBCConnectionFactory factory = LogicControl.getStorageNubHandler().getConnectionFactory(pool);            
            JDBCEntityManager em = new JDBCEntityManager(pool.getIdx(), factory);
            IFix fix =  new FixBootstrapEntries(pool, em);
            JobInterface ji = new AbstractFixJobInterface( user, fix);            
            return ji;
        }
        catch (Exception e)
        {
            Log.err("FixBootstrapEntries kann nicht erzeugt werden", e);
        }
        return null;
    }
    public FixBootstrapEntries( StoragePool pool, JDBCEntityManager em )
    {
        this.em = em;
        this.pool = pool; 
        resultData = new ResultData();
        // Default alles überschreiben
        overwriteExisting = Main.get_bool_prop(GeneralPreferences.BOOTSTRAPFIX_OVERWRITE, true );
    }

    @Override
    public boolean isAborted()
    {
        return abort;
    }
    

  

    public void setStatus( String status )
    {
        this.status = status;
    }

    public void setStatistics( String statistics )
    {
        this.statistics = statistics;
    }
    
    public static class ResultData
    {
        int totalFiles;
        int fixedFiles;
        int fixedBlocks;
        int fixedAttrs;
        int fixedLinks;
        String abortTxt;
        long lastCheck = System.currentTimeMillis();

        public ResultData()
        {
            totalFiles = 0;
            fixedBlocks = 0;
            fixedFiles = 0;
            fixedAttrs = 0;
            fixedLinks = 0;
        }
        public ResultData(ResultData r)
        {
            totalFiles = r.totalFiles;
            fixedBlocks = r.fixedBlocks;
            fixedFiles = r.fixedFiles;
            fixedAttrs = r.fixedAttrs;
            fixedLinks = r.fixedLinks;
        }
        void addFixedFile()
        {
            fixedFiles++;
        }
        void addFixedBlock()
        {
            fixedBlocks++;
        }
        void  addFixedAttrs()
        {
            fixedAttrs++;
        }
        void addFixedLink()
        {
            fixedLinks++;
        }
        void addTotalFiles()
        {
            totalFiles++;
        }

        public void setAbortTxt( String abortTxt )
        {
            this.abortTxt = abortTxt;
        }
        public void setAbortTxt (String abortTxt, Throwable t )
        {
            this.abortTxt = abortTxt + ": " + t.getMessage();
        }

        public String getAbortTxt()
        {
            return abortTxt;
        }

        @Override
        public String toString()
        {
            return "Fixed Files: " + fixedFiles + " fixed Attrs: " + fixedAttrs + " fixed Blocks: " + fixedBlocks + " fixed Links: " + fixedLinks + " " + abortTxt;
        }
        
        public String getStatistics(ResultData lastr, long startTime, long sumTotalFiles )
        {
            long now  = System.currentTimeMillis();
            int diff = (int)(now - lastCheck);
            lastCheck = now;
            if (diff == 0)
                diff = 1;
            String eta = eta( startTime, sumTotalFiles);
            return "Total: Files/s: " + ratio(totalFiles, lastr.totalFiles, diff) + " ETA: " + eta +  
                    " Fixed: Files/s: " + ratio(fixedFiles, lastr.fixedFiles, diff) + 
                    " Attrs/s: " + ratio(fixedAttrs, lastr.fixedAttrs, diff) + 
                    " Blocks/s: " + ratio(fixedBlocks, lastr.fixedBlocks, diff) + 
                    " Links/s: " + ratio(fixedLinks, lastr.fixedLinks, diff);
        }
        String ratio(int n, int o, int ms)
        {
            return Integer.toString(((n - o) * 1000) / ms);
        }
        String eta(long startTime, long sumTotalFiles )
        {
            long now  = System.currentTimeMillis();
            long diff = now - startTime;
            double fps = (totalFiles * 1000.0) / diff;
            if (fps == 0)
                fps = 1;
            
            long restFiles = sumTotalFiles - totalFiles;
            long restSeconds =  (long)(restFiles / fps);
            Date eta = new Date(now + restSeconds * 1000);
            GregorianCalendar cal = new GregorianCalendar();
            int dayToday = cal.get(GregorianCalendar.DAY_OF_YEAR);
            cal.setTime(eta);
            int readyDay = cal.get(GregorianCalendar.DAY_OF_YEAR);
            if (dayToday != readyDay)
            {
                SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
                return sdf.format( new Date(now + restSeconds * 1000));
            }
            else
            {
                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                return sdf.format( new Date(now + restSeconds * 1000));
            }
        }
    }
    
    
    @Override
    public boolean runFix() throws SQLException
    {
        Log.info("Fixing bootstrop entries started for pool " + pool.toString());
        startTime = System.currentTimeMillis();
        spHandler = new JDBCStoragePoolHandler(em, User.createSystemInternal(), pool, false);
        spHandler.add_storage_node_handlers();
        
        boolean foundSNode = false;
        for (AbstractStorageNode snode : pool.getStorageNodes(em))
        {
            if (!snode.isFS())
            {
                Log.info("Skipping non FS node " + snode.toString());
                continue;        
            }
            
            
            File fsnode = new File(snode.getMountPoint());
            if (!fsnode.exists())
            {
                Log.warn("Skipping non existant node " + snode.toString());
                continue;        
            }
            
            foundSNode = true;
            break;
        }
        if (!foundSNode)
        {
            resultData.setAbortTxt("No existing Storage Nodes found");
            Log.err(resultData.getAbortTxt());
            return false;
        }
        try (Statement st = em.getConnection().createStatement(); ResultSet rs = st.executeQuery("select count(idx) from Filesystemelemnode"))
        {
            if (rs.next())
            {
                cnt = rs.getLong(1);
            }
        }
        Log.info("Nodes to check: " + cnt);

        em.check_open_transaction();

        FileSystemElemNode root = em.em_find(FileSystemElemNode.class, pool.getRootDir().getIdx());

        checkBootstrapEntries( root, /*recurse*/true );

        em.commit_transaction();
        
        setStatus("Fertig");
        return true;
    }
    
    private void calcStatistics() throws SQLException
    {
        if (cnt > 0)
        {
            int percent = (int)(actCnt * 100 / cnt);
            if (percent != lastPercent)
            {
                lastPercent = percent;
                Log.info( percent + " % done...");
                em.commit_transaction();
            }
            if (actCnt % 100 == 0)
            {
                if (lastResultData != null)
                {
                    setStatistics(resultData.getStatistics(lastResultData, startTime, cnt ));
                }
                lastResultData = new ResultData(resultData);
            }
        }
    }

    private void checkBootstrapEntries( FileSystemElemNode node, boolean recurse ) throws SQLException
    {
        actCnt++;
        resultData.addTotalFiles();
                
        try
        {
            calcStatistics();           
            checkBootstrapEntry(node);            
        }
        catch (Exception exception)
        {
            resultData.setAbortTxt("Das Schreiben der Bootstrapdaten schlug fehl für " + node.toString(), exception);
            Log.err(resultData.getAbortTxt(), exception);
        }
        if (!node.isDirectory())
            return;
        
        setStatus("Checke Dir " + node.getName());
        level++;

        List<FileSystemElemNode> children = new ArrayList<>();
        children.addAll(node.getChildren(em));
        
        for (int i = 0; i < children.size(); i++)
        {
            if (abort)
                break;
            FileSystemElemNode actNode = children.get(i);
                                    
            if (recurse)
            {
                checkBootstrapEntries(actNode, /*recurse*/true);
            }           
        }
       
        node.getChildren().unRealize();
        level--;
    }

    private void checkBootstrapEntry( FileSystemElemNode keepNode ) throws SQLException
    {
        Set<AbstractStorageNode> snodes = new HashSet<>();
        List<PoolNodeFileLink> linkList = spHandler.get_pool_node_file_links(keepNode);
        if (linkList != null && !linkList.isEmpty())
        {
            for (PoolNodeFileLink poolNodeFileLink : linkList)
            {
                snodes.add(poolNodeFileLink.getStorageNode());
            }            
        }
        else
        {
            snodes.addAll( spHandler.get_primary_storage_nodes(false));
        }
        
        for (AbstractStorageNode snode : snodes)
        {
            if (!snode.isFS())
                continue;
            
            File fsnode = new File(snode.getMountPoint());
            if (!fsnode.exists())
                continue;
            
            try
            {
               FS_BootstrapHandle bh = new FS_BootstrapHandle(snode, keepNode);
               if (!bh.exists() || overwriteExisting)
               {
                   bh.write_bootstrap(keepNode);
                   resultData.addFixedFile();
               }
                
               
                for (FileSystemElemAttributes attr : keepNode.getHistory(em))
                {
                    // Skip first Attr (is in Node Bootstrap)
                    if (attr.getIdx() == keepNode.getAttributes().getIdx())
                        continue;
                    bh = new FS_BootstrapHandle(snode, attr);
                    if (!bh.exists() || overwriteExisting)
                    {
                       bh.write_bootstrap(attr);
                       resultData.addFixedAttrs();
                    }
                }
                for (HashBlock block : keepNode.getHashBlocks(em))
                {
                    bh = new FS_BootstrapHandle(snode, block.getDedupBlock(), block);
                    if (!bh.exists() || overwriteExisting)
                    {
                       bh.write_bootstrap(block);                                        
                       resultData.addFixedBlock();
                    }
                }                
                for (XANode block : keepNode.getXaNodes(em))
                {
                    bh = new FS_BootstrapHandle(snode, block.getDedupBlock(), block);
                    if (!bh.exists() || overwriteExisting)
                    {
                       bh.write_bootstrap(block);                                        
                       resultData.addFixedBlock();
                    }
                       
                }                
                for (PoolNodeFileLink link : keepNode.getLinks(em))
                {
                    bh = new FS_BootstrapHandle(snode, link);
                    if (!bh.exists() || overwriteExisting)
                    {
                       bh.write_bootstrap(link);                                        
                       resultData.addFixedLink();
                    }                       
                }                
            }
            catch (PathResolveException | IOException exc)
            {
                resultData.setAbortTxt("Abbruch bei node " + keepNode.toString(), exc);
                Log.err(resultData.getAbortTxt(), exc);
            }            
        }            
    }

    @Override
    public String getStatusStr()
    {
        return status;
    }

    @Override
    public String getStatisticStr()
    {
        return statistics;
    }

    @Override
    public Object getResultData()
    {
        return resultData;
    }

    @Override
    public String getProcessPercent()
    {
        return Integer.toString(lastPercent);
    }

    @Override
    public String getProcessPercentDimension()
    {
        return "%";
    }

    @Override
    public void abortJob()
    {
        abort = true;
    }

    @Override
    public void close()
    {
        
    }
}
