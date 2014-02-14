/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.lifecycle;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.LazyList;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import de.dimm.vsm.records.AbstractStorageNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class NodeMigrationManager
{


    JOBSTATE state;
    String status;
    int processPercent;
    boolean abort;
    InteractionEntry iEntry = null;
    
    AbstractStorageNode src;
    List<AbstractStorageNode> targets;
    private long totalSize;
    private long copiedSize;
    private long totalEntries;
    private long copiedEntries;

    WriteFileRunner writeRunner;

    public static final int BUFFSIZE = 1024*1024;  // 64
    public static final int BUFFCOUNT = 64;

    
    long usedSpace = 0;
    long startTime;
    double transferSpeed;
    long fileSpeed;

    String actStatus;
    boolean doClone = false;



    public NodeMigrationManager( AbstractStorageNode src, AbstractStorageNode trg )
    {
        this.src = src;
        targets = new ArrayList<AbstractStorageNode>();
        targets.add(trg);

        if (!src.isOnline() && !src.getNodeMode().equals(AbstractStorageNode.NM_EMPTYING))
            throw new IllegalArgumentException(Main.Txt("Der_Quellnode_ist_nicht_online"));

        if (!trg.isOnline())
            throw new IllegalArgumentException(Main.Txt("Der_Zielnode_ist_nicht_online"));

        writeRunner = new WriteFileRunner( BUFFSIZE, BUFFCOUNT);

    }
    public NodeMigrationManager( AbstractStorageNode src ) throws SQLException
    {
        this.src = src;
        targets = new ArrayList<AbstractStorageNode>();

        GenericEntityManager em = Main.get_control().get_util_em(src.getPool());
        
        List<AbstractStorageNode> list = em.createQuery("select T1 from AbstractStorageNode T1 where T1.pool_idx=" + src.getPool().getIdx() + " and T1.idx!=" + src.getIdx(), AbstractStorageNode.class);

        long freeSpace = 0;
        for (int i = 0; i < list.size(); i++)
        {
            AbstractStorageNode abstractStorageNode = list.get(i);
            if (abstractStorageNode.isOnline())
            {
                freeSpace += StorageNodeHandler.getFreeSpace(abstractStorageNode);
                targets.add(abstractStorageNode);
            }
        }
        
        if (targets.isEmpty())
            throw new IllegalArgumentException(Main.Txt("Es_existiert_kein_Zielnode_im_Status_Online"));

        writeRunner = new WriteFileRunner(BUFFSIZE, BUFFCOUNT);
        
    }


    public static JobInterface createMoveJob( AbstractStorageNode src, AbstractStorageNode trg, User user )
    {
        NodeMigrationManager mgr = new NodeMigrationManager(src, trg);
        JobInterface job = mgr.createJob(user);
        return job;
    }
    public static JobInterface createEmptyJob( AbstractStorageNode src, User user ) throws SQLException
    {
        NodeMigrationManager mgr = new NodeMigrationManager(src);
        JobInterface job = mgr.createJob(user);
        return job;
    }
    public static JobInterface createSyncNodeJob( AbstractStorageNode t, AbstractStorageNode cloneNode, User user  )
    {
        NodeMigrationManager mgr = new NodeMigrationManager(t, cloneNode);
        mgr.doClone = true;
        JobInterface job = mgr.createJob(user);
        return job;
    }



    JobInterface createJob(User user)
    {

        return new MigrationJob(user);
    }

    public class MigrationJob implements JobInterface
    {
        boolean finished = false;
        long lastCopiedSize = 0;
        long lastTime = System.currentTimeMillis();
        long lastEntries = 0;
        double lttransferSpeed = 0;

        Date jobStartTime;
        User user;

        public MigrationJob(User user)
        {
            this.user = user;
            jobStartTime = new Date();
        }

        @Override
        public User getUser()
        {
            return user;
        }

        @Override
        public Date getStartTime()
        {
            return jobStartTime;
        }

        @Override
        public JOBSTATE getJobState()
        {
            return state;
        }

        @Override
        public Object getResultData()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        

        @Override
        public InteractionEntry getInteractionEntry()
        {
            return iEntry;
        }

        @Override
        public String getStatusStr()
        {
            long q = System.currentTimeMillis();
            // checks

            long diff = q -lastTime;
            if (diff == 0)
                diff = 1;

            if (diff >= 1000)
            {
                transferSpeed =  (copiedSize - lastCopiedSize) * 1000 / diff;  // B/s
                fileSpeed = (copiedEntries - lastEntries) * 1000 / diff;
                lastEntries = copiedEntries;
                lastCopiedSize = copiedSize;
                lastTime = q;
                lttransferSpeed =  (copiedSize) * 1000 / (q-startTime);  // B/s
            }

            if (actStatus != null && state == JOBSTATE.RUNNING)
            {
                String ret =  actStatus + " R:" + writeRunner.readStatus + " W:" + writeRunner.writeStatus + " " + SizeStr.format(lttransferSpeed) + "/s (actual " + SizeStr.format(transferSpeed) + "/s, " + fileSpeed + " Files/s)" ;
                return ret;
            }

            return status;
        }

        @Override
        public String getProcessPercent()
        {

            if (totalEntries == 0)
                return "";
            
            processPercent = (int)((copiedEntries* 100) / totalEntries);

            return Integer.toString(processPercent);
        }

        @Override
        public void abortJob()
        {
            abort = true;
            if (finished)
            {
                setJobState( JOBSTATE.ABORTED );
            }
        }

        @Override
        public void setJobState( JOBSTATE jOBSTATE )
        {
            state = jOBSTATE;
        }

      

        @Override
        public void run()
        {
            runMigration();
            finished = true;
        }

        @Override
        public String getProcessPercentDimension()
        {
            if (totalEntries == 0)
                return "";
            return "%";
        }

        @Override
        public String getStatisticStr()
        {
            return "";
        }

        public AbstractStorageNode getSrc()
        {
            return src;
        }
        public List<AbstractStorageNode> getTargets()
        {
            return targets;
        }
        @Override
        public void close()
        {

        }



    }

    void runMigration()
    {
        try
        {
            if (targets.size() == 1)
            {
                migrateNode(src, targets.get(0));
            }
            else
            {
                emptyNode(src, targets);
            }
        }
        catch (Exception e)
        {
            Log.err("Abbruch während der Migration", e);
            status = Main.Txt("Abbruch:") + " " + e.getMessage();
            state = JOBSTATE.FINISHED_ERROR;
        }
        finally
        {
            try
            {
                writeRunner.close();
            }
            catch (IOException iOException)
            {
                Log.err("Fehler beim Schließen der Migration", iOException);
            }
        }
    }

    void emptyNode( AbstractStorageNode node, List<AbstractStorageNode> targets )
    {
        usedSpace = 0;
        if (StorageNodeHandler.isRoot(src))
            usedSpace = StorageNodeHandler.getUsedSpace(src);
        else
            usedSpace = getRealUsedSpace(src);
        
        long freeSpace = 0;
        for (int i = 0; i < targets.size(); i++)
        {
             freeSpace += StorageNodeHandler.getFreeSpace(targets.get(i));
        }
        
        if ( usedSpace >= freeSpace )
        {
            throw new IllegalArgumentException(Main.Txt("Auf_den_Zielnodes_ist_nicht_genug_Platz: Frei ") + SizeStr.format(freeSpace) + "," + 
                    Main.Txt("benötigt werden ") + SizeStr.format(usedSpace));
        }

        if ( usedSpace == 0 )
        {
            throw new IllegalArgumentException(Main.Txt("Der_Quellnode_ist_leer" ) );
        }

        status = Main.Txt("Das_Entleeren_von_Speichernodes_ist_noch_nicht_implementiert");
        state = JOBSTATE.FINISHED_ERROR;
    }


    void migrateNode( AbstractStorageNode sourceNode, AbstractStorageNode targetNode )
    {
        status = Main.Txt("Berechnen_der_Datenmenge_von") + " " + sourceNode.getName() + "...";
        state = JOBSTATE.RUNNING;

        usedSpace = 0;
        if (StorageNodeHandler.isRoot(sourceNode))
            usedSpace = StorageNodeHandler.getUsedSpace(sourceNode);
        else
            usedSpace = getRealUsedSpace(sourceNode);

        long freeSpace = StorageNodeHandler.getFreeSpace(targetNode);
        if ( usedSpace >= freeSpace )
        {
            throw new IllegalArgumentException(Main.Txt("Auf_dem_Zielnode_ist_nicht_genug_Platz: Frei ") + SizeStr.format(freeSpace) + "," + 
                    Main.Txt("benötigt werden ") + SizeStr.format(usedSpace));           
        }
        if ( usedSpace == 0 )
        {
            throw new IllegalArgumentException(Main.Txt("Der_Quellnode_ist_leer" ) );
        }

        String sizeStr = SizeStr.format(usedSpace);
        status = Main.Txt("Kopiere") + " " + sizeStr + " " + Main.Txt("Daten_von") + " " + sourceNode.getName() + " -> " + targetNode.getName() + "...";
        state = JOBSTATE.RUNNING;

        try
        {
            // SET MODE TO EMPTYING TO PREVENT ARRIVAL OF NEW DATA
            GenericEntityManager em = Main.get_control().get_util_em(sourceNode.getPool());
            sourceNode = em.em_find(AbstractStorageNode.class, sourceNode.getIdx());
            if (!doClone)
            {
                sourceNode.setNodeMode(AbstractStorageNode.NM_EMPTYING);
                sourceNode = em.em_merge(sourceNode);
            }

            // ALLOW PARENT TO REREAD, THIS SHOULDN BE NECESSARY, BUT CACHING DOESNT WORK REDUNDANT FREE ...
            if (sourceNode.getPool().getStorageNodes() instanceof LazyList)
            {
                LazyList ll = (LazyList)sourceNode.getPool().getStorageNodes();
                ll.unRealize();
            }

            
            boolean copyRet = copyCompleteStorageNodeData(sourceNode, targetNode);
            if (abort)
            {
                state = JOBSTATE.ABORTED;
                return;
            }
            if (!copyRet)
            {
                throw new Exception("Fehler_beim_Kopieren_der_Daten");
            }

            if (!doClone)
            {
                status = Main.Txt("Abgleich_der_Datenbank_von") + " " + sourceNode.getName() + " -> " + targetNode.getName() + "...";
                state = JOBSTATE.RUNNING;

                if (updateDatabase( sourceNode, targetNode ))
                {
                    sourceNode = em.em_find(AbstractStorageNode.class, sourceNode.getIdx());
                    sourceNode.setNodeMode(AbstractStorageNode.NM_EMPTIED);
                    sourceNode = em.em_merge(sourceNode);

                    status = Main.Txt("Migration_der_Daten_von") + " " + sourceNode.getName() + " -> " + targetNode.getName() + " " + Main.Txt("ist_beendet");
                    state = JOBSTATE.FINISHED_OK;
                    processPercent = 100;
                    
                }
                else
                {
                    status = Main.Txt("Abgleich der Datenbank schlug fehl") + " " + sourceNode.getName() + " -> " + targetNode.getName();
                    state = JOBSTATE.FINISHED_ERROR;
                }
            }
            else
            {
                status = Main.Txt("Kopieren_der_Daten_von") + " " + sourceNode.getName() + " -> " + targetNode.getName() + " " + Main.Txt("ist_beendet");
                state = JOBSTATE.FINISHED_OK;
                processPercent = 100;
            }
            return;
        }
        catch (SQLException e)
        {
            status = Main.Txt("SQL-Fehler") + " " + e.getMessage();
        }
        catch (IOException e)
        {
            status = Main.Txt("IO-Fehler") + " " + e.getMessage();
        }
        catch (Exception e)
        {
            status = Main.Txt("Allgemeiner-Fehler") + " " + e.getMessage();
        }
        state = JOBSTATE.FINISHED_ERROR;
    }

    boolean updateDatabase(AbstractStorageNode sourceNode, AbstractStorageNode targetNode) throws SQLException, IOException
    {
        JDBCEntityManager em = Main.get_control().get_util_em(sourceNode.getPool());

        Connection conn = em.getConnection();
        Statement st = null;
        try
        {
            st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

            String qry = "select IDX,STORAGENODE_IDX from POOLNODEFILELINK where STORAGENODE_IDX=" + sourceNode.getIdx();
            long toIdx = targetNode.getIdx();

            st.setFetchSize(1000);

            ResultSet rs = st.executeQuery(qry );

            long cnt = 0;
            conn.setAutoCommit(false);
            long now = System.currentTimeMillis();
            while (rs.next())
            {
                //long idx = rs.getLong(1);
                rs.updateLong(2, toIdx);
                rs.updateRow();
                cnt++;
                if ((cnt%1000) == 0)
                {
                    conn.commit();
                    long diff = System.currentTimeMillis() - now;
                    if (diff == 0)
                        diff = 1;

                    status = Main.Txt("Verarbeitet_wurden") + " " + cnt + " " + Main.Txt("Einträge") + " (" + cnt*1000/diff + " 1/s)";
                }
            }
            conn.commit();


            qry = "select IDX,STORAGENODE_IDX from DedupHashBlock where STORAGENODE_IDX=" + sourceNode.getIdx();
            rs = st.executeQuery(qry );

            while (rs.next())
            {
                //long idx = rs.getLong(1);
                rs.updateLong(2, toIdx);
                rs.updateRow();
                cnt++;
                if ((cnt%1000) == 0)
                {
                    conn.commit();
                    long diff = System.currentTimeMillis() - now;
                    if (diff == 0)
                        diff = 1;

                    status = Main.Txt("Verarbeitet_wurden") + " " + cnt + " " + Main.Txt("Einträge") + " (" + cnt*1000/diff + " 1/s)";
                }
            }
            
            conn.commit();
        }
        finally
        {
            if (st != null)
                st.close();
        }

        return true;
    }

    public long getRealUsedSpace(AbstractStorageNode n)
    {
        totalSize = 0;
        if (n.isFS() && n.getMountPoint() != null)
        {
            File f = new File( n.getMountPoint() );
            calcRecursiveLen( f );
        }
        return totalSize;
    }

    public void calcRecursiveLen( File f )
    {
        if (f.isDirectory())
        {
            File[] children = f.listFiles();
            for (int i = 0; i < children.length; i++)
            {
                File file = children[i];
                calcRecursiveLen(file);
            }
            status = Main.Txt("Berechnen_der_Datenmenge_von") + " " + src.getName() + ": " + totalEntries + " (" + SizeStr.format(totalSize)  + ")...";

        }
        else
        {
            totalSize += f.length();
        }
        totalEntries++;
    }

    private boolean copyCompleteStorageNodeData( AbstractStorageNode sourceNode, AbstractStorageNode targetNode )
    {
        File sourceDir = new File(sourceNode.getMountPoint());
        File targetDir = new File(targetNode.getMountPoint());

        if (!sourceDir.exists())
        {
            status = Main.Txt("StorageNode_Quelle_existiert_nicht");
        }
        if (!targetDir.exists())
        {
            status = Main.Txt("StorageNode_Ziel_existiert_nicht");
        }

        startTime = System.currentTimeMillis();
        copiedSize = 0;
        copiedEntries = 1; // THE NODE ROOT

        File[] list = sourceDir.listFiles();

        boolean ret = true;
        for (int i = 0; i < list.length; i++)
        {
            File file = list[i];
            if (abort)
                return false;

            boolean local_ret = copyRecursiveDir(file, targetDir);

            if (!local_ret)
            {
                ret = false;
                break;
            }
        }
        actStatus = null;

        return ret;
    }

    private boolean copyRecursiveDir( File file, File targetDir )
    {
        if (abort)
            return false;

        copiedEntries++;

        if (file.isDirectory())
        {
            File targetDirChild = new File(targetDir, file.getName());
            if (!targetDirChild.exists())
            {
                if (!targetDirChild.mkdir())
                {
                    status = Main.Txt("Ein_Zielverzeichnis_konnte_nicht_erstellt_werden") + ": " + targetDirChild.getAbsolutePath();
                    return false;
                }
            }
            File[] children = file.listFiles();
            for (int i = 0; i < children.length; i++)
            {
                File child = children[i];
                if (!copyRecursiveDir(child, targetDirChild))
                {
                    return false;
                }
            }
            
            return true;
        }
        else
        {
            File targetFile = new File(targetDir, file.getName());
            if (doClone && targetFile.exists() && file.length() == targetFile.length())
            {
                actStatus = Main.Txt("Überspringe Eintrag ") + copiedEntries  +  " / " + totalEntries + "...";
                return true;
            }
            actStatus = Main.Txt("Kopiere Eintrag ") + copiedEntries  +  " / " + totalEntries + "...";
            // DIFFERENT EXISTS ALREADY
            if (targetFile.exists() && file.length() != targetFile.length())
            {
                status = Main.Txt("Eine_Zieldatei_bestand_bereits") + ": " + targetFile.getAbsolutePath();
                iEntry = new InteractionEntry(InteractionEntry.INTERACTION_TYPE.OK_CANCEL, InteractionEntry.SEVERITY.ERROR, 
                        Main.Txt("Es besteht_eine_gleichnahmige_Datei_auf_dem_Ziel,_wollen_Sie_fortfahren?"), Main.Txt("Datei_überschreiben"),
                        new Date(), 0, InteractionEntry.INTERACTION_ANSWER.CANCEL);

                state = JOBSTATE.NEEDS_INTERACTION;

                while( !iEntry.wasAnswered())
                {
                    LogicControl.sleep(100);
                }
                if (iEntry.getUserAnswer() !=  InteractionEntry.INTERACTION_ANSWER.OK)
                    return false;
            }
            try
            {
                copyFile(file, targetFile );                                
                return true;
            }
            catch (Exception exception)
            {
                status = Main.Txt("Kopiervorgang_ist_fehlgeschlagen") + ": " + exception.getMessage();
                return false;
            }
        }

    }

    /** Copy source file to destination. If destination is a path then source

    file name is appended. If destination file exists then: overwrite=true -

    destination file is replaced; overwite=false - exception is thrown.

    @param src source file

    @param dst destination file or path

    @param overwrite overwrite destination file

    @exception IOException I/O problem

    @exception IllegalArgumentException illegal argument */

    private void copyFile( final File src, File dst ) throws IOException, IllegalArgumentException
    {
        if (writeRunner.writeError)
            throw new IOException("Error occured while copying " + writeRunner.errorFile);


        File dstParent = dst.getParentFile();

        if (!dstParent.exists())
        {
            if (!dstParent.mkdirs())
            {
                throw new IOException("Failed to create directory " + dstParent.getAbsolutePath());
            }
        }

        long fileSize = src.length();

        if (fileSize == 0)
        {
            dst.createNewFile();
            return;
        }
        

        if (true)
        {
            // for larger files  use streams
            FileInputStream in = new FileInputStream(src);
            FileOutputStream out = new FileOutputStream(dst);

            long restLen = fileSize;
            try
            {
                while (restLen > 0)
                {
                    HandleWriteElem elem = writeRunner.getNextFreeElem();
                    int blockSize = elem.data.length;

                    int readLen = blockSize;
                    boolean doClose = false;

                    if (restLen < blockSize)
                    {
                        readLen = (int) restLen;
                    }


                    int rlen = in.read(elem.data, 0, readLen);

                    restLen -= rlen;
                    if (restLen == 0)
                    {
                        doClose = true;
                    }

                    copiedSize += rlen;

                    elem.setVals(out, rlen, doClose, src.getAbsolutePath());
                    writeRunner.writeElem(elem);
                }
            }
            catch (IOException iOException)
            {
                Log.err(Main.Txt("Fehler beim Kopieren eines Eintrags"), iOException);

                // NORMALLY OS IS CLOSED BY WRITERUNNER
                try
                {
                    out.close();
                }
                catch (IOException iOException1)
                {
                }
            }
            finally
            {
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                }               
            }
        }
        else
        {
            // smaller files, use channels
            FileInputStream fis = new FileInputStream(src);
            FileOutputStream fos = new FileOutputStream(dst);

            FileChannel in = fis.getChannel();
            FileChannel out = fos.getChannel();

            copiedSize += fileSize;

            try
            {
                long offs = 0, doneCnt = 0, copyCnt = Math.min(BUFFSIZE, fileSize);

                do
                {
                    doneCnt = in.transferTo(offs, copyCnt, out);
                    offs += doneCnt;
                    fileSize -= doneCnt;
                }
                while (fileSize > 0);
            }
            finally
            {
                // cleanup
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                }

                try
                {
                    out.close();
                }
                catch (IOException e)
                {
                }

                try
                {
                    fis.close();
                }
                catch (IOException e)
                {
                }

                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }

        } // else

      
    } // copy
}
