/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.recovery;

import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.FSEA_Bootstrap;
import de.dimm.vsm.fsengine.FSE_Bootstrap;
import de.dimm.vsm.fsengine.HB_Bootstrap;
import de.dimm.vsm.fsengine.JDBCStoragePoolHandler;
import de.dimm.vsm.fsengine.PNFL_Bootstrap;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.XA_Bootstrap;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;


class RResultData
    {
        int totalFiles;
        int fixedFiles;
        int fixedBlocks;
        int fixedAttrs;
        int fixedLinks;
        String abortTxt;
        long lastCheck = System.currentTimeMillis();
        int totalHashes;

        public RResultData()
        {
            totalFiles = 0;
            fixedBlocks = 0;
            fixedFiles = 0;
            fixedAttrs = 0;
            fixedLinks = 0;
        }
        public RResultData(RResultData r)
        {
            totalFiles = r.totalFiles;
            fixedBlocks = r.fixedBlocks;
            fixedFiles = r.fixedFiles;
            fixedAttrs = r.fixedAttrs;
            fixedLinks = r.fixedLinks;
            totalHashes = r.totalHashes;
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
         void addHashBlock()
    {
        totalHashes++;
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
        
        public String getStatistics(RResultData lastr )
        {
            long now  = System.currentTimeMillis();
            int diff = (int)(now - lastCheck);
            lastCheck = now;
            if (diff == 0)
                diff = 1;
            return "Total: " + (totalFiles + totalHashes) + " Objects, Files/s: " + ratio(totalFiles, lastr.totalFiles, diff)+  
                    " Fixed: Files/s: " + ratio(fixedFiles, lastr.fixedFiles, diff) + 
                    " Attrs/s: " + ratio(fixedAttrs, lastr.fixedAttrs, diff) + 
                    " Blocks/s: " + ratio(fixedBlocks, lastr.fixedBlocks, diff) + 
                    " Links/s: " + ratio(fixedLinks, lastr.fixedLinks, diff) + 
                    " Hashes/s: " + ratio(totalHashes, lastr.totalHashes, diff);
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
/**
 *
 * @author Administrator
 */
public class RecoveryManager
{

    public static JobInterface createJobInterface( User user, AbstractStorageNode node )
    {
        RecoveryManager mgr = new RecoveryManager( node );
        RecoveryJobInterface ji = new RecoveryJobInterface(user, mgr);
        return ji;
    }
    
    StoragePoolHandler spHandler;
    String fsPath;
    String ddPath;
    int hashLen = 27;
    List<String> messages = new ArrayList<>();
    long actCnt = 0;
    RResultData resultData;
    RResultData lastResultData;
    
    DedupHashBlock actDedupHashBlock;
    AbstractStorageNode storageNode;
    private boolean abort;
    private String processPercent;
    private String statistics;
    private String statusStr;
            
    public RecoveryManager( StoragePoolHandler spHandler, String nodePath )
    {
        this.spHandler = spHandler;
        this.fsPath = nodePath + StorageNodeHandler.PATH_FSNODES_PREFIX;
        this.ddPath = nodePath + StorageNodeHandler.PATH_DEDUPNODES_PREFIX;
        resultData = new RResultData();
    }

    public RecoveryManager( AbstractStorageNode node )
    {
        this.storageNode = node;        
        String nodePath = node.getMountPoint();
        this.fsPath = nodePath + StorageNodeHandler.PATH_FSNODES_PREFIX;
        this.ddPath = nodePath + StorageNodeHandler.PATH_DEDUPNODES_PREFIX;
        resultData = new RResultData();
    }
    
    public void scan()
    {
        try
        {
            if (storageNode != null)
            {
                StoragePoolQry qry = StoragePoolQry.createActualRdWrStoragePoolQry(User.createSystemInternal(), /*showDeleted*/false);
                spHandler = new JDBCStoragePoolHandler( Main.get_control().get_util_em( storageNode.getPool()), storageNode.getPool(), qry);      
                spHandler.getEm().setSuppressNotFound(true);
            }
            if (checkScanAllowed())
            {
                scanFSEntries();
            }
        }
        catch (IOException | SQLException iOException)
        {
            err("Fehler beim Scannen der Dateisystemeinträge", iOException);
            abort = true;
            return;
        }
        try
        {        
            scanHashBlocks();
        }
        catch (IOException | SQLException ex)
        {
            err("Fehler beim Scannen der Blockeinträge", ex);       
            abort = true;
        }        
    }

    public boolean isAbort()
    {
        return abort;
    }
    
    void err(String st, Throwable t)
    {
        Log.err(st, t );
        statusStr = st + ": " + t.getMessage();
    }
    void err(String st)
    {
        Log.err(st );
        statusStr = st;
    }

    public void setStatistics( String statistics )
    {
        this.statistics = statistics;
    }
        
    
    
    void scanHashBlocks() throws IOException, SQLException
    {
        File f = new File( ddPath );
        scanHashBlocks( f);
    }
    void scanFSEntries() throws IOException, SQLException
    {
        File f = new File( fsPath );
        scanFSEntries( null, f);
    }

    private void scanHashBlocks( File f ) throws IOException, SQLException
    {
        actCnt++;
        
        if (actCnt % 100 == 0)
        {
            if (lastResultData != null)
            {
                setStatistics(resultData.getStatistics(lastResultData ));
            }
            lastResultData = new RResultData(resultData);
        }
     
        File[] files = f.listFiles();
        
        for (int i = 0; i < files.length; i++)
        {
            if (abort)
                break;
            File file = files[i];
            if (file.isFile() && file.getName().length() == hashLen)
            {
                // Jeder DHB hat im Ordner bootstrap einen eigenen Ordner mit gleichem namen, da sind die HB und XA Nodes als xml drin   
                String hash = file.getName();
                scanBootstrapDir( hash, new File( f.getAbsolutePath() + "/" + StorageNodeHandler.BOOTSTRAP_PATH, file.getName() ));                
            }
            else if (file.isDirectory())
            {
                scanHashBlocks(file);
            }
        }        
    }
    private void scanFSEntries( FileSystemElemNode parent, File f ) throws IOException, SQLException
    {       
        actCnt++;
        
        if (actCnt % 100 == 0)
        {
            if (lastResultData != null)
            {
                setStatistics(resultData.getStatistics(lastResultData ));
            }
            lastResultData = new RResultData(resultData);
        }
        
        
        File bootstrapDir = new File(f, StorageNodeHandler.BOOTSTRAP_PATH);
        if (bootstrapDir.exists())
        {
            scanFSBootstrapDir( parent, bootstrapDir);
        }
        File[] files = f.listFiles();
        
        
        for (int i = 0; i < files.length; i++)
        {
            if (abort)
                break;
            File file = files[i];
            if (!file.isDirectory())
                continue;
            
            if (file.getName().equals(StorageNodeHandler.BOOTSTRAP_PATH))
                continue;
            
            long idx;
            try
            {
                idx = Long.parseLong(file.getName(), 16);
            }
            catch(Exception exc)
            {
                err("Skipping unknown entry " + file.getAbsolutePath(), exc);
                continue;
            }
            

            parent = spHandler.resolve_node_from_db(FileSystemElemNode.class, idx);
            if (parent == null && idx == 1)
            {
                if (!spHandler.realizeInFs())
                {
                    abort = true;
                    throw new IOException("FS System kann nicht initialisiert werden");
                }
            }
            // Recurse down
            scanFSEntries( parent, file );
        }        
    }
    private void scanFSBootstrapDir( FileSystemElemNode parent, File bootstrapDir ) throws IOException, SQLException
    {       
        File[] files = bootstrapDir.listFiles();
                
        for (int i = 0; i < files.length; i++)
        {
            if (abort)
                break;
            File file = files[i];
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.FSEN_PREFIX))
            {
                scanFSENBootstrapFile( parent, file );
            }
        }
        for (int i = 0; i < files.length; i++)
        {
            if (abort)
                break;
            File file = files[i];
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.FSEA_PREFIX))
            {
                scanFSEABootstrapFile( file );
            }
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.PNFL_PREFIX))
            {
                scanPNFLBootstrapFile( file );
            }
        }
    }


    private void scanBootstrapDir( String hash, File entriesDir ) throws IOException, SQLException
    {
        File[] files = entriesDir.listFiles();
        
        // Wir fangen in einem neuen DHB-Recovery Ordner an
        actDedupHashBlock = null;

        // Erst die Hashblöcke
        for (int i = 0; i < files.length; i++)
        {
            if (abort)
                break;
            File file = files[i];
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.HASHBLOCK_PREFIX))
            {
                scanHashBlockBootstrapFile( file );
            }
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.XATTR_PREFIX))
            {
                scanXANodeBootstrapFile( file );
            }
        }
    }

    private void scanHashBlockBootstrapFile( File file ) throws SQLException
    {
        HB_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (HB_Bootstrap) xs.fromXML(fis);
            registerHBEntity( bt );
        }
        catch (Exception ex)
        {
            err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }
    private void scanXANodeBootstrapFile( File file ) throws SQLException
    {
        XA_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (XA_Bootstrap) xs.fromXML(fis);
            registerXAEntity( bt );
        }
        catch (Exception ex)
        {
            err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }

    private void registerHBEntity( HB_Bootstrap bt ) throws SQLException
    {
        resultData.addHashBlock();
        if (actDedupHashBlock == null)
        {            
            registerDHBEntity( bt.getDedupBlockIdx(), bt.getHashvalue(), bt.getBlockLen() );
        }
        HashBlock orig = spHandler.resolve_node_from_db(HashBlock.class, bt.getIdx());
        if (orig != null)
        {
            if (orig.getHashvalue().equals(bt.getHashvalue()) && orig.getBlockLen() == bt.getBlockLen())
                return;
            
            err( "HashBlock Duplicate found " + orig.toString());
            return;
        }
        
        HashBlock hb = bt.getHashBlock(actDedupHashBlock);
        
        if (bt.getFileNodeIdx() <= 0)
        {
            err("Leerer Filenode beim Einlesen von " + bt.toString() );
        }        
        else
        {
            FileSystemElemNode fh = spHandler.resolve_node_from_db(FileSystemElemNode.class, bt.getFileNodeIdx());
            if (fh == null)
            {
                err("Filenode kann nicht aufgelöst werden beim Einlesen von " + bt.toString() );
            }
            else
            {
                hb.setFileNode(fh);
                fh.getHashBlocks().addIfRealized(hb);
            }
        }
                
        spHandler.check_open_transaction();
        spHandler.raw_persist(hb, hb.getIdx());
        spHandler.check_commit_transaction();    
    }
    
    private void registerXAEntity( XA_Bootstrap bt ) throws SQLException
    {
        if (actDedupHashBlock == null)
        {
            registerDHBEntity(bt.getDedupBlockIdx(), bt.getHashvalue(), bt.getBlockLen());
        }
        
        XANode orig = spHandler.resolve_node_from_db(XANode.class, bt.getIdx());
        if (orig != null)
        {
            if (orig.getHashvalue().equals(bt.getHashvalue()) && orig.getBlockLen() == bt.getBlockLen())
                return;
            err( "XANode Duplicate found " + orig.toString());
            return;
        }
        
        XANode xa = bt.getXANode(actDedupHashBlock);
        if (bt.getFileNodeIdx() <= 0)
        {
            err("Leerer Filenode beim Einlesen von " + bt.toString() );
        }        
        else
        {
            FileSystemElemNode fh = spHandler.resolve_node_from_db(FileSystemElemNode.class, bt.getFileNodeIdx());
            if (fh == null)
            {
                err("Filenode kann nicht aufgelöst werden beim Einlesen von " + bt.toString() );
            }
            else
            {
                xa.setFileNode(fh);
                fh.getXaNodes().addIfRealized(xa);
            }
        }
                
        spHandler.check_open_transaction();
        spHandler.raw_persist(xa, xa.getIdx());
        spHandler.check_commit_transaction();    
    }

    private void registerDHBEntity( long idx, String hashvalue, int blockLen ) throws SQLException
    {
        DedupHashBlock orig = spHandler.resolve_node_from_db(DedupHashBlock.class, idx);
        if (orig != null)
        {
            if (orig.getHashvalue().equals(hashvalue) && orig.getBlockLen() == blockLen)
                return;
            err( "DedupHashBlock Duplicate found " + orig.toString());
            return;
        }
        DedupHashBlock he = new DedupHashBlock();
        he.setIdx(idx);        
        he.setHashvalue(hashvalue);
        he.setBlockLen(blockLen);

        spHandler.check_open_transaction();
        spHandler.raw_persist(he, he.getIdx());
        spHandler.check_commit_transaction();    
        
        actDedupHashBlock = he;
        
        resultData.addFixedBlock();
    }

    private FileSystemElemNode scanFSENBootstrapFile( FileSystemElemNode parent, File file ) throws SQLException
    {
        String n = file.getName();
        String id = n.substring(StorageNodeHandler.FSEN_PREFIX.length(), n.length() - StorageNodeHandler.BOOTSTRAP_SUFFIX.length() );
        long idx;
        try
        {
            idx = Long.parseLong(id, 16);
        }
        catch (NumberFormatException numberFormatException)
        {
            Log.err("Ungültiger Bootstrap " + file.getAbsolutePath());
            return null;
        }
        // Skip Existing root Dir
        if (idx == spHandler.getPool().getRootDir().getIdx())
        {
            return spHandler.getPool().getRootDir();
        }
        
        FSE_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (FSE_Bootstrap) xs.fromXML(fis);
            return registerFSENEntity( parent, bt, idx );
        }
        catch (Exception ex)
        {
            err("Fehler beim Einlesen von FSE_Bootstrap " + file.getAbsolutePath(), ex);
        }  
        return null;
    }
    
    private void scanFSEABootstrapFile( File file ) throws SQLException
    {
        String n = file.getName();
        String id = n.substring(StorageNodeHandler.FSEA_PREFIX.length(), n.length() - StorageNodeHandler.BOOTSTRAP_SUFFIX.length() );
        long idx;
        try
        {
            idx = Long.parseLong(id, 16);
        }
        catch (NumberFormatException numberFormatException)
        {
            err("Ungültiger Bootstrap " + file.getAbsolutePath());
            return;
        }
        
        FSEA_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (FSEA_Bootstrap) xs.fromXML(fis);
            registerFSEAEntity( bt, idx );
        }
        catch (Exception ex)
        {
            err("Fehler beim Einlesen von FSEA_Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }
    
    private void scanPNFLBootstrapFile( File file ) throws SQLException
    {
        String n = file.getName();
        String id = n.substring(StorageNodeHandler.PNFL_PREFIX.length(), n.length() - StorageNodeHandler.BOOTSTRAP_SUFFIX.length() );
        long idx;
        try
        {
            idx = Long.parseLong(id, 16);
        }
        catch (NumberFormatException numberFormatException)
        {
            err("Ungültiger Bootstrap " + file.getAbsolutePath());
            return;
        }
        
        PNFL_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (PNFL_Bootstrap) xs.fromXML(fis);
            registerPNFLEntity( bt, idx );
        }
        catch (Exception ex)
        {
            err("Fehler beim Einlesen von PNFL_Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }
        
    private FileSystemElemNode registerFSENEntity( FileSystemElemNode parent, FSE_Bootstrap bt, long idx ) throws SQLException
    {    
        // Skip Existing
        resultData.addTotalFiles();
        
        FileSystemElemNode orig = spHandler.resolve_fse_node_from_db(idx);
        if (orig != null)
        {
            if ( orig.getName().equals(bt.getName()))
            {
                return orig;
            }
                
            err("Fehler beim Einlesen von FSEA_Bootstrap, doppelter Node " + orig.toString());
            return null;
        }
        
        FileSystemElemNode node = bt.getNode(idx);
        node.setParent(parent);
        node.setPool(spHandler.getPool());
                
        spHandler.check_open_transaction();
        spHandler.raw_persist(node.getAttributes(), node.getAttributes().getIdx());
        spHandler.raw_persist(node, node.getIdx());        
        spHandler.check_commit_transaction();  
        
        statusStr = node.getName();
        resultData.addFixedFile();
        return node;
    }
    
    private void registerFSEAEntity( FSEA_Bootstrap bt, long idx ) throws SQLException
    {       
        resultData.addFixedAttrs();
        
        FileSystemElemAttributes orig = spHandler.resolve_node_from_db(FileSystemElemAttributes.class, idx);
        if (orig != null)
        {
            if ( orig.getName().equals(bt.getName()))
            {
                return;
            }                
            err("Fehler beim Einlesen von FSEA_Bootstrap, doppelter Node " + orig.toString());
            return;
        }
        
        FileSystemElemNode fsen = spHandler.resolve_fse_node_from_db(bt.getFileIdx());
        FileSystemElemAttributes attr = bt.getNode(idx, fsen);
        fsen.getHistory().addIfRealized(attr);
                
        spHandler.check_open_transaction();
        spHandler.raw_persist(attr, attr.getIdx());        
        spHandler.check_commit_transaction();    
        
        fsen.getHistory().addIfRealized(attr);
    }
    
    private void registerPNFLEntity( PNFL_Bootstrap bt, long idx ) throws SQLException
    {        
        resultData.addFixedLink();
        
        PoolNodeFileLink orig = spHandler.resolve_node_from_db(PoolNodeFileLink.class, idx);
        if (orig != null)
        {
            if ( orig.getFileNode().getIdx() == bt.getFileIdx() && orig.getStorageNode().getIdx() == bt.getNodeIdx())
            {
                return;
            }
                
            err("Fehler beim Einlesen von PNFL_Bootstrap, doppelter Node " + orig.toString());
            return;
        }
               
        PoolNodeFileLink link = bt.getNode(idx);   
        link.setFileNode(spHandler.resolve_node_from_db(FileSystemElemNode.class, bt.getFileIdx()));
        link.setStorageNode(spHandler.resolve_node_from_db(AbstractStorageNode.class, bt.getNodeIdx()));
        spHandler.check_open_transaction();
        spHandler.raw_persist(link, link.getIdx());        
        spHandler.check_commit_transaction();                    
    }
    
    public List<String> getMessages()
    {
        return messages;
    }

    void abort()
    {
        abort = true;
    }

    String getProcessPercent()
    {
        return processPercent;
    }

    String getStatisticStr()
    {
        return statistics;
    }

    String getStatusStr()
    {
        return statusStr;
    }

   
    void close()
    {
        if (storageNode != null)
        {
            try
            {
                spHandler.close_transaction();
                spHandler.close_entitymanager();
            }
            catch (SQLException ex)
            {
                Log.err("Fehler beim Schließen des EntityManagers", ex);
            }
        }    
    }

    private boolean checkScanAllowed()
    {
        return true;
    }

   
   
    
}
