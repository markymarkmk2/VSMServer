/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.recovery;

import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.fsengine.FSEA_Bootstrap;
import de.dimm.vsm.fsengine.FSE_Bootstrap;
import de.dimm.vsm.fsengine.HB_Bootstrap;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class RecoveryManager
{
    StoragePoolHandler spHandler;
    String fsPath;
    String ddPath;
    int hashLen = 27;
    
    DedupHashBlock actDedupHashBlock;

    public RecoveryManager( StoragePoolHandler spHandler, String nodePath )
    {
        this.spHandler = spHandler;
        this.fsPath = nodePath + StorageNodeHandler.PATH_FSNODES_PREFIX;
        this.ddPath = nodePath + StorageNodeHandler.PATH_DEDUPNODES_PREFIX;
    }
    
    public void scan()
    {
        try
        {
            scanFSEntries();
        }
        catch (IOException | SQLException iOException)
        {
            Log.err("Fehler beim Scannen der Dateisystemeinträge", iOException);
            return;
        }
        try
        {        
            scanHashBlocks();
        }
        catch (IOException | SQLException ex)
        {
            Log.err("Fehler beim Scannen der Blockeinträge", ex);
            return;
        }
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
        File[] files = f.listFiles();
        
        for (int i = 0; i < files.length; i++)
        {
            File file = files[i];
            if (file.isFile() && file.getName().length() == hashLen)
            {
                // Jeder DHB hat im Ordner bootstrap einen eigenen Ordner mit gleichem namen, da sind die HB und XA Nodes als xml drin   
                String hash = file.getName();
                scanBootstrapDir( hash, new File( f.getAbsolutePath() + "/" + StorageNodeHandler.BOOTSTRAP_PATH, file.getName() ));
                
            }
        }        
    }
    private void scanFSEntries( FileSystemElemNode parent, File f ) throws IOException, SQLException
    {
        File bootstrapDir = new File(f, StorageNodeHandler.BOOTSTRAP_PATH);
        if (bootstrapDir.exists())
        {
            scanFSBootstrapDir( parent, bootstrapDir);
        }
        File[] files = f.listFiles();
        
        
        for (int i = 0; i < files.length; i++)
        {
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
                Log.err("Skipping unknown entry " + file.getAbsolutePath(), exc);
                continue;
            }
            

            parent = spHandler.resolve_node_from_db(FileSystemElemNode.class, idx);
            if (parent == null && idx == 1)
            {
                if (!spHandler.realizeInFs())
                {
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
            File file = files[i];
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.FSEN_PREFIX))
            {
                scanFSENBootstrapFile( parent, file );
            }
        }
        for (int i = 0; i < files.length; i++)
        {
            File file = files[i];
            if (file.getName().endsWith(StorageNodeHandler.BOOTSTRAP_SUFFIX) && file.getName().startsWith(StorageNodeHandler.FSEA_PREFIX))
            {
                scanFSEABootstrapFile( file );
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
        catch (IOException ex)
        {
            Log.err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }
    private void scanXANodeBootstrapFile( File file ) throws SQLException
    {
        HB_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (HB_Bootstrap) xs.fromXML(fis);
            registerXAEntity( bt );
        }
        catch (IOException ex)
        {
            Log.err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }

    private void registerHBEntity( HB_Bootstrap bt ) throws SQLException
    {
        if (actDedupHashBlock == null)
        {
            registerDHBEntity( bt );
        }
        HashBlock hb = bt.getHashBlock(actDedupHashBlock);
        
        if (bt.getFileNodeIdx() <= 0)
        {
            Log.err("Leerer Filenode beim Einlesen von " + bt.toString() );
        }        
        else
        {
            FileSystemElemNode fh = spHandler.resolve_node_from_db(FileSystemElemNode.class, bt.getFileNodeIdx());
            if (fh == null)
            {
                Log.err("Filenode kann nicht aufgelöst werden beim Einlesen von " + bt.toString() );
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
    
    private void registerXAEntity( HB_Bootstrap bt ) throws SQLException
    {
        if (actDedupHashBlock == null)
        {
            registerDHBEntity( bt );
        }
        XANode xa = bt.getXANode(actDedupHashBlock);
        if (bt.getFileNodeIdx() <= 0)
        {
            Log.err("Leerer Filenode beim Einlesen von " + bt.toString() );
        }        
        else
        {
            FileSystemElemNode fh = spHandler.resolve_node_from_db(FileSystemElemNode.class, bt.getFileNodeIdx());
            if (fh == null)
            {
                Log.err("Filenode kann nicht aufgelöst werden beim Einlesen von " + bt.toString() );
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

    private void registerDHBEntity( HB_Bootstrap bt ) throws SQLException
    {
        DedupHashBlock he = new DedupHashBlock();
        he.setIdx(bt.getDedupBlockIdx());        
        he.setHashvalue(bt.getHashvalue());
        he.setBlockLen(bt.getBlockLen());

        spHandler.check_open_transaction();
        spHandler.raw_persist(he, he.getIdx());
        spHandler.check_commit_transaction();    
        
        actDedupHashBlock = he;
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
        
        FSE_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (FSE_Bootstrap) xs.fromXML(fis);
            return registerFSENEntity( parent, bt, idx );
        }
        catch (IOException ex)
        {
            Log.err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
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
            Log.err("Ungültiger Bootstrap " + file.getAbsolutePath());
            return;
        }
        
        FSEA_Bootstrap bt;
        XStream xs = new XStream();
        try (FileInputStream fis = new FileInputStream(file))
        {
            bt = (FSEA_Bootstrap) xs.fromXML(fis);
            registerFSEAEntity( bt, idx );
        }
        catch (IOException ex)
        {
            Log.err("Fehler beim Lesen von Bootstrap " + file.getAbsolutePath(), ex);
        }        
    }
    
    private FileSystemElemNode registerFSENEntity( FileSystemElemNode parent, FSE_Bootstrap bt, long idx ) throws SQLException
    {        
        FileSystemElemNode node = bt.getNode(idx);
        node.setParent(parent);
        node.setPool(spHandler.getPool());
                
        spHandler.check_open_transaction();
        spHandler.em_persist(node.getAttributes());
        spHandler.raw_persist(node, node.getIdx());        
        spHandler.check_commit_transaction();  
        return node;
    }
    private void registerFSEAEntity( FSEA_Bootstrap bt, long idx ) throws SQLException
    {        
        FileSystemElemAttributes node = bt.getNode(idx);
                
        spHandler.check_open_transaction();
        spHandler.em_persist(node);
        spHandler.check_commit_transaction();    
    }
   
    
}
