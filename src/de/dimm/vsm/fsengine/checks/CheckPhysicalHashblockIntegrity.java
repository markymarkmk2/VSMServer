/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.checks;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.FS_FileHandle;
import de.dimm.vsm.fsengine.HashCache;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class CheckPhysicalHashblockIntegrity implements ICheck {

    AbstractStorageNode snode;
    List<File> unusedDHBs = new ArrayList<File>();
    long dataSize = 0;
    StorageNodeHandler snHandler;
    StoragePoolHandler poolhandler;
    HashCache hashCache;
    boolean abort;
    String errText = "";

    @Override
    public String getErrText()
    {
        return errText;
    }



    @Override
    public boolean init( Object obj )
    {
        if (obj instanceof AbstractStorageNode)
        {
            this.snode = (AbstractStorageNode)obj;
            return true;
        }
        return false;
    }

    @Override
    public boolean check()
    {
        StoragePool pool = snode.getPool();

        try
        {
            poolhandler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, User.createSystemInternal(), false);
        }
        catch (IOException iOException)
        {
            return false;
        }
        snHandler = StorageNodeHandler.createStorageNodeHandler(snode, poolhandler);

        hashCache = LogicControl.getStorageNubHandler().getHashCache(pool);
        if (snode.isFS())
        {
            File fs = new File( snode.getMountPoint() + StorageNodeHandler.PATH_DEDUPNODES_PREFIX );

            try
            {
                checkRecursiveChildrenNodes(fs, 7, 0);
            }
            catch (Exception exception)
            {
                errText = "Fehler in checkRecursiveChildrenNodes: " + exception.getMessage();
                LogManager.err_db(errText);
                return false;
            }
        }
        return true;
    }

  

    int getIdFromFile( File file )
    {
        int didx = -1;
        try
        {
            didx = Integer.parseInt(file.getName(), 16);
        }
        catch (NumberFormatException numberFormatException)
        {
        }
        return didx;
    }

    private void checkRecursiveChildrenNodes( File fs, int i, long idx ) throws Exception
    {
        
        File[] children = fs.listFiles();
        if (children == null)
            throw new IOException("Fehler beim lesen von Verzeichnis " + fs.getAbsolutePath());

        idx <<= 8;

        if (i > 0)
        {
                       
            for (int j = 0; j < children.length; j++)
            {
                if (abort)
                    return;
                File file = children[j];
                if (!file.isDirectory())
                    continue;

                int didx = getIdFromFile(file);
                if (didx == -1)
                    continue;
               
                checkRecursiveChildrenNodes(file, i-1, idx + didx);
            }
        }
        else
        {
            for (int j = 0; j < children.length; j++)
            {
                if (abort)
                    return;

                File file = children[j];
                if (file.isDirectory())
                    continue;

                if (file.getName().length() < 26)
                    continue;

                String hash = file.getName();

                if (!checkDHBExists(hash, idx, file.getAbsolutePath()))
                {
                    unusedDHBs.add(file);
                    dataSize += file.length();
                }
            }
        }
   }

    private boolean checkDHBExists( String hash, long idx, String fullPath ) throws IOException, Exception
    {

        DedupHashBlock dhb = null;

        // READ FROM CACHE
        if (hashCache.isInited())
        {
            long check_idx = hashCache.getDhbIdx(hash);
            if (idx >> 8 != check_idx >> 8)
                throw new Exception( "Idx from FS and cache do not match");

            if (check_idx > 0)
            {
                try
                {
                    dhb = poolhandler.em_find(DedupHashBlock.class, check_idx);
                }
                catch (SQLException sQLException)
                {
                    Log.err( "Kann HashBlock aus Cache nicht auflösen " + sQLException.getMessage());
                }
                if (dhb == null)
                {

                    Log.err("Kann HashBlock aus Cache nicht finden");
                    dhb = poolhandler.findHashBlock(hash );
                }
            }
        }
        else
        {
            dhb = poolhandler.findHashBlock(hash );
        }
        if (dhb == null)
        {
            Log.debug("Unbenutzten HashBlock gefunden" + ": " + fullPath);
            return false;
        }

        if (idx >> 8  != dhb.getIdx() >> 8)
        {
            // Check if we have updated DB to newer entry, then we can get rid of this
            StringBuilder sb = new StringBuilder();

            FileHandle fh = snHandler.create_file_handle(dhb, /*create*/ false);
            if (fh.exists())
            {                
                Log.debug("Altes Duplikat von Hashblock gefunden" + ": " + fullPath);
                return false;
            }

            throw new Exception( "Idx from FS and DB do not match");
        }

        return true;
    }

    @Override
    public void close()
    {
        if (poolhandler != null)
        {
            poolhandler.close_entitymanager();
        }
    }

    @Override
    public String fillUserOptions(List<String> list)
    {
        if (unusedDHBs.isEmpty())
        {
            return Main.Txt("Es wurden keine ungenutzten Blöcke gefunden");
        }
        else
        {
            list.add( Main.Txt("Nichts unternehmen"));
            list.add( Main.Txt("Unbenutzte Blöcke löschen"));
            return Main.Txt("Es wurden ungenutzte Blöcke gefunden") + ": " + unusedDHBs.size() + " " + Main.Txt("Dateien mit insg. ") + SizeStr.format(dataSize) ;
        }

    }

    @Override
    public boolean handleUserChoice( int select, StringBuffer errText )
    {
        if (select == 1)
        {
            try
            {
                for (int i = 0; i < unusedDHBs.size(); i++)
                {
                    File file = unusedDHBs.get(i);
                    file.delete();
                }
                return true;
            }
            catch (Exception e)
            {
                errText.append("Fehler beim Löschen" + ": " + e.getMessage() );
                return false;
            }
        }
        return true;
    }

    @Override
    public void abort()
    {
        abort = true;
    }

    @Override
    public String getName()
    {
        return Main.Txt("Physikalische Blöcke prüfen");
    }



}
