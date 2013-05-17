/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.net;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.HashCache;
import de.dimm.vsm.fsengine.FS_FileHandle;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.StoragePool;
import fr.cryptohash.Digest;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class FixHashUrlBug
{
    static boolean skipFSCheck = false;

    public static void fix( LogicControl control )
    {


        List<StoragePool> pools = LogicControl.getStorageNubHandler().listStoragePools();
        for (int i = 0; i < pools.size(); i++)
        {
            StoragePool storagePool = pools.get(i);
            HashCache hc = LogicControl.getStorageNubHandler().getHashCache(storagePool);
            List<String> unsafeHashes = hc.getUrlUnsafeHashes();
            if (!unsafeHashes.isEmpty())
            {
                try
                {
                    fixHashes(control, unsafeHashes, storagePool);
                }
                catch (IOException iOException)
                {
                     Log.err( "cannot open StoragePoolHandler" );
                }
            }
        }
    }

    private static void fixHashes( LogicControl control , List<String> unsafeHashes, StoragePool storagePool ) throws IOException
    {
        StoragePoolHandler sp = StoragePoolHandlerFactory.createStoragePoolHandler(storagePool, User.createSystemInternal(), false);
        sp.add_storage_node_handlers();

        GenericEntityManager gem = control.get_util_em(storagePool);
        JDBCEntityManager jem = (JDBCEntityManager)gem;
        File abortFile = new File("abortFixUrl.txt");

        Statement st = null;
        try
        {
            st = jem.createStatement();
        }
        catch (SQLException sQLException)
        {
            throw new IOException("DB-Error", sQLException);
        }
        for (int i = 0; i < unsafeHashes.size(); i++)
        {
            System.out.println("Fixing Hash " + i + " of " + unsafeHashes.size() + " (" + (100*(i+1))/unsafeHashes.size() + "% done)");
            
            try
            {
                gem.check_open_transaction();

                String oldHash = unsafeHashes.get(i);

                List<DedupHashBlock> dhbList = gem.createQuery("select T1 from DedupHashBlock T1 where T1.hashvalue='" + oldHash + "'", DedupHashBlock.class);
                if (dhbList.size() != 1)
                {
                    throw new IOException("Wrong DHB-Size for hash " + oldHash + ": " + dhbList.size());
                }
                DedupHashBlock dhb = dhbList.get(0);

                String newHash = oldHash.replace('/', '_');
                newHash = newHash.replace('+', '-');
                // OLD HASH HAD TRAILING PADDING CHAR (27 -> 28 CHARS)
                char lastCh = newHash.charAt( newHash.length() - 1 );
                if (lastCh == '=')
                    newHash = newHash.substring(0, newHash.length() - 1);

                // RENAME PHYSICAL ENTITIES
                boolean foundOnFs = false;
                boolean doMove = false;
                boolean doDelete = false;
                List<FS_FileHandle> delList = new ArrayList<FS_FileHandle>();
                List<AbstractStorageNode> snodes = sp.get_primary_storage_nodes(/*forWrite*/ false);
                for (int j = 0; j < snodes.size(); j++)
                {
                    AbstractStorageNode snode = snodes.get(j);
                    FS_FileHandle fsfh = (FS_FileHandle) FS_FileHandle.create_dedup_handle(snode, dhb, false);
                    if (skipFSCheck)
                    {
                        foundOnFs = true;
                        doMove = true;
                        continue;
                    }

                    if (fsfh.get_fh().exists())
                    {
                        foundOnFs = true;

                        dhb.setHashvalue(newHash);
                        FS_FileHandle newFsfh = (FS_FileHandle) FS_FileHandle.create_dedup_handle(snode, dhb, false);
                        if (!newFsfh.get_fh().exists())
                        {
                            File parentDir = fsfh.get_fh().getParentFile();
                            if (!fsfh.get_fh().renameTo(newFsfh.get_fh()))
                                throw new IOException("cannot rename " + fsfh.get_fh().getAbsolutePath() + " to " + newFsfh.get_fh().getAbsolutePath());

                            doMove = true;

                            // GET RID OF EMTY DIRS

                            while (parentDir != null && parentDir.listFiles().length == 0)
                            {
                                File _f = parentDir;
                                parentDir = parentDir.getParentFile();
                                _f.delete();
                            }
                        }
                        else
                        {
                            Log.debug("Checking duplicate HashEntries");
                            if (fsfh.get_fh().length() != newFsfh.get_fh().length())
                                throw new IOException( "Invalid len" );


                            Digest digest = new fr.cryptohash.SHA1();

                            byte[] olddata = fileread( fsfh.get_fh() );
                            byte[] newdata = fileread( newFsfh.get_fh() );

                            byte[] oldhash = digest.digest(olddata);
                            byte[] newhash = digest.digest(newdata);


                            String oldHashStr = CryptTools.encodeUrlsafe( oldhash);
                            String newHashStr = CryptTools.encodeUrlsafe( newhash);
                            if (!oldHashStr.equals(newHashStr) || !newHashStr.equals(newHash))
                            {
                                throw new IOException("Hash mismatch: " + newFsfh.get_fh().getAbsolutePath());
                            }
                            // OKAY, WE ALREADY HAVE NEW BLOCK, WE ARE GOING TO UPDATE DATABASE, THEN DELETE
                            delList.add(fsfh);
                            
                            doDelete = true;
                        }
                    }
                    else
                    {
                        // NOT FOUND MAYBE TARGET EXIST ALREADY (ABORTED?)
                        dhb.setHashvalue(newHash);
                        FS_FileHandle newFsfh = (FS_FileHandle) FS_FileHandle.create_dedup_handle(snode, dhb, false);
                        if (newFsfh.get_fh().exists())
                        {
                            // THEN UPDATE DB ANYWAY
                            foundOnFs = true;
                            doMove = true;
                        }
                    }
                }

                if (!foundOnFs)
                {
                    String s = "Hash block missing: " + oldHash;
                    List<HashBlock> hlist = gem.createQuery("select T1 from HashBlock T1 where dedupBlock_idx=" + dhb.getIdx(), HashBlock.class);
                    HashMap<Long,FileSystemElemNode> nodeMap = new HashMap<Long, FileSystemElemNode>();
                    for (int j = 0; j < hlist.size(); j++)
                    {
                        HashBlock hashBlock = hlist.get(j);
                        if (!nodeMap.containsKey(hashBlock.getFileNode().getIdx()))
                        {
                            nodeMap.put(hashBlock.getFileNode().getIdx(), hashBlock.getFileNode());
                        }
                    }


                    s += " " + " Affected file(s) ";
                    Collection<FileSystemElemNode> fset = nodeMap.values();
                    for (FileSystemElemNode fileSystemElemNode : fset)
                    {
                        s += fileSystemElemNode.getName();
                        s += "\n;";
                    }
                    if (Main.get_bool_prop(GeneralPreferences.HASH_URL_FORMAT_EMPTY_LINKS, false))
                    {
                        Log.err(s);
                        continue;
                    }
                    else
                    {
                        throw new IOException(s);
                    }
                }
                

                if (doMove)
                {
                    // FILE WAS MOVED TO NEW LOCATION, UPDATE WRONG HASHVALUES IN DB
                    dhb.setHashvalue(newHash);

                    // WE HAVE TWO DIFFERENT DHB ENTRIES WITH IDENTICAL CONTENT,
                    // WE HAVE TO REMOVE THE OLD ONE (FILE WAS DELETED ALREADY) UND UPDATE DATABASE TO NEW DHB
                    dhbList = gem.createQuery("select T1 from DedupHashBlock T1 where T1.hashvalue='" + newHash + "'", DedupHashBlock.class);
                    if (dhbList.size() == 0)
                    {
                        // NO NEW HASH ENTRY EXISTS, WE UPDATE OLD HASHBLOCK AND CORRECT THE HASHENTRIES
                        jem.nativeUpdate(st, "update DedupHashBlock set hashvalue='" + newHash + "' where idx=" + dhb.getIdx() );

                        jem.nativeUpdate(st, "update HashBlock set hashvalue='" + newHash + "' where dedupBlock_idx=" + dhb.getIdx() );
                        jem.nativeUpdate(st, "update XANode set hashvalue='" + newHash + "' where dedupBlock_idx=" + dhb.getIdx() );
                    }
                    else
                    {
                        // NEW HASH ENTRY EXISTS, WE DELEET OLD HASHBLOCK AFTER CORRECTING THE HASHENTRIES
                        DedupHashBlock newDhb = dhbList.get(0);

                        // UPDATE REFERENCES ON OLD DHB TO NEW DHB
                        jem.nativeUpdate(st, "update HashBlock set hashvalue='" + newHash + "',dedupBlock_idx=" + newDhb.getIdx() + " where dedupBlock_idx=" + dhb.getIdx() );
                        jem.nativeUpdate(st, "update XANode set hashvalue='" + newHash + "',dedupBlock_idx=" + newDhb.getIdx() + " where dedupBlock_idx=" + dhb.getIdx() );

                        // FINALLY REMOVE OLD DHB
                        jem.nativeUpdate(st, "delete from DedupHashBlock where idx=" + dhb.getIdx() );
                    }


                }
                if (doDelete)
                {
                    // WE HAVE TWO DIFFERENT DHB ENTRIES WITH IDENTICAL CONTENT,
                    // WE HAVE TO REMOVE THE OLD ONE (FILE WAS DELETED ALREADY) UND UPDATE DATABASE TO NEW DHB
                    dhbList = gem.createQuery("select T1 from DedupHashBlock T1 where T1.hashvalue='" + newHash + "'", DedupHashBlock.class);
                    if (dhbList.size() != 1)
                    {
                        throw new IOException("Wrong DHB-Size for new hash " + newHash + ": " + dhbList.size());
                    }
                    DedupHashBlock newDhb = dhbList.get(0);

                    // UPDATE REFERENCES ON OLD DHB TO NEW DHB
                    jem.nativeUpdate(st, "update HashBlock set hashvalue='" + newHash + "',dedupBlock_idx=" + newDhb.getIdx() + " where dedupBlock_idx=" + dhb.getIdx() );
                    jem.nativeUpdate(st, "update XANode set hashvalue='" + newHash + "',dedupBlock_idx=" + newDhb.getIdx() + " where dedupBlock_idx=" + dhb.getIdx() );

                    // FINALLY REMOVE OLD DHB
                    jem.nativeUpdate(st, "delete from DedupHashBlock where idx=" + dhb.getIdx() );
                }

                gem.check_commit_transaction();
                
                for (int j = 0; j < delList.size(); j++)
                {
                    FS_FileHandle fsfh = delList.get(j);
                    
                    fsfh.delete();
                    
                    // GET RID OF EMTY DIRS
                    File parentDir = fsfh.get_fh().getParentFile();
                    while (parentDir != null && parentDir.listFiles().length == 0)
                    {
                        File _f = parentDir;
                        parentDir = parentDir.getParentFile();
                        _f.delete();
                    }
                }
            }
            catch (Exception exc)
            {
                try
                {
                    gem.commit_transaction();
                }
                catch (SQLException _exc)
                {
                     Log.err( _exc.getMessage(), _exc );
                }
                Log.err( exc.getMessage(), exc );

                break;
            }
            if (abortFile.exists())
            {
                Log.err( "Abbruch durch Benutzer" );
                break;
            }
        }
        try
        {
            gem.commit_transaction();
            st.close();
        }
        catch (SQLException _exc)
        {
             Log.err( _exc.getMessage(), _exc );
        }
    }

    private static byte[] fileread( File _fh ) throws IOException
    {
        byte[] data = new byte[(int)_fh.length()];
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(_fh);
            fis.read(data);

        }
        catch (IOException iOException)
        {
            throw new IOException( "readerr " + _fh.getAbsolutePath() );
        }
        finally
        {
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        return data;
    }


   
}
