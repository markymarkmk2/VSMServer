/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.lifecycle;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Exceptions.RetentionException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.LazyList;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.fsengine.DerbyStoragePoolNubHandler;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.Retention;
import de.dimm.vsm.records.Snapshot;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;




class RetentionQueryResult
{
    Class clazz;
    String fname;

    public RetentionQueryResult( Class clazz, String fname )
    {
        this.clazz = clazz;
        this.fname = fname;
    }
}

/**
 *
 * @author Administrator
 */
public class RetentionManager extends WorkerParent
{

    Thread runner;
    //GenericEntityManager em;

    static final int fIdxCol = 0;
    static final int fSizeCol = 1;
    static final int mTimeCol = 2;
    static final int fnameCol = 3;
    static final int aIdxCol = 4;
    static final int aTsCol = 5;

    boolean enabled = false;

    final RetentionQueryResult[] qryResult =
    {
        new RetentionQueryResult( Long.class, "fidx"),
        new RetentionQueryResult( Long.class, "fsize"),
        new RetentionQueryResult( Long.class, "mtime"),
        new RetentionQueryResult( String.class, "fname"),
        new RetentionQueryResult( Long.class, "aidx"),
        new RetentionQueryResult( Long.class, "ts"),
    };
    final String qryFieldStr = "f.idx as fidx, a.fsize as fsize, a.modificationDateMs as mtime, a.name as fname, a.idx as aidx, a.ts as ts";

    DerbyStoragePoolNubHandler nubHandler;

    public RetentionManager(DerbyStoragePoolNubHandler nubHandler)
    {
        super("RetentionManager");
       // em = LogicControl.createEntityManager();
        //setTaskState(TASKSTATE.PAUSED);

        this.nubHandler = nubHandler;
    }

    @Override
    public boolean initialize()
    {
        return true;
    }
    @Override
    public boolean isPersistentState()
    {
        return true;
    }



    
    @Override
    public void run()
    {      
        is_started = true;
        GregorianCalendar cal = new GregorianCalendar();
        int last_checked = cal.get(GregorianCalendar.HOUR_OF_DAY);

        setStatusTxt("");
        while(!isShutdown())
        {
            LogicControl.sleep(1000);

            if (isPaused())
            {
                last_checked = -1;
                continue;
            }
            if (!enabled)
            {
                setStatusTxt(Main.Txt("Deaktiviert"));
                setPaused(true);
                last_checked = -1;
                continue;
            }

            cal.setTime( new Date());
            
            // ONCE EVERY HOUR
            int check = cal.get(GregorianCalendar.HOUR_OF_DAY);
            if (check == last_checked)
                continue;

            last_checked = check;

            // OK, WE HAVE A NEW MINUTE, CHECK ALL SCHEDS            

            List<StoragePool> list = nubHandler.listStoragePools();

            setStatusTxt(Main.Txt("Überprüfe Retention"));
            for (int i = 0; i < list.size(); i++)
            {
                StoragePool storagePool = list.get(i);

                try
                {
                    handleRetention(storagePool);
                }
                catch (PoolReadOnlyException poolReadOnlyException)
                {
                    Log.warn("StoragePool ist schreibgeschützt",  storagePool.toString());
                }
                catch (Exception exc)
                {
                    Log.err("Fehler bei Gültigkeitsberechnug von Pool" , storagePool.toString(), exc);
                }
            }
            setStatusTxt("");
        }
        finished = true;
    }

    private boolean isStringArgType( Retention retention )
    {
        return retention.getArgType().equals(Retention.ARG_NAME);
    }

    boolean abortCleanDedups = false;
    public void abortDeleteFreeBlocks()
    {
        abortCleanDedups = true;
    }

    long handleCleanDedups( StoragePoolHandler handler) throws SQLException, IOException, PathResolveException, UnsupportedEncodingException, PoolReadOnlyException
    {
        abortCleanDedups = false;
        GenericEntityManager em = nubHandler.getUtilEm(handler.getPool());

        setStatusTxt("Berechne freien DedupSpeicher");

        /*
         * rs = st.executeQuery("select count(*), sum(bigint(DEDUPHASHBLOCK.BLOCKLEN)) from DEDUPHASHBLOCK"
                        + " LEFT OUTER JOIN XANODE  ON DEDUPHASHBLOCK.idx = XANODE.dedupblock_idx"
                        + " left outer join hashblock on DEDUPHASHBLOCK.idx = hashblock.dedupblock_idx"
                        + " where XANODE.idx is null and hashblock.idx is null
         */

        // SEARCH FÜR DEDUP NODES NOT NEEDED AS HASHBLOCK OR AS XANODE
        List<Object[]> oList = em.createNativeQuery("select DEDUPHASHBLOCK.IDX from DEDUPHASHBLOCK LEFT OUTER JOIN XANODE ON DEDUPHASHBLOCK.idx = XANODE.dedupblock_idx "
                + "left outer join hashblock on DEDUPHASHBLOCK.idx = hashblock.dedupblock_idx "
                + "where XANODE.idx is null and hashblock.idx is null", 0);
        

        long cnt = 0;
        if (!oList.isEmpty())
        {
            setStatusTxt("Entferne freien DedupSpeicher");

            
            long size = 0;
            for (int i = 0; i < oList.size(); i++)
            {
                setStatusTxt(Main.Txt("Entferne freien DedupSpeicher")  + "(Block " + i + "/" + oList.size() + ")" );
                         
                if (abortCleanDedups)
                    break;

                Object[] oarr = oList.get(i);
                if (oarr.length == 1)
                {
                    String idxStr = oarr[0].toString();

                    long l = Long.parseLong(idxStr);
                    if (l > 0)
                    {
                        DedupHashBlock hb = em.em_find(DedupHashBlock.class, l);
                        if (hb != null)
                        {
                            handler.removeDedupBlock( hb, null );                           
                            cnt++;
                            size += hb.getBlockLen();

                            handler.check_commit_transaction();
                        }
                    }
                }
            }
            Log.debug("Enfernte DedupBlöcke", Long.toString(cnt));
            Log.debug("Freigewordener DedupSpeicher", SizeStr.format(size));
        }
        if (abortCleanDedups)
            setStatusTxt("Löschen wurde abgebrochen");
        else
            setStatusTxt("");

        return cnt;
    }


    public RetentionResultList createRetentionResult( Retention retention,StoragePool pool, long startIdx, int qryCount, long absTs ) throws IOException, SQLException
    {        

        GenericEntityManager em = nubHandler.getUtilEm(pool);

        // THE FIELDS MUST BE FIXED TO THE RESULT!!!!

        // SELECT ALL DIRECT LINKED ATTRIBUTE RESULTS
        String select_str = "select " + qryFieldStr + " from FileSystemElemNode f, FileSystemElemAttributes a "
                + "where f.idx = a.file_idx and f.pool_idx=" + pool.getIdx();
                
        //StringBuilder where = new StringBuilder();

        StringBuilder retention_where = new StringBuilder();
        buildRetentionWhereString(em, retention, retention_where, absTs);

        if (retention_where.length() == 0)
            throw new IOException("Invalid Retention query" );

        // ADD RETENTION QUERY
        select_str += " and " + retention_where.toString();

        select_str += " order by a.idx asc";
       

//        // DETECT FIRST AIDX TO SPPED UP QUERY
//        String min_aidx_str = "select a.idx from FileSystemElemNode f, FileSystemElemAttributes a where f.idx = a.file_idx and f.pool_idx=" + pool.getIdx();
//        min_aidx_str += " and " + retention_where.toString();
//        min_aidx_str += " and a.idx>" + startIdx + " order by a.idx asc";
//        List<Object[]> aidxres = em.createNativeQuery(min_aidx_str,1);
//        if (aidxres.isEmpty())
//        {
//            RetentionResultList rl = new RetentionResultList(retention, new ArrayList<Object[]>());
//            return rl;
//        }
//        Object[] object = (Object[]) aidxres.get(0);
//        long first_aidx = (Long) object[0];
//
//         // ADD PARTIAL QUERY RESULTSTRING
//        select_str += " and a.idx between " + first_aidx + " and " + first_aidx + qryCount + " order by a.idx asc";

        

        List<Object[]> nres = em.createNativeQuery(select_str, 0/*, qryCount*/);


//
//        // SELECT ALL HISTORY LINKED ATTRIBUTE RESULTS
//        select_str = "select " + qryFieldStr + " from FileSystemElemNode f, FileSystemElemAttributes a where f.idx = a.file_idx and f.pool_idx=" + pool.getIdx();
//
//
//        // ADD RETENTION QUERY
//        select_str += where.toString();
//
//        // ADD PARTIAL QUERY RESULTSTRING
//        select_str += " and a.idx>" + startIdx + " order by a.idx asc";
//
//        List<Object[]> nres2 = em.createNativeQuery(select_str, qryCount);
//
//        nres.addAll(nres2);

        RetentionResultList rl = new RetentionResultList(retention, nres);
                     
        return rl;
    }

    private void buildRetentionWhereString(  GenericEntityManager em, Retention retention, StringBuilder sb, long absTs )
    {        

        if (sb.length() > 0)
        {
            if (retention.isOrWithPrevious())
            {
                sb.append(" or ");
            }
            else
            {
                sb.append(" and ");
            }
        }
        if (retention.isNeg())
        {
            sb.append(" not ");
        }
        List<Retention> list = retention.getChildren(em);

        if (list != null && !list.isEmpty())
        {

            StringBuilder child_where = new StringBuilder();
            for (int i = 0; i < list.size(); i++)
            {
                Retention child_r = list.get(i);

                buildRetentionWhereString(em, child_r, child_where, absTs);
            }
            if (child_where.length() > 0)
            {
                sb.append("(");
                sb.append(child_where);
                sb.append(")");
            }
        }
        else
        {
            sb.append(retention.getSqlFieldforArgtype());

            sb.append(Retention.getSqlOpString(retention.getArgOp()));

            if (isStringArgType(retention))
            {
                sb.append("'");
                if (retention.getArgOp().equals(Retention.OP_BEGINS) || retention.getArgOp().equals(Retention.OP_CONTAINS))
                {
                    sb.append("%");
                }
            }

            // MAKE REL TS TO ACT_TS
            if (Retention.isRelTSField(retention.getArgType()))
            {
                long ts = Long.parseLong(retention.getArgValue());
                ts = absTs - ts;
                sb.append(Long.toString(ts));
            }
            else
            {
                sb.append(retention.getArgValue());
            }

            if (isStringArgType(retention))
            {
                if (retention.getArgOp().equals(Retention.OP_ENDS) || retention.getArgOp().equals(Retention.OP_CONTAINS))
                {
                    sb.append("%");
                }

                sb.append("'");
            }
        }
    }


    public RetentionResult handleRetentionList( StoragePoolHandler sp_handler, RetentionResultList retentionList ) throws RetentionException, SQLException, PoolReadOnlyException, IOException, PathResolveException
    {
        long sum = 0;
        int cnt = 0;
        StoragePool pool = sp_handler.getPool();
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.YYYY HH:mm:ss");

        List res = retentionList.list;
        Retention retention = retentionList.retention;
        StoragePool targetPool = null;
        GenericEntityManager em = sp_handler.getEm();


        // LOAD TARGET POOL IF NECESSARY FOR MOVE OP
        if (retention.getFollowAction().equals(Retention.AC_MOVE))
        {
            String args = retention.getFollowActionParams();
            if (args.charAt(0) == 'P')
            {
                long tpool_idx = Integer.parseInt(args.substring(1));
                targetPool = em.em_find(StoragePool.class, tpool_idx);
                if (targetPool == null)
                    throw new RetentionException("Missing TargetPool for Move Retention");

                if (targetPool.getIdx() == pool.getIdx())
                    throw new RetentionException("Move failed, Target Node == Source Node");
            }
        }


        // RES IS SORTED a.idx ASC
        
        
        long lastFidx = -1;
        for (int i = res.size() - 1; i >= 0; i--)
        {
            Object[] object = (Object[]) res.get(i);
            long fidx = (Long) object[fIdxCol];
            long fsize = (Long) object[fSizeCol];
            long mtime = (Long) object[mTimeCol];
            String fname = (String) object[fnameCol];
            long aidx = (Long) object[aIdxCol];
            long ts = (Long) object[aTsCol];

            // SKIP TO NEXT NODE
            if (fidx == lastFidx)
                continue;

            cnt++;
            lastFidx = fidx;

            // LOAD NODE FROM DB
            FileSystemElemNode fse = sp_handler.resolve_fse_node_from_db(fidx);
            if (fse == null)
            {
                Log.debug("Node nicht gefunden",  fidx + " " + fname);
                continue;
//                throw new RetentionException("Node not found: " + fidx + " " + fname);
            }
            if (fse.getAttributes() == null)
            {
                Log.warn("Node ohne Attribute", fidx + " " + fname);
                throw new RetentionException("Attributes not found: " + fidx + " " + fname);
            }
            if (!fse.isFile())
            {
                // TODO RETENTION OF DIRECTORIES, LINKS ETC.
                continue;
            }
            
            // DO NOT DELETE ARTIFICIAL ROOT NODE
            if (fse.getIdx() == sp_handler.getRootDir().getIdx())
                continue;

            // NOW CHECK, WHICH BLOCKS OF THIS NODE WE CAN REMOVE
            // BUILD LIST OF ALL ATTRS TO RETENTION
            HashMap<Long,Long> retentionAttrMap = new HashMap<Long,Long>();
            retentionAttrMap.put(aidx, aidx);

            for (int j = i - 1; j >= 0; j--)
            {
                Object[] nextObjects = (Object[]) res.get(j);
                long nextFidx = (Long) nextObjects[fIdxCol];
                if (nextFidx != fidx)
                    break;

                Long lAidx = (Long) nextObjects[aIdxCol];
                retentionAttrMap.put(lAidx, lAidx);
            }

            List<FileSystemElemAttributes> history = fse.getHistory().getList(em);
            // Sort Newest first
            java.util.Collections.sort(history, new Comparator<FileSystemElemAttributes>()
            {

                @Override
                public int compare( FileSystemElemAttributes o1, FileSystemElemAttributes o2 )
                {
                    if (o1.getTs() != o2.getTs())
                    {
                        return (o1.getTs() - o2.getTs() > 0) ? 1 : -1;
                    }

                    return (o2.getIdx() - o1.getIdx() > 0) ? -1 : 1;
                }
            });
                // BUILD LIST OF ALL ATTRIBUTES TO KEEP
            List<FileSystemElemAttributes> keepList = new ArrayList<>();
            List<FileSystemElemAttributes> removeList = new ArrayList<>();


            // REMEMBER THE NEWEST ATTRIBUTE-ENTRY, THIS IS SET WITH fse.setAttribute()
            FileSystemElemAttributes newActualAttribute = null;


//            // IS ACTUAL ATTRIBUTE-ENTRY IN RETENTION LIST?
//            if (!retentionAttrMap.containsKey(fse.getAttributes().getIdx()))
//            {
//                // NO, ADD TO KEEP LIST AND SAVE AS ACTUAL ATTRIBUTE-ENTRY
//                keepList.add(fse.getAttributes());
//                newActualAttribute = fse.getAttributes();
//            }

            // ADD ALL HISTORY ATTRIBUTE-ENTRIES NOT IN RETENTIONLIST TO KEEP LIST
            FileSystemElemAttributes actualAttrribute = fse.getAttributes();
            FileSystemElemAttributes actualAttrribute2 = history.get(history.size() - 1);

            if (actualAttrribute.getIdx() != actualAttrribute2.getIdx())
            {
                throw new RetentionException("Attributes direction mismatch: " + fidx + " " + fname);
            }

            // DETECT THE NEW ACTUAL ATTRIBUTE, THIS IS THE NEWEST IN KEEPLIST
            for (int j = 0; j < history.size(); j++)
            {
                FileSystemElemAttributes fsea = history.get(j);
                if (!retentionAttrMap.containsKey(fsea.getIdx()))
                {
                    keepList.add(fsea);
                    // FIND THE NEWEST ATTRIBUTE
                    if (newActualAttribute == null || newActualAttribute.getTs() < fsea.getTs())
                        newActualAttribute = fsea;
                }                
                else
                {
                    removeList.add(fsea);
                }
            }



            // NOW CHECK; IF WE CAN DELETE A FILE COMPLETELY
            boolean deleteEntry = false;
            if (retention.getMode().equals( Retention.MD_BACKUP ))
            {
                // IN BACKUP MODE KEEP LIST HAS TO BE EMPTY *AND* LAST ATTRIBUTE HAS TO BE DELETED=TRUE
                if (keepList.isEmpty())
                {
                    if (actualAttrribute.isDeleted())
                        deleteEntry = true;
                    else
                    {
                        // OTHERWISE WE HAVE TO KEEP NEWEST ENTRY SO THAT THE FILE CAN EXIST IN FS
                        keepList.add(actualAttrribute);

                        if (newActualAttribute == null || newActualAttribute.getTs() < actualAttrribute.getTs())
                            newActualAttribute = actualAttrribute;

                    }
                }
            }
            else if(retention.getMode().equals(Retention.MD_ARCHIVE))
            {
                // IN ARCHIVAL MODE DATA IS REMOVED AFTER RETENTION TIME
                if (keepList.isEmpty())
                {
                    deleteEntry = true;
                }
            }
            else
            {
                throw new RetentionException("Invalid Retention mode " + fidx + " " + fname);
            }
            
            // NOTHING LEFT TO KEEP FROM THIS NODE?
            if ( deleteEntry )
            {
                sum += fsize;
                // HANDLE FULL ENTRY
                if (retention.getFollowAction().equals(Retention.AC_DELETE))
                {
                    Log.debug("Node wird gelöscht", fidx + ": " + fname + " size:" + fsize + " TS:" +  sdf.format(new Date(ts)) + " mtime:" + sdf.format(new Date(mtime)) );
                    if (!retention.isTestmode())
                    {
                        delete_fse_node(fse, sp_handler);
                    }
                }
                else if(retention.getFollowAction().equals(Retention.AC_MOVE))
                {
                    Log.debug("Node wird verschoben", fidx + ": " + fname + " size:" + fsize + " TS:" +  sdf.format(new Date(ts)) + " mtime:" + sdf.format(new Date(mtime)) + " -> " + targetPool.toString() );
                    if (!retention.isTestmode())
                    {
                        String args = retention.getFollowActionParams();
                        if (args.charAt(0) == 'P')
                        {                            
                            move_fse_node( fse, sp_handler, targetPool );
                        }
                    }
                }
                continue;
            }

            // DELETE OBSOLETE ATTRIBUTES WHICH ARE NOT IN keepList
            for (int j = 0; j < removeList.size(); j++)
            {
                FileSystemElemAttributes fileSystemElemAttributes = removeList.get(j);

                boolean skipDelete = false;
                for( int k = 0; k < keepList.size(); k++)
                {
                    if (keepList.get(k).getIdx() == fileSystemElemAttributes.getIdx())
                        skipDelete = true;
                }

                if (skipDelete)
                    continue;
                
                if (fse.getHistory().removeIfRealized( fileSystemElemAttributes))
                {
                    sp_handler.em_remove(fileSystemElemAttributes);
                }                    
            }

            // NOW GET THE LIST OF BLOCKS WHICH CAN BE DELETED
            List<HashBlock> retentionHashBlocks = createRetentionHashBlockList( em, fse, keepList );

            if (!retentionHashBlocks.isEmpty())
            {
                for (int j = 0; j < retentionHashBlocks.size(); j++)
                {
                    HashBlock hashBlock = retentionHashBlocks.get(j);
                    sum += hashBlock.getBlockLen();

                    // AND DELETE THEM
                    if (retention.getFollowAction().equals(Retention.AC_DELETE))
                    {
                        Log.debug("Block wird gelöscht", hashBlock.getIdx() + ": " + fname + " pos:" + hashBlock.getBlockOffset() +  " TS:" + sdf.format(hashBlock.getTs()) );
                        if (!retention.isTestmode())
                        {
                            sp_handler.em_remove(hashBlock);
                            // NO BLOCK DELETION HERE, THIS IS DONE AFTERWARDS, SO RETENTION AND BLOCK-REMOVAL CANNOT COLLIDE
                            //sp_handler.removeHashBlock( hashBlock );
                            if (!fse.getHashBlocks().removeIfRealized(hashBlock))
                                throw new RetentionException("Cannot remove deleted hashblock from Node");
                        }
                    }
                    else if(retention.getFollowAction().equals(Retention.AC_MOVE))
                    {
                        Log.debug("Block wird verschoben", hashBlock.getIdx() + ": " + fname + " pos:" + hashBlock.getBlockOffset() +  " TS:" + sdf.format(hashBlock.getTs())  + " -> " + targetPool.toString() );
                        if (!retention.isTestmode())
                        {
                            String args = retention.getFollowActionParams();
                            if (args.charAt(0) == 'P')
                            {
                                sp_handler.moveHashBlock( hashBlock, targetPool );
                            }
                        }
                    }
                }

                // RESET LOADED HISTORY LIST
                if (fse.getHistory() instanceof LazyList)
                {
                    ((LazyList)fse.getHistory()).unRealize();
                }
                // RESET LOADED HISTORY LIST
                if (fse.getHashBlocks() instanceof LazyList)
                {
                    ((LazyList)fse.getHashBlocks()).unRealize();
                }
                // UPDATE CHANGED ATTRIBUTES
                if (newActualAttribute != null && fse.getAttributes().getIdx() != newActualAttribute.getIdx())
                {
                    fse.setAttributes(newActualAttribute);
                    sp_handler.em_merge(fse);
                }
            }
            // TODO: WE HAVE TO UPDATE ACTUAL DATAFILE IN LANDINGZONE
        }

        // FLUSH CHANGES
        sp_handler.commit_transaction();
        
        RetentionResult ret = new RetentionResult(pool.getIdx(), new Date(), cnt, sum);
        return ret;
    }

    public long handleDeleteFreeBlocks( StoragePool pool ) throws Exception
    {
        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = null;

        try
        {

            sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, user, /*rdonly*/ false);
            if (pool.getStorageNodes().isEmpty(sp_handler.getEm()))
            {
                throw new RetentionException("No Storage for pool defined");
            }

            sp_handler.check_open_transaction();

            return handleCleanDedups(sp_handler);
        }
        catch (SQLException exc)
        {
            sp_handler.rollback_transaction();
            throw exc;
        }
        catch (Exception exc)
        {
             Log.err("Abbruch beim Freigeben von DedupBlöcken", exc);
             throw exc;
        }
        finally
        {
            if (sp_handler != null)
            {
                sp_handler.commit_transaction();
                sp_handler.close_transaction();
                sp_handler.close_entitymanager();
            }
        }
    }
    
    public RetentionResult handleRetention( StoragePool pool ) throws RetentionException, IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        long startIdx = 0;
        int qryCount = 10000;

        User user = User.createSystemInternal();

        // THIS IS THE TIMESTAMP FOR ALL RELATIVE TIMESTAMPS
        long absTs = System.currentTimeMillis();

        GenericEntityManager em = nubHandler.getUtilEm(pool);

        StoragePoolHandler sp_handler = null;

        RetentionResult ret = null;

        List<Snapshot> snapshots = em.createQuery("select T1 from Snapshot T1 where T1.pool_idx=" + pool.getIdx() + " order by creation desc", Snapshot.class);
        List<Retention> retentions = em.createQuery("select T1 from Retention T1 where T1.disabled=0 and T1.pool_idx=" + pool.getIdx(), Retention.class);
        if (retentions.isEmpty())
            return null;

        
        try
        {

            sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, user, /*rdonly*/ false);
            if (pool.getStorageNodes().isEmpty(sp_handler.getEm()))
            {
                throw new RetentionException("No Storage for pool defined");
            }

            ret = new RetentionResult(pool.getIdx(), new Date(), 0, 0);

            for (int i = 0; i < retentions.size(); i++)
            {
                Retention retention = retentions.get(i);

                // ONLY HANDLE ROOT NODES
                if (retention.getParent() != null)
                {
                    // CHILDREN ARE HANDLED IN buildRetentionWhereString
                    continue;
                }

                // CREATE A LIST OF ALL ENTRIES TO BE REMOVED BASED ON RETENTION PARAMS
                RetentionResultList retentionResult = createRetentionResult(retention, pool, startIdx, qryCount, absTs);

                if (retentionResult.list.isEmpty())
                {
                    break;
                }

                // CREATE SUBLIST OF RETENTION ENTRIES WHICH CAN BE REMOVED REGARDING SNAPSHOTS
                RetentionResultList snapshotRetentionResult = createSnapshotRetentionList(snapshots, retentionResult, pool);


                // DO RETENTION WITH THE FINAL LIST
                RetentionResult localret = handleRetentionList(sp_handler, snapshotRetentionResult);
                ret.add(localret);
                    
            }

            // NOW REMOVE UNUSED DEDUPBLOCKS
            // TODO, THIS SHOULD BE HANDLED FROM OWN SCHEDULER -> SPACE USED x%. AGE or SIMPLY IMMEDIATELY
            // handleCleanDedups(sp_handler);
        }
        finally
        {
            if (sp_handler != null)
            {
                sp_handler.commit_transaction();
                sp_handler.close_transaction();
                sp_handler.close_entitymanager();
            }
        }
        return ret;
    }



    private void delete_fse_node( FileSystemElemNode fse, StoragePoolHandler sp_handler ) throws RetentionException, PoolReadOnlyException
    {
        boolean remove_parent = false;
        GenericEntityManager em = sp_handler.getEm();

        // DELEET ONLY FILES
        if (!fse.isDirectory())
        {
            // DIRS ARE DELETED IF LAST FILE IS DELETED
            if (fse.getParent().getChildren(em).size() == 1)
            {
                if (fse.getParent().getChildren(em).get(0).getIdx() != fse.getIdx())
                {
                    throw new RetentionException("Parentfolder does not match");
                }

                remove_parent = true;
            }

            sp_handler.remove_fse_node(fse, false);

        }
        else
        {
            // DELETE EMPTY DIRS EXCEPT ROOT
            if (fse.getChildren(em).isEmpty() && fse.getParent() != null)
            {
                // DIRS ARE DELETED IF LAST FILE IS DELETED
                if (fse.getParent().getChildren(em).size() == 1)
                {
                    if (fse.getParent().getChildren(em).get(0).getIdx() != fse.getIdx())
                    {
                        throw new RetentionException("Parentfolder does not match");
                    }

                    remove_parent = true;
                }
                sp_handler.remove_fse_node(fse, false);
            }
        }
        if (remove_parent)
        {
            // RECURSIVELY DELETE DIRS UPWARDS
            delete_fse_node( fse.getParent(), sp_handler );
        }
    }

    private void move_fse_node( FileSystemElemNode fse, StoragePoolHandler sp_handler, StoragePool targetPool )
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public static RetentionResultList createSnapshotRetentionList( List<Snapshot> snapshots, RetentionResultList retentionResult, StoragePool pool )
    {
        // ALL ENTRIES IN THIS LIST ARE OLD ENOUGH TO BE HANDLED, NOW CHECK IF A SNAPSHOT PREVENTS US DOING THIS
        List<Object[]> list = new ArrayList<Object[]>();
        RetentionResultList snapshotRetentionResult = new RetentionResultList(retentionResult.retention, list);


        // WE CHECK IF WE HAVE TO REMOVE ENTRIES FROM LIST BECAUSE WE HAVE A VALID SNAPSHOT
        List<Object[]> res = retentionResult.list;

        for (int i = 0; i < res.size(); i++)
        {
            // GET MTIME OF THIS ENTRY
            Object[] objects = res.get(i);            
            long mtime = (Long) objects[mTimeCol];
            long ts = (Long) objects[aTsCol];
            
            boolean protectedBySnapshot = true;

            // FIRST CHECK IF WE HAVE A SNAPSHOT YOUNGER THAN THIS ENTRY
            int snIdx = exist_snap_younger( snapshots, ts );
            if (snIdx < 0)
            {
                // IF NOT, THEN THIS ENTRY IS NOT INSIDE A SNAPSHOT, IT WAS CREATED LATER
                protectedBySnapshot = false;
            }
            else
            {
                // YES, WE HAVE TO CHECK IF THERE IS AN ENTRY YOUNGER THAN US AND OLDER THAN SNAPSHOT
                if (exists_younger_entry_between_snap_and_us( snapshots.get(snIdx), res, i, ts ))
                {
                    // IF SO, THEN THIS ENTRY TOO OLD FOR NEAREST SNAPSHOT, IT WAS CREATED TOO EARLY
                    protectedBySnapshot = false;
                }
            }

            if (!protectedBySnapshot)
            {
                list.add(objects);
            }
        }

        return snapshotRetentionResult;
    }

    private static int exist_snap_younger( List<Snapshot> snapshots, long ts )
    {
        int foundIdx = -1;

        // SNAPSHOTS ARE SORTED DATE DESCENDING
        // WE STOP AT THE FIRST SN OLDER THAN TIMESTAMP
        // IF WE FOUND A SN YOUNGER, THEN foundIdx CONTAINS AUTOMATICALLY THE NEAREST SNAPSHOT (LAST ONE BEFORE BREAK)
        for (int i = 0; i < snapshots.size(); i++)
        {
            Snapshot snapshot = snapshots.get(i);
            if (snapshot.getCreation().getTime() > ts)
            {
                foundIdx = i;
            }
            else
                break;

        }
        return foundIdx;
    }

    private static boolean exists_younger_entry_between_snap_and_us( Snapshot snapshot, List<Object[]> res, int idx, long TS )
    {
        // CHECK IF THERE IS AN ENTRY YOUNGER THAN US AND OLDER THAN SNAPSHOT
        // RES IST SORTED ORDER BY AIDX ASCENDING, YOUNGER ENTRIES HAVE BIGGER INDICES
        boolean ret = false;
        long fidx = (Long)res.get(idx)[0];

        for (int i = idx + 1; i < res.size(); i++)
        {
            Object[] objects = res.get(i);
            long testFidx = (Long) objects[fIdxCol];
            
            
            // REACHED NEXT FILENODE?
            if (testFidx != fidx)
                break;

            // WE STOP AT THE FIRST OCCURENCE OF AN ENTRY YOUNGER THAN US BUT OLDER THAN SNAPSHOT
            long testTs = (Long) objects[aTsCol];
            if (testTs > TS && testTs < snapshot.getCreation().getTime())
            {
                ret = true;
                break;
            }
        }

        return ret;
    }

    static private List<HashBlock> buildRemoveHashblockList( FileSystemElemNode node, List<HashBlock> hash_block_list, List<FileSystemElemAttributes> keepList ) throws RetentionException
    {
        List<HashBlock> ret = new ArrayList<HashBlock>();
        ret.addAll(hash_block_list);

        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hash_block_list, new Comparator<HashBlock>()
        {

            @Override
            public int compare( HashBlock o1, HashBlock o2 )
            {
                if (o1.getBlockOffset() != o2.getBlockOffset())
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        // FETCH FOR EVERY ENTRY IN KEEPLIST THE SAME OR MINIMAL OLDER ENTRY IN HASHBLOCKLIST
        for (int i = 0; i < keepList.size(); i++)
        {
            FileSystemElemAttributes fsea = keepList.get(i);
            long ts = fsea.getTs();
            List<HashBlock> keepHashBlockList = new ArrayList<HashBlock>();

            // FOR EVERY HASHBLOCK
            HashBlock lastHashBlock = null;
            for (int h = 0; h < hash_block_list.size(); h++)
            {
                HashBlock hashBlock = hash_block_list.get(h);
                if (lastHashBlock == null || (lastHashBlock.getBlockOffset() != hashBlock.getBlockOffset()))
                {
//                    // SINGULAR LAST BLOCK?
//                    if (h + 1 == hash_block_list.size())
//                    {
//                        // THIS BLOCK WILL NOT BE REMOVED
//                        keepHashBlockList.add(hashBlock);
//
//                        // REMOVE FROM GLOBAL LIST, WE WANT TO RETURN A LIST OF UNUSED BLOCKS
//                        ret.remove(hashBlock);
//                    }
                    // SCROLL THROUGH ALL BLOCKS OF ONE POSITION, NEWEST IS FIRST
                    for (int hl = h; hl < hash_block_list.size(); hl++)
                    {
                        HashBlock lhashBlock = hash_block_list.get(hl);
                        if (lhashBlock.getBlockOffset() != hashBlock.getBlockOffset())
                        {
                            break;
                        }
                        // IS THIS BLOCK TO YOUNG ?
                        if (lhashBlock.getTs() > ts)
                            continue; // SKIP

                        // ADD FIRST BLOCK WITH CORRECT POSITION, THIS WILL BE THE YOUNGEST BLOCK OLDER OR EQUAL TS
                        keepHashBlockList.add(lhashBlock);

                        // REMOVE FROM GLOBAL LIST, WE WANT TO RETURN A LIST OF UNUSED BLOCKS
                        ret.remove(lhashBlock);
                        break;
                    }
                    lastHashBlock = hashBlock;
                }
            }
            // PLAUSIBILITY:
            // WE SHOULD HAVE A COMPLETE UNIQUE LISTS OF BLOCKS REPRESINTING THE CURRENT KEEPLISTENTRY
            try
            {
                checkBlocksForNodeComplete(node, fsea, keepHashBlockList);
            }
            catch (RetentionException retentionException)
            {
                Log.warn("Node ist nicht vollständig, Retention wird vermieden", node.toString(), retentionException);
                ret.clear();
            }
        }
        return ret;
    }

    private static void checkBlocksForNodeComplete( FileSystemElemNode node, FileSystemElemAttributes fsea, List<HashBlock> keepHashBlockList ) throws RetentionException
    {
        long size = 0;
        for (int i = 0; i < keepHashBlockList.size(); i++)
        {
            HashBlock hashBlock = keepHashBlockList.get(i);
            if (hashBlock.getFileNode().getIdx() != node.getIdx())
            {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") Invalid fileID at " + size );
            }

            if (hashBlock.getBlockOffset() != size)
            {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") missing block at " + size );
            }
            if (hashBlock.getTs() > fsea.getTs())
            {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") block too young at " + size );
            }
            size += hashBlock.getBlockLen();
        }
        if (size != fsea.getFsize())
        {
            throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") invalid total size " + size );
        }
    }

    private static List<HashBlock> createRetentionHashBlockList( GenericEntityManager em, FileSystemElemNode fse, List<FileSystemElemAttributes> keepList ) throws RetentionException
    {
        // READ HASHBLOCKS
        List<HashBlock> hash_block_list = fse.getHashBlocks().getList(em);

        List<HashBlock> remove_hash_block_list = buildRemoveHashblockList( fse, hash_block_list, keepList );

        return remove_hash_block_list;
    }


}
