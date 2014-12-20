/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.lifecycle;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Exceptions.RetentionException;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.IStoragePoolNubHandler;
import de.dimm.vsm.fsengine.LazyList;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.Retention;
import de.dimm.vsm.records.RetentionJob;
import de.dimm.vsm.records.Snapshot;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class RetentionEntry implements JobInterface {

    private static final int NODE_ATTR_QRYCNT = 500000;
    static final int fIdxCol = 0;
    static final int fSizeCol = 1;
    static final int mTimeCol = 2;
    //static final int fnameCol = 3;
    static final int aIdxCol = 3;
    static final int aTsCol = 4;
    public static boolean enabled = false;
    final RetentionQueryResult[] qryResult = {
        new RetentionQueryResult(Long.class, "fidx"),
        new RetentionQueryResult(Long.class, "fsize"),
        new RetentionQueryResult(Long.class, "mtime"),
        //new RetentionQueryResult( String.class, "fname"),
        new RetentionQueryResult(Long.class, "aidx"),
        new RetentionQueryResult(Long.class, "ts"),};
    final String qryFieldStr = "f.idx as fidx, a.fsize as fsize, a.modificationDateMs as mtime, a.idx as aidx, a.ts as ts";
    List<RetentionJob> jobList;
    RetentionManager manager;
    IStoragePoolNubHandler nubHandler;
    List<Snapshot> snapshots;
    StoragePool pool;
    boolean abort;
    boolean abortCleanDedups = false;
    private String statusTxt = "Startup";
    public static final String ST_IDLE = "Idle";
    JobInterface.JOBSTATE js;
    long statNodes;
    long statHashes;
    long statDedups;
    long dedupSize;
    long statAttribs;
    long statIdx;
    long statSize;
    
    long maxFseIdx = 0;
    long actFseIdx = 0;
    private boolean inited;

    public RetentionEntry( IStoragePoolNubHandler nubHandler, StoragePool pool, List<RetentionJob> jobs, RetentionManager manager ) {
        this.jobList = jobs;
        this.manager = manager;
        this.nubHandler = nubHandler;
        this.pool = pool;
        js = JobInterface.JOBSTATE.SLEEPING;
    }
    
    void init() throws SQLException {
        
        GenericEntityManager em = nubHandler.getUtilEm(pool);
        List<Object[]> result = em.createNativeQuery("select max(idx) from filesystemelemnode", 0);
        if (!result.isEmpty() ) {
            maxFseIdx = Long.parseLong(result.get(0)[0].toString());
        }
        // Read once, it doesnt change inside a retention
        if (snapshots == null) {
            
            snapshots = em.createQuery("select T1 from Snapshot T1 where T1.pool_idx=" + pool.getIdx() + " order by creation desc", Snapshot.class);
        }
        inited = true;
    }


    public void setStatusTxt( String statusTxt ) {
        this.statusTxt = statusTxt;
    }

    public List<RetentionJob> getJobs() {
        return jobList;
    }

    public void clrStatusTxt( String statusTxt ) {
        if (statusTxt.compareTo(this.statusTxt) == 0) {
            this.statusTxt = ST_IDLE;
        }
    }

    boolean isShutdown() {
        return abort || manager.isShutdown();
    }

    public void abortDeleteFreeBlocks() {
        abortCleanDedups = true;
    }

    @Override
    public String getStatisticStr() {
        if (statNodes == 0 && statAttribs == 0 && statHashes == 0) {
            return "";
        }
        return "Nodes:" + statNodes + " Attr:" + statAttribs + " Hash:" + statHashes + " Dedup:" + statDedups + " " + SizeStr.format(dedupSize);
    }

    long getStartTS() {
        long ts = System.currentTimeMillis();
        for (RetentionJob job : jobList) {
            if (ts > job.getStart().getTime()) {
                ts = job.getStart().getTime();
            }
        }
        return ts;
    }

    public RetentionResult handleRetention( RetentionJob retentionJob ) throws RetentionException, IOException, SQLException, PoolReadOnlyException, PathResolveException {
        User user = User.createSystemInternal();
        Retention retention = retentionJob.getRetention();

        // THIS IS THE TIMESTAMP FOR ALL RELATIVE TIMESTAMPS
        long absTs = getStartTS();

        StoragePoolHandler sp_handler = null;
        RetentionResult ret;

        if (!inited) {
            init();
        }
        js = JobInterface.JOBSTATE.RUNNING;

        try {
            // Schreibenden PoolHandler erstellen
            sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, user, /*rdonly*/ false);
            if (pool.getStorageNodes().isEmpty(sp_handler.getEm())) {
                throw new RetentionException("No Storage for pool defined");
            }

            // Leeres Ergebnis
            ret = new RetentionResult(pool.getIdx(), new Date(), 0, 0);

            boolean clearBlocks = false;
            if (retention.isClearFreeBlocks()) {
                clearBlocks = true;
            }

            boolean done = false;
            int qryCount = NODE_ATTR_QRYCNT;
            long startIdx = retentionJob.getStartIdx();
            while (!done && !isShutdown()) {
                while (manager.isPaused() && !isShutdown()) {
                    LogicControl.sleep(1000);
                }
                if (isShutdown()) {
                    break;
                }

                // CREATE A LIST OF ALL ENTRIES TO BE REMOVED BASED ON RETENTION PARAMS
                Log.debug("Lese createRetentionResult für", " " + pool.getName() + " " + retention.toString());
                setStatusTxt("Suche " + qryCount + " Nodes ab Idx " + startIdx);
                RetentionResultList<Object[]> retentionResult = createRetentionResult(retention, startIdx, qryCount, absTs);
                Log.debug("Size createRetentionResult für", " " + pool.getName() + " " + retention.toString() + " Cnt:" + retentionResult.list.size());

                if (retentionResult.list.isEmpty()) {
                    done = true;
                    break;
                }
                long endIdx = startIdx;
                if (!retentionResult.list.isEmpty()) {
                    String endIdxStr = retentionResult.list.get(retentionResult.list.size() - 1)[fIdxCol].toString();
                    endIdx = Long.parseLong(endIdxStr);
                }

                // CREATE SUBLIST OF RETENTION ENTRIES WHICH CAN BE REMOVED REGARDING SNAPSHOTS
                Log.debug("Lese createSnapshotRetentionList für", " " + pool.getName() + " " + retention.toString());
                setStatusTxt("Berechne Snapshots für " + qryCount + " Nodes ab Idx " + startIdx);
                RetentionResultList snapshotRetentionResult = RetentionManager.createSnapshotRetentionList(snapshots, retentionResult, pool);
                Log.debug("Size createSnapshotRetentionList für", " " + pool.getName() + " " + retention.toString() + " Cnt:" + snapshotRetentionResult.list.size());


                // DO RETENTION WITH THE FINAL LIST
                setStatusTxt(pool.getName() + ": Nodes " + startIdx + " bis " + endIdx);
                RetentionResult localret = handleRetentionList(sp_handler, snapshotRetentionResult);
                Log.debug("Result: " + startIdx, localret.toString());
                ret.add(localret);

                // Mutliple Results per IDX, daher start at last End
                startIdx = endIdx + 1;

                // Leaving Active Window?
                if (!retention.isInStartWindow(System.currentTimeMillis())) {
                    break;
                }
            }
            if (done) {
                Log.debug("Retention beendet für ", retention.getName());
                retentionJob.setFinished(true);
            }
            else {
                Log.debug("Retention unterbrochen für ", retention.getName());
                retentionJob.setStartIdx(startIdx);
            }
            
            // Update actual progress
            sp_handler.check_open_transaction();
            sp_handler.em_merge(retentionJob);
            sp_handler.commit_transaction();

            // NOW REMOVE UNUSED DEDUPBLOCKS
            // TODO, THIS SHOULD BE HANDLED FROM OWN SCHEDULER -> SPACE USED x%. AGE or SIMPLY IMMEDIATELY
            if (done && clearBlocks && !isShutdown() && !abort) {
                setStatusTxt(pool.getName() + ": Entferne Blöcke");
                handleCleanDedups(sp_handler);
            }
        }
        finally {
            if (sp_handler != null) {
                sp_handler.commit_transaction();
                sp_handler.close_transaction();
                sp_handler.close_entitymanager();
            }
        }
        return ret;
    }

    StoragePool getTargetPool( Retention retention, GenericEntityManager em ) throws RetentionException {
        StoragePool targetPool = null;
        String args = retention.getFollowActionParams();
        if (args.charAt(0) == 'P') {
            long tpool_idx = Integer.parseInt(args.substring(1));
            targetPool = em.em_find(StoragePool.class, tpool_idx);
            if (targetPool == null) {
                throw new RetentionException("Missing TargetPool for Move Retention");
            }

            if (targetPool.getIdx() == pool.getIdx()) {
                throw new RetentionException("Move failed, Target Node == Source Node");
            }
        }
        return targetPool;
    }

    public RetentionResult handleRetentionList( StoragePoolHandler sp_handler, RetentionResultList retentionList ) throws RetentionException, SQLException, PoolReadOnlyException, IOException, PathResolveException {
        long sum = 0;
        int cnt = 0;        
        
        List res = retentionList.list;
        Retention retention = retentionList.retention;

        GenericEntityManager em = sp_handler.getEm();
        // RES IS SORTED a.idx ASC
        /*
         F1 10:00
         F1 11:00
         F1 12:00
         F2 10:00
         ...
         * */

        long lastFidx = -1;
        for (int i = 0; i < res.size(); i++) {
            statIdx++;
            setStatusTxt(pool.getName() + ": Eintrag " + statIdx);
            

            while (manager.isPaused() && !isShutdown() && !abort) {
                LogicControl.sleep(1000);
            }
            if (isShutdown() || abort) {
                break;
            }

            Object[] object = (Object[]) res.get(i);
            long fidx = (Long) object[fIdxCol];
            actFseIdx = fidx;

            // THIS FSEN WAS HANDLES ALREADY
            // SKIP TO NEXT NODE
            if (fidx == lastFidx) {
                continue;
            }

            long aidx = (Long) object[aIdxCol];
            String fname = "Node_" + fidx + " Aidx:" + aidx;

            cnt++;
            lastFidx = fidx;

            // Ist beim backup überhaput was zum Recyclen da (Anz Attrib > 1)?
            if (retention.getMode().equals(Retention.MD_BACKUP)) {
                if (!RetentionManager.hasAttribHistory(em, fidx)) {
                    continue;
                }
            }

            // LOAD NODE FROM DB
            FileSystemElemNode fse = sp_handler.resolve_fse_node_from_db(fidx);
            if (fse == null) {
                Log.debug("Node nicht gefunden", fidx + " " + fname);
                continue;
            }
            if (fse.getAttributes() == null) {
                Log.warn("Node ohne Attribute", fidx + " " + fname);
                continue;
            }
            if (!fse.isFile()) {
                // TODO RETENTION OF DIRECTORIES, LINKS ETC.
                continue;
            }

            // DO NOT DELETE ARTIFICIAL ROOT NODE
            if (fse.getIdx() == sp_handler.getRootDir().getIdx()) {
                continue;
            }

            // NOW CHECK, WHICH BLOCKS OF THIS NODE WE CAN REMOVE
            // BUILD LIST OF ALL ATTRS TO RETENTION
            HashMap<Long, Long> retentionAttrMap = new HashMap<>();
            retentionAttrMap.put(aidx, aidx);

            for (int j = i + 1; j < res.size(); j++) {
                Object[] nextObjects = (Object[]) res.get(j);
                long nextFidx = (Long) nextObjects[fIdxCol];
                if (nextFidx != fidx) {
                    break;
                }

                Long lAidx = (Long) nextObjects[aIdxCol];
                retentionAttrMap.put(lAidx, lAidx);
            }

            // get History
            List<FileSystemElemAttributes> history = fse.getHistory().getList(em);
            // Sort Newest first
            java.util.Collections.sort(history, new Comparator<FileSystemElemAttributes>() {
                @Override
                public int compare( FileSystemElemAttributes o1, FileSystemElemAttributes o2 ) {
                    if (o1.getTs() != o2.getTs()) {
                        return (o2.getTs() - o2.getTs() > 0) ? 1 : -1;
                    }
                    return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
                }
            });


            List<FileSystemElemAttributes> keepList = removeAttributes(sp_handler, retention, fse, history, retentionAttrMap);
            if (keepList != null) {
                removehashBlocks(sp_handler, retention, fse, keepList);
            }
        }

        // FLUSH CHANGES
        sp_handler.commit_transaction();

        RetentionResult ret = new RetentionResult(pool.getIdx(), new Date(), cnt, sum);
        return ret;
    }

    List<FileSystemElemAttributes> removeAttributes( StoragePoolHandler sp_handler, Retention retention,
            FileSystemElemNode fse, List<FileSystemElemAttributes> history, HashMap<Long, Long> retentionAttrMap ) throws RetentionException, PoolReadOnlyException, SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.YYYY HH:mm:ss");
        long fidx = fse.getIdx();
        String fname = fse.getName();

        // BUILD LIST OF ALL ATTRIBUTES TO KEEP
        List<FileSystemElemAttributes> keepList = new ArrayList<>();
        List<FileSystemElemAttributes> removeList = new ArrayList<>();


        // REMEMBER THE NEWEST ATTRIBUTE-ENTRY, THIS IS SET WITH fse.setAttribute()
        FileSystemElemAttributes newActualAttribute = null;

        // ADD ALL HISTORY ATTRIBUTE-ENTRIES NOT IN RETENTIONLIST TO KEEP LIST
        FileSystemElemAttributes actualAttrribute = fse.getAttributes();
        FileSystemElemAttributes actualAttrribute2 = history.get(0);

        if (actualAttrribute.getIdx() != actualAttrribute2.getIdx()) {
            throw new RetentionException("Attributes direction mismatch: " + fse.getIdx() + " " + fse.getName());
        }

        // Jetzt suchen wir Attribute in der History, die NICHT abgelaufen sind
        // Wenn gefunden, dann newActualAttribute setzen und in keepList eintragen
        for (int j = 0; j < history.size(); j++) {
            FileSystemElemAttributes fsea = history.get(j);
            if (!retentionAttrMap.containsKey(fsea.getIdx())) {
                keepList.add(fsea);
                // FIND THE NEWEST ATTRIBUTE
                if (newActualAttribute == null || newActualAttribute.getTs() < fsea.getTs()) {
                    newActualAttribute = fsea;
                }
            }
            else {
                // Das ist ein abgelaufenes Attribut -> in removeList
                removeList.add(fsea);
            }
        }

        // NOW CHECK; IF WE CAN DELETE A FILE COMPLETELY
        // Mode Backup: Wir können nur löschen, wenn kein attribut aus History in keepList ist UND Datei deleted ist            
        boolean deleteEntry = false;
        switch (retention.getMode()) {
            case Retention.MD_BACKUP:
                // IN BACKUP MODE KEEP LIST HAS TO BE EMPTY *AND* LAST ATTRIBUTE HAS TO BE DELETED=TRUE
                if (keepList.isEmpty()) {
                    if (actualAttrribute.isDeleted()) {
                        deleteEntry = true;
                    }
                    else {
                        // OTHERWISE WE HAVE TO KEEP NEWEST ENTRY SO THAT THE FILE CAN EXIST IN FS
                        keepList.add(actualAttrribute);

                        // newActualAttribute mitführen
                        if (newActualAttribute == null || newActualAttribute.getTs() < actualAttrribute.getTs()) {
                            newActualAttribute = actualAttrribute;
                        }

                    }
                }
                break;
            case Retention.MD_ARCHIVE:
                // IN ARCHIVAL MODE DATA IS REMOVED AFTER RETENTION TIME
                if (keepList.isEmpty()) {
                    deleteEntry = true;
                }
                break;
            default:
                throw new RetentionException("Invalid Retention mode " + fidx + " " + fname);
        }

        // NOTHING LEFT TO KEEP FROM THIS NODE?
        if (deleteEntry) {
            statNodes++;
            statSize += newActualAttribute.getFsize();
            // HANDLE FULL ENTRY
            switch (retention.getFollowAction()) {
                case Retention.AC_DELETE:
                    Log.debug("Node wird gelöscht", fidx + ": " + fname + " size:" + newActualAttribute.getFsize() + " TS:" + sdf.format(new Date(newActualAttribute.getTs())) + " mtime:" + sdf.format(new Date(newActualAttribute.getModificationDateMs())));
                    if (!retention.isTestmode()) {
                        RetentionManager.delete_fse_node(fse, sp_handler);

                    }
                    break;
                case Retention.AC_MOVE:
                    StoragePool targetPool = getTargetPool(retention, sp_handler.getEm());
                    Log.debug("Node wird verschoben", fidx + ": " + fname + " size:" + newActualAttribute.getFsize() + " TS:" + sdf.format(new Date(newActualAttribute.getTs())) + " mtime:" + sdf.format(new Date(newActualAttribute.getModificationDateMs())) + " -> " + targetPool.toString());
                    if (!retention.isTestmode()) {
                        String args = retention.getFollowActionParams();
                        if (args.charAt(0) == 'P') {
                            RetentionManager.move_fse_node(fse, sp_handler, targetPool);
                        }
                    }
                    break;
            }
            return null;
        }

        // ELSE
        // Wir haben nicht gelöscht, wollen aber die Attribute entfernen, die in removeList stehen
        // DELETE OBSOLETE ATTRIBUTES WHICH ARE NOT IN keepList
        for (int j = 0; j < removeList.size(); j++) {
            FileSystemElemAttributes fileSystemElemAttributes = removeList.get(j);

            // Dabei die keepList berücksichtigen
            boolean skipDelete = false;
            for (int k = 0; k < keepList.size(); k++) {
                if (keepList.get(k).getIdx() == fileSystemElemAttributes.getIdx()) {
                    skipDelete = true;
                }
            }

            if (skipDelete) {
                continue;
            }

            // Attribut aus Liste und DB entfernen
            if (fse.getHistory().removeIfRealized(fileSystemElemAttributes)) {
                Log.debug("Attribut wird gelöscht", fileSystemElemAttributes.getIdx() + ": " + fse.getName() + " size:" + fileSystemElemAttributes.getFsize() + " TS:" + sdf.format(new Date(fileSystemElemAttributes.getTs())) + " mtime:" + sdf.format(new Date(fileSystemElemAttributes.getModificationDateMs())));
                sp_handler.em_remove(fileSystemElemAttributes);
                sp_handler.check_commit_transaction();
                statAttribs++;
            }
        }

        // UPDATE CHANGED ATTRIBUTES
        if (newActualAttribute != null && fse.getAttributes().getIdx() != newActualAttribute.getIdx()) {
            fse.setAttributes(newActualAttribute);
            sp_handler.em_merge(fse);
        }
        return keepList;
    }

    void removehashBlocks( StoragePoolHandler sp_handler, Retention retention, FileSystemElemNode fse, List<FileSystemElemAttributes> keepList ) throws RetentionException, SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.YYYY HH:mm:ss");
        // NOW GET THE LIST OF BLOCKS WHICH CAN BE DELETED
        List<HashBlock> retentionHashBlocks = RetentionManager.createRetentionHashBlockList(sp_handler.getEm(), fse, keepList);
        // RESET LOADED HISTORY LIST
        if (fse.getHistory() instanceof LazyList) {
            ((LazyList) fse.getHistory()).unRealize();
        }
        // RESET LOADED HISTORY LIST
        if (fse.getHashBlocks() instanceof LazyList) {
            ((LazyList) fse.getHashBlocks()).unRealize();
        }

        if (!retentionHashBlocks.isEmpty()) {
            // TODO: Zusammenfassen von zusammenhängenden Blocklisten und gemeinsam löschen: delete from hashblock where fnode_idx= and ts=
            // dabei die Remove-List berücksichtigen
            for (int j = 0; j < retentionHashBlocks.size(); j++) {
                HashBlock hashBlock = retentionHashBlocks.get(j);
                statSize += hashBlock.getBlockLen();
                // AND DELETE THEM
                switch (retention.getFollowAction()) {
                    case Retention.AC_DELETE:
                        Log.debug("HashBlock wird gelöscht", hashBlock.getIdx() + ": " + fse.getName() + " pos:" + hashBlock.getBlockOffset() + " len: " + hashBlock.getBlockLen() + " TS:" + sdf.format(hashBlock.getTs()));
                        if (!retention.isTestmode()) {
                            sp_handler.em_remove(hashBlock);
                            // NO BLOCK DELETION HERE, THIS IS DONE AFTERWARDS, SO RETENTION AND BLOCK-REMOVAL CANNOT COLLIDE
                            //sp_handler.removeHashBlock( hashBlock );
                            if (!fse.getHashBlocks().removeIfRealized(hashBlock)) {
                                throw new RetentionException("Cannot remove deleted hashblock from Node");
                            }
                        }
                        break;
                    case Retention.AC_MOVE:
                        StoragePool targetPool = getTargetPool(retention, sp_handler.getEm());
                        Log.debug("HashBlock wird verschoben", hashBlock.getIdx() + ": " + fse.getName() + " pos:" + hashBlock.getBlockOffset() + " TS:" + sdf.format(hashBlock.getTs()) + " -> " + targetPool.toString());
                        if (!retention.isTestmode()) {
                            String args = retention.getFollowActionParams();
                            if (args.charAt(0) == 'P') {
                                sp_handler.moveHashBlock(hashBlock, targetPool);
                            }
                        }
                        break;
                }
                sp_handler.check_commit_transaction();
                statHashes++;
            }
        }
    }

    public long handleDeleteFreeBlocks( StoragePool pool ) throws Exception {
        User user = User.createSystemInternal();
        StoragePoolHandler sp_handler = null;

        try {
            sp_handler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, user, /*rdonly*/ false);
            if (pool.getStorageNodes().isEmpty(sp_handler.getEm())) {
                throw new RetentionException("No Storage for pool defined");
            }
            sp_handler.check_open_transaction();

            return handleCleanDedups(sp_handler);
        }
        catch (SQLException exc) {
            if (sp_handler != null) {
                sp_handler.rollback_transaction();
            }
            throw exc;
        }
        catch (Exception exc) {
            Log.err("Abbruch beim Freigeben von DedupBlöcken", exc);
            throw exc;
        }
        finally {
            if (sp_handler != null) {
                sp_handler.commit_transaction();
                sp_handler.close_transaction();
                sp_handler.close_entitymanager();
            }
        }
    }

    public RetentionResultList<Object[]> createRetentionResult( Retention retention, long startIdx, int qryCount, long absTs ) throws IOException, SQLException {
        GenericEntityManager em = nubHandler.getUtilEm(pool);

        // THE FIELDS MUST BE FIXED TO THE RESULT!!!!
        // SELECT ALL DIRECT LINKED ATTRIBUTE RESULTS
        String select_str = "select " + qryFieldStr + " from FileSystemElemNode f, FileSystemElemAttributes a "
                + "where f.idx = a.file_idx and f.pool_idx=" + pool.getIdx();

        StringBuilder retention_where = new StringBuilder();
        RetentionManager.buildRetentionWhereString(em, retention, retention_where, absTs);

        if (retention_where.length() == 0) {
            throw new IOException("Invalid Retention query");
        }

        // ADD RETENTION QUERY
        select_str += " and " + retention_where.toString();
        if (startIdx > 0) {
            select_str += " and f.idx >= " + startIdx;
        }

        List<Object[]> nres = em.createNativeQuery(select_str, qryCount);

        Comparator<Object[]> comp = new Comparator<Object[]>() {
            @Override
            public int compare( Object[] o1, Object[] o2 ) {
                long of1 = Long.parseLong(o1[fIdxCol].toString());
                long of2 = Long.parseLong(o2[fIdxCol].toString());
                long diff = of1 - of2;
                if (diff == 0) {
                    of1 = Long.parseLong(o1[aIdxCol].toString());
                    of2 = Long.parseLong(o2[aIdxCol].toString());

                    diff = of1 - of2;
                }
                // return Signum
                return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
            }
        };
        Collections.sort(nres, comp);
        RetentionResultList<Object[]> rl = new RetentionResultList<>(retention, nres);

        return rl;
    }

    long handleCleanDedups( StoragePoolHandler handler ) throws SQLException, IOException, PathResolveException, UnsupportedEncodingException, PoolReadOnlyException {
        abortCleanDedups = false;
        GenericEntityManager em = nubHandler.getUtilEm(pool);
        setStatusTxt("Berechne freien DedupSpeicher");
        
        // TODO: XANODE 
        // SEARCH FÜR DEDUP NODES NOT NEEDED AS HASHBLOCK OR AS XANODE
        List<Object[]> oList = em.createNativeQuery("select DEDUPHASHBLOCK.IDX from DEDUPHASHBLOCK  "
                + "left outer join hashblock on DEDUPHASHBLOCK.idx = hashblock.dedupblock_idx "
                + "where hashblock.idx is null", 0);

        long cnt = 0;
        if (!oList.isEmpty()) {
            setStatusTxt("Entferne freien DedupSpeicher");

            long size = 0;
            for (int i = 0; i < oList.size(); i++) {
                setStatusTxt(Main.Txt("Entferne freien DedupSpeicher") + "(Block " + i + "/" + oList.size() + ")");

                if (abortCleanDedups) {
                    break;
                }

                Object[] oarr = oList.get(i);
                if (oarr.length == 1) {
                    String idxStr = oarr[0].toString();

                    long l = Long.parseLong(idxStr);
                    if (l > 0) {
                        DedupHashBlock hb = em.em_find(DedupHashBlock.class, l);
                        if (hb != null) {
                            handler.removeDedupBlock(hb, null);
                            cnt++;
                            size += hb.getBlockLen();

                            handler.check_commit_transaction();
                            statDedups++;
                            dedupSize += hb.getBlockLen();
                        }
                    }
                }
            }
            Log.debug("Enfernte DedupBlöcke", Long.toString(cnt));
            Log.debug("Freigewordener DedupSpeicher", SizeStr.format(size));
        }
        if (abortCleanDedups) {
            setStatusTxt("Löschen wurde abgebrochen");
        }
        else {
            setStatusTxt("");
        }
        return cnt;
    }
        
    @Override
    public JobInterface.JOBSTATE getJobState() {
        return js;
    }

    @Override
    public void setJobState( JobInterface.JOBSTATE jOBSTATE ) {
        js = jOBSTATE;
    }
    
    @Override
    public InteractionEntry getInteractionEntry() {
        return null;
    }

    @Override
    public String getStatusStr() {
        return statusTxt;
    }

    @Override
    public Date getStartTime() {
        if (!jobList.isEmpty()) {
            return jobList.get(0).getStart();
        }
        return null;
    }

    @Override
    public Object getResultData() {
        return null;
    }

    @Override
    public String getProcessPercent() {
        if (maxFseIdx > 0)
            return String.format("%.1f", 100.0*actFseIdx / maxFseIdx);
        return "";
    }

    @Override
    public String getProcessPercentDimension() {
        return "%";
    }

    @Override
    public void abortJob() {
        abort = true;
        if (js == JOBSTATE.FINISHED_ERROR)
            js = JOBSTATE.ABORTED;
            
        if (js == JOBSTATE.FINISHED_OK) {
            js = JOBSTATE.FINISHED_OK_REMOVE;
        }
    }

    @Override
    public void run() {
        RetentionResult result = new RetentionResult(pool.getIdx(), new Date(), 0, 0);
        try {
            for (RetentionJob job : jobList) {
                if (!job.isFinished()) {
                    RetentionResult ret = handleRetention(job);
                    result.add(ret);
                    if (!job.isFinished()) {
                        break;
                    }
                }
            }
            js = JOBSTATE.FINISHED_OK_REMOVE;
        }
        catch (Throwable ex) {
            Log.err("RetentionEntry wurde abgebrochen", ex);
            js = JOBSTATE.FINISHED_ERROR;
        }
        finally {
        }              
    }

    @Override
    public void close() {
    }

    @Override
    public User getUser() {
        return null;
    }
}
