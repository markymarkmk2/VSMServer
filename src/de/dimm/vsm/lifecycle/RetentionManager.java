/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.lifecycle;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Exceptions.RetentionException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.fsengine.IStoragePoolNubHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.jobs.JobEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.Retention;
import de.dimm.vsm.records.RetentionJob;
import de.dimm.vsm.records.Snapshot;
import de.dimm.vsm.records.StoragePool;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

class RetentionQueryResult {

    Class clazz;
    String fname;

    public RetentionQueryResult( Class clazz, String fname ) {
        this.clazz = clazz;
        this.fname = fname;
    }
}

/**
 *
 * @author Administrator
 */
public class RetentionManager extends WorkerParent {

    Thread runner;
    public static boolean enabled = false;
    IStoragePoolNubHandler nubHandler;

    public RetentionManager( IStoragePoolNubHandler nubHandler ) {
        super("RetentionManager");
        this.nubHandler = nubHandler;
    }

    @Override
    public boolean initialize() {
        return true;
    }

    @Override
    public boolean isPersistentState() {
        return true;
    }

    List<RetentionEntry> getActiveRetentions() {
        List<RetentionEntry> reEntries = new ArrayList<>();

        List<JobEntry> jobs = Main.get_control().getJobManager().getJobList();
        for (JobEntry entry : jobs) {
            if (entry.getJob() instanceof RetentionEntry) {
                reEntries.add((RetentionEntry) entry.getJob());
            }
        }
        return reEntries;
    }

    public boolean isActiveRetention( Retention retention ) {
        List<RetentionEntry> activeRetentions = getActiveRetentions();
        for (RetentionEntry entry : activeRetentions) {
            for (RetentionJob job : entry.getJobs()) {
                if (job.getRetention().getIdx() == retention.getIdx()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean hasActiveRetention( StoragePool pool ) {
        List<RetentionEntry> activeRetentions = getActiveRetentions();
        for (RetentionEntry entry : activeRetentions) {
            if (entry.getJobState() == JobInterface.JOBSTATE.RUNNING
                    || entry.getJobState() == JobInterface.JOBSTATE.WAITING
                    || entry.getJobState() == JobInterface.JOBSTATE.SLEEPING) {
                for (RetentionJob job : entry.getJobs()) {
                    if (job.getRetention().getPool().getIdx() == pool.getIdx()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    boolean handleRetention( StoragePool storagePool ) throws SQLException {
        // Busy ?
        if (hasActiveRetention(storagePool)) {
            return false;
        }

        GenericEntityManager em = nubHandler.getUtilEm(storagePool);
        List<RetentionJob> list = em.createQuery("select T1 from RETENTIONJOB", RetentionJob.class);

        // Get Start of last Retenteion#
        long now = System.currentTimeMillis();
        Date lastStart = null;
        for (RetentionJob job : list) {
            if (!job.getRetention().isInStartWindow(now)) {
                continue;
            }
            if (lastStart == null || lastStart.before(job.getStart())) {
                lastStart = job.getStart();
            }
            if (!job.isFinished()) {
                RetentionEntry entry = new RetentionEntry(nubHandler, storagePool, list, this);
                Main.get_control().getJobManager().addJobEntry(entry);
                return true;
            }
        }

        // Start a new Retention only once a day -> TODO: Abhängig von der parametroerten CycleDauer amchen
        if (lastStart != null && System.currentTimeMillis() - lastStart.getTime() < 86400 * 1000) {
            return false;
        }

        // No unfinished Retention Job found, Delete last Entries and start new cycle 
        em.check_open_transaction();
        for (RetentionJob job : list) {
            em.em_remove(job);
        }
        em.commit_transaction();

        em.check_open_transaction();
        List<Retention> retentions = em.createQuery("select T1 from Retention T1 where T1.disabled=0 and T1.pool_idx=" + storagePool.getIdx(), Retention.class);
        if (retentions.isEmpty()) {
            return false;
        }

        for (Retention retention : retentions) {
            // ONLY HANDLE ROOT NODES
            if (retention.getParent() != null) {
                continue;
            }
            if (!retention.isInStartWindow(now)) {
                continue;
            }
            RetentionJob job = new RetentionJob();
            job.setRetention(retention);
            em.em_persist(job);
            retention.getRetentionJobs().addIfRealized(job);
            list.add(job);
        }
        em.commit_transaction();
        list = em.createQuery("select T1 from RETENTIONJOB", RetentionJob.class);

        if (!list.isEmpty()) {
            RetentionEntry entry = new RetentionEntry(nubHandler, storagePool, list, this);
            Main.get_control().getJobManager().addJobEntry(entry);
            return true;
        }
        return false;

    }

    @Override
    public void run() {
        is_started = true;
        GregorianCalendar cal = new GregorianCalendar();
        int last_checked = 0; //cal.get(GregorianCalendar.HOUR_OF_DAY);

        setStatusTxt("");
        while (!isShutdown()) {
            LogicControl.sleep(1000);

            if (isPaused()) {
                last_checked = -1;
                continue;
            }
            if (!enabled) {
                setStatusTxt(Main.Txt("Deaktiviert"));
                setPaused(true);
                last_checked = -1;
                continue;
            }

            cal.setTime(new Date());

            // ONCE EVERY HOUR
            int check = cal.get(GregorianCalendar.HOUR_OF_DAY);
            if (check == last_checked) {
                continue;
            }

            last_checked = check;

            // OK, WE HAVE A NEW MINUTE, CHECK ALL SCHEDS            

            List<StoragePool> list = nubHandler.listStoragePools();

            setStatusTxt(Main.Txt("Überprüfe Retention"));
            for (int i = 0; i < list.size(); i++) {
                StoragePool storagePool = list.get(i);
                try {
                    if (handleRetention(storagePool)) {
                        Log.debug(Main.Txt("Eine neue Retention wurde für Pool gestartet:"), storagePool.getName());
                    }

                }
                catch (SQLException ex) {
                    Log.err("Retention konnte nicht gestartet werden", storagePool.getName(), ex);
                }
            }
            setStatusTxt("");
        }
        finished = true;
    }

    private static boolean isStringArgType( Retention retention ) {
        return retention.getArgType().equals(Retention.ARG_NAME);
    }

    static void buildRetentionWhereString( GenericEntityManager em, Retention retention, StringBuilder sb, long absTs ) {
        if (sb.length() > 0) {
            if (retention.isOrWithPrevious()) {
                sb.append(" or ");
            }
            else {
                sb.append(" and ");
            }
        }
        if (retention.isNeg()) {
            sb.append(" not ");
        }
        List<Retention> list = retention.getChildren(em);

        if (list != null && !list.isEmpty()) {

            StringBuilder child_where = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                Retention child_r = list.get(i);

                buildRetentionWhereString(em, child_r, child_where, absTs);
            }
            if (child_where.length() > 0) {
                sb.append("(");
                sb.append(child_where);
                sb.append(")");
            }
        }
        else {
            sb.append(retention.getSqlFieldforArgtype());

            sb.append(Retention.getSqlOpString(retention.getArgOp()));

            if (isStringArgType(retention)) {
                sb.append("'");
                if (retention.getArgOp().equals(Retention.OP_BEGINS) || retention.getArgOp().equals(Retention.OP_CONTAINS)) {
                    sb.append("%");
                }
            }

            // MAKE REL TS TO ACT_TS
            if (Retention.isRelTSField(retention.getArgType())) {
                long ts = Long.parseLong(retention.getArgValue());
                ts = absTs - ts;
                sb.append(Long.toString(ts));
            }
            else {
                sb.append(retention.getArgValue());
            }

            if (isStringArgType(retention)) {
                if (retention.getArgOp().equals(Retention.OP_ENDS) || retention.getArgOp().equals(Retention.OP_CONTAINS)) {
                    sb.append("%");
                }

                sb.append("'");
            }
        }
    }

    static void delete_fse_node( FileSystemElemNode fse, StoragePoolHandler sp_handler ) throws RetentionException, PoolReadOnlyException {
        boolean remove_parent = false;
        GenericEntityManager em = sp_handler.getEm();

        // DELEET ONLY FILES
        if (!fse.isDirectory()) {
            // DIRS ARE DELETED IF LAST FILE IS DELETED
            if (fse.getParent().getChildren(em).size() == 1) {
                if (fse.getParent().getChildren(em).get(0).getIdx() != fse.getIdx()) {
                    throw new RetentionException("Parentfolder does not match");
                }

                remove_parent = true;
            }

            sp_handler.remove_fse_node(fse, false);

        }
        else {
            // DELETE EMPTY DIRS EXCEPT ROOT
            if (fse.getChildren(em).isEmpty() && fse.getParent() != null) {
                // DIRS ARE DELETED IF LAST FILE IS DELETED
                if (fse.getParent().getChildren(em).size() == 1) {
                    if (fse.getParent().getChildren(em).get(0).getIdx() != fse.getIdx()) {
                        throw new RetentionException("Parentfolder does not match");
                    }

                    remove_parent = true;
                }
                sp_handler.remove_fse_node(fse, false);
            }
        }
        if (remove_parent) {
            // RECURSIVELY DELETE DIRS UPWARDS
            delete_fse_node(fse.getParent(), sp_handler);
        }
    }

    static void move_fse_node( FileSystemElemNode fse, StoragePoolHandler sp_handler, StoragePool targetPool ) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public static RetentionResultList createSnapshotRetentionList( List<Snapshot> snapshots, RetentionResultList retentionResult, StoragePool pool ) {
        // ALL ENTRIES IN THIS LIST ARE OLD ENOUGH TO BE HANDLED, NOW CHECK IF A SNAPSHOT PREVENTS US DOING THIS
        List<Object[]> list = new ArrayList<>();
        RetentionResultList snapshotRetentionResult = new RetentionResultList(retentionResult.retention, list);


        // WE CHECK IF WE HAVE TO REMOVE ENTRIES FROM LIST BECAUSE WE HAVE A VALID SNAPSHOT
        List<Object[]> res = retentionResult.list;

        for (int i = 0; i < res.size(); i++) {
            // GET MTIME OF THIS ENTRY
            Object[] objects = res.get(i);
            long mtime = (Long) objects[RetentionEntry.mTimeCol];
            long ts = (Long) objects[RetentionEntry.aTsCol];

            boolean protectedBySnapshot = true;

            // FIRST CHECK IF WE HAVE A SNAPSHOT YOUNGER THAN THIS ENTRY
            int snIdx = exist_snap_younger(snapshots, ts);
            if (snIdx < 0) {
                // IF NOT, THEN THIS ENTRY IS NOT INSIDE A SNAPSHOT, IT WAS CREATED LATER
                protectedBySnapshot = false;
            }
            else {
                // YES, WE HAVE TO CHECK IF THERE IS AN ENTRY YOUNGER THAN US AND OLDER THAN SNAPSHOT
                if (exists_younger_entry_between_snap_and_us(snapshots.get(snIdx), res, i, ts)) {
                    // IF SO, THEN THIS ENTRY TOO OLD FOR NEAREST SNAPSHOT, IT WAS CREATED TOO EARLY
                    protectedBySnapshot = false;
                }
            }

            if (!protectedBySnapshot) {
                list.add(objects);
            }
        }

        return snapshotRetentionResult;
    }

    static int exist_snap_younger( List<Snapshot> snapshots, long ts ) {
        int foundIdx = -1;

        // SNAPSHOTS ARE SORTED DATE DESCENDING
        // WE STOP AT THE FIRST SN OLDER THAN TIMESTAMP
        // IF WE FOUND A SN YOUNGER, THEN foundIdx CONTAINS AUTOMATICALLY THE NEAREST SNAPSHOT (LAST ONE BEFORE BREAK)
        for (int i = 0; i < snapshots.size(); i++) {
            Snapshot snapshot = snapshots.get(i);
            if (snapshot.getCreation().getTime() > ts) {
                foundIdx = i;
            }
            else {
                break;
            }

        }
        return foundIdx;
    }

    static boolean exists_younger_entry_between_snap_and_us( Snapshot snapshot, List<Object[]> res, int idx, long TS ) {
        // CHECK IF THERE IS AN ENTRY YOUNGER THAN US AND OLDER THAN SNAPSHOT
        // RES IST SORTED ORDER BY AIDX ASCENDING, YOUNGER ENTRIES HAVE BIGGER INDICES
        boolean ret = false;
        long fidx = (Long) res.get(idx)[0];

        for (int i = idx + 1; i < res.size(); i++) {
            Object[] objects = res.get(i);
            long testFidx = (Long) objects[RetentionEntry.fIdxCol];


            // REACHED NEXT FILENODE?
            if (testFidx != fidx) {
                break;
            }

            // WE STOP AT THE FIRST OCCURENCE OF AN ENTRY YOUNGER THAN US BUT OLDER THAN SNAPSHOT
            long testTs = (Long) objects[RetentionEntry.aTsCol];
            if (testTs > TS && testTs < snapshot.getCreation().getTime()) {
                ret = true;
                break;
            }
        }

        return ret;
    }

    static List<HashBlock> buildRemoveHashblockList( FileSystemElemNode node, List<HashBlock> hash_block_list, List<FileSystemElemAttributes> keepList ) throws RetentionException {
        List<HashBlock> ret = new ArrayList<>();
        ret.addAll(hash_block_list);

        // SORT IN BLOCKOFFSET ORDER, NEWER BLOCKS FIRST
        java.util.Collections.sort(hash_block_list, new Comparator<HashBlock>() {
            @Override
            public int compare( HashBlock o1, HashBlock o2 ) {
                if (o1.getBlockOffset() != o2.getBlockOffset()) {
                    return (o1.getBlockOffset() - o2.getBlockOffset() > 0) ? 1 : -1;
                }

                return (o2.getIdx() - o1.getIdx() > 0) ? 1 : -1;
            }
        });

        // FETCH FOR EVERY ENTRY IN KEEPLIST THE SAME OR MINIMAL OLDER ENTRY IN HASHBLOCKLIST
        for (int i = 0; i < keepList.size(); i++) {
            FileSystemElemAttributes fsea = keepList.get(i);
            long ts = fsea.getTs();
            List<HashBlock> keepHashBlockList = new ArrayList<>();

            // FOR EVERY HASHBLOCK
            HashBlock lastHashBlock = null;
            for (int h = 0; h < hash_block_list.size(); h++) {
                HashBlock hashBlock = hash_block_list.get(h);
                if (lastHashBlock == null || (lastHashBlock.getBlockOffset() != hashBlock.getBlockOffset())) {

                    // SCROLL THROUGH ALL BLOCKS OF ONE POSITION, NEWEST IS FIRST
                    // Suche den ersten Block dieser Position, das ist der neueste, den wollen wir behalten
                    for (int hl = h; hl < hash_block_list.size(); hl++) {
                        HashBlock lhashBlock = hash_block_list.get(hl);
                        if (lhashBlock.getBlockOffset() != hashBlock.getBlockOffset()) {
                            break;
                        }
                        // IS THIS BLOCK TO YOUNG ?
                        if (lhashBlock.getTs() > ts) {
                            continue; // SKIP
                        }
                        // IS THIS BLOCK Outside of this Node ?
                        if (lhashBlock.getBlockOffset() + lhashBlock.getBlockLen() > fsea.getFsize()) {
                            continue; // SKIP
                        }
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
            try {
                checkBlocksForNodeComplete(node, fsea, keepHashBlockList);
            }
            catch (RetentionException retentionException) {
                Log.warn("Node ist nicht vollständig, Retention wird vermieden", node.toString(), retentionException);
                ret.clear();
            }
        }
        return ret;
    }

    static void checkBlocksForNodeComplete( FileSystemElemNode node, FileSystemElemAttributes fsea, List<HashBlock> keepHashBlockList ) throws RetentionException {
        long size = 0;
        for (int i = 0; i < keepHashBlockList.size(); i++) {
            HashBlock hashBlock = keepHashBlockList.get(i);
            if (hashBlock.getFileNode().getIdx() != node.getIdx()) {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") Invalid fileID at " + size);
            }

            if (hashBlock.getBlockOffset() != size) {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") missing block at " + size);
            }
            if (hashBlock.getTs() > fsea.getTs()) {
                throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") block too young at " + size);
            }
            size += hashBlock.getBlockLen();
        }
        if (size != fsea.getFsize()) {
            throw new RetentionException("checkBlocksComplete failed for " + fsea.toString() + " (" + fsea.getIdx() + ") invalid total size " + size);
        }
    }

    static List<HashBlock> createRetentionHashBlockList( GenericEntityManager em, FileSystemElemNode fse, List<FileSystemElemAttributes> keepList ) throws RetentionException {
        // READ HASHBLOCKS
        List<HashBlock> hash_block_list = fse.getHashBlocks().getList(em);

        List<HashBlock> remove_hash_block_list = buildRemoveHashblockList(fse, hash_block_list, keepList);

        return remove_hash_block_list;
    }

    static boolean hasAttribHistory( GenericEntityManager em, long fidx ) throws SQLException {

        String select_str = "select file_idx from FileSystemElemAttributes a where a.file_idx=" + fidx;
        int qryCount = 2;
        List<Object[]> nres = em.createNativeQuery(select_str, qryCount);
        return nres.size() > 1;
    }

    public long handleDeleteFreeBlocks( StoragePool pool ) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void abortDeleteFreeBlocks() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
