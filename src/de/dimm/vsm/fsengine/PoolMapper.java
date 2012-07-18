/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.persistence.EntityManagerFactory;

/**
 *
 * @author Administrator
 */
public class PoolMapper
{
    StoragePool pool;
    StoragePoolNub nub;
    MiniConnectionPoolManager connectionPoolManager;
    JDBCEntityManager em;
    EntityManagerFactory poolEmf;
    FSEIndexer indexer;
    HashCache hashCache;



    public PoolMapper( StoragePool pool, StoragePoolNub nub, MiniConnectionPoolManager poolManager, JDBCEntityManager em, EntityManagerFactory poolEmf, FSEIndexer indexer )
    {
        this.pool = pool;
        this.nub = nub;
        this.connectionPoolManager = poolManager;
        this.em = em;
        this.poolEmf = poolEmf;
        this.indexer = indexer;
        hashCache = new HashCache(pool);
    }

    public void calcStats()
    {
        try
        {
            calcStats(em.getConnection());
        }
        catch (SQLException sQLException)
        {
        }
    }
    long dedupDataLen = 0;
    long actFsDataLen = 0;
    long actFileCnt = 0;
    long totalFsDataLen = 0;
    long totalFileCnt = 0;
    long removeDDCnt = 0;
    long removeDDLen = 0;

    int getActDedupRatio()
    {
        int ratio = 0;
        if (dedupDataLen > 0 && totalFsDataLen > 0 && actFsDataLen > 0)
        {
            ratio = 100 - (int)(100 * dedupDataLen / actFsDataLen + 0.5);
        }
        return ratio;
    }
    int getTotalDedupRatio()
    {
        int ratio = 0;
        if (dedupDataLen > 0 && totalFsDataLen > 0 && actFsDataLen > 0)
        {
            ratio = 100 - (int)(100 * dedupDataLen / totalFsDataLen + 0.5);
        }
        return ratio;
    }

    public void logStats()
    {
        Log.info("Block-Datenmenge für Pool", pool.getName() + ": " + SizeStr.format(dedupDataLen));
        Log.info("Virtuelle aktuelle  Dateisystemgröße für Pool", pool.getName() + ": " + SizeStr.format(actFsDataLen) + " bei " + actFileCnt + " Elementen");
        Log.info("Virtuelle komplette Dateisystemgröße für Pool", pool.getName() + ": " + SizeStr.format(totalFsDataLen) + " bei " + totalFileCnt + " Elementen");
        Log.info("Dedupfaktor aktuell  für Pool", pool.getName() + ": " + getActDedupRatio() + "%%");
        Log.info("Dedupfaktor komplett für Pool", pool.getName() + ": " + getTotalDedupRatio() + "%%");
        Log.info("Löschbare Dedupblöcke: " + removeDDCnt + " Löschbare Größe: " +  SizeStr.format(removeDDLen) );
    }
    void calcStats( Connection conn)
    {
        Statement st  = null;
        try
        {

            st = conn.createStatement();

            ResultSet rs = st.executeQuery("select sum(bigint(blockLen)) from DedupHashBlock");


            if (rs.next())
            {
                dedupDataLen = rs.getLong(1);
            }
            rs.close();

            rs = st.executeQuery("select count(*), sum(bigint(a.fsize)), sum(bigint(a.xasize)) from FileSystemElemAttributes a, FileSystemElemNode f where f.attributes_idx=a.idx");

            if (rs.next())
            {
                actFileCnt = rs.getLong(1);
                actFsDataLen = rs.getLong(2) + rs.getLong(3);
            }
            rs.close();

            rs = st.executeQuery("select count(*), sum(bigint(a.fsize)), sum(bigint(a.xasize)) from FileSystemElemAttributes a");

            if (rs.next())
            {
                totalFileCnt = rs.getLong(1);
                totalFsDataLen = rs.getLong(2) + rs.getLong(3);
            }
            rs.close();

            rs = st.executeQuery("select count(*), sum(bigint(DEDUPHASHBLOCK.BLOCKLEN)) from DEDUPHASHBLOCK"
                    + " LEFT OUTER JOIN XANODE  ON DEDUPHASHBLOCK.idx = XANODE.dedupblock_idx"
                    + " left outer join hashblock on DEDUPHASHBLOCK.idx = hashblock.dedupblock_idx"
                    + " where XANODE.idx is null and hashblock.idx is null");
            
            if (rs.next())
            {
                removeDDCnt = rs.getLong(1);
                removeDDLen = rs.getLong(2);
            }
            
        }
        catch (SQLException sQLException)
        {
            Log.err("Statistik kann nicht gerechnet werden", pool.getName(), sQLException);
        }
        finally
        {
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException sQLException)
                {
                }
            }

            if (conn != null)
            {
                try
                {
                    conn.close();
                }
                catch (SQLException sQLException)
                {
                }
            }
        }
    }



}