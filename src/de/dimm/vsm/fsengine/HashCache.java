/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 *
 * @author Administrator
 */
public class HashCache
{
    StoragePool pool;

    boolean inited = false;
    HashMap<String,Long> hashMap;

    public HashCache( StoragePool pool)
    {
        this.pool = pool;
        hashMap = new HashMap<String, Long>();
    }

    public boolean isInited()
    {
        return inited;
    }

    
    public boolean init(Connection conn )
    {
        // TODO: CALC MEM
        inited = false;
        
        Statement st  = null;
        try
        {
            
            st = conn.createStatement();

            int cnt = -1;
            ResultSet rs = st.executeQuery("select count(idx) from DedupHashBlock");
            if (rs.next())
            {
                cnt = rs.getInt(1);
            }
            rs.close();
            long maxMem = Runtime.getRuntime().maxMemory();

            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " + cnt);
            Log.info("Max. Speicher", ": " + SizeStr.format(maxMem) );

            rs = st.executeQuery("select idx, hashvalue from DedupHashBlock");

            while (rs.next())
            {
                long idx = rs.getLong(1);
                String has = rs.getString(2);

                hashMap.put(has, idx);
            }
            inited = true;
            return true;
        }
        catch (SQLException sQLException)
        {
            Log.err("HashMap kann nicht angelegt werden", pool.getName(), sQLException);
            return false;
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

    public long getDhbIdx( String hash )
    {
        Long l = hashMap.get(hash);
        if (l != null)
            return l.longValue();
        
        return -1;
    }
    public void addDhb( String hash, long idx )
    {
        hashMap.put(hash, idx);
    }

    public int size()
    {
        return hashMap.size();
    }

    void removeDhb( DedupHashBlock dhb )
    {
        hashMap.remove(dhb.getHashvalue());
    }
}
