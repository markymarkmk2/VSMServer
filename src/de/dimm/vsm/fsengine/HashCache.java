/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import gnu.trove.map.hash.THashMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Administrator
 */
public class HashCache
{
    StoragePool pool;

    boolean inited = false;
    THashMap<String,Long> hashMap;

    public HashCache( StoragePool pool)
    {
        this.pool = pool;
        hashMap = new THashMap<String, Long>();
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

//            int cnt = -1;
//            ResultSet rs = st.executeQuery("select count(idx) from DedupHashBlock");
//            if (rs.next())
//            {
//                cnt = rs.getInt(1);
//            }
//            rs.close();
            long maxMem = Runtime.getRuntime().maxMemory();
            long freeMem = Runtime.getRuntime().freeMemory();
            long totalMem = Runtime.getRuntime().totalMemory();
            Log.info("Speicher vor Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );

            ResultSet rs = st.executeQuery("select idx, hashvalue  from DedupHashBlock");

            while (rs.next())
            {
                long idx = rs.getLong(1);
                String has = rs.getString(2);

                hashMap.put(has, idx);
            }
            rs.close();
            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " + hashMap.size());

            maxMem = Runtime.getRuntime().maxMemory();
            freeMem = Runtime.getRuntime().freeMemory();
            totalMem = Runtime.getRuntime().totalMemory();
            Log.info("Speicher nach Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );
            

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

    public List<String> getUrlUnsafeHashes()
    {
        ArrayList<String> ret = new ArrayList<String>();
        Set<String> set = hashMap.keySet();

        for (Iterator<String> it = set.iterator(); it.hasNext();)
        {
            String hash = it.next();
            boolean found = false;

            char lastCh = hash.charAt( hash.length() - 1 );
            
            // DETECT PADDED HASHES
            if (lastCh == '=')
                found = true;
            
            if (!found)
            {
                for (int i = 0; i < hash.length(); i++)
                {
                    char ch = hash.charAt(i);
                    if (ch == '/' || ch == '+')
                    {
                        found = true;
                        break;
                    }
                }
            }
            if (found)
            {
                ret.add(hash);
            }
        }
        return ret;
    }
}
