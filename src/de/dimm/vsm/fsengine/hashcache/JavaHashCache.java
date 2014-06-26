/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.hashcache;

import de.dimm.vsm.fsengine.hashcache.HashCache;
import de.dimm.vsm.Utilities.BigArrayList;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Administrator
 */
public class JavaHashCache extends HashCache
{
    Map<String,Long> hashMap;

    public JavaHashCache( StoragePool pool)
    {
        super(pool);
        hashMap = new ConcurrentHashMap<>();
    }
    
    @Override
    public boolean init(Connection conn ) throws IOException
    {
        // TODO: CALC MEM
        inited = false;

        Statement st  = null;
        try
        {

            st = conn.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
            st.setFetchSize(10000);

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
            BigArrayList<Long> idxList = new BigArrayList<>();
            BigArrayList<String> hashList = new BigArrayList<>();
            
            while (rs.next())
            {
                long idx = rs.getLong(1);
                String has = rs.getString(2);

                idxList.add(Long.valueOf(idx));
                hashList.add(has);
            }
            rs.close();
            
            
            long len = idxList.size();
            int mapSize = Integer.MAX_VALUE;
            if (mapSize > len)
                mapSize = (int)len;
            
            hashMap = new ConcurrentHashMap<>(mapSize, 0.75f, 4);
            
            for ( long l = 0; l < len; l++)
            {
                hashMap.put(hashList.get(l), idxList.get(l));
            }
            
            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " +len );

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

    @Override
    public void fill( String hash, long id )
    {
        hashMap.put(hash, id);
    }
        
    @Override
    public long getDhbIdx( String hash )
    {
        Long l = hashMap.get(hash);
        if (l != null)
            return l.longValue();
        
        return -1;
    }
    @Override
    public void addDhb( String hash, long idx )
    {
        hashMap.put(hash, idx);
    }

    @Override
    public long size()
    {
        return hashMap.size();
    }

    @Override
    public void removeDhb( DedupHashBlock dhb )
    {
        hashMap.remove(dhb.getHashvalue());
    }

    
    @Override
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

    @Override
    public boolean shutdown()
    {
        hashMap.clear();
        return true;
    }

}
