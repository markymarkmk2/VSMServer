/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.hashcache;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.BigArrayList;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Administrator
 */
public class LazyJavaHashCache extends DBHashCache
{
    Map<String,Long> hashMap;
    final String preloadMtx;
    
    
    Connection conn;
    Map<String,Long> tempMap;
    List<String> tmpRemoveList;
    protected ExecutorService loadHashExecutor =Executors.newFixedThreadPool(1);
    private boolean cacheInited = false;
    
    public LazyJavaHashCache( JDBCEntityManager em, StoragePool pool)
    {
        super(em, pool);
        hashMap = null;
        preloadMtx = "M";
        tempMap = new ConcurrentHashMap<>();
        tmpRemoveList = new ArrayList<>();           
    }
    
    @Override
    public boolean init(Connection conn ) throws IOException
    {
        this.conn = conn;
        loadHashExecutor.submit( new Runnable() {

            @Override
            public void run() {
                loadHashMap();
            }
        }, "LazyJavaCacheLoader");
        
        inited = true;
        return super.init(conn);
    }
    
    void loadHashMap()
    {
        // TODO: CALC MEM
        cacheInited = false;
        

        
        Statement st  = null;
        Connection conn2 =null;
        try
        {
            // WE HAVE TO USE OWN CONNECTION BECAUSE OF LOCK ON CONCURRENTLY WORKING JOBS IN BACKUP
            conn2 = em.getConnFactory().createConnection();
            st = conn2.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
            st.setFetchSize(10000);
            long maxMem = Runtime.getRuntime().maxMemory();
            long freeMem = Runtime.getRuntime().freeMemory();
            long totalMem = Runtime.getRuntime().totalMemory();
            Log.info("LazyHash wird geladen für Pool ", this.pool.getName() );
            Log.info("Speicher vor Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );
            BigArrayList<Long> idxList;
            BigArrayList<String> hashList;
            long cnt = 1;
            long startS = System.currentTimeMillis() / 100;
            try (ResultSet rs = st.executeQuery("select idx, hashvalue  from DedupHashBlock")) {
                idxList = new BigArrayList<>();
                hashList = new BigArrayList<>();
                while (rs.next())
                {
                    long idx = rs.getLong(1);
                    String has = rs.getString(2);

                    idxList.add(Long.valueOf(idx));
                    hashList.add(has);

                    // Max 90 % Last: Alle 100 ms oder 100 Objekt kurze Pause
                    if (cnt % 1000 == 0)
                    {
                        long nowS = System.currentTimeMillis() / 100;
                        if (nowS > startS)
                        {
                            startS = nowS;
                            LogicControl.sleep(10);
                        }
                    }
                    cnt++;
                }
            }
            
            
            long len = idxList.size();
            int mapSize = Integer.MAX_VALUE;
            if (mapSize > len)
                mapSize = (int)len;
            
            Map<String,Long>localHashMap = new ConcurrentHashMap<>(mapSize, 0.75f, 4);
            
            for ( long l = 0; l < len; l++)
            {
                localHashMap.put(hashList.get(l), idxList.get(l));
            }
            
            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " +len );

            maxMem = Runtime.getRuntime().maxMemory();
            freeMem = Runtime.getRuntime().freeMemory();
            totalMem = Runtime.getRuntime().totalMemory();
            Log.info("Speicher nach Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );
            
            
            synchronized(preloadMtx)
            {
                localHashMap.putAll(tempMap);
                for (String remHash: tmpRemoveList)
                {
                    localHashMap.remove(remHash);
                }
                hashMap = localHashMap;
                cacheInited = true;                
            }
            Log.info("Hash wurde erfolgreich geladen für Pool ", this.pool.getName() );
        }
        catch (SQLException sQLException)
        {
            Log.err("HashMap kann nicht angelegt werden", pool.getName(), sQLException);
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
            if (conn2 != null)
            {
                try {
                    conn2.close();
                }
                catch (SQLException sQLException) {
                }
            }
        }
    }

    @Override
    public void fill( String hash, long id )
    {
        synchronized(preloadMtx)
        {
            if (cacheInited)
            {
                hashMap.put(hash, id);
            }
            else
            {
                tempMap.put(hash, id);
            }
        }
    }
        
    @Override
    public long getDhbIdx( String hash ) throws IOException
    {
        synchronized(preloadMtx)
        {
            if (cacheInited)
            {
                Long l = hashMap.get(hash);
                if (l != null)
                    return l.longValue();

                return -1;
            }
        }
        long l = super.getDhbIdx(hash);
        tempMap.put(hash, l);
        return l;
    }
    
    @Override
    public void addDhb( String hash, long idx )
    {
        fill(hash, idx);
    }

    @Override
    public long size()
    {
        synchronized(preloadMtx)
        {
            if (cacheInited)
            {
                return hashMap.size();
            }
            return tempMap.size();
        }
    }

    @Override
    public void removeDhb( DedupHashBlock dhb )
    {
        synchronized(preloadMtx)
        {
            if (cacheInited)
            {
                hashMap.remove(dhb.getHashvalue());
            }
            else
            {
                tempMap.remove(dhb.getHashvalue());
                tmpRemoveList.add(dhb.getHashvalue());
            }
        }
    }

    
    

    @Override
    public boolean shutdown()
    {
        synchronized(preloadMtx)
        {
            Main.get_control().getThreadPoolWatcher().shutdown_thread_pool(loadHashExecutor, 1000);
            if (cacheInited)
            {
                hashMap = new ConcurrentHashMap<>();
            }            
        }
        return true;
    }

}
