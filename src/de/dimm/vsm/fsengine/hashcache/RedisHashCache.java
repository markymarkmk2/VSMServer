/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.hashcache;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

/**
 *
 * @author Administrator
 */
public class RedisHashCache extends DBHashCache
{
    Map<String,Long> hashMap;
    final String preloadMtx;
    
    
    Connection conn;
    Map<String,Long> tempMap;
    List<String> tmpRemoveList;
    protected ExecutorService loadHashExecutor = Executors.newFixedThreadPool(1);
    Jedis jedis;
    StoragePoolNub nub;
    private boolean cacheInited = false;
    private int JEDIS_TIMEOUT = 60000;
    
    
    public RedisHashCache( JDBCEntityManager em, StoragePool pool, StoragePoolNub nub)
    {
        super(em, pool);
        this.nub = nub;
        hashMap = null;
        preloadMtx = "M";
        tempMap = new ConcurrentHashMap<>();
        tmpRemoveList = new ArrayList<>();
        //loadHashExecutor = Main.get_control().getThreadPoolWatcher().create_blocking_thread_pool("RedisCacheLoader", 1, 1);        
    }
    
    
    public boolean hasStartedClean()
    {
        return Main.get_control().getJedisManager().hasStartedClean();
    }    
    @Override
    public boolean init(Connection conn ) throws IOException
    {
        jedis = getJedisPool().getResource();
        this.conn = conn;
        
                
        if (!hasStartedClean())
        {
            Log.info("Lade RedisCache für Pool " + pool.getName() );  
            
            loadHashExecutor.submit( new Runnable() {

                @Override
                public void run() {
                    loadHashMap();
                }
            });
        }
        else
        {
            Log.info("RedisCache für Pool " + pool.getName() + " ist bereits geladen (N=" +jedis.dbSize()  + ")" );  
            cacheInited = true;
        }
        
        inited = true;
        return super.init(conn);
    }
    
    private JedisPool getJedisPool()
    {
        return  Main.get_control().getJedisManager().getJedisPool();
    }
    
    String buildRedisHash( String hash)
    {
        String ret = Long.toString(nub.getIdx()) + "&" + hash;
        return ret;
    }
    String getHashFromRedis( String redisHash)
    {
        String[] parts = redisHash.split("&");
        if (parts.length != 2)
            throw new RuntimeException("Redis Hash Delimiter ist falsch");
        Long idx = Long.parseLong(parts[0]);
        if (idx != nub.getIdx())
            throw new RuntimeException("Redis Hash stammt von falschem Nub");
        
        return parts[1];
    }
    
    void loadHashMap()
    {
        cacheInited = false;

        Statement st  = null;
        Client client = jedis.getClient();
        client.setTimeout(JEDIS_TIMEOUT);
        
        Pipeline pipeline = jedis.pipelined(); 
        
        Connection conn2 = null;
        try
        {
            // WE HAVE TO USE OWN CONNECTION BECAUSE OF LOCK ON CONCURRENTLY WORKING JOBS IN BACKUP
            conn2 = em.getConnFactory().createConnection();
            
            client.getSocket().setSoTimeout(JEDIS_TIMEOUT);
            st = conn2.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
            st.setFetchSize(10000);
            long maxMem = Runtime.getRuntime().maxMemory();
            long freeMem = Runtime.getRuntime().freeMemory();
            long totalMem = Runtime.getRuntime().totalMemory();
            Log.info("LazyRedisHash wird geladen für Pool ", this.pool.getName() );
            Log.info("Speicher vor Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );
            long cnt = 1;
            long startS = System.currentTimeMillis() / 100;
            long start = System.currentTimeMillis();
            try (ResultSet rs = st.executeQuery("select idx, hashvalue  from DedupHashBlock")) {
                while (rs.next())
                {
                    Long idx = rs.getLong(1);
                    String hash = rs.getString(2);
                    
                    pipeline.set(buildRedisHash(hash), idx.toString());


                    // Max 90 % Last: Alle 100 ms oder 1000 Objekt kurze Pause
                    if (cnt % 1000 == 0)
                    {
                        pipeline.sync();
                        long nowS = System.currentTimeMillis() / 100;
                        if (nowS > startS)
                        {
                            startS = nowS;
                            LogicControl.sleep(10);
                        }
                    }
                    cnt++;
                }
                pipeline.sync();
            }
            long end = System.currentTimeMillis();
            long objPerSecond = 0;
            if (end > start)
                objPerSecond = (cnt*1000) / (end - start);

            maxMem = Runtime.getRuntime().maxMemory();
            freeMem = Runtime.getRuntime().freeMemory();
            totalMem = Runtime.getRuntime().totalMemory();
            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " +cnt + " Speed: " + objPerSecond +  " 1/s"  );
            Log.info("Speicher nach Cache", "Max: " + SizeStr.format(maxMem) + " Total: " + SizeStr.format(totalMem) + " Free: " + SizeStr.format(freeMem)  );
            
            
            synchronized(preloadMtx)
            {
                for (Entry<String,Long> entry: tempMap.entrySet())
                {
                    jedis.set(buildRedisHash(entry.getKey()), entry.getValue().toString());                    
                }
                for (String remHash: tmpRemoveList)
                {                    
                    jedis.del(buildRedisHash(remHash));
                }
                cacheInited = true;                
            }
            Log.info("Hash wurde erfolgreich geladen für Pool ", this.pool.getName() );
        }
        catch (Exception ex)
        {
            Main.get_control().getJedisManager().invalidateShutdown();
            Log.err("HashMap kann nicht angelegt werden", pool.getName(), ex);
        }
        finally
        {
            if (st != null)
            {
                try
                {
                    st.close();
                    conn2.commit();
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
                hash = buildRedisHash(hash);
                try {
                    jedis.set(hash, Long.toString(id));   
                }
                catch (JedisException ex)
                {
                    Log.err("Jedis set:", pool.getName(), ex);
                    getJedisPool().returnBrokenResource(jedis);
                    jedis = getJedisPool().getResource();
                    jedis.set(hash, Long.toString(id));   
                }
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
                hash = buildRedisHash(hash);
                String val;
                try {
                    val = jedis.get(hash);   
                }
                catch (JedisException ex)
                {
                    Log.err("Jedis set:", pool.getName(), ex);
                    getJedisPool().returnBrokenResource(jedis);
                    jedis = getJedisPool().getResource();
                    val = jedis.get(hash);   
                }
                // Not found
                if (val == null)
                    return -1;
                return Long.parseLong(val);
            }
        }
        int retries = 3;
        IOException cause = new IOException("");
        while (retries-- > 0)
        {
            try {
                long l = super.getDhbIdx(hash);
                tempMap.put(hash, l);
                return l;
            }
            catch (IOException iOException) {
                Log.warn("Exception beim getDhbIdx, wiederhole: ", pool.getName() + ": " + iOException.getMessage());
                cause = iOException;
                LogicControl.sleep(3000);
            }
        }
        Log.err("Fehler bei getDhbIdx: ", pool.getName(), cause);
        throw cause;
        
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
                return jedis.dbSize();
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
                String redisHash = buildRedisHash(dhb.getHashvalue());
                try {
                    jedis.del(redisHash);   
                }
                catch (JedisException ex)
                {
                    Log.err("Jedis set:", pool.getName(), ex);
                    getJedisPool().returnBrokenResource(jedis);
                    jedis = getJedisPool().getResource();
                    jedis.del(redisHash);   
                }                
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
        loadHashExecutor.shutdown();
        try {
            loadHashExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException ex) {
            Logger.getLogger(RedisHashCache.class.getName()).log(Level.SEVERE, null, ex);
        }        
        synchronized(preloadMtx)
        {
            if (!cacheInited)
            {
                Main.get_control().getJedisManager().invalidateShutdown();
            }
            getJedisPool().returnResource(jedis);              
        }
        return true;
    }

}
