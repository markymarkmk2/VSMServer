/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import com.ibm.icu.impl.Assert;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 *
 * @author Administrator
 */
public class RedisTest
{
    int max_entries = 10*1000;
    int nthreads = 5;
    int nthreadsPar = 5;
    
   
    static JedisPool jedisPool; 
    private String PREFIX_STR = "1234567890_1234567890_";
    
    ExecutorService loadHashExecutor = Executors.newFixedThreadPool(nthreadsPar);
    public RedisTest()
    {
    }
    
     @BeforeClass
    public static void setUpClass() throws Exception
    {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(100);
        jedisPool = new JedisPool( config, "localhost");        
    }

    
    @AfterClass
    public static void tearDownClass()
    {
        jedisPool.destroy();
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
    
   

    /**
     * Test of alert method, of class ServerApiImpl.
     */
    @Test
    public void parallelHash()
    {
        for (int i = 0; i < nthreads; i++ )
        {
            final long thrIdx = i;       
            loadHashExecutor.submit( new Runnable() {
                
                @Override
                public void run() {
                    
                    try {
                        Jedis jedis = jedisPool.getResource();
                        long sfill = System.currentTimeMillis();
                        fillhash(jedis, 100, thrIdx, max_entries);
                        long sread = System.currentTimeMillis();
                        fillhash(jedis, 500, thrIdx, max_entries);
                        long sread2 = System.currentTimeMillis();
                        readhash(jedis, thrIdx, max_entries);
                        long sdel = System.currentTimeMillis();
                        delhash(jedis, thrIdx, max_entries);
                        long edel = System.currentTimeMillis();
                        
                        System.out.println(" Objects: " + max_entries + " Thread: " + thrIdx
                                + " Fill: " + (sread - sfill) + "ms"
                                + " Fill2: " + (sread2 - sread) + "ms"
                                + " Read: " + (sdel - sread2) + "ms"
                                + " Del : " + (edel - sdel) + "ms");
                        
                        System.out.println(" FillSpeed1 Thread " + thrIdx + ": " + (max_entries * 1000) / (sread - sfill) + " 1/s");
                        System.out.println(" FillSpeed2 Thread " + thrIdx + ": " + (max_entries * 1000) / (sread2 - sread) + " 1/s");
                        System.out.println(" ReadSpeed  Thread " + thrIdx + ": " + (max_entries * 1000) / (sdel - sread2) + " 1/s");
                        jedisPool.returnResource(jedis);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail(e);
                    }
                }
            }, "RedisHash " + i);            
        }  
        loadHashExecutor.shutdown();
        try {
            loadHashExecutor.awaitTermination(10, TimeUnit.DAYS);
        }
        catch (InterruptedException ex) {
            Logger.getLogger(RedisTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    void fillhash(Jedis jedis, int pipelen,long nub, int n)
    {        
        Pipeline pipeline = jedis.pipelined();        
        for (int i = 0; i < n; i++ )
        {
            String key = Long.toString(nub) + "&" + PREFIX_STR + i;
            
            pipeline.set(key, Long.toString(i));
            if (i % pipelen == 0)
            {
                pipeline.sync();
            }
        }
        pipeline.sync();
        
    }
    
    void readhash(Jedis jedis, long nub, int n)
    {
        for (int i = 0; i < n; i++ )
        {
            String key = Long.toString(nub) + "&" + PREFIX_STR + i;
            String val = jedis.get(key);
            if (val == null || Integer.parseInt(val) != i)
                throw new RuntimeException("Wrong val");
        }
    }
    void delhash(Jedis jedis, long nub, int n)
    {
        for (int i = 0; i < n; i++ )
        {
            String key = Long.toString(nub) + "&" + PREFIX_STR + i;
            if (jedis.del(key) != 1)
                throw new RuntimeException("Del failed");
        }
    }


}