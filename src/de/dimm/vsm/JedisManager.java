/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Administrator
 */
public class JedisManager {
    
    JedisPool jedisPool; 
    

    private String JEDIS_STATUS = "&JEDIS_STATUS";
    private String JEDIS_STARTED = "STARTED";
    private String JEDIS_SHUTDOWN = "SHUTDOWN";         
    
    String startStatus = null;
    boolean shutDownAllowed = true;
    
    void shutDown()
    {
        if (!Main.get_bool_prop(GeneralPreferences.USE_REDIS_CACHE, false))
            return;        
        
        if (shutDownAllowed)
        {
            Jedis jedis = jedisPool.getResource();
            if (jedis.dbSize()> 2)
            {
                startStatus = jedis.get(getStatusVarName());
                jedis.set(getStatusVarName(), JEDIS_SHUTDOWN);
            }    
            jedisPool.returnResource(jedis);      
            Log.info("RedisCache wurde konsistent geschlossen");  
        }
        jedisPool.destroy();                
    }
    
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    void startup() {
        if (!Main.get_bool_prop(GeneralPreferences.USE_REDIS_CACHE, false))
            return;
                
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(100);
        
        jedisPool = new JedisPool( config, "localhost");
        
        Jedis jedis = jedisPool.getResource();
        if (jedis.dbSize()> 2)
        {
            startStatus = jedis.get(getStatusVarName());
            jedis.set(getStatusVarName(), JEDIS_STARTED);
            
            if (!hasStartedClean()) 
            {
                Log.warn("RedisCache für wird geleert");  
                jedis.flushAll();
            }
            else
            {
                Log.info("RedisCache wurde konsistent geöffnet");  
            }
        }   
        else
        {
            Log.info("RedisCache wurde leer geöffnet");  
        }
        jedisPool.returnResource(jedis);
    }
    
    String getStatusVarName()
    {
        return JEDIS_STATUS;
    }
    
    public boolean hasStartedClean()
    {
        return (StringUtils.isNotEmpty(startStatus) && startStatus.equals(JEDIS_SHUTDOWN));
    }    
    public void invalidateShutdown()
    {
        shutDownAllowed = false;
    }    
     
}
