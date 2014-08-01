/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine.hashcache;


import de.dimm.vsm.fsengine.hashcache.DBHashCache;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author Administrator
 */
public abstract class HashCache
{

    public static HashCache createCache( JDBCEntityManager em, StoragePoolNub nub, StoragePool pool )
    {
        if (Main.get_bool_prop(GeneralPreferences.USE_H2_CACHE, false))
            return new H2HashCache(nub, pool);
        else if(Main.get_bool_prop(GeneralPreferences.USE_NO_CACHE, false))
            return new DBHashCache(em, pool);
        else if(Main.get_bool_prop(GeneralPreferences.USE_LAZY_CACHE, false))
            return new LazyJavaHashCache(em, pool);
        else if(Main.get_bool_prop(GeneralPreferences.USE_REDIS_CACHE, false))
            return new RedisHashCache(em, pool, nub);
        else
            return new JavaHashCache(pool);
    }
    StoragePool pool;

    protected HashCache( StoragePool pool )
    {
        this.pool = pool;
    }

    

    boolean inited = false;

    public abstract void fill( String hash, long id ) throws IOException;
    public abstract long getDhbIdx( String hash ) throws IOException;
    public abstract void addDhb( String hash, long idx ) throws IOException;
    public abstract long size();
    public abstract void removeDhb( DedupHashBlock dhb );
    public abstract List<String> getUrlUnsafeHashes();
    public abstract boolean init(Connection conn ) throws IOException;
    public boolean isLoading()
    {
        return false;
    }


    public boolean isInited()
    {
        return inited;
    }
    public abstract boolean shutdown( );

    

}
