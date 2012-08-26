/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;


import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 *
 * @author Administrator
 */
public abstract class HashCache
{

    static HashCache createCache( StoragePoolNub nub, StoragePool pool )
    {
        if (Main.get_bool_prop(GeneralPreferences.USE_H2_CACHE, false))
            return new H2HashCache(nub, pool);
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

    public boolean isInited()
    {
        return inited;
    }
    public abstract boolean shutdown( );

    public boolean init(Connection conn ) throws IOException
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

                fill( has, idx );

            }
            rs.close();
            Log.info("Block-Cachegröße für Pool", pool.getName() + ": " + size());

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

}
