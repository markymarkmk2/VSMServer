/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class DBHashCache extends HashCache
{

    JDBCEntityManager em;
    PreparedStatement ps = null;
    ArrayList<String> urlUnsafeHashes = new ArrayList<String>();

    public DBHashCache( JDBCEntityManager em, StoragePool pool )
    {
        super(pool);
        this.em = em;

    }


    @Override
    public void fill( String hash, long id )
    {
    }

    @Override
    public boolean init( Connection conn ) throws IOException
    {
        if (!Main.get_bool_prop(GeneralPreferences.HASH_URL_FORMAT_FIX, false))
            return true;

        urlUnsafeHashes.clear();
        Statement st  = null;
        try
        {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("select idx, hashvalue  from DedupHashBlock");

            while (rs.next())
            {
                long idx = rs.getLong(1);
                String hash = rs.getString(2);

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
                    urlUnsafeHashes.add(hash);
                }

            }
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

        // Do nothing
        return true;
    }

    @Override
    public long getDhbIdx( String hash ) throws IOException
    {
        long l = -1;
        
        try
        {
            if (ps == null)
            {
                ps = em.getConnection().prepareStatement("select idx from DedupHashBlock were hashvalue=?");
            }
            
            ps.setString(1, hash);

            ResultSet rs = ps.executeQuery();
            if (rs.next())
            {
                l = rs.getLong(0);
            }
            rs.close();
        }
        catch (SQLException sQLException)
        {
            throw new IOException(sQLException.getMessage());
        }
        finally
        {
            
        }
        return l;
    }

    @Override
    public void addDhb( String hash, long idx )
    {
        //
    }

    @Override
    public long size()
    {
        return -1;
    }

    @Override
    public void removeDhb( DedupHashBlock dhb )
    {
        // hashMap.remove(dhb.getHashvalue());
    }

    @Override
    public List<String> getUrlUnsafeHashes()
    {
        return urlUnsafeHashes;
    }

    @Override
    public boolean shutdown()
    {
        try
        {
            ps.close();
        }
        catch (SQLException sQLException)
        {
        }
        return true;
    }
}
