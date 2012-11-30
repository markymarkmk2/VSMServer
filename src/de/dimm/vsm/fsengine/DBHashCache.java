/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
        ArrayList<String> ret = new ArrayList<String>();

        return ret;
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
