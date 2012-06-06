/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 *
 * @author mw
 */
public class JDBCResultSet extends GenericResultSet
{
    ResultSet rs;

    public JDBCResultSet( ResultSet rs )
    {
        this.rs = rs;
    }

    @Override
    public long getLong( String idx_name ) throws GenericResultException
    {
        try
        {
            return rs.getLong(idx_name);
        }
        catch (SQLException sQLException)
        {
            throw new GenericResultException( sQLException );
        }
    }

    @Override
    public String getString( String string ) throws GenericResultException
    {
        try
        {
            return rs.getString(string);
        }
        catch (SQLException sQLException)
        {
            throw new GenericResultException( sQLException );
        }
    }

    @Override
    public Date getDate( String string ) throws GenericResultException
    {
        try
        {
            return rs.getDate(string);
        }
        catch (SQLException sQLException)
        {
            throw new GenericResultException( sQLException );
        }
    }



}
