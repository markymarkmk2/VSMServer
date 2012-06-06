/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.records;

import de.dimm.vsm.sql.GenericResultException;
import de.dimm.vsm.sql.GenericResultSet;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author mw
 */
public abstract class GenericRecord
{
    private String table_name;
    protected String idx_name;
    
    String characterset = "UNICODE_FSS";
    public static final String FIREBIRD_SQL_LONG = "BigInt";
    public static final String FIREBIRD_SQL_INT = "Integer";

    public static final String SQL_LONG = FIREBIRD_SQL_LONG;
    public static final String SQL_INT = FIREBIRD_SQL_INT;

   // public static final String[][2] type_list = { {"_LONG_", SQL_LONG} };

    HashMap<String,String> sql_types;

    public GenericRecord( String table_name, String idx_name )
    {
        this.table_name = table_name;
        this.idx_name = idx_name;

        sql_types = new HashMap<String, String>();
        sql_types.put("_LONG_", SQL_LONG);
        sql_types.put("_INT_", SQL_INT);
    }

    public String get_table_name()
    {
        return table_name;
    }

    public String get_idx_name()
    {
        return idx_name;
    }

    abstract boolean read( GenericResultSet rs ) throws GenericResultException;
   // abstract boolean update( GenericUpdater rs ) throws GenericResultException;
    abstract String get_create_cmd();
    abstract String[] get_create_indices_cmd();

    public String get_qry( String where )
    {
        String qry = "select * from " + get_table_name();
        if (where != null && where.length() > 0)
            qry += " where " + where;

        return qry;
    }

    public String get_qry( long idx )
    {
        String qry = "select * " + get_table_name() + " where " + idx_name + "=" + idx;

        return qry;
    }

    String resolve_datatypes( String s)
    {
        for (Iterator<String> it = sql_types.keySet().iterator(); it.hasNext();)
        {
            String key = it.next();
            String val = sql_types.get(key);

            s = s.replaceAll(key, val);
        }
        return s;
    }

    String[] resolve_datatypes( String[] s_list)
    {
        for (int i = 0; i < s_list.length; i++)
        {
            String s = s_list[i];

            for (Iterator<String> it = sql_types.keySet().iterator(); it.hasNext();)
            {
                String key = it.next();
                String val = sql_types.get(key);

                s = s.replaceAll(key, val);
            }
            
            s_list[i] = s;
        }
        return s_list;
    }

}
