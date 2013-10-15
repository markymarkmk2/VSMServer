/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.log.Log;
import de.dimm.vsm.Main;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class StoragePoolHandlerFactory
{
    static boolean jpa = false;
    private static boolean persistRunnerEnabled = false;


    public static StoragePoolHandler createStoragePoolHandler(StoragePool pool, User user, boolean rdonly) throws IOException
    {
        return createStoragePoolHandler( Main.get_control().getStorageNubHandler(), pool, user, rdonly);
    }
    public static StoragePoolHandler createStoragePoolHandler(StoragePoolNubHandler nubHandler, StoragePool _pool, User user, boolean rdonly) throws IOException
    {
        try
        {
            StoragePool pool;
            if ( Main.get_control() != null)
            {    
                pool = Main.get_control().getStoragePool(_pool.getIdx());
                Log.debug("Offene DB-Verbindungen", "" + Main.get_control().getStorageNubHandler().getActiveConnections(pool) );
            }
            else
            {
                pool = _pool;
            }
// RELOAD FROM LIST
            JDBCConnectionFactory conn = nubHandler.getConnectionFactory(pool);
            JDBCEntityManager em = new JDBCEntityManager(pool.getIdx(), conn);

            JDBCStoragePoolHandler sp_handler = new JDBCStoragePoolHandler( em, user, pool, rdonly );

            if (isPersistRunnerEnabled())
            {
                  HandlePersistRunner persistRunner = new HandlePersistRunner();
                  sp_handler.setPersistRunner(persistRunner);
            }

            return sp_handler;
        }
        catch (SQLException sQLException)
        {
            String msg = Main.Txt("Kann DB-Verbindung nicht öffnen");
            Log.err(msg, _pool.toString(), sQLException);
            throw new IOException( msg, sQLException);
        }
    }

    public static void setPersistRunnerEnabled( boolean persistRunnerEnabled )
    {
        StoragePoolHandlerFactory.persistRunnerEnabled = persistRunnerEnabled;
    }

    public static boolean isPersistRunnerEnabled()
    {
        return persistRunnerEnabled;
    }

    public static StoragePoolHandler createStoragePoolHandler(StoragePool pool, StoragePoolQry qry) throws IOException
    {
        return createStoragePoolHandler( Main.get_control().getStorageNubHandler(), pool, qry);
    }
    public static StoragePoolHandler createStoragePoolHandler( StoragePoolNubHandler nubHandler, StoragePool _pool, StoragePoolQry qry ) throws IOException
    {
        try
        {
            // RELOAD FROM LIST
            StoragePool pool = Main.get_control().getStoragePool(_pool.getIdx());
            JDBCConnectionFactory conn = nubHandler.getConnectionFactory(pool);
            JDBCEntityManager em = new JDBCEntityManager(pool.getIdx(), conn);
            JDBCStoragePoolHandler sp_handler = new JDBCStoragePoolHandler( em, pool, qry );
            
            if (isPersistRunnerEnabled())
            {
                  HandlePersistRunner persistRunner = new HandlePersistRunner();
                  sp_handler.setPersistRunner(persistRunner);
            }

            return sp_handler;
        }
        catch (Exception sQLException)
        {
            Log.err("Kann DB-Verbindung nicht öffnen", _pool.toString(), sQLException);
            throw new IOException(Main.Txt("Kann DB-Verbindung nicht öffnen"), sQLException);
        }
    }
}
