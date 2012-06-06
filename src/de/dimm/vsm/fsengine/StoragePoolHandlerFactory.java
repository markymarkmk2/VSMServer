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
    public static StoragePoolHandler createStoragePoolHandler(StoragePoolNubHandler nubHandler, StoragePool pool, User user, boolean rdonly) throws IOException
    {
        try
        {
            Log.debug("Offene DB-Verbindungen", "" + Main.get_control().getStorageNubHandler().getActiveConnections(pool) );

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
            Log.err("Kann DB-Verbindung nicht öffnen", pool.toString(), sQLException);
            throw new IOException(Main.Txt("Kann DB-Verbindung nicht öffnen"), sQLException);
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
    public static StoragePoolHandler createStoragePoolHandler( StoragePoolNubHandler nubHandler, StoragePool pool, StoragePoolQry qry ) throws IOException
    {
        try
        {
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
            Log.err("Kann DB-Verbindung nicht öffnen", pool.toString(), sQLException);
            throw new IOException(Main.Txt("Kann DB-Verbindung nicht öffnen"), sQLException);
        }
    }
}
