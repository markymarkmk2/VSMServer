/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.log.LogManager;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.records.StoragePoolNub;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class H2HashCache extends HashCache
{
    public static final String RELPARAMPATH = "/Hash";

    public static final String DBSTATE_SHUTDOWN_OK = "shutdown_ok";
    public static final String DBSTATE_STARTED = "started_ok";
    public static final String DBSTATE_STARTING = "starting";
    public static final String DBSTATE_SHUTTING_DOWN = "shutting_down";

    public static final int MAX_COMMIT_CNT = 10000;
    
    Connection h2conn = null;
    PreparedStatement psinsert;
    PreparedStatement psget;
    PreparedStatement psdel;
    Savepoint setSavepoint;
    long cnt = 0;
    String dbPath;


    final String getDbPath(StoragePoolNub nub)
    {
        String s = Main.get_prop(GeneralPreferences.DB_PATH, Main.DATABASEPATH );
        s = s.replace('\\', '/');
        if (!s.endsWith("/"))
            s += "/";

        String p = s + "db_" + nub.getIdx() + RELPARAMPATH;
        return p;
    }
    public H2HashCache(StoragePoolNub nub,  StoragePool pool)
    {
        super(pool);
        dbPath = getDbPath( nub );
        File dbDir = new File(dbPath);
        if (!dbDir.exists())
            dbDir.mkdir();

        dbPath += "/hashdb";
    }

    @Override
    public boolean shutdown()
    {
        try
        {
            psinsert.close();
            psget.close();
            psdel.close();

            h2conn.commit();
            setDbStatus( DBSTATE_SHUTDOWN_OK );
            h2conn.commit();
            h2conn.close();
            return true;
        }
        catch (SQLException sQLException)
        {
            LogManager.err_db("Schließen von H2 schlug fehl", sQLException);
        }
        return false;

    }


    String getDbStatus( )
    {
        Statement st = null;
        String dbstatus = "dberror";
        try
        {
            st = h2conn.createStatement();
            ResultSet sr = st.executeQuery("select varvalue from hashmapstatus where varname='dbstatus'");
            if (sr != null && sr.next())
            {
                dbstatus = sr.getString(1);
            }
            sr.close();
            st.close();
        }
        catch (SQLException sQLException)
        {
            Log.warn("Fehler beim Lesen von H2Cache dbStatus");
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
        return dbstatus;
    }
    
    boolean setDbStatus( String status )
    {
        Statement st = null;
        boolean ret = false;
        try
        {
            st = h2conn.createStatement();
            st.executeUpdate("update hashmapstatus set varvalue='" + status + "' where varname='dbstatus'");
            h2conn.commit();
            ResultSet sr = st.executeQuery("select varvalue from hashmapstatus where varname='dbstatus'");
            if (sr != null && sr.next())
            {
                String actstatus = sr.getString(1);
                if (actstatus.equals(status))
                    ret = true;
            }
            sr.close();


        }
        catch (SQLException sQLException)
        {
            LogManager.err_db("Fehler beim Schreiben von H2Cache dbStatus", sQLException);
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
        return ret;
    }
   

    @Override
    public boolean init(Connection conn ) throws IOException
    {
        boolean lastShutdownOk = false;

        try
        {
            Class.forName("org.h2.Driver");

            h2conn = DriverManager.getConnection("jdbc:h2:" + dbPath, "sa", "");
            
            h2conn.setAutoCommit(false);
            setSavepoint = h2conn.setSavepoint();


            String dbstatus = getDbStatus();
            if (dbstatus.equals(DBSTATE_SHUTDOWN_OK))
            {
                if (setDbStatus(DBSTATE_STARTED))
                {
                    lastShutdownOk = true;
                }
            }

            if (!lastShutdownOk)
            {
                if (dbstatus.equals(DBSTATE_STARTED))
                {
                    Log.warn("Cache wurde nicht korrekt heruntergefahren, wird neu aufgebaut");
                }

                Statement st = h2conn.createStatement();
                st.execute("drop table if exists hashmap");
                st.execute("drop table if exists hashmapstatus");
                st.execute("create memory table hashmap (hashval char(28) primary key hash, idxval bigint)");
                st.execute("create table hashmapstatus (varname char(80) primary key hash, varvalue char(80))");
                st.execute("insert into hashmapstatus (varname,varvalue) values ('dbstatus','')");
                h2conn.commit();
                st.close();
                if (!setDbStatus(DBSTATE_STARTING))
                {
                    throw new IOException("H2Cache kann nicht gestartet werden" );
                }
            }

            
            psinsert = h2conn.prepareStatement("insert into hashmap (hashval,idxval) values (?,?)");
            psget = h2conn.prepareStatement("select idxval from hashmap where hashval=?");
            psdel = h2conn.prepareStatement("delete from hashmap where hashval=?");
            h2conn.commit();
        }
        catch (Exception exc)
        {
            LogManager.err_db("Offnen von H2 schlug fehl", exc);
            throw new IOException("Fehler beim Aufbau des  H2 caches-> Fallback auch HashMap", exc);
        }
        if (lastShutdownOk)
        {
            inited = true;
            return true;
        }

        super.init(conn);

        try
        {
            if (!setDbStatus(DBSTATE_STARTING))
            {
                throw new IOException("Neuer H2Cache kann nicht gestartet werden" );
            }
            
            h2conn.commit();
        }
        catch (SQLException exc)
        {
            LogManager.err_db("Schließen von H2 schlug fehl", exc);
            throw new IOException("Fehler beim Aufbau des  H2 caches-> Fallback auch HashMap", exc);
        }
        return true;
    }

    @Override
    public void fill( String hash, long id ) throws IOException
    {
        try
        {
            psinsert.setString(1, hash);
            psinsert.setLong(2, id);
            psinsert.execute();

            cnt++;
            if ((cnt % MAX_COMMIT_CNT) == 0)
            {
                h2conn.commit();
            }

        }
        catch (SQLException exc)
        {
            LogManager.err_db("Füllen von H2cache schlug fehl", exc);
            throw new IOException("Fehler beim Aufbau des  H2 caches-> Fallback auch HashMap", exc);
        }       
    }

    @Override
    public long getDhbIdx( String hash ) throws IOException
    {
        ResultSet rs = null;
        try
        {
            long ret = -1;
            psget.setString(1, hash);
            rs = psget.executeQuery();
            if (rs != null && rs.next())
            {
                ret = rs.getLong(1);
            }
            return ret;
        }
        catch (SQLException exc)
        {
            LogManager.err_db("Lesen von H2cache schlug fehl", exc);
            throw new IOException("Fehler beim Aufbau des  H2 caches-> Fallback auch HashMap", exc);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException sQLException)
                {
                }
            }
        }
    }

    @Override
    public void addDhb( String hash, long idx ) throws IOException
    {
        try
        {
            psinsert.setString(1, hash);
            psinsert.setLong(2, idx);
            psinsert.execute();

            cnt++;
            if ((cnt % MAX_COMMIT_CNT) == 0)
            {
                h2conn.commit();
            }
        }
        catch (SQLException exc)
        {
            LogManager.err_db("Addieren von H2cache schlug fehl", exc);
            throw new IOException("Fehler beim Addieren zum H2 cache", exc);
        }        
    }

    @Override
    public long size()
    {
        return cnt;
    }

    @Override
    public void removeDhb( DedupHashBlock dhb )
    {
        try
        {
            psdel.setString(1, dhb.getHashvalue());
            psdel.execute();

            h2conn.commit();
        }
        catch (SQLException exc)
        {
            LogManager.err_db("Löschen von H2cache schlug fehl", exc);
        }
    }

    @Override
    public List<String> getUrlUnsafeHashes()
    {
        return new ArrayList<String>();
    }

}
