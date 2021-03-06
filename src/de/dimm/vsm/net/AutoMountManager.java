/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.fsengine.GenericEntityManager;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.records.MountEntry;
import de.dimm.vsm.records.Role;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Administrator
 */
public class AutoMountManager extends WorkerParent implements IAgentIdleManagerEntry
{
    List<MountEntry> mountList;

    public AutoMountManager()
    {
        super( "AutoMountManager" );
        mountList = Collections.synchronizedList(new ArrayList<MountEntry>() );
    }

    @Override
    public boolean isPersistentState()
    {
        return true;
    }

    @Override
    public boolean initialize()
    {
        return true;
    }

    @Override
    public boolean isVisible()
    {
        return false;
    }

    List<MountEntry> getMountEntryList()
    {
        List<MountEntry> mountEntries = new ArrayList<>();
        try
        {
            List<StoragePool> poolList = Main.get_control().getStoragePoolList();
            for (StoragePool storagePool : poolList)
            {
                GenericEntityManager em = Main.get_control().get_util_em( storagePool );
                List<MountEntry> poolMountEntries = em.createQuery( "select T1 from MountEntry", MountEntry.class );
                mountEntries.addAll( poolMountEntries);
            }
            return mountEntries;

        }
        catch (SQLException sQLException)
        {
            Log.err( "MountEntryList nicht lesbar", sQLException );

        }
        return mountEntries;
    }

    User getUser( MountEntry mountEntry )
    {
        if (StringUtils.isEmpty( mountEntry.getUsername() ))
        {
            return User.createSystemInternal();
        }
        Role role = Main.get_control().getUsermanager().getRole(mountEntry.getUsername() );
        if (role != null)
        {
            User user = User.createSystemInternal();
            user.setRole( role );
            return user;
        }
        
        User user = Main.get_control().getUsermanager().getUser( mountEntry.getUsername() );
        return user;
    }
    
    void updateMountEntry(MountEntry mountEntry) throws SQLException
    {
        GenericEntityManager em = Main.get_control().get_util_em( mountEntry.getPool() );
        em.check_open_transaction();
        em.em_merge( mountEntry );
        em.commit_transaction();
    }

    boolean isOnline( String agentIp, int agentPort )
    {
        boolean ret = false;
        AgentApiEntry apiEntry = null;
        try
        {
            apiEntry = LogicControl.getApiEntry(agentIp, agentPort, /*withMsg*/ false);
            ret = apiEntry.isOnline();
        }
        catch (UnknownHostException unknownHostException)
        {
        }
        finally
        {
            if (apiEntry != null)
            {
                try
                {
                    apiEntry.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        return ret;
    }

    void mountAllUnMounted()
    {
        List<MountEntry> mountEntries = getMountEntryList();
        Map<String,MountEntry> mountEntriesMap = new HashMap<>();
        for (MountEntry mountEntry : mountEntries)
        {
            mountEntriesMap.put( mountEntry.getKey(), mountEntry);
        }
        
        for (MountEntry mountEntry : mountEntries)
        {
            try
            {
                if (isOnline( mountEntry.getIp(), mountEntry.getPort()))
                {
                    User user = getUser( mountEntry );
                    if (user == null)
                        throw new Exception(Main.Txt( "User kann nicht aufgelöst werden") + ": " + mountEntry.getUsername() );

                    Role role = user.getRole();
                    GuiServerApiImpl guiServerApi = new GuiServerApiImpl( System.currentTimeMillis(), role, user );
                    StoragePool pool = mountEntry.getPool();

                    StoragePoolHandlerContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getContextManager();
                    StoragePoolWrapper wrapper = guiServerApi.getMounted( mountEntry.getIp(), mountEntry.getPort(), pool );

                    if (wrapper == null)
                    {
                        if ( mountEntry.isAutoMount() && !mountEntry.isDisabled())
                        {
                            mountEntry( user, guiServerApi, mountEntry);
                        }
                    }
                    else
                    {
                        if ( mountEntry.isDisabled())
                        {
                            Log.debug( "AutoMountManager", Main.Txt("Mountentrag ist disabled, wird entfernt") + ": " + mountEntry.toString() );
                            unMountEntry( guiServerApi, mountEntry);
                        }
                    }
                
                    // ADD ALL FOUND MAPPED VALUES
                    List<StoragePoolWrapper> actWrappers = contextMgr.getPoolWrappers( mountEntry.getIp(), mountEntry.getPort(), pool );
                    for (StoragePoolWrapper wr : actWrappers)
                    {
                        if (wr.getMountEntryKey() == null)
                            continue;
                        MountEntry me = mountEntriesMap.get( wr.getMountEntryKey() );
                        if (me == null)
                            continue;

                        if (!mountList.contains( me))
                        {
                            // Touch it to prevent unload during inactivity
                            contextMgr.touch(wr);
                            
                            mountList.add( me );
                        }
                    }
                }
                else
                {
                    if (mountList.contains(mountEntry))
                    {
                        Log.debug( "AutoMountManager", Main.Txt("Mountentrag ist offline, wird entfernt") + ": " + mountEntry.toString() );
                        mountList.remove( mountEntry );
                    }
                }
            }
            catch (Exception ex)
            {
                Log.err( "AutoMountManager", ex.getMessage(), ex );
            }
        }
    }
    public void unMountEntry( GuiServerApi guiServerApi, MountEntry mountEntry)
    {
        StoragePoolWrapper wrapper = guiServerApi.getMounted( mountEntry.getIp(), mountEntry.getPort(), mountEntry.getPool() );
        if (wrapper != null)
        {
            guiServerApi.unmountVolume( wrapper );  
        }
        if (mountList.contains( mountEntry))
        {
            mountList.remove( mountEntry );
        }
    }    
    public StoragePoolWrapper mountEntry( User user, GuiServerApi guiServerApi, MountEntry mountEntry) throws IOException
    {
        StoragePoolHandlerContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getContextManager();
        long timestamp = -1;
        boolean rdOnly = mountEntry.isReadOnly();
        boolean showDeleted = mountEntry.isShowDeleted();

        StoragePoolQry qry = (rdOnly) ? StoragePoolQry.createActualRdOnlyStoragePoolQry(user, showDeleted) : 
                                        StoragePoolQry.createActualRdWrStoragePoolQry(user, showDeleted);
        StoragePoolWrapper wrapper = contextMgr.createPoolWrapper( mountEntry.getIp(), mountEntry.getPort(), mountEntry.getPool(), qry, mountEntry.getSubPath(), mountEntry.getMountPath().getPath() );
        wrapper.setCloseOnUnmount( true );
        wrapper.setMountEntry( mountEntry );
        guiServerApi.mountVolume( mountEntry.getIp(), mountEntry.getPort(), wrapper, mountEntry.getMountPath().getPath() );
       

        Log.debug( "AutoMountManager", Main.Txt("Mounteintrag wird gemountet") + ": " + mountEntry.toString() );

        if (!mountList.contains( mountEntry))
        {
            mountList.add( mountEntry );
        }                                   
        return wrapper;
    }
        
    public List<MountEntry> getMountList()
    {
        return mountList;
    }    
    public static boolean isSame(StoragePoolWrapper ticket, MountEntry mountEntry)
    {
        if (mountEntry.getPool().getIdx() != ticket.getPoolIdx())
                return false;;
            
            if (!mountEntry.getIp().equals( ticket.getAgentIp()))
               return false;
            
            if (!mountEntry.getSubPath().equals( ticket.getBasePath()))
                return false;
        return true;
    }
    
    public MountEntry getMountEntry(StoragePoolWrapper ticket)
    {        
        for (MountEntry mountEntry : mountList)
        {
            if (isSame(ticket, mountEntry))            
                return mountEntry;
        }
        Log.err(Main.Txt("Kein Mounteintrag zu ticket gefunden") + ": " + ticket.toString() );
        return null;
    }    

    void unmountAllMounted()
    {
        List<MountEntry> mountEntries = getMountEntryList();
        for (MountEntry mountEntry : mountEntries)
        {
            try
            {
                User user = getUser( mountEntry );
                Role role = user.getRole();
                GuiServerApiImpl guiServerApi = new GuiServerApiImpl( System.currentTimeMillis(), role, user );
                StoragePool pool = mountEntry.getPool();

                StoragePoolHandlerContextManager contextMgr = Main.get_control().getPoolHandlerServlet().getContextManager();
                StoragePoolWrapper wrapper = guiServerApi.getMounted( mountEntry.getIp(), mountEntry.getPort(), pool );
                if (wrapper != null)
                {
                    guiServerApi.unmountVolume( wrapper );
                }
            }
            catch (Exception ex)
            {
                Logger.getLogger( AutoMountManager.class.getName() ).log( Level.SEVERE, null, ex );
            }
        }

    }

    @Override
    public int getCycleSecs()
    {
        return 10;
    }

    @Override
    public void startIdle()
    {
    }

    
    @Override
    public void stopIdle()
    {
        // STOP ON SHUTDOWN
        Log.debug("AutoMountManager is stopping");
        unmountAllMounted();
        Log.debug("AutoMountManager is stopped");
    }

    // IS HANDLED INSIDE AgentIdleManager
    @Override
    public void run()
    {
        is_started = true;
        while (!isShutdown())
        {
            LogicControl.sleep( 1 * 1000 );

        }
        finished = true;
    }

    @Override
    public void doIdle()
    {
        // START ALL PENDING ENTRIES
        mountAllUnMounted();
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }
}
