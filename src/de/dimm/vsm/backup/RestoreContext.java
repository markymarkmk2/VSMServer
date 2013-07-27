/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolQry;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class RestoreContext extends GenericContext
{


    StoragePoolQry qry;
    RemoteFSElem target;
    int flags;
    Properties agentProperties;

    /**
     * Keeps together all data needed for identifing context of ClientCalls
     * <code>resourceUri</code>.
     *
     * @param poolhandler
     *
     * @param sched
     *
     * @param clientInfo
     *
     * @param clientVolume
     *
     */
    public RestoreContext(  StoragePoolHandler poolhandler, StoragePoolQry qry, AgentApiEntry apiEntry, RemoteFSElem target, int flags, Properties p ) throws IOException
    {
        super( apiEntry, poolhandler);

        // CREATE A DUPLICATE OF THIS POOLHANDLER
        this.poolhandler = StoragePoolHandlerFactory.createStoragePoolHandler(poolhandler.getPool(), qry);

        this.qry = qry;
        this.target = target;
        this.flags = flags;
        agentProperties = p;
    }

    boolean isRecursive()
    {
        return (flags & GuiServerApi.RF_RECURSIVE) != 0;
    }

    boolean useFullPath()
    {
        return (flags & GuiServerApi.RF_FULLPATH) != 0;
    }
    boolean skipHotfolderTimestampDir()
    {
        return (flags & GuiServerApi.RF_SKIPHOTFOLDER_TIMSTAMPDIR) != 0;
    }

    public Properties getAgentProperties()
    {
        return agentProperties;
    }

    

    @Override
    public String getRemoteElemAbsPath( RemoteFSElem remoteFSElem ) throws PathResolveException
    {
        return null;
    }

    public RemoteFSElem getTarget()
    {
        return target;
    }

    boolean isInkrementalRestore()
    {
        return (flags & GuiServerApi.RF_INCREMENTAL) != 0;
    }

    boolean isWinAgent()
    {
        return agentProperties.getProperty("os.name").startsWith("Win");
    }

    @Override
    public void close()
    {
        super.close();

        if (poolhandler != null)
        {
            poolhandler.close_entitymanager();
            poolhandler = null;
        }
    }

    @Override
    public boolean isEncrypted()
    {
        return (flags & GuiServerApi.RF_ENCRYPTION) != 0;
    }

    @Override
    public boolean isCompressed()
    {
        return (flags & GuiServerApi.RF_COMPRESSION) != 0;
    }

    
}