/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolQry;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class RestoreContext extends GenericContext
{

    public static final int RF_RECURSIVE = 0x0001;
    public static final int RF_FULLPATH = 0x0002;
    public static final int RF_SKIPHOTFOLDER_TIMSTAMPDIR = 0x0004;
    public static final int RF_INCREMENTAL = 0x0008;

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
        return (flags & RF_RECURSIVE) != 0;
    }

    boolean useFullPath()
    {
        return (flags & RF_FULLPATH) != 0;
    }
    boolean skipHotfolderTimestampDir()
    {
        return (flags & RF_SKIPHOTFOLDER_TIMSTAMPDIR) != 0;
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
        return (flags & RF_INCREMENTAL) != 0;
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

    
}