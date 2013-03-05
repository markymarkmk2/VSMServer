/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.Utilities.VariableResolver;
import de.dimm.vsm.backup.Backup.BackupCDPTicket;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ClientInfo;
import de.dimm.vsm.records.ClientVolume;
import de.dimm.vsm.records.Excludes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Administrator
 */
public class BackupContext extends GenericContext implements VariableResolver
{
    fr.cryptohash.Digest digest;

    // NEEDED FOR STATISTICS AND MESSAGES ONLY
    private ClientInfo actClientInfo;
    private ClientVolume clientVolume;

    final List<BackupCDPTicket> cdpList;
    AtomicBoolean cdpMtx = new AtomicBoolean();
   

    public BackupContext(  AgentApiEntry apiEntry, StoragePoolHandler poolhandler, ClientInfo info, ClientVolume vol )
    {
        super( apiEntry, poolhandler);
        actClientInfo = info;
        clientVolume = vol;
        digest = new fr.cryptohash.SHA1();
        cdpList = new ArrayList<>();
    }

    @Override
    public List<Excludes> getExcludes()
    {
        if (actClientInfo != null)
            return actClientInfo.getExclList();
        return null;
    }

 
    @Override
    public String getRemoteElemAbsPath(RemoteFSElem remoteFSElem )
    {
        return  basePath + "/" + StoragePoolHandler.resolve_real_child_path(remoteFSElem);
    }
    
    public String getRemoteElemAbsPath( String remotePath, char seperator )
    {
        return  basePath + "/" + StoragePoolHandler.resolve_real_child_path(remotePath, seperator);
    }

    public ClientInfo getActClientInfo()
    {
        return actClientInfo;
    }
    public ClientVolume getActVolume()
    {
        return clientVolume;
    }

 @Override
    public String resolveVariableText( String s )
    {
        if (s.indexOf("$NAME") >= 0)
        {
            String f = "";
            if (actClientInfo != null)
                f = actClientInfo.getSched().getName();

            s = s.replace("$NAME", f );
        }

        if (s.indexOf("$POOL") >= 0)
        {
            String f = "";
            if (actClientInfo != null)
                f = actClientInfo.getSched().getPool().getName();

            s = s.replace("$POOL", f );
        }
        if (s.indexOf("$VOLUME") >= 0)
        {
            String f = "";
            if (clientVolume != null)
                f = clientVolume.getVolumePath().getPath();

            s = s.replace("$VOLUME", f );
        }
        if (s.indexOf("$AGENT") >= 0)
        {
            String f = "";
            if (actClientInfo != null)
                f = actClientInfo.getIp();

            s = s.replace("$AGENT", f );
        }
        return s;
    }

    @Override
    public boolean isCompressed()
    {
        if (actClientInfo != null)
            return actClientInfo.getCompression();

        return false;
    }

    @Override
    public boolean isEncrypted()
    {
        if (actClientInfo != null)
            return actClientInfo.isEncryption();

        return false;
    }

    // FIFO LISTE MIT CDP-TICKETS
    public void addCDPTicket( BackupCDPTicket cdpTticket )
    {
        synchronized(cdpList)
        {
            cdpList.add(cdpTticket);
        }
    }
    public BackupCDPTicket getCDPTicket()
    {
        synchronized(cdpList)
        {
            if (cdpList.isEmpty())
                return null;
            return cdpList.remove(0);
        }
    }

    public boolean getCDPMutex()
    {
        return cdpMtx.compareAndSet(false, true);
    }

    public void releaseCDPMutex()
    {
        cdpMtx.set(false);
    }
}


