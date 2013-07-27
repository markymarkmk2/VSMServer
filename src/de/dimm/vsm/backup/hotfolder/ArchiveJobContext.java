/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.backup.GenericContext;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.HotFolder;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class ArchiveJobContext extends GenericContext
{    
    String relPath;
    ArchiveJobNameGenerator nameGen;
    HotFolder hotfolder;

    public void setRelPath( String relPath )
    {
        this.relPath = relPath;
    }

    public void setNameGen( ArchiveJobNameGenerator nameGen )
    {
        this.nameGen = nameGen;
        basePath = nameGen.getBasePath();
    }

    public HotFolder getHotfolder()
    {
        return hotfolder;
    }
    
    

    public ArchiveJobContext( HotFolder hf, RemoteFSElem remoteFSElem, AgentApiEntry apiEntry, StoragePoolHandler poolhandler )
    {
        super(apiEntry, poolhandler);

        // WE DO NOT ALLOW ERRORS
        abortOnError = true;

        hotfolder = hf;

        nameGen = new DirectoryArchiveJobNameGenerator(hf, remoteFSElem, apiEntry, poolhandler);

        basePath = nameGen.getBasePath();

        relPath = remoteFSElem.getPath();
    }
    public ArchiveJobContext( String relPath, ArchiveJobNameGenerator nameGen, AgentApiEntry apiEntry, StoragePoolHandler poolhandler )
    {
        super(apiEntry, poolhandler);

        // WE DO NOT ALLOW ERRORS
        abortOnError = true;

        hotfolder = null;

        this.nameGen = nameGen;

        if (nameGen != null)
        {
            basePath = nameGen.getBasePath();
        }

        this.relPath = relPath;
    }

    @Override
    public String getRemoteElemAbsPath( RemoteFSElem remoteFSElem ) throws PathResolveException
    {

        if (remoteFSElem.getPath().length() < relPath.length())
        {
            throw new PathResolveException("Hotfolder path does not match: " + remoteFSElem.getPath() + " : " + relPath);
        }
        if (remoteFSElem.getPath().length() == relPath.length())
            return basePath;

        String _relPath = remoteFSElem.getPath().substring(this.relPath.length() + 1);

        if (remoteFSElem.getSeparatorChar() != '/')
        {
            _relPath = _relPath.replace( remoteFSElem.getSeparatorChar(), '/' );
        }


        return basePath + "/" + _relPath;
    }

    @Override
    public String getBasePath()
    {
        return nameGen.getBasePath();
    }


    boolean isErrorFree()
    {
        return getErrList().isEmpty();
    }

    void createJob() throws SQLException, PoolReadOnlyException, PathResolveException, IOException
    {
        archiveJob = nameGen.createJob();
    }


    void updateArchiveJob() throws SQLException
    {
        try
        {
            poolhandler.em_merge(archiveJob);
        }
        catch (SQLException sQLException)
        {
            Log.err("Cannot save ArchiveJob result", sQLException);
        }
    }

    @Override
    public StoragePoolHandler getPoolhandler()
    {
        return poolhandler;
    }

    @Override
    public boolean isCompressed()
    {
        return hotfolder.isHfcompression();
    }

    @Override
    public boolean isEncrypted()
    {
        return hotfolder.isHfencryption();
    }

}