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
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HotFolder;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Administrator
 */
public class DirectoryArchiveJobNameGenerator implements ArchiveJobNameGenerator
{
    HotFolder hf;
    RemoteFSElem remoteFSElem;
    AgentApiEntry apiEntry;
    StoragePoolHandler poolhandler;
    Date actTimeStamp;

    public DirectoryArchiveJobNameGenerator( HotFolder hf, RemoteFSElem remoteFSElem, AgentApiEntry apiEntry, StoragePoolHandler poolhandler )
    {
        this.hf = hf;
        this.remoteFSElem = remoteFSElem;
        this.apiEntry = apiEntry;
        this.poolhandler = poolhandler;
        actTimeStamp = new Date();
    }

    @Override
    public String createName()
    {
        String jobName = remoteFSElem.getName();
        return jobName;
    }

    @Override
    public String getBasePath()
    {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy-HH:mm:ss");
        String dateStamp = null;
        if (hf.isCreateDateSubdir())
        {
            dateStamp = sdf.format(actTimeStamp);
        }

        String jobName = remoteFSElem.getName();
        String path = null;

        if (hf.getBasePath() == null || hf.getBasePath().length() == 0)
        {
            path = "/" + HotFolder.HOTFOLDERBASE + GenericContext.getClientInfoRootPath(apiEntry.getAddr(), apiEntry.getPort()) + "/" + jobName;
        }
        else
        {
            path = hf.getBasePath() + "/" + jobName;
        }

        if (dateStamp != null)
            path += "/" + dateStamp;

        return path;
    }

    @Override
    public ArchiveJob createJob() throws SQLException, IOException, PoolReadOnlyException, PathResolveException
    {        
        ArchiveJob job = new ArchiveJob();

        String basePath = getBasePath();

        FileSystemElemNode startDir = poolhandler.resolve_elem_by_path(basePath);

        if (startDir == null)
        {
            startDir = poolhandler.create_fse_node(basePath, remoteFSElem, System.currentTimeMillis());
            if (hf.isCreateDateSubdir())
                startDir.setFlag(FileSystemElemNode.FL_DATEDIR);
            
            poolhandler.register_fse_node_to_db(startDir);
        }

        job.setDirectory( startDir );
        job.setStartTime( actTimeStamp );
        job.setLastAccess(job.getStartTime());
        job.setSourceType(ArchiveJob.AJ_SOURCE_HF);
        job.setSourceIdx(hf.getIdx());
        job.setName( createName() );

        poolhandler.em_persist(job);

        return job;
    }
}
