/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.servlets.AgentApiEntry;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.records.ArchiveJob;
import de.dimm.vsm.records.HotFolder;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public class MMArchivJobGenerator  implements ArchiveJobNameGenerator
{
    String jobName;
    long jobIdx;
    StoragePoolHandler poolhandler;
    RemoteFSElem remoteFSElem;

    private MMArchivJobGenerator( HotFolder hf, String jobName, long jobIdx, RemoteFSElem remoteFSElem, AgentApiEntry apiEntry, StoragePoolHandler poolhandler )
    {
        this.poolhandler = poolhandler;
        this.jobName = jobName;
        this.jobIdx = jobIdx;
        this.remoteFSElem = remoteFSElem;
    }

    @Override
    public String createName()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getBasePath()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ArchiveJob createJob() throws SQLException, IOException, PoolReadOnlyException, PathResolveException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

//    @Override
//    public String createName()
//    {
//        return jobName;
//    }
//
//    @Override
//    public ArchiveJob createJob() throws SQLException, IOException, PoolReadOnlyException, PathResolveException
//    {
//        ArchiveJob job = new ArchiveJob();
//
////        String basePath = getBasePath();
////
////        FileSystemElemNode startDir = poolhandler.resolve_elem_by_path(basePath);
////
////        if (startDir == null)
////        {
////            startDir = poolhandler.create_fse_node(basePath, remoteFSElem, System.currentTimeMillis());
////            poolhandler.register_fse_node_to_db(startDir);
////        }
////
////        job.setDirectory( startDir );
////        job.setStartTime( new Date() );
////        job.setLastAccess(job.getStartTime());
////        job.setSourceType(ArchiveJob.AJ_SOURCE_MM);
////        job.setSourceIdx(jobIdx);
////        job.setName( createName() );
////
////        poolhandler.em_persist(job);
//
//        return job;
//    }
//
//    @Override
//    public String getBasePath()
//    {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }

}
