/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup.hotfolder;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.records.ArchiveJob;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public interface ArchiveJobNameGenerator
{
    String createName();
    String getBasePath();
    ArchiveJob createJob() throws SQLException, IOException, PoolReadOnlyException, PathResolveException;
}