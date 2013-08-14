/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import java.io.File;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author Administrator
 */
public class DDEntriesDir_BootstrapHandle extends FS_BootstrapHandle<DedupHashBlock>
{

    public DDEntriesDir_BootstrapHandle( AbstractStorageNode fs_node, DedupHashBlock block ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(block, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    @Override
    public boolean delete()
    {
        File entriesDir = fh;
        if (entriesDir.exists())
        {
            File[] entries = entriesDir.listFiles();
            if (entries.length > 0)
            {
                Log.err("Found orphaned entries in bootstrap dir " + entriesDir.getAbsolutePath());
                for (int j = 0; j < entries.length; j++)
                {
                    File file = entries[j];
                    file.delete();
                }
            }
            entriesDir.delete();
        }
        return true;
    }
}
