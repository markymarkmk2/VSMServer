/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.Main;
import de.dimm.vsm.net.StoragePoolWrapper;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public class VSMFSInputStream extends VSMInputStream
{

    StoragePoolWrapper wrapper;

    public VSMFSInputStream( StoragePoolHandler sp, FileSystemElemNode node )
    {
        this(sp, node, null);
    }
    
    public VSMFSInputStream( StoragePoolHandler sp, FileSystemElemNode node, FileSystemElemAttributes attrs )
    {
        super(sp, node, attrs);     
        wrapper = Main.get_control().getPoolHandlerServlet().getContextManager().createPoolWrapper(sp, "", 0, "");

    }
    public VSMFSInputStream( StoragePoolHandler sp, long idx, long attrIdx  )
    {
        super(sp, idx, attrIdx);
        wrapper = Main.get_control().getPoolHandlerServlet().getContextManager().createPoolWrapper(sp, "", 0, "");
    }


    @Override
    public void close() throws IOException
    {
        if (fileNo >= 0)
        {
            Main.get_control().getPoolHandlerServlet().close_fh(wrapper, fileNo);
        }
        Main.get_control().getPoolHandlerServlet().getContextManager().removePoolWrapper(wrapper);
    }

}
