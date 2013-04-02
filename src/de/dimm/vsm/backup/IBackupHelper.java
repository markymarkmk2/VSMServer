/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.Utilities.StatCounter;
import de.dimm.vsm.fsengine.HashCache;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public interface IBackupHelper {

    StoragePoolHandler getPoolHandler();
    StatCounter getStat();
    HashCache getHashCache();
    void flushWriteRunner() throws IOException, InterruptedException;
}
