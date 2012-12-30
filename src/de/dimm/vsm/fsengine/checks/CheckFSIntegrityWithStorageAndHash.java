/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.checks;

/**
 *
 * @author mw
 */
public class CheckFSIntegrityWithStorageAndHash extends CheckFSIntegrity{

    @Override
    protected boolean isExistanceCheckEnabled() {
        return true;
    }

    @Override
    protected boolean isHashCheckEnabled() {
        return true;
    }
    
    
    
}
