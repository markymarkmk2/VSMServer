/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.checks;

/**
 *
 * @author mw
 */
public class CheckFSIntegrityWithStorage extends CheckFSIntegrity{

    @Override
    protected boolean isExistanceCheckEnabled() {
        return true;
    }
    
    
    
}
