/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

/**
 *
 * @author mw
 */
public interface IAgentIdleManager {
    void startIdle();
    void stopIdle();
    void doIdle() ; 
    int getCycleSecs();
    
}
