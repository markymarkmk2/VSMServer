/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

/**
 *
 * @author mw
 */
public interface IAgentIdleManagerEntry {
    void startIdle();
    void stopIdle();
    void doIdle() ; 
    int getCycleSecs();
    void setStatusTxt(String statusTxt);
    String getStatusTxt();
    
}
