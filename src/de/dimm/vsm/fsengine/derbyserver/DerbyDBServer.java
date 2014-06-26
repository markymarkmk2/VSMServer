/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.derbyserver;

/**
 *
 * @author Administrator
 */
public interface DerbyDBServer
{
    void start();
    void stop();
    boolean isSame( String server, int port );    
}