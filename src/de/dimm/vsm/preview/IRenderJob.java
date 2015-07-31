/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

/**
 *
 * @author Administrator
 */
public interface IRenderJob {

    void render();
    void setAbort( boolean abort );
    public boolean isAbort();    
}
