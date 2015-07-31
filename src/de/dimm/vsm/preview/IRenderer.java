/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import java.io.File;
import java.io.InputStream;

/**
 *
 * @author Administrator
 */
public interface IRenderer {
    
    public void render( String suffix, InputStream fis, File outFile ) throws Exception;

    public boolean supportsSuffix(String suffix);
    
}
