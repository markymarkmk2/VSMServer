/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.preview.IRenderer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Administrator
 */
public abstract class AbstractRenderer implements IRenderer {
    private final Set<String> supportedSuffixes = new HashSet<>();
    
    protected int width; 
    protected int height;

    public AbstractRenderer( int width, int height ) {
        this.width = width;
        this.height = height;
    }
    
    

    public void setSupportedSuffixes( String[] args ) {
        supportedSuffixes.clear();
        for (String string : args) {
            supportedSuffixes.add(string.toLowerCase());
        }
    }
    public void setSupportedSuffixes( List<String> args ) {
        supportedSuffixes.clear();
        for (String string : args) {
            supportedSuffixes.add(string.toLowerCase());
        }
    }

    
    @Override
    public boolean supportsSuffix( String suffix ) {
        return supportedSuffixes.contains(suffix.toLowerCase());
    }
    
}
