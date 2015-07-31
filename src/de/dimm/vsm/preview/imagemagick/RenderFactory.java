/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.preview.IRenderer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class RenderFactory {
    
    static List<IRenderer> renderers;

    public static void init() {
        int width =  Main.get_int_prop(GeneralPreferences.PREVIEW_WIDTH, 640); 
        int height = Main.get_int_prop(GeneralPreferences.PREVIEW_HEIGHT, 480);
        
        
        renderers = new ArrayList<>();
        renderers.add( new GMPreviewRenderer( width, height));
        renderers.add( new IMPreviewRenderer( width, height));
        renderers.add( new GMPdfRenderer( width, height));
    }
    
    
    public static List<IRenderer> getRenderers() {        
        return renderers;
    }
    
    public static IRenderer getRenderer( String suffix) {
        IRenderer lastFoundRenderer = null;
        for (IRenderer iRenderer : renderers) {
            if (iRenderer.supportsSuffix(suffix)) {
                lastFoundRenderer = iRenderer; 
            }            
        }
        return lastFoundRenderer;        
    }
    
}
