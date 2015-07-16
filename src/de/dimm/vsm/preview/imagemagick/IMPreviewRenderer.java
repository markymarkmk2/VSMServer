/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.preview.IRenderer;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;

/**
 *
 * @author Administrator
 */
public class IMPreviewRenderer implements IRenderer {

    ConvertCmd cmd;
    IMOperation streamOp;
        
    static final Set<String> supportedSuffixes = new HashSet<>();
    static {
        System.setProperty("im4java.useGM", "true" );
        supportedSuffixes.addAll(Arrays.asList(
           "bmp", "dcm", "epdf", "eps", "fax", "gif", "ico", "jng", "jpeg", "jpg", 
           "pct", "pcx", "pict", "png", "ps", "svg", "svgz", "tga", "tiff", "tif",
           "wmf"
                ));
    }
   
    public IMPreviewRenderer( String imPath, int width, int height) {
        
        System.setProperty("im4java.useGM", "true" );
        cmd = new ConvertCmd();        
        cmd.setSearchPath(imPath);
        
        // create the operation, add images and operators/options
        streamOp = new IMOperation();
        streamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
        streamOp.addImage("-"); // Input from Stream
//        streamOp.thumbnail(640, 480);
        //op.resize(640, 480);
        streamOp.scale(width, height);
        streamOp.gravity("center");
        streamOp.extent(width, height);
        streamOp.addImage();
    }
    
    static boolean _pdf = true;

    @Override
    public void render( String suffix, InputStream fis, File outFile ) throws Exception {
        Pipe pipeIn  = new Pipe(fis,null);

        // set up command
        ConvertCmd convert = new ConvertCmd();
        convert.setInputProvider(pipeIn);

        convert.run(streamOp, outFile.getAbsolutePath());

        fis.close();
    }

   
    @Override
    public boolean supportsSuffix( String suffix ) {
        return supportedSuffixes.contains(suffix.toLowerCase());
    }
}
