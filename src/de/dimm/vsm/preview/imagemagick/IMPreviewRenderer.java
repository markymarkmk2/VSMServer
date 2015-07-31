/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import static de.dimm.vsm.Main.get_prop;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;

/**
 *
 * @author Administrator
 */
public class IMPreviewRenderer extends AbstractRenderer {

    //ConvertCmd cmd;
    //IMOperation streamOp;
    
    
    public IMPreviewRenderer( int width, int height) {
                
        super(width, height);
        
        String[] suffixes = get_prop(GeneralPreferences.IM_SUFFIXES, "").split("[ ,]");
        if (suffixes.length > 1) {
            setSupportedSuffixes(suffixes);
        }
        else  {
            setSupportedSuffixes(Arrays.asList( "psd" ));
        }
        
        this.width = width;
        this.height = height;
        
//        cmd = new ConvertCmd(false);        
//        cmd.setSearchPath(imPath);
//        
//        // create the operation, add images and operators/options
//        streamOp = new IMOperation();
//        streamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
//        streamOp.addImage("-"); // Input from Stream
//        
//        
//        String[] im_opts = Main.get_prop(GeneralPreferences.IM_CMD_OPTS, "").split(" ");
//        if (im_opts.length > 1) {
//            streamOp.addRawArgs(im_opts);
//        } else {
//            streamOp.addRawArgs("-flatten");
//        }
//        
//        streamOp.scale(width, height);
//        streamOp.gravity("center");
//        streamOp.extent(width, height);
//        streamOp.addImage();
    }
    
    static boolean _pdf = true;

    @Override
    public void render( String suffix, InputStream fis, File outFile ) throws Exception {
        
// create the operation, add images and operators/options
        IMOperation streamOp = new IMOperation();
        //streamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
        streamOp.addImage("-"); // Input from Stream
//        streamOp.thumbnail(640, 480);
        //op.resize(640, 480);
        
        
        String[] im_opts = Main.get_prop(GeneralPreferences.IM_CMD_OPTS, "").split(" ");
        if (im_opts.length > 1) {
            streamOp.addRawArgs(im_opts);
        } else {
            streamOp.addRawArgs("-flatten");
        }
        
        streamOp.scale(width, height);
        streamOp.gravity("center");
        streamOp.extent(width, height);
        
        streamOp.addImage();        
        Pipe pipeIn  = new Pipe(fis,null);

        // set up command
        String imPath = Main.get_prop(GeneralPreferences.PREVIEW_IMPATH);        
        ConvertCmd convert = new ConvertCmd(false);
        convert.setSearchPath(imPath);
        convert.setInputProvider(pipeIn);

        convert.run(streamOp, outFile.getAbsolutePath());

        fis.close();
    }
}
