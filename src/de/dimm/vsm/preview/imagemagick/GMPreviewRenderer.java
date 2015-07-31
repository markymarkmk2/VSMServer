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
public class GMPreviewRenderer extends  AbstractRenderer {

    //ConvertCmd cmd;
    //IMOperation streamOp;
           
    public GMPreviewRenderer( int width, int height) {
        
        super(width, height);
        
        //System.setProperty("im4java.useGM", "true" );
        String[] suffixes = get_prop(GeneralPreferences.GM_SUFFIXES, "").split("[ ,]");
        if (suffixes.length > 1) {
            setSupportedSuffixes(suffixes);
        }
        else  {
            setSupportedSuffixes(Arrays.asList(
            "bmp", "dcm", "epdf", "eps", "fax", "gif", "ico", "jng", "jpeg", "jpg", 
            "pct", "pcx", "pict", "png", "ps", "svg", "svgz", "tga", "tiff", "tif",
            "wmf"));
        }
        
//        cmd = new ConvertCmd(true);        
//        cmd.setSearchPath(imPath);
        
    }
    
    static boolean _pdf = true;

    @Override
    public void render( String suffix, InputStream fis, File outFile ) throws Exception {
        Pipe pipeIn  = new Pipe(fis,null);
        
        // create the operation, add images and operators/options
        IMOperation streamOp = new IMOperation();
        streamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
        streamOp.addImage("-"); // Input from Stream
        
        String[] gm_opts = Main.get_prop(GeneralPreferences.GM_CMD_OPTS, "").split(" ");
        if (gm_opts.length > 1) {
            streamOp.addRawArgs(gm_opts);
        }
        
        streamOp.scale(width, height);
        streamOp.gravity("center");
        streamOp.extent(width, height);
        streamOp.addImage();
        

        // set up command
        ConvertCmd convert = new ConvertCmd(true);
        String gmPath = Main.get_prop(GeneralPreferences.PREVIEW_GMPATH);
        convert.setSearchPath(gmPath);
        convert.setInputProvider(pipeIn);

        convert.run(streamOp, outFile.getAbsolutePath());

        fis.close();
    }

  
}
