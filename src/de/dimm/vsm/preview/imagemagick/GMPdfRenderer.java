/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.imagemagick;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
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
public class GMPdfRenderer extends AbstractRenderer {
   
    public GMPdfRenderer(  int width, int height ) {        
       super(width, height);       
       setSupportedSuffixes(Arrays.asList("pdf"));
    }
    
    @Override
    public void render( String suffix, InputStream fis, File outFile ) throws Exception {
                 
        Pipe pipeIn  = new Pipe(fis,null);
        
         // create the operation, add images and operators/options
        IMOperation pdfStreamOp = new IMOperation();
        pdfStreamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
        pdfStreamOp.addImage("pdf:-[0]"); // Input from Stream Page 1 only
        pdfStreamOp.scale(width, height);
        pdfStreamOp.gravity("center");
        pdfStreamOp.extent(width, height);
        pdfStreamOp.addImage();

        // set up command
        ConvertCmd convert = new ConvertCmd(true);
        String gmPath = Main.get_prop(GeneralPreferences.PREVIEW_GMPATH);
        convert.setSearchPath(gmPath);
        
        convert.setInputProvider(pipeIn);

        convert.run(pdfStreamOp, outFile.getAbsolutePath());

        fis.close();                                       
    }
}
