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
public class IMPdfRenderer implements IRenderer {

    ConvertCmd cmd;
    IMOperation pdfStreamOp;
    
    static final Set<String> supportedSuffixes = new HashSet<>();
    static {
        System.setProperty("im4java.useGM", "true" );
        supportedSuffixes.addAll(Arrays.asList(
           "pdf"
                ));
    }
   
    public IMPdfRenderer( String gmPath, int width, int height ) {
        
        System.setProperty("im4java.useGM", "true" );
        cmd = new ConvertCmd();        
        cmd.setSearchPath(gmPath);
        
        // create the operation, add images and operators/options
        pdfStreamOp = new IMOperation();
        pdfStreamOp.addRawArgs("-limit", "memory", "8192" );  // Max 8GB Ram
        pdfStreamOp.addImage("pdf:-[0]"); // Input from Stream Page 1 only
        pdfStreamOp.scale(width, height);
        pdfStreamOp.gravity("center");
        pdfStreamOp.extent(width, height);
        pdfStreamOp.addImage();
    }
    
    @Override
    public void render( String suffix, InputStream fis, File outFile ) throws Exception {
                 
        Pipe pipeIn  = new Pipe(fis,null);

        // set up command
        ConvertCmd convert = new ConvertCmd();
        convert.setInputProvider(pipeIn);

        convert.run(pdfStreamOp, outFile.getAbsolutePath());

        fis.close();                                       
    }

   
    @Override
    public boolean supportsSuffix( String suffix ) {
        return supportedSuffixes.contains(suffix.toLowerCase());
    }
}
