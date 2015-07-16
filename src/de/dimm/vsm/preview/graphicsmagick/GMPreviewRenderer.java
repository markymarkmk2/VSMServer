/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview.graphicsmagick;

import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.preview.IPreviewRenderer;
import de.dimm.vsm.records.FileSystemElemNode;
import java.io.File;
import java.io.IOException;
//import java.util.List;
//import org.gm4java.engine.support.GMConnectionPoolConfig;
//import org.gm4java.engine.support.PooledGMService;
//import org.gm4java.im4java.GMBatchCommand;
//import org.gm4java.im4java.GMOperation;
//
///**
// *
// * @author Administrator
// */
//public class GMPreviewRenderer implements IPreviewRenderer {
//
//    GMConnectionPoolConfig config;
//    PooledGMService service;
//    GMOperation operation;
//    StoragePoolHandler sp_handler;
//    
//    public GMPreviewRenderer(StoragePoolHandler sp_handler ) {
//        this.sp_handler = sp_handler;
//        
//        System.setProperty("im4java.useGM", "true" );
//        
//        String gmPath="F:\\Program Files\\GraphicsMagick-1.3.20-Q16\\gm.exe";
//        
//        config = new GMConnectionPoolConfig();
//        config.setGMPath(gmPath);
//        
//        service = new PooledGMService(config);
//
//        
//        // create the operation, add images and operators/options
//        operation = new GMOperation();
//        
//        operation.addImage();
////        op.thumbnail(640, 480);
//        //operation.size(640, 480);
//        operation.scale(640, 480);
//        operation.gravity("center");
//        operation.addRawArgs("-extent", "640x480");
//        operation.addImage();
//        
//    }
//    
//    private String[] mergeFiles(GMOperation operation, File... files ) {
//        List<String> result = operation.getCmdArgs();
//        
//        for (File file: files) {
//            for(int i = 0; i < result.size(); i++) {
//                if (result.get(i).equals(GMOperation.IMG_PLACEHOLDER)) {
//                    result.set(i, file.getAbsolutePath());
//                    break;
//                }
//            }
//        }        
//        
//        return result.toArray(new String[0]);
//    }
//    
//
//    @Override
//    public File renderPreviewFile( FileSystemElemNode node ) throws IOException {
//        final String[] comands = new String[] {
//                
//                "-limit", "threads", "16",
//                "-size", "640x480", "D:\\Bilder\\Terrassentür.png",
//                "-scale", "640x480",
//                "-gravity", "center",
//                "-extent", "640x480",
//               "D:\\Bilder\\Terrassentür_previewe_eckig.png"};
//                        
//        try {
//            File f1 = new File("D:\\Bilder\\Terrassentür.png");
//            File f2 = new File("D:\\Bilder\\Terrassentür_previewe_eckig.png");
//            //operation.set("D:\\Bilder\\Terrassentür.png", "D:\\Bilder\\Terrassentür_previewe_eckig.png");
//            GMBatchCommand command = new GMBatchCommand(service, "convert");
//            command.run(operation, "D:\\Bilder\\Terrassentür.png", "D:\\Bilder\\Terrassentür_previewscale_eckig.png");
//            //service.execute("convert", mergeFiles(operation, f1, f2) );
////            service.execute("convert", comands);
//            return f2;
//            
//        }
//        catch (IOException exception) {
//            throw  exception;
//        }
//        catch (Exception exception) {
//            throw new IOException("Fehler beim Rendern der Preview: " + exception.getMessage(), exception);
//        }        
//    }
//
//    @Override
//    public void renderPreviewDir( FileSystemElemNode node ) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    public void clearPreviewFile( FileSystemElemNode node ) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    public void clearPreviewDir( FileSystemElemNode node ) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//}
