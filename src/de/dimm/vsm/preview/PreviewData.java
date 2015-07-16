/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import java.io.File;

/**
 *
 * @author Administrator
 */
public class PreviewData implements IPreviewData {

    long attrIdx;
    File file;
    IMetaData metaData;
    String name;

    public PreviewData( long attrIdx, File file, IMetaData metaData, String name ) {
        this.attrIdx = attrIdx;
        this.file = file;
        this.metaData = metaData;
        this.name = name;
    }
    
    @Override
    public File getPreviewImageFile() {
        return file;
    }

    @Override
    public IMetaData getMetaData() {
        return metaData;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getAttrIdx() {
        return attrIdx;
    }

    @Override
    public void setPreviewImageFile( File file ) {
        file = this.file;
    }    
}
