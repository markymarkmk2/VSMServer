/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.webdav;

import de.dimm.vsm.records.FileSystemElemNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Administrator
 */
public interface IVsmContentService  {
	void setFileContent( FileSystemElemNode file, InputStream in) throws FileNotFoundException, IOException;
	InputStream getFileContent(FileSystemElemNode file) throws FileNotFoundException;
}
