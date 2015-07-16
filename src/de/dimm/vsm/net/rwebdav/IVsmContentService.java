/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.net.RemoteFSElem;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public interface IVsmContentService  {
	void setFileContent( RemoteFSElem file, InputStream in) throws FileNotFoundException, IOException;
	InputStream getFileContent(RemoteFSElem file) throws FileNotFoundException, SQLException;
}
