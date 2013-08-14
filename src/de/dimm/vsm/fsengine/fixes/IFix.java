/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.fixes;

import java.sql.SQLException;

/**
 *
 * @author Administrator
 */
public interface IFix
{

    void runFix() throws SQLException;
    public void setAbort( boolean abort );
    
}
