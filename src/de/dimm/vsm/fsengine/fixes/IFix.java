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

    public boolean runFix() throws SQLException;
    public void abortJob();
    public boolean isAborted();

    public String getStatusStr();

    public String getStatisticStr();

    public Object getResultData();

    public String getProcessPercent();

    public String getProcessPercentDimension();


    public void close();
    
}
