/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

/**
 *
 * @author mw
 */
public interface StatusHandler
{
    

    public String get_status_txt();
    public int get_status_code();
    public void  set_status( int code, String text);


}
