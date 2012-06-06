/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

/**
 *
 * @author mw
 */
public class StatusEntry
{
    public static final int SLEEPING = 1;
    public static final int BUSY = 2;
    public static final int WAITING = 3;
    public static final int ERROR = 4;

    String status_txt;
    int status_code;
    long ts;

    // NOT FOR USE OUTSIDE
    public StatusEntry()
    {
        status_txt = "";
        status_code = SLEEPING;
        ts = System.currentTimeMillis();
    }

    public void set_status( int c, String s)
    {
        status_txt = s;
        status_code = c;
        ts = System.currentTimeMillis();
    }
    public void set_status( int c)
    {
        status_code = c;
        ts = System.currentTimeMillis();
    }
    public String get_status_txt()
    {
        return status_txt;
    }
    public int get_status_code()
    {
        return status_code;
    }
    public long get_ts()
    {
        return ts;
    }


}
