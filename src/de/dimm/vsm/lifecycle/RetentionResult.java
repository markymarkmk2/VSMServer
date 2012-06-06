/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.lifecycle;

import java.util.Date;

/**
 *
 * @author Administrator
 */
public class RetentionResult
{
    long pool_idx;
    Date when;
    int files;
    long size;

    public RetentionResult( long pool_idx, Date when, int files, long size )
    {
        this.pool_idx = pool_idx;
        this.when = when;
        this.files = files;
        this.size = size;
    }

    void add( RetentionResult localret )
    {
        files += localret.files;
        size += localret.size;
    }

    public int getFiles()
    {
        return files;
    }

    public long getSize()
    {
        return size;
    }

    public Date getWhen()
    {
        return when;
    }
    

}
