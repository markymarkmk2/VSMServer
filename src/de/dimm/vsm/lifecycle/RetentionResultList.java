/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.lifecycle;

import de.dimm.vsm.records.Retention;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class RetentionResultList
{
    Retention retention;
    List list;

    public RetentionResultList( Retention retention, List list )
    {
        this.retention = retention;
        this.list = list;
    }

    public List getList()
    {
        return list;
    }

}