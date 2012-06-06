/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.sql;

import java.util.Date;

/**
 *
 * @author mw
 */
abstract public class GenericResultSet
{

    abstract public long getLong( String idx_name ) throws GenericResultException;
    abstract public String getString( String string ) throws GenericResultException;

    abstract public Date getDate( String string ) throws GenericResultException;
}
