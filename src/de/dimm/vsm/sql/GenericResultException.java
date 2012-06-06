/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.sql;

/**
 *
 * @author mw
 */
public class GenericResultException extends Exception {

    /**
     * Creates a new instance of <code>GenericResultException</code> without detail message.
     */
    public GenericResultException(Exception exc)
    {
        super(exc);
    }


    /**
     * Constructs an instance of <code>GenericResultException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public GenericResultException(String msg)
    {
        super(msg);
    }

}
