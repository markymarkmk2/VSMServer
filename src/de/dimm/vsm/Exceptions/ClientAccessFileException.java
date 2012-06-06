/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Exceptions;

/**
 *
 * @author Administrator
 */
public class ClientAccessFileException extends Exception {

    /**
     * Creates a new instance of <code>ClientAccessFileException</code> without detail message.
     */
    public ClientAccessFileException()
    {
    }


    /**
     * Constructs an instance of <code>ClientAccessFileException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public ClientAccessFileException(String msg) {
        super(msg);
    }
}
