/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Exceptions;

import java.io.IOException;

/**
 *
 * @author Administrator
 */
public class RetentionException extends IOException {

    /**
     * Creates a new instance of <code>DBConnException</code> without detail message.
     */
    public RetentionException() {
    }


    /**
     * Constructs an instance of <code>DBConnException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public RetentionException(String msg) {
        super(msg);
    }
    public RetentionException(String msg, Exception e) {
        super(msg, e);
    }

}
