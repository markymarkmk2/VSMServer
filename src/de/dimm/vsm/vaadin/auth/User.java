/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.auth;

/**
 *
 * @author Administrator
 */
public class User
{
    String name;

    public User( String name )
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return name;
    }    
}
