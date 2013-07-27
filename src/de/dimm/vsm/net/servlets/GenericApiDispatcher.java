/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.servlets;

/**
 *
 * @author Administrator
 */
public class GenericApiDispatcher
{
    protected boolean ssl;
    protected String keystore;
    protected String keypwd;
    protected boolean tcp;
    
    public GenericApiDispatcher(  boolean ssl, String keystore, String keypwd, boolean tcp )
    {
        this.ssl = ssl;
        this.keystore = keystore;
        this.keypwd = keypwd;
        this.tcp = tcp;        
    }    
    
    public boolean isSsl()
    {
        return ssl;
    }

}
