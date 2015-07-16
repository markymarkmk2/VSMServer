/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import io.milton.servlet.MiltonFilter;
import java.util.Enumeration;
import java.util.Vector;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

/**
 *
 * @author Administrator
 */
public class VSMWebDavFilter extends MiltonFilter {
    
    private static final String CONFIG = "milton.configurator";
    
    long wrapperIdx;
    long loginIdx;
    boolean isSearch;
    String wrapperParamName;

    public VSMWebDavFilter(long loginIdx, long wrapperIdx, boolean isSearch) {       
        this.loginIdx = loginIdx;
        this.wrapperIdx = wrapperIdx;
        wrapperParamName = isSearch ? VSMMiltonConfigurator.VSM_SEARCHWRAPPER_IDX : VSMMiltonConfigurator.VSMWRAPPER_IDX;
    }

    @Override
    public void init( final FilterConfig config ) throws ServletException {
        FilterConfig fc = new FilterConfig() {

            @Override
            public String getFilterName() {
                return config.getFilterName();
            }

            @Override
            public ServletContext getServletContext() {
                return config.getServletContext();
            }

            @Override
            public String getInitParameter( String string ) {
                if (string.equals(CONFIG)) {
                    return VSMMiltonConfigurator.class.getName();
                }
                if (string.equals(wrapperParamName)) {
                    return Long.toString(wrapperIdx);
                }
                if (string.equals(VSMMiltonConfigurator.VSM_LOGIN_IDX)) {
                    return Long.toString(loginIdx);
                }
                return config.getInitParameter(string);
            }

            @Override
            public Enumeration<String> getInitParameterNames() {
                
                
                Vector<String> params = new Vector<>();
                Enumeration<String> parentParams = config.getInitParameterNames();
                while (parentParams.hasMoreElements()) {
                    params.add(parentParams.nextElement());
                }
                if (!params.contains(CONFIG)) {
                    params.add(CONFIG);
                }
                
                if (!params.contains(wrapperParamName)) {
                    params.add(wrapperParamName);
                }
                if (!params.contains(VSMMiltonConfigurator.VSM_LOGIN_IDX)) {
                    params.add(VSMMiltonConfigurator.VSM_LOGIN_IDX);
                }
                return params.elements();
            }
        };
        
        super.init(fc); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
}
