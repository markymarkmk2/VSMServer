/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;


import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.net.rwebdav.VsmDavResourceFactory;
import io.milton.cache.CacheManager;
import io.milton.cache.LocalCacheManager;
import io.milton.http.HttpManager;
import io.milton.http.fs.SimpleLockManager;
import io.milton.http.fs.SimpleSecurityManager;
import io.milton.servlet.Config;
import io.milton.servlet.DefaultMiltonConfigurator;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import org.apache.commons.lang.StringUtils;


/**
 *
 * @author Administrator
 */
public class VSMMiltonConfigurator extends DefaultMiltonConfigurator {
    public static final String VSM_LOGIN_IDX = "vsm.loginIdx";
    public static final String VSMWRAPPER_IDX = "vsm.wrapperIdx";
    public static final String VSM_SEARCHWRAPPER_IDX = "vsm.searchwrapperIdx";
    

    IWrapper wrapper;
    long loginIdx = -1;
    

    public VSMMiltonConfigurator() {
    }
    
    @Override
    protected void build() {
        Map<String, String> userMap = new HashMap<>();
        userMap.put("v", "v");
        SimpleSecurityManager securityManager = new SimpleSecurityManager("/*", userMap);
        CacheManager cacheManager = new LocalCacheManager();
        SimpleLockManager lockManager = new SimpleLockManager(cacheManager);
        builder.setSecurityManager(securityManager);       // set your own security manager and other properties
        builder.setCacheManager(cacheManager);
        GuiServerApi api =  Main.get_control().getLoginManager().getApi(loginIdx);
        VsmDavResourceFactory resourceFactory = new VsmDavResourceFactory( api, wrapper, securityManager);
        builder.setResourceFactory(resourceFactory);
        resourceFactory.setLockManager(lockManager);
        super.build();
    }

    @Override
    public HttpManager configure( Config config ) throws ServletException {
        
        String loginIdxStr = config.getInitParameter(VSM_LOGIN_IDX);
        if (!StringUtils.isEmpty(loginIdxStr)) {
            loginIdx = Long.parseLong(loginIdxStr);            
        }
        if (loginIdx == -1)  {          
            throw new IllegalArgumentException("Angabe \"vsm.loginIdx\" fehlt in VSMMiltonConfigurator.configure");
        }        
        
        String wrapperIdx = config.getInitParameter(VSMWRAPPER_IDX);
        if (!StringUtils.isEmpty(wrapperIdx)) {
            long wrIdx = Long.parseLong(wrapperIdx);
            wrapper  = Main.get_control().getPoolContextManager().getPoolWrapper(wrIdx);
        }
        else {
            wrapperIdx = config.getInitParameter(VSM_SEARCHWRAPPER_IDX);
            if (!StringUtils.isEmpty(wrapperIdx)) {
                long wrIdx = Long.parseLong(wrapperIdx);
                wrapper = Main.get_control().getPoolHandlerServlet().getSearchContextManager().getSearchWrapper(wrIdx);
            }            
        }
        if (wrapper == null)  {          
            throw new IllegalArgumentException("Angabe \"vsm.wrapperIdx\" fehlt in VSMMiltonConfigurator.configure");
        }        
        return super.configure(config); 
    }
    
    
}

