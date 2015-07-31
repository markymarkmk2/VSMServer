/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.StoragePoolHandlerServlet;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.net.interfaces.IWrapper;
import de.dimm.vsm.records.FileSystemElemNode;
import io.milton.http.LockManager;
import io.milton.http.ResourceFactory;
import io.milton.resource.Resource;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Administrator
 */
public final class VsmDavResourceFactory  implements ResourceFactory {

    private static final Logger log = LoggerFactory.getLogger(io.milton.http.fs.FileSystemResourceFactory.class);
    private IVsmContentService contentService; // = new IVsmContentService();
    FileSystemElemNode root;
    io.milton.http.SecurityManager securityManager;
    LockManager lockManager;
    Long maxAgeSeconds;
    String contextPath;
    boolean allowDirectoryBrowsing;
    String defaultPage;
    boolean digestAllowed = true;
    private String ssoPrefix;
    
    GuiServerApi api;
    IWrapper wrapper;
    StoragePoolHandler spHandler;
    

    protected void init(FileSystemElemNode sRoot, io.milton.http.SecurityManager securityManager) {
        setRoot(sRoot);
        setSecurityManager(securityManager);
    }

    /**
     *
     * @param root - the root folder of the filesystem to expose. This must
     * include the context path. Eg, if you've deployed to webdav-fs, root must
     * contain a folder called webdav-fs
     * @param securityManager
     */
    public VsmDavResourceFactory(GuiServerApi api, IWrapper wrapper, io.milton.http.SecurityManager securityManager) {
        this.api = api;
        this.wrapper = wrapper;
        this.spHandler = StoragePoolHandlerServlet.getPoolHandlerByWrapper(wrapper);
        setRoot(spHandler.getRootDir());
        setSecurityManager(securityManager);
        contentService = new VsmContentService( api,  wrapper);
    }

    /**
     *
     * @param root - the root folder of the filesystem to expose. called
     * webdav-fs
     * @param securityManager
     * @param contextPath - this is the leading part of URL's to ignore. For
     * example if you're application is deployed to
     * http://localhost:8080/webdav-fs, the context path should be webdav-fs
     */
    public VsmDavResourceFactory(GuiServerApi api, IWrapper wrapper, io.milton.http.SecurityManager securityManager, String contextPath) {
        this(api, wrapper, securityManager);
        setContextPath(contextPath);
    }

    public FileSystemElemNode getRoot() {
        return root;
    }

    public final void setRoot(FileSystemElemNode root) {
        log.debug("root: " + root.getName());
        this.root = root;
        if (!root.isDirectory()) {
            log.warn("Root exists but is not a directory: " + root.getName());
        }
    }

    @Override
    public Resource getResource(String host, String url) {
        log.debug("getResource: host: " + host + " - url:" + url);
        url = stripContext(url);
        
        if (url.endsWith(VsmDirectoryResource.MAGIC_PREVIEW_FOLDER)) {
            url = url.substring(0, url.length() - VsmDirectoryResource.MAGIC_PREVIEW_FOLDER.length() );
            RemoteFSElem requested = resolvePath(root, url);
            if (requested == null) {
                return null;
            }
            return new VsmPreviewDirectoryResource(host, this, requested, contentService);            
        }
        if (url.contains(VsmDirectoryResource.MAGIC_PREVIEW_FOLDER)) {
            url = url.replace("/" + VsmDirectoryResource.MAGIC_PREVIEW_FOLDER, "");
            if (url.endsWith(".png")) {
                url = url.substring(0, url.length() - 4);
            }
            RemoteFSElem requested = resolvePath(root, url);
            if (requested == null) {
                return null;
            }
            return new VsmPreviewFileResource(host, this, requested, contentService);            
        }
        
        RemoteFSElem requested = resolvePath(root, url);
        if (requested == null) {
            return null;
        }
        return resolveFile(host, requested);
    }


    public VsmResource resolveFile(String host, RemoteFSElem file) {
        VsmResource r;
        /*if (!file.exists()) {
            log.debug("file not found: " + file.getAbsolutePath());
            return null;
        } else*/
        if (file.isDirectory()) {
            r = new VsmDirectoryResource(host, this, file, contentService);
        } else {
            r = new VsmFileResource(host, this, file, contentService);
        }
        r.ssoPrefix = ssoPrefix;        
        return r;
    }
    private String stripToken( String path) {
        if (path.equals("/" + wrapper.getWebDavToken()))
            return "/";
        
        if (path.startsWith("/" + wrapper.getWebDavToken())) {
            return  path.substring(wrapper.getWebDavToken().length() + 1);
        }
        return path;
    }  
    public RemoteFSElem resolvePath(FileSystemElemNode root, String url) {
        //Path path = Path.path(url);
        if (url.length() > 1 && url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        RemoteFSElem file;
        try {
            //FileSystemElemNode child = spHandler.resolve_node(url);
            file = Main.get_control().getPoolHandlerServlet().resolve_node(wrapper, stripToken(url));
            return file;
        }
        catch (SQLException sQLException) {
            log.warn("Node zu Pfad nicht gefunden: " + url, sQLException);
            return null;
        }
        // TODO
//        //FileSystemElemNode f = root;
//        for (FileSystemElemNode child: root.getChildren()) {
//            if (child.getName().equals(url.substring(url.lastIndexOf('/') + 1))) {
//                return child;
//            }
//        }
//        return null;
//        for (String s : path.getParts()) {
//            f = new File(f, s);
//        }
        //return f;
    }

    public String getRealm(String host) {
        String s = securityManager.getRealm(host);
		if( s == null ) {
			throw new NullPointerException("Got null realm from securityManager: " + securityManager + " for host=" + host);
		}
		return s;
    }

    /**
     *
     * @return - the caching time for files
     */
    public Long maxAgeSeconds(VsmResource resource) {
        return maxAgeSeconds;
    }

    public void setSecurityManager(io.milton.http.SecurityManager securityManager) {
        if (securityManager != null) {
            log.debug("securityManager: " + securityManager.getClass());
        } else {
            log.warn("Setting null FsSecurityManager. This WILL cause null pointer exceptions");
        }
        this.securityManager = securityManager;
    }

    public io.milton.http.SecurityManager getSecurityManager() {
        return securityManager;
    }

    public void setMaxAgeSeconds(Long maxAgeSeconds) {
        this.maxAgeSeconds = maxAgeSeconds;
    }

    public Long getMaxAgeSeconds() {
        return maxAgeSeconds;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public void setLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    public void setContextPath(String contextPath) {
        this.contextPath = contextPath;
    }

    public String getContextPath() {
        return contextPath;
    }

    /**
     * Whether to generate an index page.
     *
     * @return
     */
    public boolean isAllowDirectoryBrowsing() {
        return allowDirectoryBrowsing;
    }

    public void setAllowDirectoryBrowsing(boolean allowDirectoryBrowsing) {
        this.allowDirectoryBrowsing = allowDirectoryBrowsing;
    }

    /**
     * if provided GET requests to a folder will redirect to a page of this name
     * within the folder
     *
     * @return - E.g. index.html
     */
    public String getDefaultPage() {
        return defaultPage;
    }

    public void setDefaultPage(String defaultPage) {
        this.defaultPage = defaultPage;
    }

    private String stripContext(String url) {
        if (this.contextPath != null && contextPath.length() > 0) {
            url = url.replaceFirst('/' + contextPath, "");
            log.debug("stripped context: " + url);
            return url;
        } else {
            return url;
        }
    }

    boolean isDigestAllowed() {
        boolean b = digestAllowed && securityManager != null && securityManager.isDigestAllowed();
        if (log.isTraceEnabled()) {
            log.trace("isDigestAllowed: " + b);
        }
        return b;
    }

    public void setDigestAllowed(boolean digestAllowed) {
        this.digestAllowed = digestAllowed;
    }

    public void setSsoPrefix(String ssoPrefix) {
        this.ssoPrefix = ssoPrefix;
    }

    public String getSsoPrefix() {
        return ssoPrefix;
    }

    public IVsmContentService getContentService() {
        return contentService;
    }

    public void setContentService(IVsmContentService contentService) {
        this.contentService = contentService;
    }

    List<RemoteFSElem> getChildren( RemoteFSElem file ) throws SQLException {
        return api.listDir(wrapper, file);
    }

    RemoteFSElem getChildren( VsmDirectoryResource aThis, String name ) {        
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    String getSubPath( RemoteFSElem file ) throws SQLException, PathResolveException {
        if (file.getPath().startsWith("/")) {
            return file.getPath();  
        }
        String fullPath = api.resolvePath(wrapper, file);
        int idx = fullPath.indexOf('/', 1);
        if (idx > 0) {
            fullPath = fullPath.substring(idx);
        }
        return fullPath;
    }

    RemoteFSElem getChild( VsmDirectoryResource aThis, String name ) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
