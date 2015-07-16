/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.net.RemoteFSElem;
import io.milton.http.Auth;
import io.milton.http.LockInfo;
import io.milton.http.LockResult;
import io.milton.http.LockTimeout;
import io.milton.http.LockToken;
import io.milton.http.Request;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.http11.auth.DigestResponse;
import io.milton.resource.CollectionResource;
import io.milton.resource.CopyableResource;
import io.milton.resource.DigestResource;
import io.milton.resource.LockableResource;
import io.milton.resource.MoveableResource;
import io.milton.resource.Resource;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Administrator
 */
public abstract class VsmResource implements Resource, MoveableResource, CopyableResource, LockableResource, DigestResource {

    private static final Logger log = LoggerFactory.getLogger(io.milton.http.fs.FsResource.class);
    RemoteFSElem file;
    final VsmDavResourceFactory factory;
    final String host;
    String ssoPrefix;

    protected abstract void doCopy(String dest);

    public VsmResource(String host, VsmDavResourceFactory factory, RemoteFSElem file) {
        this.host = host;
        this.file = file;
        this.factory = factory;
    }

    public RemoteFSElem getFile() {
        return file;
    }

    @Override
    public String getUniqueId() {
        String s = Long.toString(file.getIdx());
        return s;//.hashCode() + "";
    }

	@Override
    public String getName() {
        return file.getName();
    }

	@Override
    public Object authenticate(String user, String password) {
        return factory.getSecurityManager().authenticate(user, password);
    }

    @Override
    public Object authenticate(DigestResponse digestRequest) {
        return factory.getSecurityManager().authenticate(digestRequest);
    }

    @Override
    public boolean isDigestAllowed() {
        return factory.isDigestAllowed();
    }

    @Override
    public boolean authorise(Request request, Request.Method method, Auth auth) {
        //boolean b = factory.getSecurityManager().authorise(request, method, auth, this);
        boolean b = true;
        if( log.isTraceEnabled()) {
            log.trace("authorise: result=" + b);
        }
        return b;
    }

	@Override
    public String getRealm() {
        String r = factory.getRealm(this.host);
		if( r == null ) {
			throw new NullPointerException("Got null realm from: " + factory.getClass() + " for host=" + this.host);
		}
		return r;
    }

	@Override
    public Date getModifiedDate() {
        return file.getMtime();
    }

    public Date getCreateDate() {
        return file.getCtime();
    }

    public int compareTo(Resource o) {
        return this.getName().compareTo(o.getName());
    }

    @Override
    public void moveTo(CollectionResource newParent, String newName) {
        throw new RuntimeException("Move operation is not supported");
    }

    @Override
    public void copyTo(CollectionResource newParent, String newName) {
        throw new RuntimeException("Copy operation is not supported");
    }

    public void delete() {
        throw new RuntimeException("Delete operation is not supported");
    }

    static long tkid = 0;
    static LockToken tk = null;
    @Override
    public LockResult lock(LockTimeout timeout, LockInfo lockInfo) throws NotAuthorizedException {
        if (tk != null) {
            return LockResult.failed(LockResult.FailureReason.ALREADY_LOCKED);
        }
        tk = new LockToken(Long.toString(tkid++), lockInfo, timeout);
        return LockResult.success(tk);
                
//        return factory.getLockManager().lock(timeout, lockInfo, this);
    }

    @Override
    public LockResult refreshLock(String token) throws NotAuthorizedException {
        if (tk != null) {
            tk.setFrom(new Date());
            return LockResult.success(tk);
        }
        return LockResult.failed(LockResult.FailureReason.PRECONDITION_FAILED);
//        return factory.getLockManager().refresh(token, this);
    }

    @Override
    public void unlock(String tokenId) throws NotAuthorizedException {
        if (tk != null) {
            if (tk.tokenId.equals(tokenId)) {
                tk = null;
            }
        }
//        factory.getLockManager().unlock(tokenId, this);
    }

    @Override
    public LockToken getCurrentLock() {
        return tk;
//        if (factory.getLockManager() != null) {
//            return factory.getLockManager().getCurrentToken(this);
//        } else {
//            log.warn("getCurrentLock called, but no lock manager: file: " + file.getAbsolutePath());
//            return null;
//        }
    }
}