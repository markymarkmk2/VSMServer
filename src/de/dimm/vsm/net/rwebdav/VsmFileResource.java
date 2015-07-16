/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.net.RemoteFSElem;
import io.milton.common.ContentTypeUtils;
import io.milton.common.RangeUtils;
import io.milton.common.ReadingException;
import io.milton.common.WritingException;
import io.milton.http.Auth;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.resource.CopyableResource;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.MoveableResource;
import io.milton.resource.PropFindableResource;
import io.milton.resource.ReplaceableResource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Administrator
 */
public class VsmFileResource extends VsmResource implements CopyableResource, DeletableResource, GetableResource, MoveableResource, PropFindableResource, ReplaceableResource {

    private static final Logger log = LoggerFactory.getLogger(io.milton.http.fs.FsFileResource.class);
    private final IVsmContentService contentService;

    /**
     *
     * @param host - the requested host. E.g. www.mycompany.com
     * @param factory
     * @param file
     */
    public VsmFileResource( String host, VsmDavResourceFactory factory, RemoteFSElem file, IVsmContentService contentService ) {
        super(host, factory, file);
        this.contentService = contentService;
    }

    @Override
    public Long getContentLength() {
        return file.getDataSize();
    }

    @Override
    public String getContentType( String preferredList ) {
        String mime = ContentTypeUtils.findContentTypes(this.file.getName());
        String s = ContentTypeUtils.findAcceptableContentType(mime, preferredList);
        if (log.isTraceEnabled()) {
            log.trace("getContentType: preferred: {} mime: {} selected: {}", new Object[]{preferredList, mime, s});
        }
        return s;
    }

    @Override
    public String checkRedirect( Request arg0 ) {
        return null;
    }

    @Override
    public void sendContent( OutputStream out, Range range, Map<String, String> params, String contentType ) throws IOException, NotFoundException {
        InputStream in = null;
        try {
            in = contentService.getFileContent(file);
            if (range != null) {
                log.debug("sendContent: ranged content: " + file.getName());
                RangeUtils.writeRange(in, range, out);
            }
            else {
                log.debug("sendContent: send whole file " + file.getName());
                IOUtils.copy(in, out);
            }
            out.flush();
        }
        catch (FileNotFoundException e) {
            throw new NotFoundException("Couldnt locate content");
        }
        catch (ReadingException e) {
            throw new IOException(e);
        }
        catch (WritingException e) {
            throw new IOException(e);
        }
        catch (SQLException ex) {
            java.util.logging.Logger.getLogger(VsmFileResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * @{@inheritDoc}
     */
    @Override
    public Long getMaxAgeSeconds( Auth auth ) {
        return factory.maxAgeSeconds(this);
    }

    /**
     * @{@inheritDoc}
     */
    @Override
    protected void doCopy( String dest ) {
        throw new IllegalArgumentException("doCopy File not supported: " + dest);
//        try {
//            FileUtils.copyFile(file, dest);
//        } catch (IOException ex) {
//            throw new RuntimeException("Failed doing copy to: " + dest.getAbsolutePath(), ex);
//        }
    }

    @Override
    public void replaceContent( InputStream in, Long length ) throws BadRequestException, ConflictException, NotAuthorizedException {
        throw new IllegalArgumentException("replaceContent File not supported: " + file.getName());

//		try {
//			contentService.setFileContent(file, in);
//		} catch (IOException ex) {
//			throw new BadRequestException("Couldnt write to: " + file.getAbsolutePath(), ex);
//		}
    }
}