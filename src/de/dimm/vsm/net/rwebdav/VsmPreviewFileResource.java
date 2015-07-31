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
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.NotFoundException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Administrator
 */
public class VsmPreviewFileResource extends VsmFileResource  {

    private static final Logger log = LoggerFactory.getLogger(io.milton.http.fs.FsFileResource.class);
    

    /**
     *
     * @param host - the requested host. E.g. www.mycompany.com
     * @param factory
     * @param file
     */
    public VsmPreviewFileResource( String host, VsmDavResourceFactory factory, RemoteFSElem file, IVsmContentService contentService ) {
        super(host, factory, file, contentService);
    }

    @Override
    public Long getContentLength() {
        return previewData.getPreviewImageFile().length();
    }

    @Override
    public String getName() {
        return super.getName() + ".png"; //To change body of generated methods, choose Tools | Templates.
    }
    

    @Override
    public String getContentType( String preferredList ) {
        String mime = ContentTypeUtils.findContentTypes(getName());
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
            in = new FileInputStream(previewData.getPreviewImageFile());
            
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
        
        finally {
            IOUtils.closeQuietly(in);
        }
    }    
}