/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.net.RemoteFSElem;
import io.milton.http.Range;
import io.milton.http.XmlWriter;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.fs.FsDirectoryResource;
import io.milton.resource.Resource;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Administrator
 */
public class VsmPreviewDirectoryResource  extends VsmDirectoryResource {

    private static final Logger log = LoggerFactory.getLogger(FsDirectoryResource.class);
    
   
    public VsmPreviewDirectoryResource(String host, VsmDavResourceFactory factory, RemoteFSElem dir, IVsmContentService contentService) {
        super(host, factory, dir, contentService);
    }
    
    @Override
    public String getName() {
        return MAGIC_PREVIEW_FOLDER;
    }

    @Override
    public Resource child(String name) {        
        RemoteFSElem node = factory.getChild( this, name);
        return factory.resolveFile(this.host, node);
    }

    @Override
    public List<? extends Resource> getChildren() {
        ArrayList<VsmResource> list = new ArrayList<>();
        List<RemoteFSElem> files;
        try {
            files = factory.getChildren(file);
        }
        catch (SQLException sQLException) {
            log.error("Couldnt list files {} "+ file.getName(),sQLException );
            return list;
        }
        if (files != null) {
            for (RemoteFSElem fchild : files) {
                VsmPreviewFileResource res = new VsmPreviewFileResource(host, factory, fchild, contentService);
                if (res.getPreviewData() != null) {
                    list.add(res);    
                }
            }
        }
        return list;
    }

    

    /**
     * Will generate a listing of the contents of this directory, unless the
     * factory's allowDirectoryBrowsing has been set to false.
     *
     * If so it will just output a message saying that access has been disabled.
     *
     * @param out
     * @param range
     * @param params
     * @param contentType
     * @throws IOException
     * @throws NotAuthorizedException
     */
    @Override
    public void sendContent(OutputStream out, Range range, Map<String, String> params, String contentType) throws IOException, NotAuthorizedException {
        //String uri = "/" + factory.getContextPath() + subpath;
        XmlWriter w = new XmlWriter(out);
        w.open("html");
        w.open("head");
        


        w.begin("meta").writeAtt("http-equiv", "content-type").writeAtt("content","text/html").writeAtt("charset", "utf-8").close(true);
        w.begin("meta").writeAtt("charset", "utf-8").close(true);
        
        w.begin("style").writeAtt("type", "text/css").writeText(
                  "body{ font: 80% \"Trebuchet MS\", sans-serif; margin: 20px;}\n" +
                  "#leerespalte { width: 20px; }\n" +
                  "#spalteName     { width: 50%; }\n" +
                  "#spalteSize     { width: 80px; }\n" +
                  "#spalteDate     { width: 200px; }\n" +
                  "table { font: 100% \"Trebuchet MS\", sans-serif;}").close(true);

   
        w.close("head");
        w.open("body");
        
        String subpath = null;
        try {
            subpath = factory.getSubPath(getFile());
        }
        catch (SQLException | PathResolveException e) {
            w.begin("a").writeText("Fehler beim Ermitteln des Unterpfades: " + e.getMessage()).close();
        }
        String uri = subpath;
        
        w.begin("h1").open().writeText("Previews " + this.getName()).close();
        w.begin("p").close();
        w.begin("p").close();
        w.open("table");
        w.open("colgroup");
        w.begin("col").writeAtt("id","spalteName").close(true);
        w.begin("col").writeAtt("id","spalteDate").close(true);
        w.begin("col").writeAtt("id","spalteSize").close(true);
        w.close("colgroup");
        
        SimpleDateFormat fmt = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        
        for (Resource r : getChildren()) {
            if (!(r instanceof VsmFileResource)) {
                continue;
            }
            VsmFileResource vsmFile = (VsmFileResource)r;
            w.open("tr");

            // Name
            w.open("td");
            String path = buildHref(uri + "/" + VsmPreviewDirectoryResource.MAGIC_PREVIEW_FOLDER, r.getName());
            w.begin("a").writeAtt("href", path ).open().writeText(r.getName()).close();            
            w.close("td");            
            
            // Date
            w.begin("td").open().writeText(fmt.format(r.getModifiedDate())).close();

            // Size
            w.begin("td").open().writeText(SizeStr.format(vsmFile.file.getDataSize())).close();
            
            w.close("tr");
        }
        w.close("table");
        w.close("body");
        w.close("html");
        w.flush();
    }
    
    
}
