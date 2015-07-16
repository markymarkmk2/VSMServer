/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net.rwebdav;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.net.RemoteFSElem;
import io.milton.http.Auth;
import io.milton.http.LockInfo;
import io.milton.http.LockTimeout;
import io.milton.http.LockToken;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.XmlWriter;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.fs.FsDirectoryResource;
import static io.milton.http.fs.FsDirectoryResource.insertSsoPrefix;
import io.milton.resource.CollectionResource;
import io.milton.resource.CopyableResource;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.LockingCollectionResource;
import io.milton.resource.MakeCollectionableResource;
import io.milton.resource.MoveableResource;
import io.milton.resource.PropFindableResource;
import io.milton.resource.PutableResource;
import io.milton.resource.Resource;
import java.io.IOException;
import java.io.InputStream;
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
public class VsmDirectoryResource  extends VsmResource implements MakeCollectionableResource, PutableResource, CopyableResource, DeletableResource, MoveableResource, PropFindableResource, LockingCollectionResource, GetableResource {

    private static final Logger log = LoggerFactory.getLogger(FsDirectoryResource.class);
    
    private final IVsmContentService contentService;

    public VsmDirectoryResource(String host, VsmDavResourceFactory factory, RemoteFSElem dir, IVsmContentService contentService) {
        super(host, factory, dir);
        this.contentService = contentService;
        
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Is not a directory: " + dir.getName());
        }
    }

    @Override
    public CollectionResource createCollection(String name) {
        throw new IllegalArgumentException("Create Directory not supported: " + name);
//        File fnew = new File(file, name);
//        boolean ok = fnew.mkdir();
//        if (!ok) {
//            throw new RuntimeException("Failed to create: " + fnew.getAbsolutePath());
//        }
//        return new VsmDirectoryResource(host, factory, fnew, contentService);
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
                VsmResource res = factory.resolveFile(this.host, fchild);
                if (res != null) {
                    list.add(res);
                } else {
                    log.error("Couldnt resolve file {}", fchild.getName());
                }
            }
        }
        return list;
    }

    /**
     * Will redirect if a default page has been specified on the factory
     *
     * @param request
     * @return
     */
    @Override
    public String checkRedirect(Request request) {
        if (factory.getDefaultPage() != null) {
            return request.getAbsoluteUrl() + "/" + factory.getDefaultPage();
        } else {
            return null;
        }
    }

    @Override
    public Resource createNew(String name, InputStream in, Long length, String contentType) throws IOException {
         return null;

//		File dest = new File(this.getFile(), name);
//		contentService.setFileContent(dest, in);        
//        return factory.resolveFile(this.host, dest);

    }

    @Override
    protected void doCopy(String dest) {
        throw new IllegalArgumentException("doCopy Dir not supported: " + dest);
//        try {
//            FileUtils.copyDirectory(this.getFile(), dest);
//        } catch (IOException ex) {
//            throw new RuntimeException("Failed to copy to:" + dest.getAbsolutePath(), ex);
//        }
    }

    @Override
    public LockToken createAndLock(String name, LockTimeout timeout, LockInfo lockInfo) throws NotAuthorizedException {
        throw new IllegalArgumentException("Create File not supported: " + name);
//        File dest = new File(this.getFile(), name);
//        createEmptyFile(dest);
//        VsmFileResource newRes = new VsmFileResource(host, factory, dest, contentService);
//        LockResult res = newRes.lock(timeout, lockInfo);
//        return res.getLockToken();
    }
//
//    private void createEmptyFile(File file) {
//        FileOutputStream fout = null;
//        try {
//            fout = new FileOutputStream(file);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } finally {
//            IOUtils.closeQuietly(fout);
//        }
//    }

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
        
        
        w.writeText(""
                + "<script type=\"text/javascript\" language=\"javascript1.1\">\n"
                + "    var fNewDoc = false;\n"
                + "  </script>\n"
                + "  <script LANGUAGE=\"VBSCRIPT\">\n"
                + "    On Error Resume Next\n"
                + "    Set EditDocumentButton = CreateObject(\"SharePoint.OpenDocuments.3\")\n"
                + "    fNewDoc = IsObject(EditDocumentButton)\n"
                + "  </script>\n"
                + "  <script type=\"text/javascript\" language=\"javascript1.1\">\n"
                + "    var L_EditDocumentError_Text = \"The edit feature requires a SharePoint-compatible application and Microsoft Internet Explorer 4.0 or greater.\";\n"
                + "    var L_EditDocumentRuntimeError_Text = \"Sorry, couldnt open the document.\";\n"
                + " function CallMe(strPath)\n"
                + " {\n"
                + "  window.open(strPath);\n"
                + " }\n"

                + "    function editDocument(strDocument) {\n"
				+ "      strDocument = 'http://192.168.1.2:8080' + strDocument; "
                + "      if (fNewDoc) {\n"
                + "        if (!EditDocumentButton.EditDocument(strDocument)) {\n"
                + "          alert(L_EditDocumentRuntimeError_Text + ' - ' + strDocument); \n"
                + "        }\n"
                + "      } else { \n"
                + "        alert(L_EditDocumentError_Text + ' - ' + strDocument); \n"
                + "      }\n"
                + "    }\n"
                + "  </script>\n");

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
        
        w.begin("h1").open().writeText(this.getName()).close();
//        String basePath = "\\\\192.168.1.145:58080\\DavWWWRoot";
//        w.begin("a").writeAtt("href", basePath).open().writeText("WebDav").close(true);
//        w.begin("a").writeAtt("href", "").writeAtt("folder", "http://192.168.1.145:58080\\DavWWWRoot").open().writeText("WebDav").close(true);
        w.begin("p").close();
        w.begin("p").close();
        String osxWebDavUrl = "http://v:v@" + host + "/" + factory.wrapper.getWebDavToken() + "/";
        String winWebDavUrl = "\\\\" + host + "\\" + factory.wrapper.getWebDavToken() + "\\DavWWWRoot\\";
        String winFileWebDavUrl = host + "\\\\" + factory.wrapper.getWebDavToken() + "\\\\DavWWWRoot\\\\";
        String absPath = "file:////" + winFileWebDavUrl;
        SimpleDateFormat fmt = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        w.begin("a").writeAtt("href", "#").writeAtt("onclick", "CallMe('" + absPath + "')").open().writeText("WebDav Windows IE").close();
        w.begin("p").close();
//        w.begin("a").writeText("WebDav Windows (öffnen in Windows Explorer): " + winWebDavUrl).close();
//        w.begin("p").close();
//        w.begin("a").writeText("WebDav OSX (öffnen mit Finder -> Mit Server verbinden...): " + osxWebDavUrl).close();
//        w.begin("p").close();
        w.open("table");
        w.open("colgroup");
        w.begin("col").writeAtt("id","spalteName").close(true);
        w.begin("col").writeAtt("id","spalteDate").close(true);
        w.begin("col").writeAtt("id","spalteSize").close(true);
        w.close("colgroup");
        
        for (Resource r : getChildren()) {
            w.open("tr");

            // Name
            w.open("td");
            String path = buildHref(uri, r.getName());
            w.begin("a").writeAtt("href", path).open().writeText(r.getName()).close();
            //String absPath = "file:////192.168.1.145@58080\\\\DavWWWRoot\\\\" + path.substring(1);
            //w.begin("a").writeAtt("href", "#").writeAtt("onclick", "CallMe('" + absPath + "')").open().writeText("   (DateiSystem)").close();

            w.close("td");
            
            // Date
            w.begin("td").open().writeText(fmt.format(r.getModifiedDate())).close();

            // Size
            if (r instanceof VsmFileResource) {
                VsmFileResource vsmFile = (VsmFileResource)r;
                w.begin("td").open().writeText(SizeStr.format(vsmFile.file.getDataSize())).close();
            }
            else  {
                w.begin("td").open().writeText(" ").close();
            }
            w.close("tr");
        }
        w.close("table");
        w.close("body");
        w.close("html");
        w.flush();
    }
    
    @Override
    public Long getMaxAgeSeconds(Auth auth) {
        return null;
    }

    @Override
    public String getContentType(String accepts) {
        return "text/html";
    }

    @Override
    public Long getContentLength() {
        return null;
    }

    private String buildHref(String uri, String name) {
        String abUrl = uri;

        if (!abUrl.endsWith("/")) {
            abUrl += "/";
        }
        if (!abUrl.startsWith("/")) {
            abUrl = "/" + abUrl;
        }
        if (ssoPrefix == null) {
            return "/" + factory.wrapper.getWebDavToken() + abUrl + name;
        } else {
            // This is to match up with the prefix set on SimpleSSOSessionProvider in MyCompanyDavServlet
            String s = insertSsoPrefix(abUrl, ssoPrefix);
            return s += name;
        }
    }

    public static String insertSsoPrefix(String abUrl, String prefix) {
        // need to insert the ssoPrefix immediately after the host and port
        int pos = abUrl.indexOf("/", 8);
        String s = abUrl.substring(0, pos) + "/" + prefix;
        s += abUrl.substring(pos);
        return s;
    }
}
