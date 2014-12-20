/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.search.IndexImpl;
import java.util.List;
import org.apache.lucene.document.Document;

/**
 *
 * @author Administrator
 */
public class IndexResult 
{
      FileSystemElemNode node;
      Document doc;  

        public IndexResult( FileSystemElemNode node, Document doc ) {
            this.node = node;
            this.doc = doc;
        }

        public Document getDoc() {
            return doc;
        }

        public FileSystemElemNode getNode() {
            return node;
        }
      
        public FileSystemElemAttributes getAttributes( GenericEntityManager em) {
            long ts = IndexImpl.doc_get_hex_long(doc, "ts" );
            List<FileSystemElemAttributes> attrs = node.getHistory(em);
            for (FileSystemElemAttributes attr: attrs) {
                if (attr.getTs() == ts)
                    return attr;
            }
            return node.getAttributes();
        }
      
        
  }