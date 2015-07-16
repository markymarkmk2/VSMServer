/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.preview;

import static de.dimm.vsm.preview.IMetaData.ATTR_RENDER_STATE;
import static de.dimm.vsm.preview.IMetaData.RENDER_STATE_ERROR;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Administrator
 */
public class MetaData implements IMetaData {

    Map<String,String> attributes = new HashMap<>();
    
    @Override
    public String getAttribute( String key ) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute( String key, String value ) {
        attributes.put(key, value);
    }

    @Override
    public Collection<String> getKeys() {
        return attributes.keySet();
    }
    
    @Override
    public void setBusy() {
        attributes.put(ATTR_RENDER_STATE, RENDER_STATE_BUSY);
    }
    @Override
    public void setDone() {
        attributes.put(ATTR_RENDER_STATE, RENDER_STATE_DONE);
    }
    @Override
    public void setError() {
        attributes.put(ATTR_RENDER_STATE, RENDER_STATE_ERROR);
    }
    @Override
    public void setError(String text) {
        attributes.put(ATTR_RENDER_STATE, RENDER_STATE_ERROR);
        attributes.put(ATTR_RENDER_ERROR, text );
    }
    @Override
    public void setTimeout() {
        attributes.put(ATTR_RENDER_STATE, RENDER_STATE_TIMEOUT);
    }
    @Override
    public boolean isBusy() {
        return existsAttribute(ATTR_RENDER_STATE, RENDER_STATE_BUSY);
    }
    @Override
    public boolean isDone() {
        return existsAttribute(ATTR_RENDER_STATE, RENDER_STATE_DONE);
    }
    @Override
    public boolean isError() {
        return existsAttribute(ATTR_RENDER_STATE, RENDER_STATE_ERROR);
    }
    @Override
    public boolean isTimeout() {
        return existsAttribute(ATTR_RENDER_STATE, RENDER_STATE_TIMEOUT);
    }

    private boolean existsAttribute( String key, String value ) {
        String attr = attributes.get(key);
        if (StringUtils.isNotEmpty(attr)) {
            return attr.equals(value);
        }
        return false;
    }    
}
