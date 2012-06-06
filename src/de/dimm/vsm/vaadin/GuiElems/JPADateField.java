/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.DateField;

/**
 *
 * @author Administrator
 */
public class JPADateField extends JPAField
{
    String format;
    public JPADateField(String caption, String fieldName, String format)
    {
        super( caption, fieldName );
        this.format = format;
    }

    @Override
    public AbstractField createGui(Object _node, ValueChangeListener _changeListener)
    {
        node = _node;
        this.changeListener = _changeListener;

        final MethodProperty p = new MethodProperty(node,fieldName);
        p.addListener(changeListener);


        DateField tf = new DateField(caption, p);
        tf.setDateFormat(format);
        
        gui = tf;
        
        tf.addListener(new ValueChangeListener()
        {
            @Override
            public void valueChange( ValueChangeEvent event )
            {
                changeListener.valueChange(event);
            }
        });
        gui.setPropertyDataSource(p);

        return gui;
    }
}
