/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.event.FieldEvents.TextChangeEvent;
import com.vaadin.event.FieldEvents.TextChangeListener;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.TextField;

/**
 *
 * @author Administrator
 */
public class JPATextField extends JPAField
{
    public JPATextField(String caption, String fieldName)
    {
        super( caption, fieldName );        
    }

    @Override
    public AbstractField createGui(Object _node, ValueChangeListener _changeListener)
    {
        node = _node;
        this.changeListener = _changeListener;

        final MethodProperty p = new MethodProperty(node,fieldName);
        p.addListener(changeListener);


        TextField tf = new TextField(caption, p);
        gui = tf;
        tf.addListener(new TextChangeListener() {

            @Override
            public void textChange( TextChangeEvent event )
            {
                changeListener.valueChange(null);
            }
        });
        gui.setPropertyDataSource(p);

        return gui;
    }
}
