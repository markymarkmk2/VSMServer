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
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.Select;
import com.vaadin.ui.TextField;

/**
 *
 * @author Administrator
 */
public class JPACheckBox extends JPAField
{
    public JPACheckBox(String caption, String fieldName)
    {
        super( caption, fieldName );        
    }

    @Override
    public AbstractField createGui(Object _node, ValueChangeListener _changeListener)
    {
        node = _node;
        this.changeListener = _changeListener;

        final MethodProperty p = new MethodProperty(node,fieldName);
        p.addListener(_changeListener);




        CheckBox checkBox = new CheckBox(caption);
        checkBox.setPropertyDataSource(p);
        checkBox.setReadOnly(false);
        checkBox.setInvalidAllowed(false);
        checkBox.setImmediate(true);

        gui = checkBox;
        checkBox.addListener( new ClickListener()
        {

            @Override
            public void buttonClick( ClickEvent event )
            {
                changeListener.valueChange( null );
            }
        });

        return gui;
    }
}
