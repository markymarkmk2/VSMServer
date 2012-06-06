/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.ui.AbstractField;

/**
 *
 * @author Administrator
 */
public abstract class JPAField
{
    Object node;
    String caption;
    String fieldName;
    ValueChangeListener changeListener;
    AbstractField gui;

    public JPAField(  String caption, String fieldName )
    {
        this.caption = caption;
        this.fieldName = fieldName;
    }


    public abstract AbstractField createGui(Object node, ValueChangeListener changeListener);

    public String getFieldName()
    {
        return fieldName;
    }

    public String getCaption()
    {
        return caption;
    }

    public Object getNode()
    {
        return node;
    }

    public AbstractField getGui()
    {
        return gui;
    }

    public void setReadOnly( boolean b )
    {
        if (gui != null)
        {
            gui.setReadOnly(b);
            if (gui.getPropertyDataSource() != null)
                gui.getPropertyDataSource().setReadOnly(b);
        }
    }


}
