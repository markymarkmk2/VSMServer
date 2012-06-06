/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.Item;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.data.util.IndexedContainer;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Select;
import java.util.List;


class JPAComboMethodProperty extends MethodProperty
{
    List<ComboEntry> entries;

    public JPAComboMethodProperty( Object node,String fieldName, List<ComboEntry> entries)
    {
        super(node,fieldName);
        this.entries = entries;
    }

    @Override
    public Object getValue()
    {
        Object dbVal = super.getValue();
        if (dbVal == null)
            return null;

        for (int i = 0; i < entries.size(); i++)
        {
            ComboEntry comboEntry = entries.get(i);
            if (comboEntry.isDbEntry(dbVal.toString()))
            {
                System.out.println("JPAComboMethodProperty.getValue returns " + comboEntry.getGuiEntryKey());
                return comboEntry.getGuiEntryKey();
            }
        }
        System.out.println("JPAComboMethodProperty.getValue returns empty");
        return "";
    }

    @Override
    public void setValue( Object newValue ) throws ReadOnlyException, ConversionException
    {
        if (newValue != null)
        {

            for (int i = 0; i < entries.size(); i++)
            {
                ComboEntry comboEntry = entries.get(i);
                if (comboEntry.isGuiEntry(newValue.toString()))
                    newValue = comboEntry.getDbEntry();
            }
        }

        System.out.println("JPAComboMethodProperty.setValue " + newValue);
        super.setValue(newValue);
    }

    @Override
    protected void invokeSetMethod( Object value )
    {
        super.invokeSetMethod(value);
    }

    @Override
    public String toString()
    {
        return super.toString();
    }

}
/**
 *
 * @author Administrator
 */
public class JPAComboField extends JPAField
{
    List<ComboEntry> entries;
    public JPAComboField(String caption, String fieldName, List<ComboEntry> entries)
    {
        super( caption, fieldName );
        this.entries = entries;
    }

    public List<ComboEntry> getEntries()
    {
        return entries;
    }

    

    @Override
    public AbstractField createGui(Object _node, ValueChangeListener _changeListener)
    {
        node = _node;
        this.changeListener = _changeListener;

        final MethodProperty p = new JPAComboMethodProperty(node,fieldName, entries);
        p.addListener(_changeListener);


        ComboBox comboBox = new ComboBox(caption, entries);
        IndexedContainer container = new IndexedContainer();
        container.addContainerProperty(fieldName, String.class, null);

        for (int i = 0; i < entries.size(); i++)
        {
            ComboEntry comboEntry = entries.get(i);
            Item it = container.addItem(comboEntry.getGuiEntryKey());
            it.getItemProperty(fieldName).setValue(comboEntry.getDbEntry());
        }        
        

        comboBox.setContainerDataSource(container);
        comboBox.setPropertyDataSource(p);
        comboBox.setItemCaptionMode(Select.ITEM_CAPTION_MODE_EXPLICIT_DEFAULTS_ID);
        
        comboBox.setNullSelectionAllowed(false);
        comboBox.setReadOnly(false);
        comboBox.setInvalidAllowed(false);
        comboBox.setNewItemsAllowed(false);
        comboBox.setImmediate(true);


        for (int i = 0; i < entries.size(); i++)
        {
            ComboEntry comboEntry = entries.get(i);
            if (p.getValue().toString().equals(comboEntry.getGuiEntryKey()))
            {
                comboBox.setValue(comboEntry.getGuiEntryKey());
                break;
            }
        }

        gui = comboBox;
        comboBox.addListener(changeListener);

        return gui;
    }
}
