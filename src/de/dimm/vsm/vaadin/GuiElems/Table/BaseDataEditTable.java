/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems.Table;

import com.vaadin.data.Property;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.event.ItemClickEvent.ItemClickListener;
import com.vaadin.ui.AbstractLayout;
import com.vaadin.ui.Component;
import com.vaadin.ui.Label;
import com.vaadin.ui.Table;
import com.vaadin.ui.VerticalLayout;
import de.dimm.vsm.vaadin.GuiElems.ComboEntry;
import de.dimm.vsm.vaadin.GuiElems.JPAComboField;
import de.dimm.vsm.vaadin.GuiElems.JPAField;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;



/** Formats the value in a column containing Double objects. */
class ComboColumnGenerator implements Table.ColumnGenerator
{
    JPAComboField field; /* Format string for the Double values. */

    static final String COMBO_MAGIC_PREFIX = "COMBO.";

    /**
     * Creates double value column formatter with the given
     * format string.
     */
    public ComboColumnGenerator(JPAComboField fld)
    {
        this.field = fld;
    }

    /**
     * Generates the cell containing the Double value.
     * The column is irrelevant in this use case.
     */
    @Override
    public Component generateCell(Table source, Object itemId,
                                  Object columnId) {
        // Get the object stored in the cell as a property
        if (columnId.toString().startsWith(COMBO_MAGIC_PREFIX))
            columnId = columnId.toString().substring(COMBO_MAGIC_PREFIX.length());

        Property prop = source.getItem(itemId).getItemProperty(columnId);
        if (prop != null && prop.getValue() != null)
        {
            String txt = prop.getValue().toString();

            for (int i = 0; i < field.getEntries().size(); i++)
            {
                ComboEntry ce = field.getEntries().get(i);
                if (ce.getDbEntry().equals(prop.getValue()) )
                {
                    txt =ce.getGuiEntryKey();
                    break;
                }                
            }
            String s = source.getStyleName();
            Label label = new Label(txt);

            // Set styles for the column: one indicating that it's
            // a value and a more specific one with the column
            // name in it. This assumes that the column name
            // is proper for CSS.

            label.addStyleName("ComboColumn");
            
            return label;
        }
        return null;
    }
}



/**
 *
 * @author Administrator
 */
public   class  BaseDataEditTable<T> extends Table
{
    ArrayList<JPAField> fieldList;
    EditTools tools;
    T activeNode;
    EntityManager em;
    BeanContainer bc;

    static String dateFormat = "dd.MM.yyyy HH:mm";

    public static String getDateFormat()
    {
        return dateFormat;
    }



    public BaseDataEditTable(EntityManager em, BeanContainer bc, ArrayList<JPAField> _fieldList, ItemClickListener listener)
    {
        this.em = em;
        this.bc = bc;
        this.fieldList = _fieldList;

        setSizeFull();

        setContainerDataSource(bc);
        setSelectable(true);
        setMultiSelect(false);
        setImmediate(true); // react at once when something is selected
        setColumnReorderingAllowed(true);
        setColumnCollapsingAllowed(true);

        String[] columns = new String[fieldList.size()];
        String[] columnLabels = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++)
        {
            JPAField jPAField = fieldList.get(i);
            columnLabels[i] = jPAField.getCaption();

            if (jPAField instanceof JPAComboField)
            {
                JPAComboField fld = (JPAComboField)jPAField;

                // COMBOFIELDS HAVE AN OWN COLUMN BECAUSE OF TRANSLATION BETWEEN COMBOENTRIES AND VISUAL ENTRIES
                // WE CREATE A MAGIC COLUMNNAME AND REFER TO A GENERATED COLUMN WHICH HANDLES THE TRANSLATION
                columns[i] = ComboColumnGenerator.COMBO_MAGIC_PREFIX + jPAField.getFieldName();

                addGeneratedColumn(columns[i], new ComboColumnGenerator(fld));
            }
            else
            {
                columns[i] = jPAField.getFieldName();
            }

            setItemCaption(items, ALIGN_LEFT);
        }

        setVisibleColumns(columns);
        setColumnHeaders(columnLabels);


        // Add a footer click handler
        if (listener != null)
            addListener(listener);

    }

    @Override
    protected String formatPropertyValue(Object rowId,
            Object colId, Property property)
    {
        // Format by property type
        if (property.getType() == Date.class)
        {
            SimpleDateFormat df = new SimpleDateFormat(dateFormat);
            return df.format((Date)property.getValue());
        }

        return super.formatPropertyValue(rowId, colId, property);
    }

    public ArrayList<JPAField> getFieldList()
    {
        return fieldList;
    }

    EditTools createEditTools()
    {
        if (tools == null)
        {
            TableEditor editor = new TableEditor()
            {
                @Override
                public void setReadonly( boolean b )
                {
                    for (int i = 0; i < fieldList.size(); i++)
                    {
                        fieldList.get(i).setReadOnly(b);
                    }
                }

                @Override
                public void action_new()
                {
                    activeNode = newActiveObject();

                    if (activeNode != null)
                    {
                        updateItem( activeNode );
                    }
                    requestRepaint();
                }

                @Override
                public void action_del()
                {
                    deleteActiveObject();

                    requestRepaint();
                }

                @Override
                public void action_sav()
                {
                    saveActiveObject();

                    updateItem( activeNode );
                    requestRepaint();
                }
            };
            tools = new EditTools( editor );
        }

        tools.getEditor().setReadonly(tools.isReadOnly());

        return tools;
    }

    public AbstractLayout createEditWin(T node)
    {
         activeNode = node;
        final VerticalLayout editWin  = new VerticalLayout();
        editWin.setSizeFull();
        editWin.setMargin(true);
        editWin.setSpacing(true);
        editWin.setStyleName("editWin");
        final BaseDataEditTable b = this;



        // CALLBACK TO ENABLE SAVE BUTTON
        com.vaadin.data.Property.ValueChangeListener changeListener = new com.vaadin.data.Property.ValueChangeListener()
        {
            @Override
            public void valueChange( com.vaadin.data.Property.ValueChangeEvent event )
            {
                if (tools != null)
                {
                    tools.enableSave(true);
                }
            }
        };

        // CREATE GUI ITEMS
        for (int i = 0; i < fieldList.size(); i++)
        {
            JPAField jPAField = fieldList.get(i);
            editWin.addComponent( jPAField.createGui(node, changeListener) );
        }

        // CREATE EDITBUTTONLIST
        EditTools nodeEditTools = createEditTools();

        // BUILD PANEL
        VerticalLayout vl = new VerticalLayout();
        vl.setSizeFull();
        vl.addComponent(editWin);
        vl.setExpandRatio(editWin, 1.0f);
        vl.addComponent(nodeEditTools);

        return vl;
    }
    void deleteActiveObject()
    {
    }
    T newActiveObject()
    {
        return null;
    }


    void saveActiveObject()
    {        
        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try
        {
            em.merge(activeNode);
            tx.commit();

            updateItem( activeNode );
            this.requestRepaint();
//            updateValues();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            tx.rollback();
        }
    }
    public void updateItem(Object o)
    {
        Object itemId = null;


        // UPDATE BY SETTING PROPERTY AGAIN
        try
        {
            Method getIdx = o.getClass().getMethod("getIdx");
            itemId = getIdx.invoke(o, (Object[])null);

            if (bc.containsId(itemId))
            {
                BeanItem oldItem = bc.getItem(itemId);
                if (oldItem != null)
                {
                    for (int i = 0; i < fieldList.size(); i++)
                    {
                        String property = fieldList.get(i).getFieldName();
                        Object v = oldItem.getItemProperty(property).getValue();                        
                        oldItem.getItemProperty(property).setValue(v);
                    }
                }
            }
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
    }

}
