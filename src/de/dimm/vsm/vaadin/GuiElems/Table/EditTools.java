package de.dimm.vsm.vaadin.GuiElems.Table;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.NativeButton;
import de.dimm.vsm.Main;



public class EditTools extends HorizontalLayout
{
    TableEditor editor;
    boolean readOnly = true;

    NativeButton bt_new = new NativeButton(Main.Txt("New"));
    NativeButton bt_del = new NativeButton(Main.Txt("Delete"));
    NativeButton bt_save = new NativeButton(Main.Txt("Save"));
    NativeButton cb_edit = new NativeButton(Main.Txt("Edit"));

    @Override
    public boolean isReadOnly()
    {
        return readOnly;
    }

    public TableEditor getEditor()
    {
        return editor;
    }

    public void enableSave( boolean b )
    {
        bt_save.setEnabled(b);
    }

    public EditTools( TableEditor _ed )
    {
        this.editor = _ed;
        bt_new.setEnabled(false);
        bt_del.setEnabled(false);
        bt_save.setEnabled(false);
        
        cb_edit.setStyleName("lockClosed");


        this.addComponent(bt_new);
        this.addComponent(bt_del);
        this.addComponent(bt_save);
        this.addComponent(cb_edit);
        this.setComponentAlignment(bt_new, Alignment.MIDDLE_LEFT);
        this.setComponentAlignment(bt_del, Alignment.MIDDLE_LEFT);
        this.setComponentAlignment(bt_save, Alignment.MIDDLE_LEFT);
        this.setComponentAlignment(cb_edit, Alignment.MIDDLE_RIGHT);

        this.setWidth("100%");
        this.setHeight("40px");
        this.setStyleName("editTools");


        bt_new.addListener( new ClickListener()
        {
            @Override
            public void buttonClick( ClickEvent event )
            {
                if (editor != null)
                    editor.action_new();
            }
        });
        bt_save.addListener( new ClickListener()
        {
            @Override
            public void buttonClick( ClickEvent event )
            {
                if (editor != null)
                    editor.action_sav();
            }
        });
        bt_del.addListener( new ClickListener()
        {
            @Override
            public void buttonClick( ClickEvent event )
            {
                if (editor != null)
                    editor.action_del();
            }
        });

        cb_edit.addListener( new ClickListener()
        {
            @Override
            public void buttonClick( ClickEvent event )
            {
                readOnly = !readOnly;

                cb_edit.setStyleName(!readOnly ? "lockOpen" : "lockClosed");
                bt_new.setEnabled(!readOnly);
                bt_del.setEnabled(!readOnly);
                //bt_save.setEnabled(!readOnly);
                if (editor != null)
                {
                    editor.setReadonly(readOnly);
                }
            }
        });
    }
}