/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.NativeButton;
import de.dimm.vsm.vaadin.GenericMain;

/**
 *
 * @author Administrator
 */
public abstract class GUIElem
{
    protected GenericMain parentWin;
    protected GuiElemActionCallback callback;
    protected NativeButton guiButton;
    protected MenuItem guiMenuItem;

    public GUIElem( GenericMain parentWin )
    {
        this.parentWin = parentWin;
    }

    abstract String getItemText();
    abstract void action();

    public MenuItem attachTo( MenuItem it )
    {
        guiMenuItem = it.addItem( getItemText(), getCommand() );
        return guiMenuItem;
    }
    public Button createButton()
    {
        guiButton = new NativeButton(getItemText(), getClickListener());
        guiButton.setStyleName("loginButton");
        return guiButton;
    }

    protected String Txt(String s)
    {
        return GenericMain.Txt(s);
    }

    public void setCallback( GuiElemActionCallback callback )
    {
        this.callback = callback;
    }
    public void updateGui()
    {
        if (guiButton != null)
            guiButton.setCaption(getItemText());
        if (guiMenuItem != null)
            guiMenuItem.setText(getItemText());
    }
    


    public Command getCommand()
    {
        Command loginCommand = new Command()
        {
            @Override
            public void menuSelected(MenuItem selectedItem)
            {
                action();
            }
        };
        return loginCommand;
    }
    public ClickListener getClickListener()
    {
        ClickListener listener = new ClickListener() {

            @Override
            public void buttonClick( ClickEvent event )
            {
                action();
            }
        };
        return listener;
    }


}
