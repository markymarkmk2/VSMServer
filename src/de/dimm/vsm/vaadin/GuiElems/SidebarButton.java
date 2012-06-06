/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.ui.AbstractComponentContainer;
import com.vaadin.ui.Component;
import com.vaadin.ui.NativeButton;
import java.util.Iterator;

/**
 *
 * @author Administrator
 */
public class SidebarButton extends NativeButton
{

    SidebarButtonCallback callback;

    public SidebarButton(String text, SidebarButtonCallback _callback)
    {
        super( text );
        this.callback = _callback;

        setWidth("100%");
        addListener(new ClickListener()
        {
            @Override
            public void buttonClick( ClickEvent event )
            {

                setSelected();
                
                if (callback != null)
                    callback.action();
            }
        });



    }
    public void setSelected()
    {
        AbstractComponentContainer sidebar = (AbstractComponentContainer)getParent();

        for (Iterator<Component> it = sidebar.getComponentIterator(); it.hasNext();)
        {
            Component object = it.next();
            object.setStyleName(null);
        }

        setStyleName("selected");
    }

    public SidebarButtonCallback getCallback()
    {
        return callback;
    }
    

}
