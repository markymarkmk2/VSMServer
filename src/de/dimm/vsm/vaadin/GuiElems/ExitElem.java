/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import de.dimm.vsm.vaadin.GenericMain;

/**
 *
 * @author Administrator
 */
public class ExitElem extends GUIElem
{

    public ExitElem(GenericMain m)
    {
        super(m);
    }

    @Override
    String getItemText()
    {
        return Txt("Exit");
    }

    @Override
    void action()
    {
        parentWin.exitApp();
    }

}
