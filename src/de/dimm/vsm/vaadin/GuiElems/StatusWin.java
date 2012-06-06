/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.ui.VerticalLayout;
import de.dimm.vsm.vaadin.VSMCMain;

/**
 *
 * @author Administrator
 */
public class StatusWin extends VerticalLayout
{
    VSMCMain main;

    public StatusWin( VSMCMain main )
    {
        this.main = main;
        this.setStyleName("statusWin");
    }



}
