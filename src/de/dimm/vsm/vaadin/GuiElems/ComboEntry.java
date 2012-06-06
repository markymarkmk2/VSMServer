/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

/**
 *
 * @author Administrator
 */
public class ComboEntry
{
    String dbEntry;
    String guiEntryKey;

    public ComboEntry( String dbEntry, String guiEntryKey )
    {
        this.dbEntry = dbEntry;
        this.guiEntryKey = guiEntryKey;
    }


    public String getGuiEntryKey()
    {
        return guiEntryKey;
    }

    public String getDbEntry()
    {
        return dbEntry;
    }

    
    public boolean isDbEntry( String e )
    {
        return dbEntry.equals(e);
    }
    public boolean isGuiEntry( String e )
    {
        return guiEntryKey.equals(e);
    }


    @Override
    public String toString()
    {
        return guiEntryKey;
    }
}
