/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.records.TextBase;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class TextBaseManager
{
    static HashMap<String, String> missing_transl_tokens = new HashMap<String, String>();
    static HashMap<String, String> transl_tokens = new HashMap<String, String>();

    public static String lastLang;


    public static String Txt(String key, String lang )
    {
        if (key == null)
            return "";

        // CHECK FOR NEW TEXTUPDATES
        checkUpdate();

        if (lastLang == null || !lastLang.equals( lang ) )
        {
            transl_tokens.clear();
            missing_transl_tokens.clear();
            lastLang = lang;
        }

        // CACHED
        String val = transl_tokens.get(key);
        if (val != null)
            return val;

        // CACHED MISSING
        if (missing_transl_tokens.containsKey(key))
        {
            return missing_transl_tokens.get(key);
        }

        // A NEW TEXT


        String de = null;
        String en = null;
        String la = null;

        String ret = null;

        try
        {
            List<TextBase> l = LogicControl.get_txt_em().createQuery("select x from TextBase x where T1.messageId='" + key + "'", TextBase.class);
            
            for (int i = 0; i < l.size(); i++)
            {
                TextBase textBase = l.get(i);
                if (textBase.getLangId().equals(lang))
                {
                    la = textBase.getMessageText();
                    break;
                }
                // FALLBACKS
                if (textBase.getLangId().equals("DE"))
                {
                    de = textBase.getMessageText();

                }
                if (textBase.getLangId().equals("EN"))
                {
                    en = textBase.getMessageText();

                }
            }

            if (la != null)
            {
                transl_tokens.put(key, la);
                ret = la;
            }
            else
            {
                if (en != null)
                {
                    transl_tokens.put(key, en);
                    ret = en;
                }
                else if (de != null)
                {
                    transl_tokens.put(key, de);
                    ret = de;
                }
            }
        }
        catch (Exception e)
        {
        }

        if (ret == null)
        {
            ret = key.replace('_', ' ');
            missing_transl_tokens.put(key, ret);
        }
        return ret;
    }

    private static void checkUpdate()
    {
        File f = new File("VsmTextBaseUpdate.txt");
        if (!f.exists())
            return;

        XStream xs = new XStream();

        Object o = null;
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(f);
            o = xs.fromXML(fis);

        }
        catch (IOException iOException)
        {
        }
        finally
        {
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }

        // ONESHOT
        f.delete();

        List<TextBase> ret = null;
        if (ret.getClass().isAssignableFrom(o.getClass()))
        {
            ret = (List<TextBase>) o;
        }
        if (ret == null)
        {
            Log.err(  "TextDB-Update kann nicht gelesen werden" );
            return;
        }
        
        JDBCEntityManager em = LogicControl.get_txt_em();
        for (int i = 0; i < ret.size(); i++)
        {
            TextBase newTextBase = ret.get(i);
            String qry = "select x from TextBase x where T1.langId='" + newTextBase.getLangId() + "' and T1.messageId='" + newTextBase.getMessageId() + "'";
            try
            {
                List<TextBase> l = em.createQuery(qry, TextBase.class);
                if (l.isEmpty())
                {
                    em.em_persist(newTextBase);
                }
                else
                {
                    l.get(0).setMessageText(newTextBase.getMessageText());
                    em.em_merge(l.get(0));
                }
            }
            catch (SQLException sQLException)
            {
                Log.err( "Fehler beim Update der Textdatenbank", sQLException);
            }
        }       
    }

}
