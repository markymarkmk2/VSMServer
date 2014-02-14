/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm;

import de.dimm.vsm.log.Log;
import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.Utilities.TextProvider;
import de.dimm.vsm.fsengine.JDBCEntityManager;
import de.dimm.vsm.records.TextBase;
import de.dimm.vsm.text.MissingTextException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class TextBaseManager implements TextProvider
{
    static HashMap<String, String> missing_transl_tokens = new HashMap<>();
    static HashMap<String, String> transl_tokens = new HashMap<>();
    
    static char[] invalidChars = {'\n', '\r','\'','\"', ';'};

    private static boolean isValidTextEntry( String messageId )
    {
        for (int i = 0; i < invalidChars.length; i++)
        {
            char ch = invalidChars[i];
            if (messageId.indexOf(ch) != -1)
                return false;
        }
        return true;
    }
    static String toExp( String s )
    {        
        String t = s.replaceAll("\n", "\\\\n");
        t = t.replaceAll("\t", "\\\\t");
        return t;
    }
    static String fromExp( String s )
    {
        String t = s.replaceAll("\\\\n", "\n");
        t = t.replaceAll("\\\\t", "\t");
        return t;
    }

    String lang;
    private static boolean openTxtGuiException;

    public TextBaseManager( String lang )
    {
        setLang(lang);
    }

    public static void setOpenTxtGuiException( boolean openTxtGuiException )
    {
        TextBaseManager.openTxtGuiException = openTxtGuiException;
    }
       
    public final void setLang( String lang )
    {
        if (this.lang == null || !this.lang.equals( lang ) )
        {
            transl_tokens.clear();
            missing_transl_tokens.clear();
            this.lang = lang;
        }
    }



    @Override
    public String GuiTxt( String key )
    {
        return TxtWithExc(key, openTxtGuiException);
    }
    @Override
    public String Txt( String key )
    {
        return TxtWithExc(key, false);
    }


    public String TxtWithExc( String key, boolean exc  )
    {
        if (key == null)
            return "";

        // MAGIC MSG FÜR UPADTE
        if (key.equals("$UPD$"))
            return checkUpdate();
        
        // CHECK FOR NEW TEXTUPDATES
        checkUpdate();


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

        if (exc && ret == null)
        {
            throw new MissingTextException(key);
        }
        
        if (ret == null)
        {
            ret = key.replace('_', ' ');
            missing_transl_tokens.put(key, ret);
        }
        return ret;
    }

    private static String checkUpdate()
    {
        File f = new File("VsmTextBaseUpdate.txt");
        if (!f.exists())
            return "not found";

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
            return "exc: " + iOException.getMessage();
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

        List<TextBase> ret = new ArrayList<>();
        if (ret.getClass().isAssignableFrom(o.getClass()))
        {
            ret = (List<TextBase>) o;
        }
        if (ret == null)
        {
            Log.err( "TextDB-Update kann nicht gelesen werden" );
            return "TextDB-Update kann nicht gelesen werden";
        }
        
        JDBCEntityManager em = LogicControl.get_txt_em();
        em.check_open_transaction();
        for (int i = 0; i < ret.size(); i++)
        {
            TextBase newTextBase = ret.get(i);
            if (!isValidTextEntry(newTextBase.getMessageId()) ||
                 !isValidTextEntry(newTextBase.getMessageText())    )
            {
                String msg = "Fehler beim Update der Textdatenbank, ungültige Zeichen in Eintrag " + newTextBase.getMessageId();
                Log.err( msg );
                return msg;
            }
                
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
                em.commit_transaction();
            }
            catch (SQLException sQLException)
            {
                Log.err( "Fehler beim Update der Textdatenbank", sQLException);
                return "TextDB-Update kann nicht gelesen werden";
            }
        } 
        return "added " + ret.size();
    }
    
    public static void importTextCsv(String filename, String code) throws IOException, SQLException            
    {
        File f = new File(filename);
        if (!f.exists())
            throw new IOException(filename + " not found");

        Charset cs = Charset.forName(code);
        
        String langCode = null;

        Object o = null;
        BufferedReader fr = null;
        int cnt = 0;
        try
        {
            fr = new BufferedReader( new InputStreamReader( new FileInputStream(f), cs) );
            boolean done = false;
            while (!done)
            {
                String line = fr.readLine();
                if (line == null)
                    break;
                line = line.trim();
                if (line.startsWith("#"))
                    continue;
                int idx = line.indexOf(';');
                
                // Einzelene Zeile: Ländercode
                if (idx == -1)
                {
                    langCode = line.trim().toUpperCase();
                    
                    if (langCode.length() != 2)
                    {
                        throw new IOException("Ungültiger Ländercode: " + langCode +"\nFormat: je Zeile Ländercode 2-stellig oder Key;Value");
                    }
                    Log.debug("Aktuelle Sprache: " + langCode);
                    continue;
                }
                String key = line.substring(0,idx);
                String val = line.substring(idx + 1);
                
                if (val.isEmpty())
                {
                    val = key.replace('_', ' ');
                }
                
                if (langCode == null)
                     throw new IOException("Ländercode fehlt: " + line);                
                
                addToTb( langCode, key, val);
                cnt++;
            }            
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        Log.debug("Insg. " + cnt + " neue Texte in Textbank eingelesen ");
        
    }
    
    private static void addToTb( String langCode, String key, String val)  throws IOException, SQLException      
    {            
        // ONESHOT
        TextBase newTextBase = new TextBase();
        newTextBase.setLangId(langCode);
        newTextBase.setMessageId(key);
        newTextBase.setMessageText(val);
        
        JDBCEntityManager em = LogicControl.get_txt_em();
        em.check_open_transaction();
        String qry = "select x from TextBase x where T1.langId='" + newTextBase.getLangId() + "' and T1.messageId='" + newTextBase.getMessageId() + "'";
        List<TextBase> l = em.createQuery(qry, TextBase.class);
        if (l.isEmpty())
        {
            em.em_persist(newTextBase);
        }
        else
        {
            if (!l.get(0).getMessageText().equals(newTextBase.getMessageText()))
            {
                l.get(0).setMessageText(newTextBase.getMessageText());
                em.em_merge(l.get(0));
            }
        }
        em.commit_transaction();
    }  
    
    public static void exportTextCsv(String filename, String code) throws IOException, SQLException        
    {        
        File f = new File(filename);
        
        String qry = "select x from TextBase x order by T1.langId";        
        JDBCEntityManager em = LogicControl.get_txt_em();
        List<TextBase> l = em.createQuery(qry, TextBase.class);
               
        Charset cs = Charset.forName(code);
        
        String langId = "";

        BufferedWriter fw = null;
        try
        {
            fw = new BufferedWriter( new FileWriter(f) );
            for (TextBase textBase : l)
            {
                if (!langId.equals(textBase.getLangId()))
                {
                    langId = textBase.getLangId();
                    fw.write(langId + "\n");
                }
                String key = new String(textBase.getMessageId().getBytes( cs));
                String val =  new String(textBase.getMessageText().getBytes( cs));
                
                key = toExp(key);
                val = toExp(val);
                fw.write( key + ";" + val + "\n");
            }
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        Log.debug("Insg. " + l.size() + " Texte in " + filename + " geschrieben");                 
    }    
}
