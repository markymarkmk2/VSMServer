/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.txtscan;

import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.tasks.TaskInterface;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class TxtScan
{

   
    static boolean dotest = false;
    static boolean verbose = false;
    public static String CODE = "cp1252";
    static String[] skipFolders = {".svn", "tags"};
    
    public TxtScan()
    {
    }
    
    static void debug(String s)
    {
        System.out.println(s);
    }
    static void error(String s)
    {
        System.err.println(s);
    }
    
    public static void main(String[] args)
    {
        TxtScan main = new TxtScan();  
        if (dotest)
        {
            test();
            System.exit(0);
        }
        // Mac: /Users/mw/Desktop/Develop/VSM/Trunk
//        File start = new File("J:\\Develop\\VSM\\V1.0");
        File start = new File("/Users/mw/Desktop/Develop/VSM/Trunk");
        if (args != null && args.length == 1) {
           
            start = new File(args[0]);
        }
        if (!start.isDirectory()) {
            error("Startpfad " + start.getAbsolutePath() + " muss ein Verzeichnis sein");
            System.exit(1);
        }
        List<String> keys = new ArrayList<>();
        try
        {
            main.scan(start, keys);            
        }
        catch (IOException iOException)
        {
            iOException.printStackTrace();
            System.exit(1);
        }
        try
        {
            File f = new File("txtscan.exp");
            Charset cs = Charset.forName(CODE);
            try (BufferedWriter fw = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(f), cs)))
            {
                Collections.sort(keys);
                for (int i = 0; i < keys.size(); i++)
                {                
                    String val = keys.get(i);
                    //String val =  new String(string.getBytes(cs));
                    val = toExp(val);
                    fw.write(val + "\n");                
                }
                JobInterface.JOBSTATE[] jarr = JobInterface.JOBSTATE.values();
                for (int i = 0; i < jarr.length; i++)
                {
                    JobInterface.JOBSTATE state = jarr[i];
                    fw.write(state.toString() + "\n");
                }
                TaskInterface.TASKSTATE[] tarr =  TaskInterface.TASKSTATE.values();
                for (int i = 0; i < tarr.length; i++)
                {
                     TaskInterface.TASKSTATE state = tarr[i];
                    fw.write(state.toString() + "\n");
                }
                
            }
        }
        catch (IOException iOException)
        {
            iOException.printStackTrace();
            System.exit(1);
        }
    }

    private void scan(File dir,  List<String> keys) throws FileNotFoundException, IOException
    {
        File[] children = dir.listFiles( new FilenameFilter() {

            @Override
            public boolean accept( File dir, String name )
            {
                File file = new File(dir, name);
 
                if (file.isDirectory())
                {
                    for (int i = 0; i < skipFolders.length; i++)
                    {
                        String string = skipFolders[i];
                        if (name.equals(string))
                            return false;
                    }                    
                    return true;
                }
                
                if (file.isFile() && name.endsWith(".java"))
                    return true;
                
                return false;                
            }
        });
        
        for (int i = 0; i < children.length; i++)
        {
            File file = children[i];
            if (file.isDirectory()) {
                scan(file, keys);
            }
            else {
                List<String> list = readTxt( file );
                for (int j = 0; j < list.size(); j++)
                {
                    String string = list.get(j);
                    if (!keys.contains(string))
                        keys.add(string);
                }
            }            
        }        
    }
    
    String readFile(File file) throws FileNotFoundException, IOException
    {
        byte[] data;
        try (InputStream is = new FileInputStream(file))
        {
            data = new byte[(int)file.length()];
            is.read(data);
        }
        return new String(data);
    }

    private List<String> readTxt( File file ) throws FileNotFoundException, IOException
    {
        List<String> list = new ArrayList<>();
         
        String java = readFile(file);
        // Damit das nicht gefunden wird...
        String token = "T" + "xt("; 
        int idx = java.indexOf(token);
        while (idx > 0)
        {
            char prevCh = java.charAt(idx-1);
            if (!Character.isLetterOrDigit(prevCh))
            {
                idx += token.length();
                int endidx = java.indexOf(")", idx);
                if (endidx < 0)
                    throw new IOException(file.getAbsolutePath() + ": Kann Textende nicht finden: " + java.substring(idx - (token.length() + 1), idx + 80));
                String val = java.substring(idx, endidx);
                int cnt = getDblQuoteCnt( val);
                while ((cnt & 1) != 0)
                {
                    endidx = java.indexOf(")", endidx + 1);
                    if (endidx < 0)
                        throw new IOException(file.getAbsolutePath() + ": Kann Textende nicht finden: " + java.substring(idx - (token.length() + 1), idx + 80));
                    val = java.substring(idx, endidx);
                    cnt = getDblQuoteCnt( val);
                }
                val = val.trim();
                if (val.startsWith("\""))
                {             
                    if (verbose)
                        val += "                                     " + file.getAbsolutePath() + ": " + getLines( java, idx);
                    if (!list.contains(val))
                        list.add(val);
                }
                else
                    debug("Skipped: " + val + " in " + file.getName());

                idx= java.indexOf( token, endidx);
            }
            else
            {
                int endidx = idx + 80;
                if (endidx > java.length())
                    endidx = java.length();
                //debug("Skipped: " + java.substring(idx - 1 ,endidx) + " in " + file.getName());
                idx= java.indexOf( token, idx + 1);                
            }
        }
        if (!list.isEmpty())
            debug("Found " + list.size() + " entries in " + file.getAbsolutePath());
        return list;
    }        

    static String toExp( String s )
    {        
        String t = s.replaceAll("\n", "\\\\n");
        t = t.replaceAll("\t", "\\\\t");
        if (t.startsWith("\"") && t.endsWith("\""))
        {
            t = t.substring(1, t.length() - 1);
        }
        return t;
    }    
    private static int getDblQuoteCnt( String val )
    {
        char[] charr = val.toCharArray();
        int cnt = 0;
        boolean escaped = false;
        for (int i = 0; i < charr.length; i++)
        {
            char c = charr[i];
            if (c == '\\')
                escaped = true;
            else
            {
                if (c == '"' && ! escaped)
                    cnt++;
                escaped = false;
            }
        }
        return cnt;
    }
    
    private static void test()
    {
        if (getDblQuoteCnt( "\"blah fasel\"") != 2)
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        if (getDblQuoteCnt( "\"blah fasel\" + \"(Wollen Wir so haben\"") != 4)
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        if (getDblQuoteCnt( "\"blah fasel\" + \"\"") != 4)
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }    

    private int getLines( String java, int idx )
    {
        int cnt = 0;
        char[] charr = java.toCharArray();
        for (int i = 0; i < idx; i++)
        {
            char c = charr[i];
            if (c == '\n')
                cnt++;
        }
        return cnt;
    }
    
    public static List<String> readTxtScan()
    {
        
        List<String> keys = new ArrayList<>();
        
        try
        {
            File f = new File("txtscan.exp");
            Charset cs = Charset.forName(CODE);
            try (BufferedReader fw = new BufferedReader(new InputStreamReader( new FileInputStream(f), cs)))
            {
                while(true)
                {
                    String line = fw.readLine();
                    if (line == null)
                        break;

                    keys.add( line );
                }
            }
        }
        catch (IOException iOException)
        {
            iOException.printStackTrace();
            System.exit(1);
        }
        return keys;
    }
}
