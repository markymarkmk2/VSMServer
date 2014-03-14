/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.net.interfaces.AgentApi;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class TestAgent
{

    static void check_fs_speed( int port, boolean ssl, String keystore, String keypwd )
    {
        System.setProperty("javax.net.ssl.trustStore", keystore);

        try
        {
            InetAddress addr = InetAddress.getLocalHost();
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, "net", ssl, true, 2000, 2000);

            AgentApi api = (AgentApi) factory.create(AgentApi.class);


            Properties p = api.get_properties();
            System.out.println("Agent ver: " + p.getProperty(AgentApi.OP_AG_VER));

            ArrayList<RemoteFSElem> list = api.list_roots();
            s = System.currentTimeMillis();
            last_s = s;

            for (int i = 0; i < list.size(); i++)
            {
                RemoteFSElem root_elem = list.get(i);
                System.out.println("Root: " + root_elem.getPath());


                if (!get_dir_listing(api, root_elem))
                {
                    break;
                }
            }

            long now = System.currentTimeMillis();


            double dur_s = (now - s) / 1000.0;
            System.out.println("It took " + Double.toString(dur_s) + " s to check " + max_t + " entries: " + total_d_count + " dirs, " + total_f_count + " files (" + Long.toString((long) (max_t / dur_s)) + "/s");


        }
        catch (Exception malformedURLException)
        {
            System.out.println("Err: " + malformedURLException.getMessage());
        }
    }
    static int total_f_count = 0;
    static int total_d_count = 0;
    static long s;
    static long last_s;
    static int last_t;
    static int max_t = 100 * 1000 * 1000;

    static boolean get_dir_listing( AgentApi api, RemoteFSElem dir )
    {
        ArrayList<RemoteFSElem> flist = api.list_dir(dir, true);
        for (int j = 0; j < flist.size(); j++)
        {
            RemoteFSElem f_elem = flist.get(j);

            if (f_elem.isDirectory())
            {
                //System.out.println("Dir : " + f_elem.getPath() + " CTime: " + f_elem.getCtimeMs().toString());
                total_d_count++;
                if (check_stat())
                {
                    return false;
                }
                if (!get_dir_listing(api, f_elem))
                {
                    return false;
                }
            }
            else
            {
                //System.out.println("File: " + f_elem.getPath() + " Len: " + f_elem.getDataSize() + " CTime: " + f_elem.getCtimeMs().toString());
                total_f_count++;
                if (check_stat())
                {
                    return false;
                }
            }

        }
        return true;
    }

    static void check_throughput( int port, boolean ssl, String keystore, String keypwd, long len, int bsize )
    {
        System.setProperty("javax.net.ssl.trustStore", keystore);

        try
        {
            InetAddress addr = InetAddress.getLocalHost();
            RemoteCallFactory factory = new RemoteCallFactory(addr, port, "net", ssl, /*tcp*/ true, 2000, 2000);

            AgentApi api = (AgentApi) factory.create(AgentApi.class);


            Properties p = api.get_properties();
            System.out.println("Agent ver: " + p.getProperty(AgentApi.OP_AG_VER));

            s = System.currentTimeMillis();
            last_s = s;

            long blocks = (int)(len / bsize);

            for (int i = 0; i < blocks; i++)
            {
                byte[] ret = api.fetch_null_data(bsize);

                if (i == 0 || i == blocks - 1)
                {
                    for (int k = 0; k < ret.length; k++)
                    {
                        if (ret[k] != (byte)(k&0xFF))
                        {
                            System.out.println("Data error");
                            break;
                        }
                    }
                }
            }

            long now = System.currentTimeMillis();


            double dur_ms = (now - s);
            System.out.println("It took " + Double.toString(dur_ms / 1000) + " s to fetch " + blocks + " entries of " + bsize/1024 + " kB " +
                    Long.toString((long) ((len/1024.0) / dur_ms)) + "MB/s");

        }
        catch (Exception malformedURLException)
        {
            malformedURLException.printStackTrace();
            System.out.println("Err: " + malformedURLException.getMessage());
        }
    }



    static boolean check_stat()
    {
        int t = total_d_count + total_f_count;

        long now = System.currentTimeMillis();

        if (now - last_s > 1000)
        {
            double dur_s = (now - last_s) / 1000.0;
            last_s = now;

            System.out.println("Found " + t + " entries: " + total_d_count + " dirs, " + total_f_count + " files (" + Long.toString((long) ((t - last_t) / dur_s)) + " entries/s)");
            last_t = t;
        }
        return (t > max_t);
    }

    public static void main( String[] args ) throws Exception
    {
        //check_fs_speed(8082, false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456");
        long len = 10000L*1000*1000;
        //check_throughput(8082, false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456", len, 8*1024);
        //check_throughput(8082, false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456", len, 32*1024);
//        check_throughput(8082, false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456", len, 64*1024);
//        check_throughput(8082, false, "J:\\Develop\\VSM\\Eval\\ProjectEval\\vsmkeystore2.jks", "123456", len, 512*1024);
//        len *= 2;
        check_throughput(8082, false, "vsmkeystore2.jks", "123456", len, 1024*1024);
        
    }
}
