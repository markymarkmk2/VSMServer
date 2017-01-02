/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.Utilities;

import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.Main;
import de.dimm.vsm.license.DemoLicenseTicket;
import de.dimm.vsm.license.HWIDLicenseTicket;
import de.dimm.vsm.license.LicenseTicket;
import de.dimm.vsm.license.ValidTicketContainer;
import de.dimm.vsm.log.LogManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 *
 * @author mw
 */
public class LicenseChecker
{

    private static final String LICENSE_PATH = "license/";
    private static final String NEW_LICENSE_PATH = "licenses/";

    private static final long DEMO_TICKET_DAYS = 30;  // 30 DAYS DEMO AT STARTUP

    private final List<ValidTicketContainer> ticket_list;
    private static final String LICFILE_NAME = "license.xml";

    final String lic_mtx = "lic_lock";

    private final String productBase;
    private String overrideLicPath;

    public LicenseChecker(String productBase, int demo_units, int demo_modules)
    {
        this.productBase = productBase;
        ticket_list = new ArrayList<>();

        File lic_path = getRealLicPath();

        if (!lic_path.exists()) {
            lic_path.mkdir();
        }
        check_create_demo_ticket(demo_units, demo_modules);
    }

    private File getRealLicPath() {
        File lic_path = new File(NEW_LICENSE_PATH);
        if (!lic_path.exists()) {
            File old_lic_path = new File(LICENSE_PATH);
            if (old_lic_path.exists() && old_lic_path.isDirectory()) {
                lic_path = old_lic_path;
            }
        }
        return lic_path;
    }

    public void setOverrideLicPath( String overrideLicPath ) {
        this.overrideLicPath = overrideLicPath;
    }

    public String getOverrideLicPath() {
        return overrideLicPath;
    }
    
    
    public void create_ticket(int serial, int demo_units, int demo_modules) throws IOException {
        // CREATE        
        HWIDLicenseTicket ticket = new HWIDLicenseTicket();
        
        try
        {
            ticket.createTicket(productBase, serial, demo_units, demo_modules, HWIDLicenseTicket.generate_hwid());

            write_ticket(ticket);
        }
        catch (IOException  iOException)
        {
            LogManager.msg_license( LogManager.LVL_ERR, "Cannot create ticket:", iOException);
            throw new IOException("Cannot create ticket:" + iOException.getMessage(), iOException);
        }
    }

    private void check_create_demo_ticket(int demo_units, int demo_modules)
    {
       // TRICK:
        // INSTALLER CREATES Demo_license.xml, WE DETECT THIS FILE, DELETE IT AND CREATE A REAL DEMO LICENSE
        //IN MailSecurer_license.xml
        // SO THIS WILL HAPPEN ONLY ONCE AFTER INSTALLATION!
        boolean create_lic = false;

        if (exists_ticket("Demo"))
        {
            File trick_demo_file = get_lic_file("Demo");

            // MAKE THIS ONE-SHOT, HEHE
            trick_demo_file.delete();

            if (!exists_ticket(productBase))
                create_lic = true;
        }

        if (create_lic)
        {
            if (!exists_ticket(productBase))
            {
                // CREATE
                Date exp = new Date(System.currentTimeMillis() + (long) DEMO_TICKET_DAYS * 86400 * 1000);
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime(exp);
                // 1 Monat Demo
                cal.add(GregorianCalendar.MONTH, 1);
                DemoLicenseTicket ticket = new DemoLicenseTicket();
                try
                {
                    ticket.createTicket(productBase, demo_units,  demo_modules,
                            cal.get(GregorianCalendar.DAY_OF_MONTH), cal.get(GregorianCalendar.MONTH),
                            cal.get(GregorianCalendar.YEAR));

                    write_ticket(ticket);
                }
                catch (IOException | ParseException iOException)
                {
                    LogManager.msg_license( LogManager.LVL_ERR, "Cannot create demo ticket:", iOException);
                }
            }
        }
    }
    
    public String get_first_hwid()
    {
        try
        {
            String hwid = HWIDLicenseTicket.generate_hwid();
            return hwid;
        }
        catch (IOException iOException)
        {
            LogManager.msg_license( LogManager.LVL_ERR, "Cannot create hwid:", iOException);
            return null;
        }
    }


    ValidTicketContainer get_ticket( String product )
    {
        synchronized(lic_mtx)
        {
        for (int i = 0; i < ticket_list.size(); i++)
        {
            ValidTicketContainer licenseTicket = ticket_list.get(i);
            if (licenseTicket.getTicket().getProduct().equalsIgnoreCase(product))
                return licenseTicket;
        }
        }
        return null;
    }
   
    public boolean is_licensed(String product)
    {
        ValidTicketContainer ticket = get_ticket(product);
        if (ticket != null)
        {
            return ticket.isValid();
        }
        return false;
    }
    public boolean is_licensed(String product, int mod)
    {
        ValidTicketContainer ticket = get_ticket(product);
        if (ticket != null)
        {
            if (ticket.isValid())
            {
                return (ticket.getTicket().hasModule(mod));
            }
        }
        return false;
    }
    
    public void check_licenses()
    {
        read_licenses();
        
     }
    public List<ValidTicketContainer> get_ticket_list()    {
        return ticket_list;
    }

    public int get_max_units(String product)
    {
        ValidTicketContainer ticket = get_ticket( product );
        if (ticket != null)
        {
            if (ticket.isValid())
            {
                return ticket.getTicket().getUnits();
            }
        }
        return 0;
    }

    // THIS CAN BE TIME CONSUMING, NOT TOO OFTEN; IS NOT CRITICAL
    public void do_idle()
    {
        long now = System.currentTimeMillis();

    }

    public boolean exists_ticket(String product)
    {
        // DO NOT RECREATE EVERY TIME, LICENSE FILE HAS TO BE MISSING
        File lic_path = get_lic_file( product );
        return lic_path.exists();
    }
    public int get_serial()
    {
        ValidTicketContainer ticket = get_ticket( productBase );
        if (ticket != null)
        {
            if (ticket.isValid())
            {
                return ticket.getTicket().getSerial();
            }
        }
        return 0;
    }

    File get_lic_file( String product )
    {
        if (overrideLicPath != null) {            
            return new File(overrideLicPath, product + "_" + LICFILE_NAME);
        }
        File lic_path = new File(getRealLicPath(), product + "_" + LICFILE_NAME);
        return lic_path;
    }
    File get_lic_file( LicenseTicket ticket )
    {
        return get_lic_file(ticket.getProduct());
    }

    public boolean delete_license(String product)
    {
        try
        {
            synchronized (lic_mtx)
            {
                File lic_path = get_lic_file(product);
                lic_path.delete();
                ValidTicketContainer vtck = get_ticket(product);
                if (vtck != null)
                {
                    ticket_list.remove(vtck);
                    return true;
                }
            }
        }
        catch (Exception e)
        {
        }
        return false;
    }

    private ValidTicketContainer read_license(String product)
    {
        File lic_path = get_lic_file(product);
        return read_license(lic_path);
    }
    private ValidTicketContainer read_license(File lic_path)
    {
        if (lic_path.exists())
        {
            FileInputStream fis = null;
            XStream xs = new XStream();
            try
            {
                fis = new FileInputStream(lic_path);
                Object o = xs.fromXML(fis);
                if (o instanceof LicenseTicket)
                {
                    LicenseTicket t = (LicenseTicket)o;
                    boolean valid = t.isValid();
                    ValidTicketContainer vtck = new ValidTicketContainer(t, valid);
                    if (valid)
                        LogManager.msg_license( LogManager.LVL_INFO, Main.Txt("Found_valid_license") + ": " + vtck.getTicket().toString());
                    else
                        LogManager.msg_license( LogManager.LVL_ERR,  vtck.getTicket().getLastErrMessage());
                    
                    return vtck;
                }
            }
            catch (Exception exc)
            {
                LogManager.msg_license( LogManager.LVL_WARN,  "Found invalid license ticket " + lic_path + ": " + exc);

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
        }
        else
        {
            LogManager.msg_license( LogManager.LVL_WARN, Main.Txt("No License was found"));
        }
        return null;
    }


    public void write_ticket( LicenseTicket ticket )
    {
        synchronized(lic_mtx)
        {
        File lic_path = get_lic_file( ticket );
        
        FileOutputStream fos = null;
        XStream xs = new XStream();
        try
        {
            fos = new FileOutputStream(lic_path);
            xs.toXML(ticket, fos);
        }
        catch (Exception exception)
        {
            LogManager.msg_license( LogManager.LVL_ERR, Main.Txt("Cannot_write_license_ticket") + ": " + exception.getLocalizedMessage());
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        }
    }

    public void read_licenses()
    {
        synchronized(lic_mtx)
        {
            ticket_list.clear();

            File lic_dir = get_lic_file("1234").getParentFile();

            if (lic_dir.exists()) {
                File[] lic_list = lic_dir.listFiles();
                for (int i = 0; i < lic_list.length; i++) {
                    File file = lic_list[i];
                    if (!file.getName().endsWith("_license.xml")) {
                        continue;
                    }

                    ValidTicketContainer vtck = read_license(file);
                    if (vtck != null) {
                        ticket_list.add(vtck);
                    }
                }
            }
        }
    }
}
