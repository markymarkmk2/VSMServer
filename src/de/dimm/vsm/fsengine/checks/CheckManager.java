/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.checks;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.WorkerParent;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.jobs.CheckJobInterface;
import de.dimm.vsm.jobs.InteractionEntry;
import de.dimm.vsm.jobs.JobInterface;
import de.dimm.vsm.jobs.JobInterface.JOBSTATE;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author mw
 */
public class CheckManager  extends WorkerParent{


    public class CheckJob implements CheckJobInterface {

        JOBSTATE state;
        User user;
        ICheck check;
        Date startTime = new Date();
        InteractionEntry ie;

        public CheckJob( User user, ICheck check) {
            this.state = JOBSTATE.RUNNING;
            this.user = user;
            this.check = check;
            this.ie = null;
        }

        @Override
        public ICheck getCheck() {
            return check;
        }
        
        
  
        @Override
        public JOBSTATE getJobState() {
            return state;
        }

        @Override
        public void setJobState(JOBSTATE jOBSTATE) {
            state = jOBSTATE;
        }

        @Override
        public InteractionEntry getInteractionEntry() {
            
            if (ie == null)
            {
                List<String> userOptions = new ArrayList<>();
                String text = check.fillUserOptions(userOptions);
                ie = new InteractionEntry( text, "", new Date(), /*timeout*/0, userOptions, 0 );
            }
            return ie;
        }

        @Override
        public String getStatusStr() {
            return check.getStatus();
        }

        @Override
        public String getStatisticStr() {
            return check.getStatisticStr();
        }

        @Override
        public Date getStartTime() {
            return startTime;
        }

        @Override
        public Object getResultData() {
            return check.getErrText();
        }

        @Override
        public int getProcessPercent() {
            return check.getProcessPercent();
        }

        @Override
        public String getProcessPercentDimension() {
            return check.getProcessPercentDimension();
        }

        @Override
        public void abortJob() {
            check.abort();
            setJobState(JOBSTATE.ABORTED);
        }

        @Override
        public void run() {
            boolean ret = check.check();
            if (!ret) {
                setJobState(JOBSTATE.FINISHED_ERROR);
                return;
            }
            setJobState(JOBSTATE.NEEDS_INTERACTION);
            while(getJobState() != JOBSTATE.ABORTED){
                if (ie != null)
                {
                    if (ie.wasAnswered())
                    {
                        break;
                    }
                }
                LogicControl.sleep(1000);
            }
            if (ie.wasAnswered()) {
                StringBuffer sb = new StringBuffer();
                if (!check.handleUserChoice(ie.getUserSelect(), sb))
                {
                    setJobState(JOBSTATE.FINISHED_ERROR);
                }
                else
                {
                    setJobState(JOBSTATE.FINISHED_OK);
                }
            }
            else
                setJobState(JOBSTATE.ABORTED);
        }

        @Override
        public User getUser() {
            return user;
        }

        @Override
        public void close() {
            check.close();
        }
        
    }
    public JobInterface createCheckJob(String checkName, Object arg, Object optArg, User user) {
       
        for (int i = 0; i < checks.size(); i++) {
            if (checks.get(i).getName().equals(checkName)) {
                ICheck check = LogicControl.getCheck(checks.get(i).getClassName());
                check.init(arg, optArg);
                CheckJob job = new CheckJob(user, check);
                return job;
                
            }                        
        }

        return null;
    }
    
    static class CheckDescriptor {
        String name;
        String className;

        public String getName() {
            return name;
        }

        public String getClassName() {
            return className;
        }

        public CheckDescriptor(String name, String className) {
            this.name = name;
            this.className = className;
        }
        
    }
    List<CheckDescriptor> checks;
    
    public CheckManager() {
        super("CheckManager");
        
    }
    
    public List<String> getCheckNames() {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < checks.size(); i++) {
            CheckDescriptor check = checks.get(i);
            ret.add(check.getName());
        }
        return ret;
    }
    
    static private void addCheck(List<CheckDescriptor> checkList, String txt, String descTxt) {
        checkList.add( new CheckDescriptor(txt, descTxt));
    }

    @Override
    public boolean initialize() {
        checks = new ArrayList<>();
        addCheck( checks, Main.Txt("Physikalische Hashblöcke prüfen"), "CheckPhysicalHashblockIntegrity");
        addCheck( checks, Main.Txt("VSM-Dateisystem prüfen"), "CheckFSIntegrity");
        addCheck( checks, Main.Txt("VSM-Dateisystem und Node prüfen"), "CheckFSIntegrityWithStorage");
        addCheck( checks, Main.Txt("VSM-Dateisystem und Node und Datenprüfen"), "CheckFSIntegrityWithStorageAndHash");

        
        return true;
    }

    @Override
    public void run() {
        
    }
}
