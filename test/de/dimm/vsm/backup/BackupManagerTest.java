/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.fsengine.ArrayLazyList;
import de.dimm.vsm.records.Job;
import java.util.Date;
import de.dimm.vsm.records.Schedule;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Administrator
 */
public class BackupManagerTest {

    public BackupManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

   
    

    /**
     * Test of check_requirements method, of class BackupManager.
     */
    @Test
    public void testcalcNextStart()
    {
        Schedule sched = new Schedule();
        sched.setName("S1");

        GregorianCalendar baseCal = new GregorianCalendar();
        baseCal.set( GregorianCalendar.HOUR_OF_DAY, 0);
        baseCal.set( GregorianCalendar.MINUTE, 0);
        baseCal.set( GregorianCalendar.SECOND, 0);
        baseCal.set(GregorianCalendar.MILLISECOND, 0);
        baseCal.add(GregorianCalendar.YEAR, -14);

        // BASE IS 00:00:00
        sched.setScheduleStart( baseCal.getTime() );

        // ITS 12:30:00 NOW
        GregorianCalendar checkCal = new GregorianCalendar();
        checkCal.set( GregorianCalendar.HOUR_OF_DAY, 12);
        checkCal.set( GregorianCalendar.MINUTE, 30);
        checkCal.set( GregorianCalendar.SECOND, 0);
        checkCal.set(GregorianCalendar.MILLISECOND, 0);
        long now = checkCal.getTimeInMillis();

        ScheduleStart st = BackupManager.calcNextStart(sched, now);
        assertNull(st);

        // JOB IS 4h CYCLE WITH OFFSET 1h: 01:00 05:00 09:00 13:00 17:00
        Job job = new Job();
        job.setSched(sched);
        sched.setIsCycle(true);
        sched.setCycleLengthMs(4*3600*1000l);
        sched.setScheduleStart( new Date( baseCal.getTime().getTime() + 3600*1000));
        
        ArrayLazyList<Job> jl = new ArrayLazyList<Job>();
        sched.setJobs(jl );
        jl.addIfRealized( job );

        st = BackupManager.calcNextStart(sched, now);
        
        // NEXT START SHOULD BE IN HALF AN HOUR
        assertEquals((st.nextStart - now)/1000, 3600/2);
        
        sched.setCycleLengthMs(24*3600*1000l);
        st = BackupManager.calcNextStart(sched, now);        
        // NEXT START SHOULD BE IN HALF AN HOUR
        assertEquals((st.nextStart - now)/1000, -3600/2 + 12*3600 + 3600);
        
        

        // NOW CHECK IF START TIME IS JUST OUT OF STARTWINDOW
        checkCal.set( GregorianCalendar.HOUR_OF_DAY, 13);
        checkCal.set( GregorianCalendar.MINUTE, 0);
        checkCal.set( GregorianCalendar.SECOND, 1);

        now = checkCal.getTimeInMillis();
        st = BackupManager.calcNextStart(sched, now);

        // NEXT START SHOULD BE 3:59:59
        assertEquals((st.nextStart - now) / 1000, 3*3600 + 59*60 + 59);


        sched.setJobs( new ArrayLazyList<Job>());
        job = new Job();
        job.setSched(sched);
        sched.setIsCycle(false);
        sched.setCycleLengthMs(14*86400*1000l);
        sched.setScheduleStart( new Date( baseCal.getTime().getTime() ));
        job.setOffsetStartMs((13*3600 + 5*60)* 1000);
        job.setDayNumber(0);

        sched.getJobs().addIfRealized( job );
        st = BackupManager.calcNextStart(sched, now);

        // NEXT START SHOULD BE 3:59:59
        
        assertEquals((st.nextStart - now) / 1000, 4*60 + 59);

        job.setDayNumber(3);

        sched.getJobs().addIfRealized( job );
        st = BackupManager.calcNextStart(sched, now);

        // NEXT START SHOULD BE 3:59:59
        assertEquals((st.nextStart - now) / 1000, 3*86400 + 4*60 + 59);
        
        
        
    }

   

}