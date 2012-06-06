/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.Utilities;

import de.dimm.vsm.CS_Constants;
import de.dimm.vsm.GeneralPreferences;
import de.dimm.vsm.Main;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.ScheduleStatusEntry;

/**
 *
 * @author Administrator
 */
public class StatCounter {

    long statTotalSize;
    long statTotalFiles;
    long statTotalDirs;
    long statCheckedBlocks;
    long statDedupBlocks;

    long lastStatTotalSize;
    long lastStatTotalFiles;
    long lastStatTotalDirs;
    long lastStatCheckedBlocks;
    long lastStatDedupBlocks;

    long lastStamp;
    long firstStamp;

    long statTransferedSize;
    long statTransferedBlocks;
    long statTransferedFiles;
    long statTransferedDirs;
    long lastStatTransferedSize;
    long lastStatTransferedBlocks;
    long lastStatTransferedFiles;
    long lastStatTransferedDirs;
    double bytePerSec;
    double filesPerSec ;
    double byteTransferedPerSec;
    double filesTransferedPerSec;
    double dirsTransferedPerSec;
    long statDhbCacheHit = 0;
    long statDhbCacheMiss = 0;
    long statDhbCacheSize = 0;

    double speedPerSec;


    String name;
    int blocksize;

    public StatCounter(String name)
    {
        this.name = name;
        initStat();
    }



    public final void initStat()
    {
        statTotalSize = 0;
        statTotalFiles = 0;
        statTotalDirs = 0;
        lastStatTotalSize = 0;
        lastStatTotalFiles = 0;
        lastStatTotalDirs = 0;

        lastStamp = System.currentTimeMillis();
        firstStamp = System.currentTimeMillis();

        statTransferedSize = 0;
        statTransferedFiles = 0;
        statTransferedDirs = 0;
        statTransferedBlocks = 0;
        lastStatTransferedSize = 0;
        lastStatTransferedFiles = 0;
        lastStatTransferedDirs = 0;
        lastStatTransferedBlocks = 0;

        bytePerSec = 0;
        filesPerSec = 0 ;
        byteTransferedPerSec = 0;
        filesTransferedPerSec = 0;
        dirsTransferedPerSec = 0;
        statDhbCacheHit = 0;
        statDhbCacheMiss = 0;
        statDhbCacheSize = 0;

        blocksize = Main.get_int_prop(GeneralPreferences.FILE_HASH_BLOCKSIZE, CS_Constants.FILE_HASH_BLOCKSIZE);

    }

    public void addTransferLen( int l )
    {
        statTransferedSize += l;
    }
    public void addTransferBlock()
    {
        statTransferedBlocks++;
    }
    public void addCheckBlock()
    {
        statCheckedBlocks++;
    }
    public void addDedupBlock()
    {
        statDedupBlocks++;
    }
    public void addTotalStat( RemoteFSElem remoteFSElem )
    {
        if (remoteFSElem.isDirectory())
        {
            statTotalDirs++;
        }
        else
        {
            statTotalFiles++;
            statTotalSize += remoteFSElem.getDataSize();
        }
    }
    public void addTransferStat( RemoteFSElem remoteFSElem )
    {
        if (remoteFSElem.isDirectory())
        {
            statTransferedDirs++;
        }
        else
        {
            statTransferedFiles++;
        }
    }
    public void check_stat()
    {
        check_stat(false);
    }
    public void check_stat(boolean force)
    {
        long now = System.currentTimeMillis();
        double diff = now - lastStamp;

        if (diff > 1000 | force)
        {
            // MB / S
            if (diff == 0)
                diff = 10;
            
            bytePerSec = (statTotalSize - lastStatTotalSize)*1000 / diff;
            filesPerSec = ((statTotalFiles - lastStatTotalFiles)*1000) / (diff);
            byteTransferedPerSec = (statTransferedSize - lastStatTransferedSize)*1000 / diff;
            filesTransferedPerSec = ((statTransferedFiles - lastStatTransferedFiles)*1000) / diff;
            dirsTransferedPerSec = ((statTransferedDirs - lastStatTransferedDirs)*1000) / diff;

            speedPerSec = filesPerSec;
            double blocksPerSec = (statCheckedBlocks - lastStatCheckedBlocks)*1000.0 / diff;
            blocksPerSec += (statTransferedBlocks - lastStatTransferedBlocks)*1000.0 / diff;
            blocksPerSec += (statDedupBlocks - lastStatDedupBlocks)*1000.0 / diff;
            speedPerSec += blocksPerSec*blocksize;

            SizeStr bps_str = new SizeStr(bytePerSec);
            SizeStr fps_str = new SizeStr(filesPerSec, true);
            SizeStr btps_str = new SizeStr(byteTransferedPerSec);
            SizeStr ftps_str = new SizeStr(filesTransferedPerSec, true);
            SizeStr st = new SizeStr(statTotalSize);
            SizeStr ft = new SizeStr(statTotalFiles, true);
            SizeStr dt = new SizeStr(statTotalDirs, true);
            SizeStr sx = new SizeStr(statTransferedSize);
            SizeStr fx = new SizeStr(statTransferedFiles, true);

            if (Main.isPerformanceDiagnostic())
            {
                System.out.println("Status for " + name );
                System.out.println("Total : " + statTotalSize + " (" + st.toString() + "), " + statTotalDirs + " (" + dt.toString() + ") Dirs, " + statTotalFiles + " (" + ft.toString() + ") Files, Speed: " + bps_str.toString()  + "/s  Files: " + fps_str.toString() + "/s");
                System.out.println("Transf: " + statTransferedSize + " (" + sx.toString() + "), " + statTransferedFiles + " (" + fx.toString() + " Files, Speed: " + btps_str.toString()  + "/s  Files: " + ftps_str.toString() + "/s");
                System.out.println("Blocks: " + statCheckedBlocks + " checked,  " + statTransferedBlocks + " transfered, " + statDedupBlocks + " dedup");
                int pc = 0;
                long cnt = statDhbCacheHit + statDhbCacheMiss;
                if (cnt > 0)
                    pc = (int)((statDhbCacheHit*100.0) / cnt);

                System.out.println("DhbCacheSize: " + statDhbCacheSize + " CacheHits: " + statDhbCacheHit + " CacheMisses: " + statDhbCacheMiss + " (" + pc + "%)");
            }

            lastStamp = now;

            lastStatTotalSize = statTotalSize;
            lastStatTotalFiles = statTotalFiles;
            lastStatTotalDirs = statTotalDirs;
            lastStatTransferedSize = statTransferedSize;
            lastStatTransferedFiles = statTransferedFiles;
            lastStatTransferedDirs = statTransferedDirs;
            lastStatDedupBlocks = statDedupBlocks;
            lastStatCheckedBlocks = statCheckedBlocks;
            lastStatTransferedBlocks = statTransferedBlocks;
        }
    }

    public void add( StatCounter stat )
    {
        lastStatTotalSize = statTotalSize;
        lastStatTotalFiles = statTotalFiles;
        lastStatTotalDirs = statTotalDirs;
        lastStatTransferedSize = statTransferedSize;
        lastStatTransferedFiles = statTransferedFiles;
        lastStatTransferedDirs = statTransferedDirs;
        lastStatTransferedBlocks = statTransferedBlocks;
        

        statTotalSize += stat.statTotalSize;
        statTotalFiles += stat.statTotalFiles;
        statTotalDirs += stat.statTotalDirs;

        statTransferedSize += stat.statTransferedSize;
        statTransferedFiles += stat.statTransferedFiles;
        statTransferedDirs += stat.statTransferedDirs;

        statTransferedBlocks += stat.statTransferedBlocks;
        statCheckedBlocks += stat.statCheckedBlocks;
        statDedupBlocks += stat.statDedupBlocks;
        statDhbCacheSize += stat.statDhbCacheSize;
        statDhbCacheHit += stat.statDhbCacheHit;
        statDhbCacheMiss += stat.statDhbCacheMiss;
    }

    public void setName( String name )
    {
        this.name = name;
    }

    public void fillStatusEntry(ScheduleStatusEntry entry)
    {
        SizeStr bps_str = new SizeStr(bytePerSec);
        SizeStr fps_str = new SizeStr(filesPerSec, true);
        SizeStr btps_str = new SizeStr(byteTransferedPerSec);
        SizeStr ftps_str = new SizeStr(filesTransferedPerSec, true);
        SizeStr st = new SizeStr(statTotalSize);
        SizeStr ft = new SizeStr(statTotalFiles, true);
        SizeStr dt = new SizeStr(statTotalDirs, true);
        SizeStr sx = new SizeStr(statTransferedSize);
        SizeStr fx = new SizeStr(statTransferedFiles, true);


        entry.add( Main.Txt("Data_total"), "stotal", st );
        entry.add( Main.Txt("Files_total"), "ftotal", ft );
        entry.add( Main.Txt("Speed"), "bps", bps_str );
        entry.add( Main.Txt("Files/s"), "fps", fps_str );
        entry.add( Main.Txt("Data_transfered"), "sx", sx );
        entry.add( Main.Txt("Files_transfered"), "fx", fx );
        entry.add( Main.Txt("Speed_transfered"), "btps", btps_str );
        entry.add( Main.Txt("Files/s_transfered"), "ftps", ftps_str );
        entry.add( Main.Txt("Blocks_checked"), "cblocks", statCheckedBlocks );
        entry.add( Main.Txt("Blocks_transfered"), "tblocks", statTransferedBlocks );
        entry.add( Main.Txt("Blocks_deduped"), "dblocks", statDedupBlocks );
        entry.add( Main.Txt("DhbCacheHit"), "dhbHit", statDhbCacheHit );
        entry.add( Main.Txt("DhbCacheMiss"), "dhbMiss", statDhbCacheMiss );
    }

    public double getByteTransferedPerSec()
    {
        return byteTransferedPerSec;
    }

    public double getFilesTransferedPerSec()
    {
        return filesTransferedPerSec;
    }
    public double getDirsTransferedPerSec()
    {
        return dirsTransferedPerSec;
    }

    public long getByteTransfered()
    {
        return statTransferedSize;
    }

    @Override
    public String toString()
    {
        SizeStr st = new SizeStr(statTotalSize);
        SizeStr ft = new SizeStr(statTotalFiles, true);
        SizeStr dt = new SizeStr(statTotalDirs, true);
        SizeStr sx = new SizeStr(statTransferedSize);
        return st.toString() + ", " + sx.toString() + " " + Main.Txt("neu")  + ", " +  statTotalDirs + " (" + dt.toString() + ") " +  Main.Txt("Verz.") + ", "+ statTotalFiles + " (" + ft.toString() + ") " + Main.Txt("Dat.");
    }

    public long getDedupBlocks()
    {
        return statDedupBlocks;
    }

    public long getFilesChecked()
    {
        return statTotalFiles;
    }

    public long getFilesTransfered()
    {
        return statTransferedFiles;
    }

    public long getDataChecked()
    {
        return statTotalSize;
    }

    public long getDataTransfered()
    {
        return statTransferedSize;
    }

    // SAME AS getDataChecked, BUT BETTER NAME FOR HOTFOLDER, ARCHIVE ETC.
    public long getTotalSize()
    {
        return statTotalSize;
    }

    public void addDhbCacheHit()
    {
        statDhbCacheHit++;
    }
    public void addDhbCacheMiss()
    {
        statDhbCacheMiss++;
    }

    public void setDhbCacheSize( long statDhbCacheSize )
    {
        this.statDhbCacheSize = statDhbCacheSize;
    }

    public double Speed()
    {
        return speedPerSec;
    }



    

}
