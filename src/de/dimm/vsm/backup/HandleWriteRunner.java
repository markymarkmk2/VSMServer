/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.backup;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Main;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.log.Log;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.PoolNodeFileLink;
import de.dimm.vsm.records.XANode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class HandleWriteElem
{
    FileHandle fh;
    byte[] data;
    long offset;
    int len;
    boolean written;
    boolean doClose;
    int idx;

    HandleWriteElem()
    {
    }
   
    public HandleWriteElem( FileHandle fh, byte[] data, long offset, int len, boolean doClose, int idx )
    {
        this.fh = fh;
        this.data = data;
        this.offset = offset;
        this.len = len;
        this.doClose = doClose;
        this.idx = idx;
        this.written = false;
    }

    protected void write() throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        fh.writeFile(data, len, offset);
        //System.out.println("Wrote Block " + idx);
    }
    boolean isFiller()
    {
        return false;
    }
}

class NullElem extends HandleWriteElem
{
    public NullElem(  )
    {
    }

    @Override
    protected void write() throws IOException, PathResolveException
    {
    }
    @Override
    boolean isFiller()
    {
        return true;
    }
}

class XABootstrap extends HandleWriteElem
{
    StoragePoolHandler poolhandler;
    XANode node;
    DedupHashBlock block;

    public XABootstrap( StoragePoolHandler poolhandler, XANode node, DedupHashBlock block )
    {
        this.poolhandler = poolhandler;
        this.node = node;
        this.block = block;
    }

    @Override
    protected void write() throws IOException, PathResolveException
    {
        poolhandler.write_bootstrap_data(block, node);
        //System.out.println("Wrote XA BT");

    }
}

class DedupBootstrap extends HandleWriteElem
{
    StoragePoolHandler poolhandler;
    HashBlock node;
    DedupHashBlock block;

    public DedupBootstrap( StoragePoolHandler poolhandler, HashBlock node, DedupHashBlock block )
    {
        this.poolhandler = poolhandler;
        this.node = node;
        this.block = block;
    }

    @Override
    protected void write() throws IOException, PathResolveException
    {
        poolhandler.write_bootstrap_data(block, node);
        //System.out.println("Wrote Dedup BT");
    }
}

class NodeBootstrap extends HandleWriteElem
{
    StoragePoolHandler poolhandler;
    FileSystemElemNode node;

    public NodeBootstrap( StoragePoolHandler poolhandler, FileSystemElemNode node )
    {
        this.poolhandler = poolhandler;
        this.node = node;
    }

    @Override
    protected void write() throws IOException, PathResolveException
    {
        poolhandler.write_bootstrap_data(node);
        //System.out.println("Wrote Node BT");
    }
}

class AttributesBootstrap extends HandleWriteElem
{
    StoragePoolHandler poolhandler;
    FileSystemElemAttributes attr;

    public AttributesBootstrap( StoragePoolHandler poolhandler, FileSystemElemAttributes attr )
    {
        this.poolhandler = poolhandler;
        this.attr = attr;
    }

    @Override
    protected void write() throws IOException, PathResolveException
    {
        poolhandler.write_bootstrap_data(attr);
        //System.out.println("Wrote Node BT");
    }
}



class WriteRunner implements Runnable
{
    final HandleWriteRunner aiw;

    public WriteRunner( HandleWriteRunner aiw )
    {
        this.aiw = aiw;
    }

    @Override
    public void run()
    {
        while (aiw.keepRunning)
        {
            try
            {
                HandleWriteElem elem = aiw.workList.poll(aiw.sleepMilisecondOnEmpty, TimeUnit.MILLISECONDS);
                if (elem != null)
                {
                    if (elem.isFiller())
                        continue;
                    
                    elem.write();

                    // BLOCKING FOR CHECKING IS READY
                    try
                    {
                        aiw.readyLock.lock();
                        elem.written = true;
                        aiw.isReady.signal();
                        
                    }
                    finally
                    {
                        try
                        {
                            if (elem.doClose)
                            {
                                elem.fh.close();
                            }
                        }
                        catch (Exception iOException)
                        {
                            Log.err( "Exception on close in WriteRunner", iOException );
                        }
                        aiw.readyLock.unlock();
                    }
                }
            }
            catch (Exception e)
            {
                Log.err( "Exception in WriteRunner", e );
                aiw.writeError = true;
            }
        }
        aiw.isWRunning = false;
    }
}

/**
 *
 * @author Administrator
 */
public class HandleWriteRunner
{
    public static final int MAX_WRITE_QUEUE_LEN = 1000;
    public static final int DATABLOCK_PER_FILLER = 100*1024;  // EVERY 100KB WE HAVE A FILLER -> 1MB BLOCK ADDS 10 FILLERS
    final BlockingQueue<HandleWriteElem> workList;

    

    boolean keepRunning = true;
    boolean isWRunning;
    boolean writeError;

    long sleepMilisecondOnEmpty = 100;
    ReentrantLock readyLock;
    Condition isReady;
    boolean isIdle;

    HandleWriteElem lastAddedElem;
    Thread writeThread;
    int lastQueueLen;

    NullElem filler = new NullElem();



    public HandleWriteRunner()
    {
        workList = new ArrayBlockingQueue<>(MAX_WRITE_QUEUE_LEN);
        
        readyLock = new ReentrantLock();
        isReady = readyLock.newCondition();

        writeThread = new Thread( new WriteRunner(this), "WriteRunner");

        startThread();
    }

    private void startThread()
    {
        writeThread.start();
    }
    public void close()
    {
        try
        {
            flush();
        }
        catch (Exception exc)
        {
        }
        keepRunning = false;
        try
        {
            writeThread.join(30 * 1000);
        }
        catch (InterruptedException interruptedException)
        {
        }
        if (writeThread.isAlive())
            Log.err("writeThread.join timeout");
        lastAddedElem = null;
    }

    int idxCnt = 0;
    void waitForFinish() throws InterruptedException
    {
        try
        {
            readyLock.lock();

            // THIS IS SYNCHRONIZING WITH LAST READY READY CONDITION AFTER WRITE OF LAST_ELEM
            if (lastAddedElem != null && !lastAddedElem.written)
            {
                // AWAIT RELEASES AND REAQUIRES LOCK
                if (!isReady.await(5000, TimeUnit.MILLISECONDS))
                {
                    Log.err("waitForFinish failed, remaining size: " + workList.size());
                }
            }
        }
        catch(Exception exc)
        {
            Log.err("waitForFinish failed", exc);
            writeError = true;
        }
        finally
        {
            readyLock.unlock();
        }
    }

    public void flush() throws InterruptedException
    {
        // WAIT UP TO 30 SECONDS TO EMPTY CACHE
        int maxCnt = 3000;
        synchronized(workList)
        {
            while (!workList.isEmpty() && maxCnt-- > 0)
            {
                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException ex)
                {

                }
            }

            if (maxCnt <= 0)
            {
                Log.err("Flush failed, remaining size: " + workList.size());
            }

            waitForFinish();
        }
    }

    public boolean isWriteError()
    {
        return writeError;
    }

    public void resetError()
    {
        writeError = false;
    }

    private void addElem( HandleWriteElem elem)
    {
        try
        {            
            synchronized(workList)
            {   lastAddedElem = elem;
                workList.put(elem);

                int addFillers = elem.len / DATABLOCK_PER_FILLER;
                for (int i = 0; i < addFillers; i++)
                {
                    workList.put(filler);
                }
            }
            
//            if (Main.isPerformanceDiagnostic())
//            {
//                int len = workList.size();
//                if (len != lastQueueLen)
//                {
//                    if (Main.isPerformanceDiagnostic())
//                        System.out.println("WriteQueuelen: " + len);
//                    lastQueueLen = len;
//                }
//            }
        }
        catch (InterruptedException interruptedException)
        {
            Log.err("Interrupted in addElem", interruptedException );
            writeError = true;
        }
    }

    public void addElem( FileHandle fh, byte[] data, int len, long offset )
    {
        HandleWriteElem elem = new HandleWriteElem(fh, data, offset, len, /*doClose*/ false, idxCnt++);

        addElem( elem );
    }

    public void addAndCloseElem( FileHandle fh, byte[] data,  int len, long offset )
    {
        HandleWriteElem elem = new HandleWriteElem(fh, data, offset, len, /*doClose*/ true, idxCnt);
        addElem( elem );
    }

    public void addBootstrap( StoragePoolHandler poolhandler, DedupHashBlock block, HashBlock node )
    {
        DedupBootstrap bt = new DedupBootstrap(poolhandler, node, block);
        addElem( bt );
    }

    public void addBootstrap( StoragePoolHandler poolhandler, DedupHashBlock block, XANode node )
    {
        XABootstrap bt = new XABootstrap(poolhandler, node, block);
        addElem( bt );
    }

    public void addBootstrap( StoragePoolHandler poolhandler, FileSystemElemNode node )
    {
        NodeBootstrap bt = new NodeBootstrap(poolhandler, node);
        addElem( bt );
    }
    public void addBootstrap( StoragePoolHandler poolhandler, FileSystemElemAttributes attr )
    {
        AttributesBootstrap bt = new AttributesBootstrap(poolhandler, attr);
        addElem( bt );
    }    

    public int getQueueLen()
    {
        return workList.size();
    }
    
}
