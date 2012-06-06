/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.lifecycle;

import de.dimm.vsm.Main;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class HandleWriteElem
{
    OutputStream os;
    byte[] data;
    
    int len;
    
    boolean doClose;
    String fullPath;
    

    HandleWriteElem()
    {
    }
   
    public HandleWriteElem( byte[] data)
    {
        this.os = null;
        this.data = data;
        
        this.len = 0;
        this.doClose = false;
    }

    void setVals( OutputStream os, int len, boolean doClose, String _fullPath)
    {
        this.os = os;
        this.len = len;
        this.doClose = doClose;
        fullPath = _fullPath;
    }

    protected void write() throws IOException
    {
        os.write(data, 0, len);      
    }
    protected void close() throws IOException
    {
        os.close();
    }

    @Override
    public String toString()
    {
        return fullPath;
    }

}




class WriteRunner implements Runnable
{
    final WriteFileRunner aiw;

    public WriteRunner( WriteFileRunner aiw )
    {
        this.aiw = aiw;
    }

    @Override
    public void run()
    {
        while (aiw.keepRunning)
        {
            HandleWriteElem elem = null;
            try
            {
                aiw.writeStatus = 'p';
                elem = aiw.workList.poll(aiw.sleepMilisecondOnEmpty, TimeUnit.MILLISECONDS);
                if (elem != null)
                {
                    try
                    {
                        aiw.writeStatus = 'w';
                        elem.write();
                        if (elem.doClose)
                        {
                            aiw.writeStatus = 'c';
                            elem.close();
                        }
                        aiw.writeStatus = ' ';
                    }
                    finally
                    {
                        aiw.readyList.add(elem);
                    }
                }
            }
            catch (Exception e)
            {
                //e.printStackTrace();
                aiw.writeError = true;
                if (elem != null)
                {
                    try
                    {
                        elem.close();
                    }
                    catch (IOException ex)
                    {
                        aiw.writeError = true;
                        aiw.errorFile = elem.fullPath;
                    }
                }
            }
        }
        aiw.isWRunning = false;
    }
}

/**
 *
 * @author Administrator
 */
public class WriteFileRunner
{
   
    BlockingQueue<HandleWriteElem> workList;
    BlockingQueue<HandleWriteElem> readyList;

    boolean keepRunning = true;
    boolean isWRunning;
    boolean writeError;

    long sleepMilisecondOnEmpty = 100;
    boolean isIdle;

    HandleWriteElem lastAddedElem;
    Thread writeThread;
    int lastQueueLen;
    int blockSize;
    String errorFile;


    public WriteFileRunner( int blockSize, int elems)
    {
        this.blockSize = blockSize;
        workList = new ArrayBlockingQueue<HandleWriteElem>(elems);
        readyList = new ArrayBlockingQueue<HandleWriteElem>(elems);

        for (int i = 0; i < elems; i++)
        {
            byte[] data = new byte[blockSize];
            readyList.add( new HandleWriteElem( data  ));
        }
        writeThread = new Thread( new WriteRunner(this), "WriteRunner");

        startThread();
    }

    private void startThread()
    {
        writeThread.start();
    }
    public void close() throws IOException
    {
        keepRunning = false;
        try
        {
            writeThread.join(30 * 1000);
        }
        catch (InterruptedException interruptedException)
        {
        }
        if (!workList.isEmpty())
        {
            throw new IOException( Main.Txt("WriteFileRunner kann nicht geschlossen werden"));
        }

        workList.clear();
        readyList.clear();
    }

    int idxCnt = 0;
    

    public boolean isWriteError()
    {
        return writeError;
    }

    void reset()
    {
        writeError = false;
    }
    char readStatus = ' ';
    char writeStatus = ' ';

    private void addElem( HandleWriteElem elem)
    {
        try
        {            
            lastAddedElem = elem;
            workList.put(elem);
            int len = workList.size();
            if (len != lastQueueLen)
            {
                if (Main.isPerformanceDiagnostic())
                     System.out.println("Readstate: " + readStatus + " Writestate: " + writeStatus + " Queuelen: " + len);
                lastQueueLen = len;
            }
        }
        catch (InterruptedException interruptedException)
        {
            writeError = true;
        }
    }


    HandleWriteElem getNextFreeElem()
    {
        HandleWriteElem elem = null;
        try
        {
            readStatus = 't';
            elem = readyList.take();
            readStatus = 'r';
        }
        catch (InterruptedException interruptedException)
        {
            writeError = true;
        }
        return elem;
    }

    void writeElem( HandleWriteElem elem )
    {
        readStatus = 'p';
        addElem( elem );
        readStatus = ' ';
    }
    
}
