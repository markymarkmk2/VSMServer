/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.log;

import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.LogQuery;
import de.dimm.vsm.records.MessageLog;
import de.dimm.vsm.records.TextBase;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class Log implements DBLogger
{
    static boolean verbose;
    static boolean debugEnabled;

    public static boolean isVerbose()
    {
        return verbose;
    }

    public static void setVerbose( boolean v )
    {
        verbose = v;
    }
    
    public static boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    public static void setDebugEnabled( boolean debugEnabled )
    {
        Log.debugEnabled = debugEnabled;
    }

    @Override
    public void saveLog( int level, String key, String addText, Throwable t, int user)
    {
        log(level, key, addText, t, user);
    }
   
    public static void log( int level, String key, String addText, Throwable t)
    {
        log(level, key, addText, t, MessageLog.UID_SYSTEM);
    }
    public static void log( int level, String key, String addText, Throwable t, int user)
    {
        String caller = "?";
        String debug_caller;

        if (level == MessageLog.ML_DEBUG && !isDebugEnabled())
            return;


        try
        {
            StackTraceElement[] elArr = Thread.currentThread().getStackTrace();
            if (elArr.length > 2)
            {
                int idx = 1;
                StackTraceElement el = elArr[idx];
                while (el.getClassName().startsWith("de.dimm.vsm.log"))
                {
                    idx++;
                    el = elArr[idx];
                }

                caller = el.getClassName() + "." + el.getMethodName();
                debug_caller = caller + " " + el.getFileName() + ":" + el.getLineNumber();
            }


            MessageLog log = null;
            if (t != null)
            {
                log = new MessageLog(level, user, caller, key, addText, t);
            }
            else
            {
                log = new MessageLog(level, user, caller, key, addText);
            }
            
            String msg = log.getModuleName() + " " + Main.Txt(log.getMessageId());
            if (addText != null)
                msg += " " + addText;

            try
            {
                LogicControl.get_log_em().check_open_transaction();
                LogicControl.get_log_em().em_persist(log);
                LogicControl.get_log_em().commit_transaction();
            }
            catch (SQLException sQLException)
            {
                System.out.println("Error adding to MessageLog-DB: " + sQLException.getMessage());
            }
            finally
            {
                if (level == MessageLog.ML_ERROR)
                {
                    if (t != null)
                    {
                        VSMFSLogger.getLog().error(msg, t);

                    }
                    else
                    {
                        VSMFSLogger.getLog().error(msg);

                    }
                }
                else if (level == MessageLog.ML_WARN)
                {
                    if (t != null)
                    {
                        VSMFSLogger.getLog().warn(msg, t);

                    }
                    else
                    {
                        VSMFSLogger.getLog().warn(msg);

                    }
                }
                else if (level == MessageLog.ML_INFO)
                {
                    VSMFSLogger.getLog().info(msg);
                }
                else if (level == MessageLog.ML_DEBUG)
                {
                    if (t != null)
                    {
                        VSMFSLogger.getLog().debug(msg, t);

                    }
                    else
                    {
                        VSMFSLogger.getLog().debug(msg);
                    }

                   /* msg = MessageLog.getDateString(log.getCreation()) + " "
                            + MessageLog.getErrLevelName(log.getErrLevel()) + " "
                            + msg;

                    System.err.println(msg);*/
                }

            }
        }
        catch (Exception e)
        {
            System.out.println("Error in log: " + e.getMessage() + ":" + key + (t!=null? " Exc: " + t.getMessage():""));
            e.printStackTrace(System.out);
        }
        finally
        {
            logCounter++;
        }
    }


    public static void err( String key, Throwable t)
    {
        log(MessageLog.ML_ERROR, key, null, t);
    }
    public static void err( String key, String add, Throwable t)
    {
        log(MessageLog.ML_ERROR, key, add, t);
    }
    public static void err( String key)
    {
        err( key, null, (Object[])null );
    }
    public static void err( String key, String add)
    {
        err( key, add, (Object[])null );
    }
    public static void err( String key, String fmt, Object ... a)
    {
        String s = null;
        if (fmt != null)
            s = String.format(fmt, a);
        Log.log(MessageLog.ML_ERROR, key, s, null);
    }

    public static void warn( String key, Throwable t)
    {
        Log.log(MessageLog.ML_WARN, key, null, t);
    }
    public static void warn( String key)
    {
        warn( key, null, (Object[])null );
    }
    public static void warn( String key, String add)
    {
        warn( key, add, (Object[])null );
    }
    public static void warn( String key, String add, Throwable t)
    {
        log(MessageLog.ML_WARN, key, add, t);
    }

    public static void warn( String key, String fmt, Object ... a)
    {
        String s = null;
        if (fmt != null)
            s = String.format(fmt, a);

        log(MessageLog.ML_WARN, key, s, null);
    }

    public static void debug( String key, Throwable t)
    {
        log(MessageLog.ML_DEBUG, key, null, t);
    }
    public static void debug( String key)
    {
        debug( key, null, (Object[])null );
    }
    public static void debug( String key, String add)
    {
        debug( key, add, (Object[])null );
    }

    public static void debug( String key, String fmt, Object ... a)
    {
        String s = fmt;
        if (fmt != null && a != null)
        {
            try
            {
                s = String.format(fmt, a);
            }
            catch (Exception e)
            {
            }
        }

        log(MessageLog.ML_DEBUG, key, s, null);
    }


    public static void info( String key)
    {
        info( key, null,(Object[])null );
    }
    public static void info( String key, String fmt, Object ... a)
    {
        String s = null;
        if (fmt != null)
            s = String.format(fmt, a);

        log(MessageLog.ML_INFO, key, s, null);
    }
    public static void info( String key, String add)
    {
        info( key, add, (Object[])null );
    }
    static long logCounter = 0;


    private static String buildQryString( LogQuery lq )
    {
        StringBuilder sb = new StringBuilder();

        if (lq.getOlderThan() != null)
        {
            if (sb.length() > 0)
                sb.append( " and ");
            sb.append( " creation < Timestamp('");
            sb.append( LogicControl.get_log_em().getTimestamp(lq.getOlderThan()) );
            sb.append(  "')" );
        }

        if (lq.getLevelFlags() > 0)
        {
            if (sb.length() > 0)
                sb.append( " and ");

            sb.append( " (");

            StringBuilder orsb = new StringBuilder();
            if ((lq.getLevelFlags() & LogQuery.LV_DEBUG) != 0)
            {
                if (orsb.length() > 0)
                    orsb.append(" or ");

                orsb.append( " errLevel=" );
                orsb.append( MessageLog.ML_DEBUG );
            }
            if ((lq.getLevelFlags() & LogQuery.LV_ERROR) != 0)
            {
                if (orsb.length() > 0)
                    orsb.append(" or ");

                orsb.append( " errLevel=" );
                orsb.append( MessageLog.ML_ERROR );
            }
            if ((lq.getLevelFlags() & LogQuery.LV_INFO) != 0)
            {
                if (orsb.length() > 0)
                    orsb.append(" or ");

                orsb.append( " errLevel=" );
                orsb.append( MessageLog.ML_INFO );
            }
            if ((lq.getLevelFlags() & LogQuery.LV_WARN) != 0)
            {
                if (orsb.length() > 0)
                    orsb.append(" or ");

                orsb.append( " errLevel=" );
                orsb.append( MessageLog.ML_WARN );
            }

            sb.append( orsb.toString() );
            sb.append(  ")" );
        }
        if (lq.getQry() != null && !lq.getQry().isEmpty())
        {
            if (sb.length() > 0)
                sb.append( " and ");

            sb.append( " (");
            StringBuilder orsb = new StringBuilder();

            if (lq.getQry().startsWith("="))
            {
                orsb.append("additionText = '");
                orsb.append(lq.getQry().substring(1));
                orsb.append("'");
            }
            else
            {
                orsb.append("additionText like '%");
                orsb.append(lq.getQry());
                orsb.append("%' or ");

                orsb.append("exceptionText like '%");
                orsb.append(lq.getQry());
                orsb.append("%' or ");

                orsb.append("moduleName like '%");
                orsb.append(lq.getQry());
                orsb.append("%' or ");

                orsb.append("messageId like '%");
                orsb.append(lq.getQry());
                orsb.append("%'");
            }

            String txqry = "select x from TextBase x where T1.messageText like '%" + lq.getQry() + "%'";
            List<TextBase> li = null;
            try
            {
                li = LogicControl.get_txt_em().createQuery(txqry, TextBase.class);
            }
            catch (SQLException sQLException)
            {
            }
            if (li != null && !li.isEmpty())
            {
                orsb.append("%' or messageId in (");
                for (int i = 0; i < li.size(); i++)
                {
                    TextBase textBase = li.get(i);
                    if (i > 0)
                        orsb.append(",");
                    orsb.append("'");
                    orsb.append(textBase.getMessageId());
                    orsb.append("'");
                }
                orsb.append(")");
            }


            sb.append( orsb.toString() );
            sb.append(  ")" );
        }

        return sb.toString();
        
    }

    public static MessageLog[] listLogs( int cnt, long offsetIdx, LogQuery lq )
    {
        String qry = "select max(idx) from messagelog";
        List<Object[]> ret = null;
        try
        {
            ret = LogicControl.get_log_em().createNativeQuery(qry, 1, 10/*timeout*/);
        }
        catch (SQLException e)
        {
            System.out.println("Error in Listlog: " + e.getMessage() );
        }
        
        if (ret == null)
        {
            MessageLog l = new MessageLog(0, 0, "", "Timeout");
            MessageLog[] arr = new MessageLog[1];
            arr[0] = l;
            return arr;
        }
        if (ret.isEmpty()|| ret.get(0)[0] == null)
        {
            return new MessageLog[0];
        }

        long maxIdx = (Long)ret.get(0)[0];

        String addQry = buildQryString(lq);
        if (!addQry.isEmpty())
        {

            qry = qry + " where " + addQry;
            if (maxIdx > 100000)
                qry += " and idx > " + Long.toString(maxIdx - 100000);

            try
            {
                ret = LogicControl.get_log_em().createNativeQuery(qry, 1, 30/*timeout*/);
            }
            catch (SQLException e)
            {
                System.out.println("Error in Listlog: " + e.getMessage() );
            }
            if (ret != null && !ret.isEmpty() && ret.get(0)[0] != null)
            {
                maxIdx = (Long)ret.get(0)[0];
            }
        }

        if (ret == null)
        {
            MessageLog l = new MessageLog(0, 0, "", "Timeout");
            MessageLog[] arr = new MessageLog[1];
            arr[0] = l;
            return arr;
        }

        if (ret.isEmpty()|| ret.get(0)[0] == null)
        {
            return new MessageLog[0];
        }


        if (addQry.isEmpty())
        {

            long endIdx = maxIdx - offsetIdx;
            long startIdx = endIdx - cnt;
            if (endIdx < 0 )
                return new MessageLog[0];

            if (startIdx < 0)
                startIdx = 0;

            List<MessageLog> list = null;
            try
            {
                list = LogicControl.get_log_em().createQuery("select x from Messagelog x where T1.idx between "
                        + startIdx + " and " + endIdx + " order by T1.idx desc", MessageLog.class, /*max*/0, /*timeout*/30);
            }
            catch (SQLException e)
            {
                System.out.println("Error in Listlog: " + e.getMessage() );
                return new MessageLog[0];
            }
            return list.toArray( new MessageLog[0]);
        }
        else
        {
            long blockOffset = 0;
            int blockLen = 1000;
            List<MessageLog> list = new ArrayList<MessageLog>();
            while (list.size() < cnt)
            {
                long endIdx = maxIdx - offsetIdx - blockOffset;
                long startIdx = endIdx - blockLen;

                // REACHED START OF LOG
                if (startIdx < 0)
                    break;

                qry = "select x from Messagelog x where " + addQry + " and T1.idx between "
                        + startIdx + " and " + endIdx + " order by T1.idx desc";


                List<MessageLog> localList = null;
                try
                {
                    localList = LogicControl.get_log_em().createQuery(qry, MessageLog.class, blockLen, /*timeout*/30);
                    int rest = cnt - list.size();
                    if (localList.size() <= rest)
                    {
                        list.addAll(localList);
                    }
                    else
                    {
                        for (int i = 0; i < rest; i++)
                        {
                           list.add(localList.get(i));
                        }
                    }
                }
                catch (SQLException e)
                {
                    System.out.println("Error in Listlog: " + e.getMessage() );
                    return new MessageLog[0];
                }
            }
            return list.toArray( new MessageLog[0]);
        }
    }

    public static MessageLog[] listLogsSinceIdx(long idx, LogQuery lq )
    {
        String qry = "select x from Messagelog x where T1.idx > " + idx;

        String addQry = buildQryString(lq);
        if (!addQry.isEmpty())
        {
            qry = qry + " and " + addQry;
        }
        qry += " order by T1.idx desc";

        List<MessageLog> list = null;
        try
        {
            list = LogicControl.get_log_em().createQuery(qry, MessageLog.class);
        }
        catch (SQLException e)
        {
            System.out.println("Error in listLogsSinceIdx: " + e.getMessage() );
            return new MessageLog[0];
        }
        return list.toArray( new MessageLog[0]);
    }

    public static long getLogCounter()
    {
        return logCounter;
    }


}
