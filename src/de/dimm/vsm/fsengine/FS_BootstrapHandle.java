/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine;

import de.dimm.vsm.net.interfaces.BootstrapHandle;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.XANode;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.eclipse.persistence.indirection.IndirectList;

class IndirectListToEmptyListConverter implements Converter
{

    @Override
    public boolean canConvert( Class clazz )
    {
        return clazz == IndirectList.class;
    }

    @Override
    public void marshal( Object value, HierarchicalStreamWriter writer,
            MarshallingContext context )
    {
        // WE DO NOT WRITE INDIRECT LISTS OUT, WE ONLY WANT THE DATA FOR THE OBJECT
    }

    @Override
    public Object unmarshal( HierarchicalStreamReader reader,
            UnmarshallingContext context )
    {
        // RETRUN EMPTY LIST
        return new IndirectList();
    }
}


/**
 *
 * @author Administrator
 */
public class FS_BootstrapHandle<T> implements BootstrapHandle
{

    File fh;

    public FS_BootstrapHandle( AbstractStorageNode fs_node, FileSystemElemNode node ) throws PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(node, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    public FS_BootstrapHandle( AbstractStorageNode fs_node, FileSystemElemAttributes attr ) throws PathResolveException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(attr, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    public FS_BootstrapHandle( AbstractStorageNode fs_node, DedupHashBlock dedup_block ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(dedup_block, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    public FS_BootstrapHandle( AbstractStorageNode fs_node, XANode node ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(node, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }
    public FS_BootstrapHandle( AbstractStorageNode fs_node, HashBlock node ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(node, sb);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    public FS_BootstrapHandle( AbstractStorageNode fs_node, T node ) throws PathResolveException, UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        StorageNodeHandler.build_bootstrap_path(sb, node);
        fh = new File(fs_node.getMountPoint() + sb.toString());
    }

    @Override
    public String toString()
    {
        return fh.getAbsolutePath();
    }


    @Override
    public boolean delete()
    {
        if (fh.exists())
        {
            return fh.delete();
        }
        // IS GONE  -> OKAY
        return true;
    }

    @Override
    public void write_bootstrap( FileSystemElemNode node ) throws IOException
    {
        FileWriter fw = null;
        FSE_Bootstrap t = new FSE_Bootstrap(node);

        try
        {
            try
            {
                fw = new FileWriter(fh);
            }
            catch (IOException iOException)
            {
                File parent = fh.getParentFile();
                if (!parent.exists())
                {
                    parent.mkdir();
                }
                fw = new FileWriter(fh);
            }

            XStream xstream = new XStream();
            xstream.toXML(t, fw);
            fw.close();
            
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

    @Override
    public void read_bootstrap( FileSystemElemNode node ) throws IOException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(fh);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof FSE_Bootstrap)
            {
                FSE_Bootstrap t = (FSE_Bootstrap) o;
                t.setNode(node);
            }
            throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

    @Override
    public void write_bootstrap( FileSystemElemAttributes attr ) throws IOException
    {
        FileWriter fw = null;
        FSEA_Bootstrap t = new FSEA_Bootstrap(attr);

        try
        {
            try
            {
                fw = new FileWriter(fh);
            }
            catch (IOException iOException)
            {
                File parent = fh.getParentFile();
                if (!parent.exists())
                {
                    parent.mkdir();
                }
                fw = new FileWriter(fh);
            }

            XStream xstream = new XStream();
            xstream.toXML(t, fw);
            fw.close();

        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

    @Override
    public void read_bootstrap( FileSystemElemAttributes attr ) throws IOException
    {
       FileReader fr = null;
        try
        {
            fr = new FileReader(fh);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof FSEA_Bootstrap)
            {
                FSEA_Bootstrap t = (FSEA_Bootstrap) o;
                t.setNode(attr);
            }
            throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }




    public static FSE_Bootstrap read_FSE_bootstrap( File path ) throws IOException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(path);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof FSE_Bootstrap)
            {
                return (FSE_Bootstrap) o;
            }
            throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }
    public static HB_Bootstrap read_HB_bootstrap( File path ) throws IOException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(path);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof HB_Bootstrap)
            {
                return (HB_Bootstrap) o;
            }
            throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }



    @Override
    public void write_bootstrap( HashBlock hb ) throws IOException
    {
        FileWriter fw = null;
        HB_Bootstrap t = new HB_Bootstrap(hb);

        try
        {
            try
            {
                fw = new FileWriter(fh);
            }
            catch (IOException iOException)
            {
                File parent = fh.getParentFile();
                if (!parent.exists())
                {
                    parent.mkdirs();
                }
                fw = new FileWriter(fh);
            }


            
            XStream xstream = new XStream();
            xstream.toXML(t, fw);            
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }

    }

    @Override
    public void write_bootstrap( XANode xa ) throws IOException
    {
        FileWriter fw = null;
        XA_Bootstrap t = new XA_Bootstrap(xa);

        try
        {
            try
            {
                fw = new FileWriter(fh);
            }
            catch (IOException iOException)
            {
                File parent = fh.getParentFile();
                if (!parent.exists())
                {
                    parent.mkdirs();
                }
                fw = new FileWriter(fh);
            }


            fw = new FileWriter(fh);
            XStream xstream = new XStream();
            xstream.toXML(t, fw);            
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

    @Override
    public void read_bootstrap( HashBlock hb ) throws IOException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(fh);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof HB_Bootstrap)
            {
                HB_Bootstrap t = (HB_Bootstrap) o;
                t.setBlock(hb);
                
            }
            else
            {
                throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
            }
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        
    }

    @Override
    public <T> T read_object( T test ) throws IOException
    {

        FileReader fr = null;
        try
        {
            fr = new FileReader(fh);
            XStream xstream = new XStream();
            xstream.registerConverter(new IndirectListToEmptyListConverter());
            Object o = xstream.fromXML(fr);
            return (T) o;
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }        
    }

    @Override
    public <T> void write_object( T object ) throws IOException
    {
        FileWriter fw = null;

        try
        {
            try
            {
                fw = new FileWriter(fh);
            }
            catch (IOException iOException)
            {
                File parent = fh.getParentFile();
                if (!parent.exists())
                {
                    parent.mkdir();
                }
                fw = new FileWriter(fh);
            }
            
            
            XStream xstream = new XStream();

            xstream.registerConverter(new IndirectListToEmptyListConverter());

            xstream.toXML(object, fw);                       
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

    @Override
    public void read_bootstrap( XANode hb ) throws IOException
    {
        FileReader fr = null;
        try
        {
            fr = new FileReader(fh);
            XStream xstream = new XStream();
            Object o = xstream.fromXML(fr);
            fr.close();

            if (o instanceof XA_Bootstrap)
            {
                XA_Bootstrap t = (XA_Bootstrap) o;
                t.setBlock(hb);

            }
            else
            {
                throw new IOException("Wrong type of Bootstrap object: " + o.getClass().toString());
            }
        }
        catch (IOException iOException)
        {
            throw iOException;
        }
        finally
        {
            if (fr != null)
            {
                try
                {
                    fr.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
    }

}
