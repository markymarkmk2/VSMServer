/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.fsengine;

import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.StoragePool;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Administrator
 */
public class JavaHashCache extends HashCache
{
    Map<String,Long> hashMap;

    public JavaHashCache( StoragePool pool)
    {
        super(pool);
        hashMap = new ConcurrentHashMap<>();
    }


    @Override
    public void fill( String hash, long id )
    {
        hashMap.put(hash, id);
    }
    
    
    @Override
    public long getDhbIdx( String hash )
    {
        Long l = hashMap.get(hash);
        if (l != null)
            return l.longValue();
        
        return -1;
    }
    @Override
    public void addDhb( String hash, long idx )
    {
        hashMap.put(hash, idx);
    }

    @Override
    public long size()
    {
        return hashMap.size();
    }

    @Override
    public void removeDhb( DedupHashBlock dhb )
    {
        hashMap.remove(dhb.getHashvalue());
    }

    
    @Override
    public List<String> getUrlUnsafeHashes()
    {
        ArrayList<String> ret = new ArrayList<String>();
        Set<String> set = hashMap.keySet();

        for (Iterator<String> it = set.iterator(); it.hasNext();)
        {
            String hash = it.next();
            boolean found = false;

            char lastCh = hash.charAt( hash.length() - 1 );

            // DETECT PADDED HASHES
            if (lastCh == '=')
                found = true;

            if (!found)
            {
                for (int i = 0; i < hash.length(); i++)
                {
                    char ch = hash.charAt(i);
                    if (ch == '/' || ch == '+')
                    {
                        found = true;
                        break;
                    }
                }
            }
            if (found)
            {
                ret.add(hash);
            }
        }
        return ret;
    }

    @Override
    public boolean shutdown()
    {
        hashMap.clear();
        return true;
    }

}
