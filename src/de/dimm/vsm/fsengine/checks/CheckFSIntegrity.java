/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsengine.checks;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Main;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.Utilities.SizeStr;
import de.dimm.vsm.auth.User;
import de.dimm.vsm.backup.Restore;
import de.dimm.vsm.fsengine.StorageNodeHandler;
import de.dimm.vsm.fsengine.StoragePoolHandler;
import de.dimm.vsm.fsengine.StoragePoolHandlerFactory;
import de.dimm.vsm.net.interfaces.FileHandle;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.DedupHashBlock;
import de.dimm.vsm.records.FileSystemElemAttributes;
import de.dimm.vsm.records.FileSystemElemNode;
import de.dimm.vsm.records.HashBlock;
import de.dimm.vsm.records.StoragePool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;


/**
 *
 * @author Administrator
 */
public class CheckFSIntegrity implements ICheck {

    AbstractStorageNode snode;
    StorageNodeHandler snHandler;
    StoragePoolHandler poolhandler;
    List<ErrBuff> badFiles;
    fr.cryptohash.Digest digest;
    String status = "";
    int sumEntries = 0;
    long files;
    long dirs;
    long sumData;

    protected boolean isExistanceCheckEnabled() {
        return false;
    }

    protected boolean isHashCheckEnabled() {
        return false;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public int getProcessPercent() {
        return sumEntries;
    }

    @Override
    public String getProcessPercentDimension() {
        return "Entries";
    }

    @Override
    public String getStatisticStr() {
        return "Files: " + files + " Dirs: " + dirs + " Data: " + SizeStr.format(sumData);
    }
    
    class ErrBuff 
    {
        FileSystemElemNode node;
        String reason;

        public ErrBuff(FileSystemElemNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }
        
    }

    boolean abort;
    String errText = "";

    @Override
    public boolean init(Object obj, Object optArg) {
        if (obj instanceof AbstractStorageNode) {
            this.snode = (AbstractStorageNode) obj;
            badFiles = new ArrayList<ErrBuff>();
            digest = new fr.cryptohash.SHA1();
            return true;
        }
        return false;
    }

    @Override
    public boolean check() {
        StoragePool pool = snode.getPool();

        try
        {
            poolhandler = StoragePoolHandlerFactory.createStoragePoolHandler(pool, User.createSystemInternal(), false);
        }
        catch (IOException iOException)
        {
            return false;
        }
        snHandler = StorageNodeHandler.createStorageNodeHandler(snode, poolhandler);
        
        FileSystemElemNode root = pool.getRootDir();
        
        boolean ret = checkExistance( root );
        status = Main.Txt("Prüfung beendet");

        
        return ret;
    }
    
    private boolean checkExistance( FileSystemElemNode node) {
        boolean ret = true;
        sumEntries++;
        
        if (node.isFile()) {
            files++;
            try {
                checkFileExistance(node);
            } catch (Exception iOException) {
                badFiles.add(new ErrBuff(node, iOException.getMessage()));
                
            }
        }

        if (node.isDirectory())
        {
            dirs++;
            status = Main.Txt("Prüfe") +" " + node.getName();
            
            List<FileSystemElemNode> children = node.getChildren(poolhandler.getEm());
            for (int i = 0; i < children.size(); i++) {
                if (abort) {
                    errText = Main.Txt("Abgebrochen");
                    ret = false;
                }
                FileSystemElemNode childNode = children.get(i);
                try {
                    checkExistance(childNode);
                }
                catch( Exception exc )
                {
                    badFiles.add(new ErrBuff(childNode, exc.getMessage()));
                    
                }                
            }
        }
        return ret;
    }

    @Override
    public void abort() {
        abort = true;
    }

    @Override
    public String getName() {
        return Main.Txt("VSM Dateisystem prüfen");
    }

    @Override
    public String getDescription() {
        return Main.Txt("Prüfen, ob alle logischen Blöcke des Dateisystems physikalisch");
    }

    @Override
    public String getErrText() {
        
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(errText)) {
            sb.append( errText);
        }
        for (int i = 0; i < badFiles.size(); i++) {
            ErrBuff errBuff = badFiles.get(i);
            sb.append(errBuff.reason);
            sb.append( ": ");
            sb.append( errBuff.node);
            sb.append("\n");            
        }
        return sb.toString();
    }

    @Override
    public String fillUserOptions(List<String> userSelect) {
        return "";
    }

    @Override
    public boolean handleUserChoice(int select, StringBuffer errText) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        if (poolhandler != null)
        {
            poolhandler.close_entitymanager();
        }
    }

    private void checkFileExistance(FileSystemElemNode node) throws IOException, PathResolveException {
        List<FileSystemElemAttributes> attrs = node.getHistory(poolhandler.getEm());
        List<HashBlock>blocks = node.getHashBlocks(poolhandler.getEm());
        
        for (int i = 0; i < attrs.size(); i++) {
            if (abort)
                break;
            FileSystemElemAttributes fileSystemElemAttributes = attrs.get(i);
            
            List<HashBlock>filteredBlocks = Restore.filter_hashblocks(blocks, fileSystemElemAttributes.getTs());
            checkComplete( fileSystemElemAttributes, filteredBlocks);
        }
        
        for (int i = 0; i < blocks.size(); i++) {
            if (abort)
                break;
            HashBlock hashBlock = blocks.get(i);
            DedupHashBlock dhb = poolhandler.findHashBlock(hashBlock.getHashvalue() );
            if (dhb == null)
                throw new IOException("Fehlender DedupHashblock für HashBlock " + hashBlock.toString());        
            
            sumData+= dhb.getBlockLen();
            
            if (isExistanceCheckEnabled()) {
                FileHandle fh = poolhandler.check_exist_dedupblock_handle(dhb);
                if ( fh == null) {
                    throw new IOException("DedupHashblock existiert nicht  " + dhb.toString()); 
                }
                if (fh.length() != dhb.getBlockLen()){
                    throw new IOException("DedupHashblock hat falsche Länge " + dhb.toString()); 
                }
                if (isHashCheckEnabled()) {
                    String hashValue = createHashFromFile( fh, dhb );
                    if (!hashValue.equals(dhb.getHashvalue())) {
                        throw new IOException("DedupHashblock hat falschen hash " + dhb.toString() + ": " + hashValue); 
                    }
                }
            }
        }
    }

    private String createHashFromFile( FileHandle fh, DedupHashBlock dhb) throws IOException {
        try {
            byte[] data = fh.read(dhb.getBlockLen(), 0);
            fh.close();
            byte[] hash = digest.digest(data);
            String hashValue = CryptTools.encodeUrlsafe(hash);
            return hashValue;
        } catch (IOException iOException) {
            throw new IOException("Fehler beim Erzeugen des Hashes von  " + dhb.toString() + ": " + iOException.getMessage()); 
        }
    }
    
    private void checkComplete(FileSystemElemAttributes fileSystemElemAttributes, List<HashBlock> filteredBlocks) throws IOException {
        
        long offset = 0;
        
        for (int i = 0; i < filteredBlocks.size(); i++) {
            if (abort)
                break;
            HashBlock hashBlock = filteredBlocks.get(i);
            if (hashBlock.getBlockOffset() != offset)
                throw new IOException("Lücke in Hashblock bei Pos  " + offset + " nächste pos " + hashBlock.getBlockOffset());
            offset += hashBlock.getBlockLen();
            
        }
        if (offset != fileSystemElemAttributes.getFsize()) {
                throw new IOException("Länge der Hashblöcke passt nicht " + offset + " erwartete Länge " + fileSystemElemAttributes.getFsize());
        }
    }    
}
