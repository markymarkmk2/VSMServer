/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.Property;
import de.dimm.vsm.vaadin.GuiElems.Table.BaseDataEditTable;
import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.event.ItemClickEvent;
import com.vaadin.event.ItemClickEvent.ItemClickListener;
import com.vaadin.terminal.Sizeable;
import com.vaadin.ui.AbstractLayout;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Label;
import com.vaadin.ui.Table;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.VerticalSplitPanel;
import de.dimm.vsm.LogicControl;
import de.dimm.vsm.Main;
import de.dimm.vsm.records.AbstractStorageNode;
import de.dimm.vsm.records.StoragePool;
import de.dimm.vsm.vaadin.GuiElems.Table.EditTools;
import de.dimm.vsm.vaadin.VSMCMain;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.TypedQuery;
import org.vaadin.addons.lazyquerycontainer.CompositeItem;
import org.vaadin.addons.lazyquerycontainer.EntityContainer;
import org.vaadin.addons.lazyquerycontainer.LazyQueryView;
import org.vaadin.addons.lazyquerycontainer.QueryItemStatus;




/**
 *
 * @author Administrator
 */
public class PoolEditor extends HorizontalSplitPanel
{
    VSMCMain main;

    VerticalSplitPanel poolSplitter;

    VerticalSplitPanel nodeSplitter;

    BaseDataEditTable nodeTable;
    BaseDataEditTable poolTable;



    public PoolEditor(VSMCMain main)
    {
        this.main = main;
        setStyleName("editHsplitter");
        
        TypedQuery tq = main.get_em().createQuery("select p from StoragePool p", StoragePool.class);

        List<StoragePool> list = tq.getResultList();
       
        BeanContainer<Long, StoragePool> bc = new BeanContainer<Long, StoragePool>(StoragePool.class);
        bc.setBeanIdProperty("idx");

        for (int i = 0; i < list.size(); i++)
        {
            bc.addBean( list.get(i) );
        }
 

        ArrayList<JPAField> fieldList = new ArrayList<JPAField>();
        fieldList.add(new JPATextField("Name", "name"));
        fieldList.add(new JPADateField("Creation", "creation", BaseDataEditTable.getDateFormat()));

        
        ItemClickListener l = new ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                Object item = event.getItem();
                if (item instanceof CompositeItem)
                {
                    item = ((CompositeItem)item).getItem("bean");
                }

                setActiveStoragePool( item );
            }
        };
        poolTable = new BaseDataEditTable( main.get_em(), bc, fieldList, l);
        

        HorizontalSplitPanel mainHoriPanel = this;

        mainHoriPanel.setSplitPosition(50, Sizeable.UNITS_PERCENTAGE);
        poolSplitter = new VerticalSplitPanel();
        poolSplitter.setSplitPosition(30, Sizeable.UNITS_PERCENTAGE);
        nodeSplitter = new VerticalSplitPanel();
        nodeSplitter.setSplitPosition(30, Sizeable.UNITS_PERCENTAGE);

               
        final VerticalLayout tableWin  = new VerticalLayout();
        tableWin.setSizeFull();
        tableWin.setSpacing(true);
        Label ll = new Label(Main.Txt("Liste der StoragePools:"));
        ll.setStyleName("Tablename");
        tableWin.addComponent( ll);
        tableWin.addComponent(poolTable);
        tableWin.setExpandRatio(poolTable, 1.0f);
                

        poolSplitter.setFirstComponent(tableWin);

        mainHoriPanel.setFirstComponent(poolSplitter);
        mainHoriPanel.setSecondComponent(nodeSplitter);
    }
    private void setActiveStoragePool( Object item )
    {
        BeanItem bean = (BeanItem) item;

        if (bean.getBean() instanceof StoragePool)
        {
            StoragePool pool = (StoragePool) bean.getBean();

            AbstractLayout panel = poolTable.createEditWin(pool);

            poolSplitter.setSecondComponent(panel);
            nodeSplitter.setSecondComponent(new Label(""));

            createNodeWin( pool );

        }       
    }

    void createNodeWin( StoragePool pool )
    {
        BeanContainer<Long, AbstractStorageNode> bc = new BeanContainer<Long, AbstractStorageNode>(AbstractStorageNode.class);
        bc.setBeanIdProperty("idx");

        for (int i = 0; i < pool.getStorageNodes().size(); i++)
        {
            AbstractStorageNode node = pool.getStorageNodes().get(i);
            bc.addBean( node );
        }
        

        ArrayList<JPAField> fieldList = new ArrayList<JPAField>();

        fieldList.add(new JPATextField("Name", "name"));
        fieldList.add(new JPATextField("Path", "mountPoint"));
        ArrayList<ComboEntry> entries = new ArrayList<ComboEntry>();
        entries.add( new ComboEntry(AbstractStorageNode.NM_FARLINE_OFFLINE, Main.Txt("Farline_Offline")));
        entries.add( new ComboEntry(AbstractStorageNode.NM_FARLINE_ONLINE, Main.Txt("Farline_Online")));
        entries.add( new ComboEntry(AbstractStorageNode.NM_NEARLINE_OFFLINE, Main.Txt("Nearline_Offline")));
        entries.add( new ComboEntry(AbstractStorageNode.NM_NEARLINE_ONLINE, Main.Txt("Nearline_Online")));        
        fieldList.add(new JPAComboField("Mode", "nodeMode", entries));

        entries = new ArrayList<ComboEntry>();
        entries.add( new ComboEntry(AbstractStorageNode.NT_FILESYSTEM, Main.Txt("Dateisystem")));
        
        if (LogicControl.LicenseHandler.isFTPStorageLicensed())
            entries.add( new ComboEntry(AbstractStorageNode.NT_FTP, Main.Txt("FTP")));

        if (LogicControl.LicenseHandler.isS3StorageLicensed())
            entries.add( new ComboEntry(AbstractStorageNode.NT_S3, Main.Txt("S3")));

        fieldList.add(new JPAComboField("Typ", "nodeType", entries));
        
      
        ItemClickListener l = new ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                Object item = event.getItem();
                if (item instanceof CompositeItem)
                {
                    item = ((CompositeItem)item).getItem("bean");
                }
                setActiveStorageNode( item );
            }
        };
        nodeTable = new BaseDataEditTable( main.get_em(), bc, fieldList, l);

        final VerticalLayout tableWin  = new VerticalLayout();
        tableWin.setSizeFull();
        tableWin.setSpacing(true);
        
        Label ll = new Label(Main.Txt("StorageNodes des aktuellen StoragePools:"));
        ll.setStyleName("Tablename");
        tableWin.addComponent( ll);
        tableWin.addComponent(nodeTable);
        tableWin.setExpandRatio(nodeTable, 1.0f);

        nodeSplitter.setFirstComponent(tableWin);
    }

    private void setActiveStorageNode( Object item )
    {
        BeanItem bean = (BeanItem) item;

        if (bean.getBean() instanceof AbstractStorageNode)
        {            
            AbstractStorageNode node = (AbstractStorageNode) bean.getBean();

            AbstractLayout panel = nodeTable.createEditWin(node);
            
            nodeSplitter.setSecondComponent(panel);
        }
    }


    
    static long node_id_cnt = 0;
    public class DemoNode
    {
        long id;
        String name;

        public DemoNode(  String name )
        {
            this.id = ++node_id_cnt;
            this.name = name;
        }
        public void setName( String name )
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        public long getId()
        {
            return id;
        }

    }


    static long id_cnt = 0;
    public class DemoPool
    {
        long id;
        String name;

        ArrayList<DemoNode> nodes;


        public DemoPool( String name )
        {
            this.name = name;
            id = ++id_cnt;
            nodes = new ArrayList<DemoNode>();
            for (int i = 0; i < 4; i++)
            {
                DemoNode demoNode = new DemoNode("Node" + Long.toString(id*4 + i));
                nodes.add(demoNode);
            }
        }

        public void setName( String name )
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        public long getId()
        {
            return id;
        }

    }
}
