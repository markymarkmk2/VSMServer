/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.vaadin.GuiElems;

import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.event.ItemClickEvent;
import com.vaadin.event.ItemClickEvent.ItemClickListener;
import com.vaadin.terminal.Sizeable;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Label;
import com.vaadin.ui.Table;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.VerticalSplitPanel;
import de.dimm.vsm.vaadin.VSMCMain;

import java.util.ArrayList;

/**
 *
 * @author Administrator
 */
public class BackupEditor extends HorizontalSplitPanel
{
    VSMCMain main;

    VerticalSplitPanel poolSplitter;

    VerticalSplitPanel nodeSplitter;

    public BackupEditor(VSMCMain main)
    {
        this.main = main;
        setStyleName("editHsplitter");
        
 /*
        entityContainer = new EntityContainer<StoragePool>(LogicControl.get_em(), true, true, true, StoragePool.class, 100, new Object[] { "name" }, new boolean[] { true });
        entityContainer.addContainerProperty(LazyQueryView.PROPERTY_ID_ITEM_STATUS, QueryItemStatus.class, QueryItemStatus.None, true, false);
        entityContainer.addContainerProperty("name", String.class, "", true, true);
  * */
        BeanContainer<Long, DemoSchedule> bc = new BeanContainer<Long, DemoSchedule>(DemoSchedule.class);
        bc.setBeanIdProperty("id");
        bc.addBean( new DemoSchedule("Schedule1"));
        bc.addBean( new DemoSchedule("Schedule2"));
        bc.addBean( new DemoSchedule("Schedule3"));



        Table table = new Table("StoragePool Table");
        table.setSizeFull();

        table.setContainerDataSource(bc);
        table.setSelectable(true);
        table.setMultiSelect(false);
        table.setImmediate(true); // react at once when something is selected
        table.setColumnReorderingAllowed(true);
        table.setColumnCollapsingAllowed(true);

        final ArrayList<Object> visibleColumnIds = new ArrayList<Object>();
        final ArrayList<String> visibleColumnLabels = new ArrayList<String>();

        visibleColumnIds.add("id");
        visibleColumnIds.add("name");

        visibleColumnLabels.add("Id");
        visibleColumnLabels.add("Name");

        table.setVisibleColumns(visibleColumnIds.toArray());
        table.setColumnHeaders(visibleColumnLabels.toArray(new String[0]));

        /*table.setColumnWidth(LazyQueryView.PROPERTY_ID_ITEM_STATUS, 16);
        table.addGeneratedColumn(LazyQueryView.PROPERTY_ID_ITEM_STATUS, new QueryItemStatusColumnGenerator());*/
        table.setImmediate(true);

        // Add a footer click handler
        table.addListener(new ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {

                setActiveStorageSchedule( event.getItem() );
            }

        });

        HorizontalSplitPanel mainHoriPanel = this;

        mainHoriPanel.setSplitPosition(50, Sizeable.UNITS_PERCENTAGE);
        poolSplitter = new VerticalSplitPanel();
        poolSplitter.setSplitPosition(30, Sizeable.UNITS_PERCENTAGE);
        nodeSplitter = new VerticalSplitPanel();
        nodeSplitter.setSplitPosition(30, Sizeable.UNITS_PERCENTAGE);


        poolSplitter.setFirstComponent(table);

        mainHoriPanel.setFirstComponent(poolSplitter);
        mainHoriPanel.setSecondComponent(nodeSplitter);
    }
    private void setActiveStorageSchedule( Object item )
    {
        BeanItem bean = (BeanItem) item;

        if (bean.getBean() instanceof DemoSchedule)
        {
            DemoSchedule pool = (DemoSchedule) bean.getBean();
            VerticalLayout editWin  = new VerticalLayout();
            editWin.setMargin(true);
            editWin.setSizeFull();
            editWin.setSpacing(true);
            editWin.setStyleName("editWin");

            TextField name = new TextField(VSMCMain.Txt("Name"), new MethodProperty(pool, "name"));
            editWin.addComponent(name);
            poolSplitter.setSecondComponent(editWin);
            nodeSplitter.setSecondComponent(new Label(""));

            createJobWin( pool );

        }       
    }

    void createJobWin( DemoSchedule pool )
    {
        BeanContainer<Long, DemoJob> bc = new BeanContainer<Long, DemoJob>(DemoJob.class);
        bc.setBeanIdProperty("id");

        for (int i = 0; i < pool.jobs.size(); i++)
        {
            DemoJob node = pool.jobs.get(i);
             bc.addBean( node );
        }



        Table table = new Table("StorageNode Table");
        table.setSizeFull();

        table.setContainerDataSource(bc);
        table.setSelectable(true);
        table.setMultiSelect(false);
        table.setImmediate(true); // react at once when something is selected
        table.setColumnReorderingAllowed(true);
        table.setColumnCollapsingAllowed(true);

        final ArrayList<Object> visibleColumnIds = new ArrayList<Object>();
        final ArrayList<String> visibleColumnLabels = new ArrayList<String>();

        visibleColumnIds.add("id");
        visibleColumnIds.add("name");

        visibleColumnLabels.add("Id");
        visibleColumnLabels.add("Name");

        table.setVisibleColumns(visibleColumnIds.toArray());
        table.setColumnHeaders(visibleColumnLabels.toArray(new String[0]));

        /*table.setColumnWidth(LazyQueryView.PROPERTY_ID_ITEM_STATUS, 16);
        table.addGeneratedColumn(LazyQueryView.PROPERTY_ID_ITEM_STATUS, new QueryItemStatusColumnGenerator());*/
        table.setImmediate(true);

        // Add a footer click handler
        table.addListener(new ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {

                setActiveStorageJob( event.getItem() );
            }

        });
        nodeSplitter.setFirstComponent(table);
    }
    private void setActiveStorageJob( Object item )
    {
        BeanItem bean = (BeanItem) item;

        if (bean.getBean() instanceof DemoJob)
        {
            DemoJob node = (DemoJob) bean.getBean();
            VerticalLayout editWin  = new VerticalLayout();
            editWin.setSizeFull();
            editWin.setMargin(true);
            editWin.setSpacing(true);
            editWin.setStyleName("editWin");

            TextField name = new TextField(VSMCMain.Txt("Name"), new MethodProperty(node, "name"));
            editWin.addComponent(name);
            nodeSplitter.setSecondComponent(editWin);
        }
    }


    
    static long node_id_cnt = 0;
    public class DemoJob
    {
        long id;
        String name;

        public DemoJob(  String name )
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
    public class DemoSchedule
    {
        long id;
        String name;

        ArrayList<DemoJob> jobs;


        public DemoSchedule( String name )
        {
            this.name = name;
            id = ++id_cnt;
            jobs = new ArrayList<DemoJob>();
            for (int i = 0; i < 4; i++)
            {
                DemoJob demoNode = new DemoJob("Job" + Long.toString(id*4 + i));
                jobs.add(demoNode);
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
