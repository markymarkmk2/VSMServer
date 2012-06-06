package de.dimm.vsm.vaadin;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.vaadin.addons.lazyquerycontainer.EntityContainer;
import org.vaadin.addons.lazyquerycontainer.LazyQueryView;
import org.vaadin.addons.lazyquerycontainer.QueryItemStatus;
import org.vaadin.addons.lazyquerycontainer.QueryItemStatusColumnGenerator;

import com.vaadin.Application;
import com.vaadin.ui.AbstractSelect.MultiSelectMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Table;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.themes.Runo;

import de.dimm.vsm.records.StoragePool;

/**
* Example application demonstrating the Lazy Query Container features.
* @author Tommi S.E. Laukkanen
*/
@SuppressWarnings("rawtypes")
public class VaadinApplication extends Application implements ClickListener 
{
    private static final long serialVersionUID = 1L;

    public static final String PERSISTENCE_UNIT = "VSM";
    private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
    private EntityManager entityManager;

    private TextField nameFilterField;

    private Button refreshButton;
    private Button editButton;
    private Button saveButton;
    private Button cancelButton;
    private Button addItemButton;
    private Button removeItemButton;
    private Button myGui;

    private Table table;
    private EntityContainer<StoragePool> entityContainer;

    private final ArrayList<Object> visibleColumnIds = new ArrayList<Object>();
    private final ArrayList<String> visibleColumnLabels = new ArrayList<String>();

    
    @Override
    public void init() {

        final Window mainWindow = new Window("Lazycontainer Application");
        

        final VerticalLayout mainLayout = new VerticalLayout();
        mainLayout.setMargin(true);
        mainLayout.setSpacing(true);
        mainWindow.setContent(mainLayout);

        final Panel filterPanel = new Panel();
        filterPanel.addStyleName(Runo.PANEL_LIGHT);
        final HorizontalLayout filterLayout = new HorizontalLayout();
        filterLayout.setMargin(false);
        filterLayout.setSpacing(true);
        filterPanel.setContent(filterLayout);
        mainWindow.addComponent(filterPanel);

        final Panel buttonPanel = new Panel();
        buttonPanel.addStyleName(Runo.PANEL_LIGHT);
        final HorizontalLayout buttonLayout = new HorizontalLayout();
        buttonLayout.setMargin(false);
        buttonLayout.setSpacing(true);
        buttonPanel.setContent(buttonLayout);
        mainWindow.addComponent(buttonPanel);

        final Panel buttonPanel2 = new Panel();
        buttonPanel2.addStyleName(Runo.PANEL_LIGHT);
        final HorizontalLayout buttonLayout2 = new HorizontalLayout();
        buttonLayout2.setMargin(false);
        buttonLayout2.setSpacing(true);
        buttonPanel2.setContent(buttonLayout2);
        mainWindow.addComponent(buttonPanel2);

        nameFilterField = new TextField("Name");
        filterPanel.addComponent(nameFilterField);

        refreshButton = new Button("Refresh");
        refreshButton.addListener(this);
        buttonPanel.addComponent(refreshButton);

        editButton = new Button("Edit");
        editButton.addListener(this);
        buttonPanel.addComponent(editButton);

        saveButton = new Button("Save");
        saveButton.addListener(this);
        saveButton.setEnabled(false);
        buttonPanel2.addComponent(saveButton);

        cancelButton = new Button("Cancel");
        cancelButton.addListener(this);
        cancelButton.setEnabled(false);
        buttonPanel2.addComponent(cancelButton);

        addItemButton = new Button("Add Row");
        addItemButton.addListener(this);
        addItemButton.setEnabled(false);
        buttonPanel2.addComponent(addItemButton);

        removeItemButton = new Button("Remove Row");
        removeItemButton.addListener(this);
        removeItemButton.setEnabled(false);
        buttonPanel2.addComponent(removeItemButton);
        
        myGui = new Button("Huhu!!!");
        myGui.addListener(new ClickListener()
        {
            
            @Override
            public void buttonClick(ClickEvent event)
            {
                // TODO Auto-generated method stub
                Window myw = new Window("Test");
                final VerticalLayout vl = new VerticalLayout();
                vl.setMargin(true);
                vl.setSpacing(true);
                mainWindow.setContent(vl);                
                myw.addComponent(new VaadinGuitest());
                mainWindow.addWindow(myw);
                
               
            }
        });
        myGui.setEnabled(true);
        buttonPanel2.addComponent( myGui );
        

        visibleColumnIds.add(LazyQueryView.PROPERTY_ID_ITEM_STATUS);
        visibleColumnIds.add("name");

        visibleColumnLabels.add("");
        visibleColumnLabels.add("Name");

        entityManager = ENTITY_MANAGER_FACTORY.createEntityManager();

        entityContainer = new EntityContainer<StoragePool>(entityManager, true, true, true, StoragePool.class, 100, new Object[] { "name" },
                new boolean[] { true });
        entityContainer.addContainerProperty(LazyQueryView.PROPERTY_ID_ITEM_STATUS, QueryItemStatus.class,
                QueryItemStatus.None, true, false);
        entityContainer.addContainerProperty("name", String.class, "", true, true);

       
       

        table = new Table();
        Application app = table.getApplication();
        mainWindow.addComponent(table);
        app = table.getApplication();
        setMainWindow(mainWindow);
        app = table.getApplication();
        

        table.setCaption("JpaQuery");
        table.setPageLength(40);

        table.setContainerDataSource(entityContainer);

        table.setColumnWidth("name", 135);
        table.setColumnWidth("reporter", 135);
        table.setColumnWidth("assignee", 135);

        table.setVisibleColumns(visibleColumnIds.toArray());
        table.setColumnHeaders(visibleColumnLabels.toArray(new String[0]));

        table.setColumnWidth(LazyQueryView.PROPERTY_ID_ITEM_STATUS, 16);
        table.addGeneratedColumn(LazyQueryView.PROPERTY_ID_ITEM_STATUS, new QueryItemStatusColumnGenerator());

        table.setImmediate(true);
        table.setEditable(false);
        table.setMultiSelect(true);
        table.setMultiSelectMode(MultiSelectMode.DEFAULT);
        table.setSelectable(true);
        table.setWriteThrough(true);

       
        
        
    }
    

    private void setEditMode(final boolean editMode) {
        if (editMode) {
            table.setEditable(true);
            table.setSortDisabled(true);
            refreshButton.setEnabled(false);
            editButton.setEnabled(false);
            saveButton.setEnabled(true);
            cancelButton.setEnabled(true);
            addItemButton.setEnabled(true);
            removeItemButton.setEnabled(true);
            nameFilterField.setEnabled(false);
        } else {
            table.setEditable(false);
            table.setSortDisabled(false);
            refreshButton.setEnabled(true);
            editButton.setEnabled(true);
            saveButton.setEnabled(false);
            cancelButton.setEnabled(false);
            addItemButton.setEnabled(false);
            removeItemButton.setEnabled(false);
            nameFilterField.setEnabled(true);
        }
    }

    @Override
    public void buttonClick(final ClickEvent event) {
        if (event.getButton() == refreshButton) {
            final String nameFilter = (String) nameFilterField.getValue();
            if (nameFilter != null && nameFilter.length() != 0) {
                final Map<String, Object> whereParameters = new HashMap<String, Object>();
                whereParameters.put("name", nameFilter);
                entityContainer.filter("e.name=:name", whereParameters);
            } else {
                entityContainer.filter(null, null);
            }
        }
        if (event.getButton() == editButton) {
            setEditMode(true);
        }
        if (event.getButton() == saveButton) {
            entityContainer.commit();
            entityContainer.refresh();
            setEditMode(false);
        }
        if (event.getButton() == cancelButton) {
            entityContainer.discard();
            entityContainer.refresh();
            setEditMode(false);
        }
        if (event.getButton() == addItemButton) {
            entityContainer.addItem();
        }
        if (event.getButton() == removeItemButton) {
            final Object selection = table.getValue();
            if (selection == null) {
                return;
            }
            if (selection instanceof Integer) {
                final Integer selectedIndex = (Integer) selection;
                if (selectedIndex != null) {
                    entityContainer.removeItem(selectedIndex);
                }
            }
            if (selection instanceof Collection) {
                final Collection selectionIndexes = (Collection) selection;
                for (final Object selectedIndex : selectionIndexes) {
                    entityContainer.removeItem((Integer) selectedIndex);
                }
            }
        }
    }

}


