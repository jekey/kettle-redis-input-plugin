package org.pentaho.di.trans.steps.redis;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import redis.clients.jedis.HostAndPort;

import java.util.Set;

public class RedisInputDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = RedisInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    private RedisInputMeta input;
    private boolean gotPreviousFields = false;
    private RowMetaInterface previousFields;

    private Label wlKeyField;
    private CCombo wKeyField;
    private FormData fdlKeyField, fdKeyField;

    private Label wlKeyTypeField;
    private CCombo wKeyTypeField;
    private FormData fdlKeyTypeField, fdKeyTypeField;

    private Label wlKey2Field;
    private CCombo wKey2Field;
    private FormData fdlKey2Field, fdKey2Field;

    private Label wlValueField;
    private TextVar wValueField;
    private FormData fdlValueField, fdValueField;

    private Label wlTypeField;
    private CCombo wTypeField;
    private FormData fdlTypeField, fdTypeField;

    private Composite wServersComp;
    private Label wlServers;
    private TableView wServers;
    private FormData fdlServers, fdServers;

    public RedisInputDialog(Shell parent, Object in, TransMeta tr, String sname) {
        super(parent, (BaseStepMeta) in, tr, sname);
        input = (RedisInputMeta) in;
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "RedisInputDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "RedisInputDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);

        // Key field
        wlKeyField = new Label(shell, SWT.RIGHT);
        wlKeyField.setText(BaseMessages.getString(PKG, "RedisInputDialog.KeyField.Label"));
        props.setLook(wlKeyField);
        fdlKeyField = new FormData();
        fdlKeyField.left = new FormAttachment(0, 0);
        fdlKeyField.right = new FormAttachment(middle, -margin);
        fdlKeyField.top = new FormAttachment(wStepname, margin);
        wlKeyField.setLayoutData(fdlKeyField);
        wKeyField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wKeyField);
        wKeyField.addModifyListener(lsMod);
        fdKeyField = new FormData();
        fdKeyField.left = new FormAttachment(middle, 0);
        fdKeyField.top = new FormAttachment(wStepname, margin);
        fdKeyField.right = new FormAttachment(100, 0);
        wKeyField.setLayoutData(fdKeyField);
        wKeyField.addFocusListener(new FocusListener() {
            public void focusLost(org.eclipse.swt.events.FocusEvent e) {
            }

            public void focusGained(org.eclipse.swt.events.FocusEvent e) {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                getFieldsInto(wKeyField);
                shell.setCursor(null);
                busy.dispose();
            }
        });
        //key type fiels
        wlKeyTypeField = new Label(shell, SWT.RIGHT);
        wlKeyTypeField.setText(BaseMessages.getString(PKG, "RedisInputDialog.KeyTypeField.Label"));
        props.setLook(wlKeyTypeField);
        fdlKeyTypeField = new FormData();
        fdlKeyTypeField.left = new FormAttachment(0, 0);
        fdlKeyTypeField.right = new FormAttachment(middle, -margin);
        fdlKeyTypeField.top = new FormAttachment(wlKeyField, margin);
        wlKeyTypeField.setLayoutData(fdlKeyTypeField);
        wKeyTypeField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wKeyTypeField);
        wKeyTypeField.addModifyListener(lsMod);
        fdKeyTypeField = new FormData();
        fdKeyTypeField.left = new FormAttachment(middle, 0);
        fdKeyTypeField.top = new FormAttachment(wlKeyField, margin);
        fdKeyTypeField.right = new FormAttachment(100, 0);
        wKeyTypeField.setLayoutData(fdKeyTypeField);
        String[] typeitiems = {"string","hash","hashall","list","set","zset","keys"};
        wKeyTypeField.setItems(typeitiems);
        wKeyTypeField.addSelectionListener(
                new SelectionListener() {
                    @Override
                    public void widgetSelected(SelectionEvent selectionEvent) {
                        String val = wKeyTypeField.getText();
                        if(val.equals("hash")){
                            wKey2Field.setEnabled(true);
                        }else{
                            wKey2Field.setText("");
                            wKey2Field.setEnabled(false);
                        }
                    }

                    @Override
                    public void widgetDefaultSelected(SelectionEvent selectionEvent) {

                    }
                }
        );
        // Key2 field
        wlKey2Field = new Label(shell, SWT.RIGHT);
        wlKey2Field.setText(BaseMessages.getString(PKG, "RedisOutputDialog.Key2Field.Label"));
        props.setLook(wlKey2Field);
        fdlKey2Field = new FormData();
        fdlKey2Field.left = new FormAttachment(0, 0);
        fdlKey2Field.right = new FormAttachment(middle, -margin);
        fdlKey2Field.top = new FormAttachment(wKeyTypeField, margin);
        wlKey2Field.setLayoutData(fdlKey2Field);
        wKey2Field = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wKey2Field);
        wKey2Field.addModifyListener(lsMod);
        fdKey2Field = new FormData();
        fdKey2Field.left = new FormAttachment(middle, 0);
        fdKey2Field.top = new FormAttachment(wKeyTypeField, margin);
        fdKey2Field.right = new FormAttachment(100, 0);
        wKey2Field.setLayoutData(fdKey2Field);
        wKey2Field.addFocusListener(new FocusListener() {
            public void focusLost(org.eclipse.swt.events.FocusEvent e) {
            }

            public void focusGained(org.eclipse.swt.events.FocusEvent e) {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                getFieldsInto(wKey2Field);
                shell.setCursor(null);
                busy.dispose();
            }
        });

        // Value field
        wlValueField = new Label(shell, SWT.RIGHT);
        wlValueField.setText(BaseMessages.getString(PKG, "RedisInputDialog.ValueField.Label"));
        props.setLook(wlValueField);
        fdlValueField = new FormData();
        fdlValueField.left = new FormAttachment(0, 0);
        fdlValueField.right = new FormAttachment(middle, -margin);
        fdlValueField.top = new FormAttachment(wKey2Field, margin);
        wlValueField.setLayoutData(fdlValueField);
        wValueField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wValueField);
        wValueField.addModifyListener(lsMod);
        fdValueField = new FormData();
        fdValueField.left = new FormAttachment(middle, 0);
        fdValueField.top = new FormAttachment(wKey2Field, margin);
        fdValueField.right = new FormAttachment(100, 0);
        wValueField.setLayoutData(fdValueField);

        // Type field
        wlTypeField = new Label(shell, SWT.RIGHT);
        wlTypeField.setText(BaseMessages.getString(PKG, "RedisInputDialog.TypeField.Label"));
        props.setLook(wlTypeField);
        fdlTypeField = new FormData();
        fdlTypeField.left = new FormAttachment(0, 0);
        fdlTypeField.right = new FormAttachment(middle, -margin);
        fdlTypeField.top = new FormAttachment(wValueField, margin);
        wlTypeField.setLayoutData(fdlTypeField);
        wTypeField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
        props.setLook(wTypeField);
        wTypeField.addModifyListener(lsMod);
        fdTypeField = new FormData();
        fdTypeField.left = new FormAttachment(middle, 0);
        fdTypeField.top = new FormAttachment(wValueField, margin);
        fdTypeField.right = new FormAttachment(100, 0);
        wTypeField.setLayoutData(fdTypeField);
        wTypeField.setItems(ValueMeta.getAllTypes());

        ColumnInfo[] colinf =
                new ColumnInfo[]{
                        new ColumnInfo(BaseMessages.getString(PKG, "RedisInputDialog.HostName.Column"),
                                ColumnInfo.COLUMN_TYPE_TEXT, false),
                        new ColumnInfo(BaseMessages.getString(PKG, "RedisInputDialog.Port.Column"),
                                ColumnInfo.COLUMN_TYPE_TEXT, false),};

        // Servers
        wServersComp = new Composite(wTypeField, SWT.NONE);
        props.setLook(wServersComp);

        FormLayout fileLayout = new FormLayout();
        fileLayout.marginWidth = 3;
        fileLayout.marginHeight = 3;
        wServersComp.setLayout(fileLayout);

        wlServers = new Label(shell, SWT.RIGHT);
        wlServers.setText(BaseMessages.getString(PKG, "RedisInputDialog.Servers.Label"));
        props.setLook(wlServers);
        fdlServers = new FormData();
        fdlServers.left = new FormAttachment(0, 0);
        fdlServers.right = new FormAttachment(middle / 4, -margin);
        fdlServers.top = new FormAttachment(wTypeField, margin);
        wlServers.setLayoutData(fdlServers);

        wServers =
                new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 5/* FieldsRows */, lsMod,
                        props);

        fdServers = new FormData();
        fdServers.left = new FormAttachment(middle / 4, 0);
        fdServers.top = new FormAttachment(wTypeField, margin * 2);
        fdServers.right = new FormAttachment(100, 0);
        // fdServers.bottom = new FormAttachment( 100, 0 );
        wServers.setLayoutData(fdServers);

        // Some buttons
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wCancel}, margin, wServers);

        // Add listeners
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wStepname.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return stepname;
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    public void getData() {
        if (!Const.isEmpty(input.getKeyFieldName())) {
            wKeyField.setText(input.getKeyFieldName());
        }
        if (!Const.isEmpty(input.getKeyTypeFieldName())) {
            wKeyTypeField.setText(input.getKeyTypeFieldName());
        }
        if (!Const.isEmpty(input.getKey2FieldName())) {
            wKey2Field.setText(input.getKey2FieldName());
        }
        if (!Const.isEmpty(input.getValueFieldName())) {
            wValueField.setText(input.getValueFieldName());
        }
        if (!Const.isEmpty(input.getValueTypeName())) {
            wTypeField.setText(input.getValueTypeName());
        }

        int i = 0;
        Set<HostAndPort> servers = input.getServers();
        if (servers != null) {
            for (HostAndPort addr : input.getServers()) {

                TableItem item = wServers.table.getItem(i);
                int col = 1;

                item.setText(col++, addr.getHost());
                item.setText(col++, Integer.toString(addr.getPort()));
            }
        }

        wServers.setRowNums();
        wServers.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();
    }

    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    private void ok() {
        if (Const.isEmpty(wStepname.getText()))
            return;

        stepname = wStepname.getText(); // return value
        input.setKeyFieldName(wKeyField.getText());
        input.setKeyTypeFieldName(wKeyTypeField.getText());
        input.setKey2FieldName(wKey2Field.getText());
        input.setValueFieldName(wValueField.getText());
        input.setValueTypeName(wTypeField.getText());

        int nrServers = wServers.nrNonEmpty();

        input.allocate(nrServers);

        Set<HostAndPort> servers = input.getServers();

        for (int i = 0; i < nrServers; i++) {
            TableItem item = wServers.getNonEmpty(i);
            servers.add(new HostAndPort(item.getText(1), Integer.parseInt(item.getText(2))));
        }
        input.setServers(servers);

        dispose();
    }

    private void getFieldsInto(CCombo fieldCombo) {
        try {
            if (!gotPreviousFields) {
                previousFields = transMeta.getPrevStepFields(stepname);
            }

            String field = fieldCombo.getText();

            if (previousFields != null) {
                fieldCombo.setItems(previousFields.getFieldNames());
            }

            if (field != null)
                fieldCombo.setText(field);
            gotPreviousFields = true;

        } catch (KettleException ke) {
            new ErrorDialog(shell, BaseMessages.getString(PKG, "RedisInputDialog.FailedToGetFields.DialogTitle"),
                    BaseMessages.getString(PKG, "RedisInputDialog.FailedToGetFields.DialogMessage"), ke);
        }
    }
}
