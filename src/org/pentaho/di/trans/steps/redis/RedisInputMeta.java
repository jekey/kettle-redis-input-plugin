package org.pentaho.di.trans.steps.redis;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.w3c.dom.Node;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Redis Input step looks up value objects, from the given key names, from Redis server(s).
 */
@Step(id = "RedisInput", image = "redis-input.png", name = "Redis Input",
        description = "Reads from a Redis instance", categoryDescription = "Input")
public class RedisInputMeta extends BaseStepMeta implements StepMetaInterface {
    private static Class<?> PKG = RedisInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    private String keyFieldName;
    private String keyTypeFieldName;
    private String key2FieldName;
    private String valueFieldName;
    private String valueTypeName;
    private Set<HostAndPort> servers;

    public HostAndPort getJedisServer() {
        return jedisServer;
    }

    public void setJedisServer(HostAndPort jedisServer) {
        this.jedisServer = jedisServer;
    }

    private HostAndPort jedisServer;

    public RedisInputMeta() {
        super(); // allocate BaseStepMeta
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databaseMetas,Map<String, Counter> stringCounterMap) throws KettleXMLException {
        readData(stepnode);
    }

    public Object clone() {
        RedisInputMeta retval = (RedisInputMeta) super.clone();
        retval.setKeyFieldName(this.keyFieldName);
        retval.setKeyTypeFieldName(this.keyTypeFieldName);
        retval.setKey2FieldName(this.key2FieldName);
        retval.setValueFieldName(this.valueFieldName);
        retval.setValueTypeName(this.valueTypeName);
        return retval;
    }

    public void allocate(int nrfields) {
        servers = new HashSet<HostAndPort>();
    }

    public void setDefault() {
        this.keyFieldName = null;
        this.keyTypeFieldName = null;
        this.key2FieldName = null;
        this.valueFieldName = null;
        this.valueTypeName = null;
    }

    public void getFields(RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space) throws KettleStepException {
        if (!Const.isEmpty(this.valueFieldName)) {
            // Add value field meta if not found, else set it
            ValueMetaInterface v = new ValueMeta();
            v.setName(this.valueFieldName);
            v.setType(ValueMeta.getType(this.valueTypeName));
            v.setOrigin(origin);
            int valueFieldIndex = inputRowMeta.indexOfValue(this.valueFieldName);
            if (valueFieldIndex < 0) {
                inputRowMeta.addValueMeta(v);
            } else {
                inputRowMeta.setValueMeta(valueFieldIndex, v);
            }
        } else {
            throw new KettleStepException(BaseMessages
                    .getString(PKG, "RedisInputMeta.Exception.ValueFieldNameNotFound"));
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                 Trans trans) {
        return new RedisInput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    public StepDataInterface getStepData() {
        return new RedisInputData();
    }

    public String getKeyFieldName() {
        return keyFieldName;
    }

    public void setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }

    public String getValueFieldName() {
        return valueFieldName;
    }

    public void setValueFieldName(String valueFieldName) {
        this.valueFieldName = valueFieldName;
    }

    public String getValueTypeName() {
        return valueTypeName;
    }

    public void setValueTypeName(String mapFieldName) {
        this.valueTypeName = mapFieldName;
    }


    public String getKeyTypeFieldName() {
        return keyTypeFieldName;
    }

    public void setKeyTypeFieldName(String keyTypeFieldName) {
        this.keyTypeFieldName = keyTypeFieldName;
    }

    public String getKey2FieldName() {
        return key2FieldName;
    }

    public void setKey2FieldName(String key2FieldName) {
        this.key2FieldName = key2FieldName;
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder();
        retval.append("    " + XMLHandler.addTagValue("keyfield", this.getKeyFieldName()));
        retval.append("    " + XMLHandler.addTagValue("keytypefield", this.getKeyTypeFieldName()));
        retval.append("    " + XMLHandler.addTagValue("key2field", this.getKey2FieldName()));
        retval.append("    " + XMLHandler.addTagValue("valuefield", this.getValueFieldName()));
        retval.append("    " + XMLHandler.addTagValue("valuetype", this.getValueTypeName()));
        retval.append("    <servers>").append(Const.CR);
        Set<HostAndPort> servers = this.getServers();
        if (servers != null) {
            for (HostAndPort addr : servers) {
                retval.append("      <server>").append(Const.CR);
                retval.append("        ").append(XMLHandler.addTagValue("hostname", addr.getHost()));
                retval.append("        ").append(XMLHandler.addTagValue("port", addr.getPort()));
                retval.append("      </server>").append(Const.CR);
            }
        }
        retval.append("    </servers>").append(Const.CR);

        return retval.toString();
    }

    @Override
    public void check(List<CheckResultInterface> remarks,
                      TransMeta transMeta, StepMeta stepMeta,
                      RowMetaInterface prev,
                      String[] input, String[] output,
                      RowMetaInterface info) {
        CheckResult cr;
        if (prev == null || prev.size() == 0) {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG,
                            "RedisInputMeta.CheckResult.NotReceivingFields"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "RedisInputMeta.CheckResult.StepRecevingData", prev.size() + ""), stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "RedisInputMeta.CheckResult.StepRecevingData2"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                            "RedisInputMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepMeta);
            remarks.add(cr);
        }
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            this.keyFieldName = XMLHandler.getTagValue(stepnode, "keyfield");
            this.keyTypeFieldName = XMLHandler.getTagValue(stepnode, "keytypefield");
            this.key2FieldName = XMLHandler.getTagValue(stepnode, "key2field");
            this.valueFieldName = XMLHandler.getTagValue(stepnode, "valuefield");
            this.valueTypeName = XMLHandler.getTagValue(stepnode, "valuetype");
            Node serverNodes = XMLHandler.getSubNode(stepnode, "servers");
            int nrservers = XMLHandler.countNodes(serverNodes, "server");
            System.out.println(serverNodes.toString());
            allocate(nrservers);

            for (int i = 0; i < nrservers; i++) {
                Node fnode = XMLHandler.getSubNodeByNr(serverNodes, "server", i);
                String hostname = XMLHandler.getTagValue(fnode, "hostname");
                int port = Integer.parseInt(XMLHandler.getTagValue(fnode, "port"));
                HostAndPort hostAndPort = new HostAndPort(hostname, port);
                if (i == 0) {
                    setJedisServer(hostAndPort);
                }
                servers.add(hostAndPort);
            }
        } catch (Exception e) {
            throw new KettleXMLException(BaseMessages.getString(PKG, "RedisInputMeta.Exception.UnableToReadStepInfo"),
                    e);
        }
    }

    public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleException {
        try {
            this.keyFieldName = rep.getStepAttributeString(id_step, "keyfield");
            this.keyTypeFieldName = rep.getStepAttributeString(id_step, "keytypefield");
            this.key2FieldName = rep.getStepAttributeString(id_step, "key2field");
            this.valueFieldName = rep.getStepAttributeString(id_step, "valuefield");
            this.valueTypeName = rep.getStepAttributeString(id_step, "valuetype");
            int nrservers = rep.countNrStepAttributes(id_step, "server");
            allocate(nrservers);
            for (int i = 0; i < nrservers; i++) {
                servers.add(new HostAndPort(rep.getStepAttributeString(id_step, i, "hostname"), Integer.parseInt(rep
                        .getStepAttributeString(id_step, i, "port"))));
            }

        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "RedisInputMeta.Exception.UnexpectedErrorReadingStepInfo"), e);
        }
    }
    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new RedisInputDialog(shell, meta, transMeta, name);
    }
    public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
            throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "keyfield", this.keyFieldName);
            rep.saveStepAttribute(id_transformation, id_step, "keytypefield", this.keyTypeFieldName);
            rep.saveStepAttribute(id_transformation, id_step, "key2field", this.key2FieldName);
            rep.saveStepAttribute(id_transformation, id_step, "valuefield", this.valueFieldName);
            rep.saveStepAttribute(id_transformation, id_step, "valuetype", this.valueTypeName);
            int i = 0;
            Set<HostAndPort> servers = this.getServers();
            if (servers != null) {
                for (HostAndPort addr : servers) {
                    rep.saveStepAttribute(id_transformation, id_step, i++, "hostname", addr.getHost());
                    rep.saveStepAttribute(id_transformation, id_step, i++, "port", addr.getPort());
                }
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "RedisInputMeta.Exception.UnexpectedErrorSavingStepInfo"), e);
        }
    }

    public Set<HostAndPort> getServers() {
        return servers;
    }

    public void setServers(Set<HostAndPort> servers) {
        this.servers = servers;
    }

}
