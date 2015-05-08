package org.pentaho.di.trans.steps.redis;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Redis Input step looks up value objects, from the given key names, from Redis server(s).
 */
public class RedisInput extends BaseStep implements StepInterface {
    private static Class<?> PKG = RedisInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    protected RedisInputMeta meta;
    protected RedisInputData data;
    String keytype="string" ;
    String key2 = "default";
    String valuetype ;
    protected JedisCluster redisCluster = null;
    protected Jedis jedis = null;
    public RedisInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }



    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            try {
                // Create client and connect to redis server(s)
                Set<HostAndPort> jedisClusterNodes = ((RedisInputMeta) smi).getServers();
                // Jedis Cluster will attempt to discover cluster nodes automatically
                //redisCluster = new JedisCluster(jedisClusterNodes);
                HostAndPort hostAndPort = ((RedisInputMeta) smi).getJedisServer();
                jedis = new Jedis(hostAndPort.getHost(),hostAndPort.getPort());
                return true;
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "RedisInput.Error.ConnectError"), e);
                return false;
            }
        } else {
            return false;
        }
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (RedisInputMeta) smi;
        data = (RedisInputData) sdi;
        Object[] r = getRow(); // get row, set busy!
        // If no more input to be expected, stop
        if (r == null) {
            setOutputDone();
            return false;
        }

        if (first) {
            first = false;

            // clone input row meta for now, we will change it (add or set inline) later
            data.outputRowMeta = getInputRowMeta().clone();
            // Get output field types
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
            keytype = meta.getKeyTypeFieldName();
            logBasic("keytype:"+keytype);
            valuetype = meta.getValueTypeName();
            logBasic("valuetype:"+valuetype);
        }

        // Get value from redis, don't cast now, be lazy. TODO change this?
        int keyFieldIndex = getInputRowMeta().indexOfValue(meta.getKeyFieldName());
        if (keyFieldIndex < 0) {
            throw new KettleException(BaseMessages.getString(PKG, "RedisInputMeta.Exception.KeyFieldNameNotFound"));
        }
        int key2Index = -1;
        if (keytype.equals("hash")) {
            key2Index = getInputRowMeta().indexOfValue(meta.getKey2FieldName());
            if (key2Index < 0) {
                throw new KettleException(BaseMessages.getString(PKG, "RedisOutputMeta.Exception.Key2FieldNameNotFound"));
            }
        }

        StringBuffer fetchedValue =new StringBuffer("");

        if(keytype.equals("string")){
            fetchedValue.append(jedis.get((String) (r[keyFieldIndex]))).append("|");
        }else if(keytype.equals("hash")){
            String res  = jedis.hget((String)r[keyFieldIndex],(String) (r[key2Index]));
            fetchedValue.append(res+"|");
        }else if(keytype.equals("hashall")){
            Map<String,String> map  = jedis.hgetAll((String)r[keyFieldIndex]);
            for(Map.Entry<String, String> entry:map.entrySet()){
                fetchedValue.append(entry.getKey()+":"+entry.getValue()+"|");
            }
        }else if(keytype.equals("list")){
            List<String> list  = jedis.lrange((String)r[keyFieldIndex],0,-1);
            for(String s:list){
                fetchedValue.append(s).append("|");
            }
        }else if(keytype.equals("set")){
            Set<String> set  = jedis.smembers((String)r[keyFieldIndex]);
            for(String s:set){
                fetchedValue.append(s).append("|");
            }
        }else if(keytype.equals("zset")){
            Set<String> set = jedis.zrangeByScore((String)r[keyFieldIndex],0,-1);
            for(String s:set){
                fetchedValue.append(s).append("|");
            }
        }else if(keytype.equals("keys")){
            Set<String> set = jedis.keys((String)r[keyFieldIndex]);
            for(String s:set){
                fetchedValue.append(s).append("|");
            }
        }
        String output;
        if(fetchedValue.length()>1)
            output=fetchedValue.substring(0,fetchedValue.length()-1);
        else
            output=fetchedValue.toString();
        // Add Value data name to output, or set value data if already exists
        Object[] outputRowData = r;
        int valueFieldIndex = getInputRowMeta().indexOfValue(meta.getValueFieldName());
        if (valueFieldIndex < 0 || valueFieldIndex > outputRowData.length) {
            // Not found so add it
            outputRowData = RowDataUtil.addValueData(r, getInputRowMeta().size(), output);
        } else {
            // Update value in place
            outputRowData[valueFieldIndex] = output;
        }

        putRow(data.outputRowMeta, outputRowData); // copy row to possible alternate rowset(s).

        if (checkFeedback(getLinesRead())) {
            if (log.isBasic()) {
                logBasic(BaseMessages.getString(PKG, "RedisInput.Log.LineNumber") + getLinesRead());
            }
        }
        jedis.close();
        return true;
    }
}
