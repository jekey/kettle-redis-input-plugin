package org.pentaho.di.trans.steps.redis;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * @author Matt Burgess
 */
public class RedisInputData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface outputRowMeta;

    /**
     *
     */
    public RedisInputData() {
        super();
    }

}
