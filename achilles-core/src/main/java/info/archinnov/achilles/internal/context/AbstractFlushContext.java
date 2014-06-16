/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.context;

import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BatchStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class AbstractFlushContext {
    protected AsyncUtils asyncUtils = new AsyncUtils();
    protected DaoContext daoContext;

    protected List<AbstractStatementWrapper> statementWrappers = new ArrayList<>();
    protected List<AbstractStatementWrapper> counterStatementWrappers = new ArrayList<>();

    protected ConsistencyLevel consistencyLevel;

    public AbstractFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel) {
        this.daoContext = daoContext;
        this.consistencyLevel = consistencyLevel;
    }

    protected AbstractFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
            ConsistencyLevel consistencyLevel) {
        this.statementWrappers = statementWrappers;
        this.daoContext = daoContext;
        this.consistencyLevel = consistencyLevel;
    }

    protected ListenableFuture<ResultSet> executeBatch(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers) {
        if (statementWrappers.size() > 1) {
            final BatchStatementWrapper batchStatementWrapper = new BatchStatementWrapper(batchType, statementWrappers);
            return daoContext.execute(batchStatementWrapper);
        } else {
            AbstractStatementWrapper wrapper;
            if (batchType == BatchStatement.Type.LOGGED) {
                wrapper = new BatchStatementWrapper(batchType, statementWrappers);
            } else {
                wrapper = statementWrappers.get(0);
            }
            return daoContext.execute(wrapper);
        }

    }

    public void pushStatement(AbstractStatementWrapper statementWrapper) {
        statementWrappers.add(statementWrapper);
    }

    public void pushCounterStatement(AbstractStatementWrapper statementWrapper) {
        counterStatementWrappers.add(statementWrapper);
    }

    public ListenableFuture<ResultSet> execute(AbstractStatementWrapper statementWrapper) {
        return daoContext.execute(statementWrapper);
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public abstract void startBatch();

    public abstract ListenableFuture<List<ResultSet>> flush();

    public abstract ListenableFuture<List<ResultSet>> flushBatch();

    public abstract FlushType type();

    public abstract AbstractFlushContext duplicate();

    public abstract void triggerInterceptor(EntityMeta meta, Object entity, Event event);

    public static enum FlushType {
        BATCH, IMMEDIATE;
    }

    @Override
    public String toString() {
        return type().toString();
    }
}
