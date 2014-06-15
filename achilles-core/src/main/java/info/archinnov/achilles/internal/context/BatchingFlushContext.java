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

import static com.google.common.util.concurrent.Futures.allAsList;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.EmptyFutureResultSets;
import info.archinnov.achilles.internal.interceptor.EventHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

public class BatchingFlushContext extends AbstractFlushContext {

    private static final Logger log = LoggerFactory.getLogger(BatchingFlushContext.class);
    protected List<EventHolder> eventHolders = new ArrayList<>();


    public BatchingFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel) {
        super(daoContext, consistencyLevel);
    }

    private BatchingFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
            ConsistencyLevel consistencyLevel) {
        super(daoContext, statementWrappers, consistencyLevel);
    }

    @Override
    public void startBatch() {
        log.debug("Starting a new batch");
    }

    @Override
    public ListenableFuture<List<ResultSet>> flush() {
        log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
        return new EmptyFutureResultSets();
    }

    @Override
    public ListenableFuture<List<ResultSet>> flushBatch() {
        log.debug("Ending current batch");

        FutureCallback<ResultSet> applyTriggers = new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                for (EventHolder eventHolder : eventHolders) {
                    eventHolder.triggerInterception();
                }
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        ListenableFuture<List<ResultSet>> aggregated;

        if (!statementWrappers.isEmpty() && !counterStatementWrappers.isEmpty()) {
            final ListenableFuture<ResultSet> resultSetFutureFields = executeBatch(BatchStatement.Type.UNLOGGED, statementWrappers);
            final ListenableFuture<ResultSet> resultSetFutureCounters = executeBatch(BatchStatement.Type.COUNTER, counterStatementWrappers);
            asyncUtils.addTriggersListenerToResultSet(resultSetFutureFields, applyTriggers);
            asyncUtils.addTriggersListenerToResultSet(resultSetFutureCounters, applyTriggers);
            aggregated = allAsList(resultSetFutureFields, resultSetFutureCounters);
        } else if (!statementWrappers.isEmpty()) {
            final ListenableFuture<ResultSet> resultSetFutureFields = executeBatch(BatchStatement.Type.UNLOGGED, statementWrappers);
            asyncUtils.addTriggersListenerToResultSet(resultSetFutureFields, applyTriggers);
            aggregated = allAsList(resultSetFutureFields);
        } else if (!counterStatementWrappers.isEmpty()) {
            final ListenableFuture<ResultSet> resultSetFutureCounters = executeBatch(BatchStatement.Type.COUNTER, counterStatementWrappers);
            asyncUtils.addTriggersListenerToResultSet(resultSetFutureCounters, applyTriggers);
            aggregated = allAsList(resultSetFutureCounters);
        } else {
            aggregated = new EmptyFutureResultSets();
        }
        return aggregated;
    }


    @Override
    public FlushType type() {
        return FlushType.BATCH;
    }

    @Override
    public BatchingFlushContext duplicate() {
        return new BatchingFlushContext(daoContext, statementWrappers, consistencyLevel);
    }

    @Override
    public void triggerInterceptor(EntityMeta meta, Object entity, Event event) {
        if (event == Event.POST_LOAD) {
            meta.intercept(entity, Event.POST_LOAD);
        } else {
            this.eventHolders.add(new EventHolder(meta, entity, event));
        }
    }

    public BatchingFlushContext duplicateWithNoData(ConsistencyLevel defaultConsistencyLevel) {
        return new BatchingFlushContext(daoContext, new ArrayList<AbstractStatementWrapper>(), defaultConsistencyLevel);
    }
}
