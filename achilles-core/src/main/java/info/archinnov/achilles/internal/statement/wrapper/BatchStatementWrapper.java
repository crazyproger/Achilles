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

package info.archinnov.achilles.internal.statement.wrapper;

import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.type.ConsistencyLevel;

public class BatchStatementWrapper extends AbstractStatementWrapper {

    private BatchStatement.Type batchType;
    private List<AbstractStatementWrapper> statementWrappers;
    private BatchStatement batchStatement;
    private ConsistencyLevel consistencyLevel;

    public BatchStatementWrapper(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers) {
        super(null, null);
        this.batchType = batchType;
        this.statementWrappers = statementWrappers;
        this.batchStatement = createBatchStatement(batchType, statementWrappers);
    }

    private BatchStatement createBatchStatement(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers) {
        BatchStatement batch = new BatchStatement(batchType);
        boolean tracingEnabled = false;
        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            statementWrapper.activateQueryTracing();
            tracingEnabled |= statementWrapper.isTracingEnabled();
            batch.add(statementWrapper.getStatement());
        }
        if (tracingEnabled) {
            batch.enableTracing();
        }
        return batch;
    }

    @Override
    public String getQueryString() {
        List<String> queries = new ArrayList<>();
        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            queries.add(statementWrapper.getQueryString());
        }
        return Joiner.on("\n").join(queries);
    }

    @Override
    public ListenableFuture<ResultSet> executeAsync(Session session, ExecutorService executorService) {
        return super.executeAsyncInternal(session, this, executorService);
    }

    @Override
    public Statement getStatement() {
        return this.batchStatement;
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled() || batchStatement.isTracing()) {
            AbstractStatementWrapper.writeDMLStartBatch(batchType);
        }

        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            statementWrapper.logDMLStatement(indentation);
        }
        if (dmlLogger.isDebugEnabled() || batchStatement.isTracing()) {
            AbstractStatementWrapper.writeDMLEndBatch(batchType, consistencyLevel);
        }
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        this.batchStatement.setConsistencyLevel(getCQLLevel(consistencyLevel));
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }
}
