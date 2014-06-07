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

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.async.ResultSetFutureWrapper;
import info.archinnov.achilles.listener.CASResultListener;

public class SimpleStatementWrapper extends AbstractStatementWrapper {


    private SimpleStatement simpleStatement;

    public SimpleStatementWrapper(String query, Object[] values, Optional<CASResultListener> casResultListener) {
        super(null, values);
        super.casResultListener = casResultListener;
        this.simpleStatement = new SimpleStatement(query);
    }

    @Override
    public ResultSetFutureWrapper executeAsync(Session session) {

        final String queryString = simpleStatement.getQueryString();
        simpleStatement = new SimpleStatement(queryString, values);
        activateQueryTracing();
        ResultSetFuture resultSet = session.executeAsync(simpleStatement);
        //logDMLStatement("");
        // tracing(resultSet);
        // checkForCASSuccess(queryString, resultSet);
        return new ResultSetFutureWrapper(resultSet, this);
    }

    @Override
    public SimpleStatement getStatement() {
        return simpleStatement;
    }

    @Override
    public String getQueryString() {
        return simpleStatement.getQueryString();
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled()) {
            String queryType = "Simple statement";
            String queryString = simpleStatement.getQueryString();
            String consistencyLevel = simpleStatement.getConsistencyLevel() == null ? "DEFAULT" : simpleStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }
}
