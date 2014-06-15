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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.async.ResultSetFutureWrapper;
import info.archinnov.achilles.listener.CASResultListener;

public class RegularStatementWrapper extends AbstractStatementWrapper {

    private RegularStatement regularStatement;

    public RegularStatementWrapper(Class<?> entityClass, RegularStatement regularStatement, Object[] boundValues,
            ConsistencyLevel consistencyLevel, Optional<CASResultListener> casResultListener) {
        super(entityClass, boundValues);
        this.regularStatement = regularStatement;
        super.casResultListener = casResultListener;
        regularStatement.setConsistencyLevel(consistencyLevel);
    }

    @Override
    public ResultSetFutureWrapper executeAsync(Session session) {
        activateQueryTracing();
        ResultSetFuture resultSet = session.executeAsync(regularStatement);
        // logDMLStatement("");
        //tracing(resultSet);
        //checkForCASSuccess(regularStatement.getQueryString(), resultSet);
        return new ResultSetFutureWrapper(resultSet, this);
    }

    @Override
    public RegularStatement getStatement() {
        return regularStatement;
    }

    @Override
    public String getQueryString() {
        return regularStatement.getQueryString();
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled()) {
            String queryType = "Parameterized statement";
            String queryString = regularStatement.getQueryString();
            String consistencyLevel = regularStatement.getConsistencyLevel() == null ? "DEFAULT" : regularStatement
                    .getConsistencyLevel().name();
            writeDMLStatementLog(queryType, queryString, consistencyLevel, values);
        }
    }
}
