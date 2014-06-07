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

package info.archinnov.achilles.internal.async;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;

public class MultiWrapperToFuture<V> extends WrapperToFuture<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrapperToFuture.class);

    public MultiWrapperToFuture(ResultSetFutureWrapper wrapper, Function<ResultSet, V> function) {
        super(wrapper, function);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        LOGGER.trace("Get future for wrappers {}", wrapper.toString());
        V result = null;
        final List<ResultSetFuture> resultSetFutures = wrapper.getResultSetFutures();
        final List<AbstractStatementWrapper> statementWrappers = wrapper.getStatementWrappers();
        if (wrapper.hasResultSetFuture()) {
            for (int i = 0; i < resultSetFutures.size(); i++) {
                final ResultSet resultSet = resultSetFutures.get(i).get();
                final AbstractStatementWrapper statementWrapper = statementWrappers.get(i);
                applyLoggingAndInvokeCASListener(resultSet, statementWrapper);
                result = function.apply(resultSet);
            }
        }
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.trace("Get future for wrappers {} and timeout {} {}", wrapper.toString(), timeout, unit);
        V result = null;
        final List<ResultSetFuture> resultSetFutures = wrapper.getResultSetFutures();
        final List<AbstractStatementWrapper> statementWrappers = wrapper.getStatementWrappers();
        if (wrapper.hasResultSetFuture()) {
            for (int i = 0; i < resultSetFutures.size(); i++) {
                final ResultSet resultSet = resultSetFutures.get(i).get(timeout, unit);
                final AbstractStatementWrapper statementWrapper = statementWrappers.get(i);
                applyLoggingAndInvokeCASListener(resultSet, statementWrapper);
                result = function.apply(resultSet);
            }
        }
        return result;
    }
}
