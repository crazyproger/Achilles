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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractFuture;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;

public class WrapperToFuture<V> extends AbstractFuture<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrapperToFuture.class);

    protected final ResultSetFutureWrapper wrapper;
    protected final Function<ResultSet, V> function;

    public WrapperToFuture(ResultSetFutureWrapper wrapper, Function<ResultSet, V> function) {
        this.wrapper = wrapper;
        this.function = function;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        LOGGER.trace("Cancel the future with mayInterruptIfRunning = {}", mayInterruptIfRunning);
        boolean result = false;
        for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
            result &= resultSetFuture.cancel(mayInterruptIfRunning);
        }
        return result;
    }

    @Override
    public boolean isCancelled() {
        boolean result = false;
        for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
            result |= resultSetFuture.isCancelled();
        }
        return result;
    }

    @Override
    public boolean isDone() {
        boolean result = false;
        for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
            result &= resultSetFuture.isDone();
        }
        return result;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        LOGGER.trace("Get future for wrapper {}", wrapper.toString());
        V result = null;
        if (wrapper.hasResultSetFuture()) {
            final ResultSet resultSet = wrapper.getResultSetFutures().get(0).get();
            final AbstractStatementWrapper statementWrapper = wrapper.getStatementWrappers().get(0);
            applyLoggingAndInvokeCASListener(resultSet, statementWrapper);
            result = function.apply(resultSet);
        }

        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.trace("Get future for wrapper {} and timeout {} {}", wrapper.toString(), timeout, unit);
        V result = null;
        if (wrapper.hasResultSetFuture()) {
            final ResultSet resultSet = wrapper.getResultSetFutures().get(0).get(timeout, unit);
            final AbstractStatementWrapper statementWrapper = wrapper.getStatementWrappers().get(0);
            applyLoggingAndInvokeCASListener(resultSet, statementWrapper);
            result = function.apply(resultSet);
        }
        return result;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
            resultSetFuture.addListener(listener, executor);
        }
    }

    protected void applyLoggingAndInvokeCASListener(ResultSet resultSet, AbstractStatementWrapper statementWrapper) {
        LOGGER.trace("Apply Logging and maybe invoke CAS listener for resultSet {} and query '{}'", resultSet.toString(), statementWrapper.getQueryString());
        statementWrapper.logDMLStatement("");
        statementWrapper.tracing(resultSet);
        statementWrapper.checkForCASSuccess(statementWrapper.getQueryString(), resultSet);
    }
}
