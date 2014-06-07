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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.validation.Validator;

public class ResultSetFutureWrapper {

    private final List<ResultSetFuture> resultSetFutures = new ArrayList<>();
    private final List<AbstractStatementWrapper> statementWrappers = new ArrayList<>();


    private ResultSetFutureWrapper() {
    }

    public ResultSetFutureWrapper(ResultSetFuture resultSetFuture, AbstractStatementWrapper statementWrapper) {
        Validator.validateNotNull(resultSetFuture, "Added ResultSetFuture should not be null");
        Validator.validateNotNull(statementWrapper, "Added StatementWrapper should not be null");
        this.resultSetFutures.add(resultSetFuture);
        this.statementWrappers.add(statementWrapper);
    }

    public ResultSetFutureWrapper(List<ResultSetFuture> resultSetFutures, List<AbstractStatementWrapper> statementWrappers) {
        Validator.validateNotEmpty(resultSetFutures, "Added ResultSetFuture list should not be empty");
        Validator.validateNotNull(statementWrappers, "Added StatementWrapper list should not be empty");
        Validator.validateTrue(resultSetFutures.size() == statementWrappers.size(), "There should be as many ResultSetFutures as StatementWrappers");
        this.resultSetFutures.addAll(resultSetFutures);
        this.statementWrappers.addAll(statementWrappers);
    }

    public boolean hasResultSetFuture() {
        return resultSetFutures.size() > 0;
    }

    public int size() {
        return resultSetFutures.size();
    }

    public List<ResultSetFuture> getResultSetFutures() {
        return resultSetFutures;
    }

    public List<AbstractStatementWrapper> getStatementWrappers() {
        return statementWrappers;
    }

    public void addListener(Runnable listener, Executor executor) {
        for (ResultSetFuture resultSetFuture : resultSetFutures) {
            resultSetFuture.addListener(listener, executor);
        }
    }

    private void appendResultSetFuture(List<ResultSetFuture> resultSetFutures) {
        this.resultSetFutures.addAll(resultSetFutures);
    }

    private void appendStatementWrapper(List<AbstractStatementWrapper> statementWrappers) {
        this.statementWrappers.addAll(statementWrappers);
    }

    public static ResultSetFutureWrapper merge(ResultSetFutureWrapper... wrappers) {
        final ResultSetFutureWrapper finalWrapper = new ResultSetFutureWrapper();
        for (ResultSetFutureWrapper wrapper : wrappers) {
            finalWrapper.appendResultSetFuture(wrapper.getResultSetFutures());
            finalWrapper.appendStatementWrapper(wrapper.getStatementWrappers());
        }
        return finalWrapper;
    }

    public static ResultSetFutureWrapper emptyWrapper() {
        return new ResultSetFutureWrapper();
    }

    @Override
    public String toString() {
        final Iterable<String> statementStrings = Iterables.transform(statementWrappers, new Function<AbstractStatementWrapper, String>() {
            @Override
            public String apply(AbstractStatementWrapper statementWrapper) {
                return statementWrapper.getQueryString();
            }
        });

        return Joiner.on("|").join(statementStrings);
    }
}
