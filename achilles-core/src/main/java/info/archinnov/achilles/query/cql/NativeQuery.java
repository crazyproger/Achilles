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
package info.archinnov.achilles.query.cql;

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.statement.wrapper.SimpleStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.TypedMap;

public class NativeQuery {
    private static final Logger log = LoggerFactory.getLogger(NativeQuery.class);

    private static final Optional<CASResultListener> NO_CAS_LISTENER = Optional.absent();

    private DaoContext daoContext;
    private NativeQueryMapper mapper = new NativeQueryMapper();
    private AsyncUtils asyncUtils = new AsyncUtils();

    private ExecutorService executorService;
    protected String queryString;

    protected Object[] boundValues;

    protected Options options;

    public NativeQuery(DaoContext daoContext, ConfigurationContext configContext, String queryString, Options options, Object... boundValues) {
        this.daoContext = daoContext;
        this.queryString = queryString;
        this.options = options;
        this.boundValues = boundValues;
        this.executorService = configContext.getExecutorService();
    }

    /**
     * Return found rows. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return List<TypedMap>
     */
    public List<TypedMap> get() {
        log.debug("Get results for native query '{}'", queryString);
        return asyncGet().getImmediately();
    }

    /**
     * Return found rows asynchronously. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return AchillesFuture<List<TypedMap>>
     */
    public AchillesFuture<List<TypedMap>> asyncGet(FutureCallback<Object>... asyncListeners) {
        log.debug("Get results for native query '{}' asynchronously", queryString);
        final SimpleStatementWrapper statementWrapper = new SimpleStatementWrapper(queryString, boundValues, NO_CAS_LISTENER);

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);

        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS);

        Function<List<Row>, List<TypedMap>> rowsToTypedMaps = new Function<List<Row>, List<TypedMap>>() {
            @Override
            public List<TypedMap> apply(List<Row> rows) {
                return mapper.mapRows(rows);
            }
        };
        final ListenableFuture<List<TypedMap>> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMaps);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners, executorService);

        return asyncUtils.buildInterruptible(futureTypedMap);
    }

    /**
     * Return the first found row. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return TypedMap
     */
    public TypedMap first() {
        log.debug("Get first result for native query {}", queryString);
        return asyncFirst().getImmediately();
    }

    /**
     * Return the first found row asynchronously. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return AchillesFuture<TypedMap>Map
     */
    public AchillesFuture<TypedMap> asyncFirst(FutureCallback<Object>... asyncListeners) {
        log.debug("Get first result for native query '{}' asynchronously", queryString);
        final SimpleStatementWrapper statementWrapper = new SimpleStatementWrapper(queryString, boundValues, NO_CAS_LISTENER);
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS);

        Function<List<Row>, TypedMap> rowsToTypedMap = new Function<List<Row>, TypedMap>() {
            @Override
            public TypedMap apply(List<Row> rows) {
                List<TypedMap> result = mapper.mapRows(rows);
                if (result.isEmpty()) {
                    return null;
                } else {
                    return result.get(0);
                }
            }
        };
        final ListenableFuture<TypedMap> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMap);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners, executorService);

        return asyncUtils.buildInterruptible(futureTypedMap);
    }

    /**
     * Execute statement without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public void execute() {
        log.debug("Execute native query '{}'", queryString);
        asyncExecute().getImmediately();
    }

    /**
     * Execute statement asynchronously without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public AchillesFuture<Empty> asyncExecute(FutureCallback<Object>... asyncListeners) {
        log.debug("Execute native query '{}' asynchronously", queryString);
        final SimpleStatementWrapper statementWrapper = new SimpleStatementWrapper(queryString, boundValues, options.getCasResultListener());
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);
        final ListenableFuture<Empty> futureEmpty = asyncUtils.transformFutureToEmpty(resultSetFuture);

        asyncUtils.maybeAddAsyncListeners(futureEmpty, asyncListeners, executorService);
        return asyncUtils.buildInterruptible(futureEmpty);
    }


}
