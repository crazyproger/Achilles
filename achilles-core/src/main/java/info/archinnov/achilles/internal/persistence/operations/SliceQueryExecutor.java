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
package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ITERATOR;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.StatementGenerator;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.query.slice.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;

public class SliceQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryExecutor.class);

    private StatementGenerator generator = new StatementGenerator();
    private EntityMapper mapper = new EntityMapper();
    private EntityProxifier proxifier = new EntityProxifier();
    private AsyncUtils asyncUtils = new AsyncUtils();
    private PersistenceContextFactory contextFactory;
    private DaoContext daoContext;
    private ConsistencyLevel defaultReadLevel;
    private ExecutorService executorService;

    public SliceQueryExecutor(PersistenceContextFactory contextFactory, ConfigurationContext configContext, DaoContext daoContext) {
        this.contextFactory = contextFactory;
        this.daoContext = daoContext;
        this.defaultReadLevel = configContext.getDefaultReadConsistencyLevel();
        this.executorService = configContext.getExecutorService();
    }

    public <T> List<T> get(SliceQuery<T> sliceQuery) {
        return asyncGet(sliceQuery).getImmediately();
    }

    public <T> AchillesFuture<List<T>> asyncGet(SliceQuery<T> sliceQuery) {
        log.debug("Get slice query");
        final EntityMeta meta = sliceQuery.getMeta();

        CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<>(sliceQuery, defaultReadLevel);
        RegularStatementWrapper statementWrapper = generator.generateSelectSliceQuery(cqlSliceQuery);

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS);
        Function<List<Row>, List<T>> rowsToEntities = new Function<List<Row>, List<T>>() {
            @Override
            public List<T> apply(List<Row> rows) {
                List<T> clusteredEntities = new ArrayList<>();
                for (Row row : rows) {
                    T clusteredEntity = meta.instanciate();
                    mapper.setNonCounterPropertiesToEntity(row, meta, clusteredEntity);
                    meta.intercept(clusteredEntity, Event.POST_LOAD);
                    clusteredEntities.add(clusteredEntity);
                }
                return clusteredEntities;
            }
        };
        final ListenableFuture<List<T>> futureEntities = asyncUtils.transformFuture(futureRows, rowsToEntities);

        asyncUtils.maybeAddAsyncListeners(futureEntities, sliceQuery.getAsyncListeners(), executorService);

        final ListenableFuture<List<T>> futureProxies = asyncUtils.transformFuture(futureEntities, this.<T>getProxyListTransformer());
        return asyncUtils.buildInterruptible(futureProxies);
    }

    public <T> Iterator<T> iterator(final SliceQuery<T> sliceQuery) {
        log.debug("Get iterator for slice query");
        return asyncIterator(sliceQuery).getImmediately();
    }

    public <T> AchillesFuture<Iterator<T>> asyncIterator(final SliceQuery<T> sliceQuery) {
        log.debug("Get iterator for slice query asynchronously");
        final CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<>(sliceQuery, defaultReadLevel);
        RegularStatementWrapper statementWrapper = generator.generateSelectSliceQuery(cqlSliceQuery);
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);

        final ListenableFuture<Iterator<Row>> futureIterator = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ITERATOR);

        Function<Iterator<Row>, Iterator<T>> wrapperToIterator = new Function<Iterator<Row>, Iterator<T>>() {
            @Override
            public Iterator<T> apply(Iterator<Row> rowIterator) {
                PersistenceContext context = buildContextForQuery(sliceQuery);
                return new SliceQueryIterator<>(cqlSliceQuery, context, rowIterator);
            }
        };
        final ListenableFuture<Iterator<T>> listenableFuture = asyncUtils.transformFuture(futureIterator, wrapperToIterator);
        asyncUtils.maybeAddAsyncListeners(listenableFuture, sliceQuery.getAsyncListeners(), executorService);
        return asyncUtils.buildInterruptible(listenableFuture);
    }

    public <T> void remove(final SliceQuery<T> sliceQuery) {
        asyncRemove(sliceQuery).getImmediately();
    }

    public <T> AchillesFuture<Empty> asyncRemove(final SliceQuery<T> sliceQuery) {
        log.debug("Slice remove");
        final CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<>(sliceQuery, defaultReadLevel);
        cqlSliceQuery.validateSliceQueryForRemove();
        final RegularStatementWrapper statementWrapper = generator.generateRemoveSliceQuery(cqlSliceQuery);
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(statementWrapper);
        final ListenableFuture<Empty> listenableFuture = asyncUtils.transformFutureToEmpty(resultSetFuture);
        asyncUtils.maybeAddAsyncListeners(listenableFuture, sliceQuery.getAsyncListeners(), executorService);
        return asyncUtils.buildInterruptible(listenableFuture);
    }

    protected <T> PersistenceContext buildContextForQuery(SliceQuery<T> sliceQuery) {
        log.trace("Build PersistenceContext for slice query");
        ConsistencyLevel cl = sliceQuery.getConsistencyLevel() == null ? defaultReadLevel : sliceQuery.getConsistencyLevel();
        return contextFactory.newContextForSliceQuery(sliceQuery.getEntityClass(), sliceQuery.getPartitionComponents(), cl);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    private <T> Function<List<T>, List<T>> getProxyListTransformer() {
        return new Function<List<T>, List<T>>() {
            @Override
            public List<T> apply(List<T> clusteredEntities) {
                final List<T> proxies = new ArrayList<>();
                for (T clusteredEntity : clusteredEntities) {
                    PersistenceContext context = contextFactory.newContext(clusteredEntity);
                    proxies.add(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(clusteredEntity, context.getEntityFacade()));
                }
                return proxies;
            }
        };
    }
}
