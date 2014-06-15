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

import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DECR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.SELECT_ALL;
import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.async.ResultSetFutureWrapper;
import info.archinnov.achilles.internal.async.WrapperToFuture;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.facade.DaoOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.StatementGenerator;
import info.archinnov.achilles.internal.statement.cache.CacheManager;
import info.archinnov.achilles.internal.statement.cache.StatementCacheKey;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementBinder;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class DaoContext {
    private static final Logger log = LoggerFactory.getLogger(DaoContext.class);

    private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;

    private Map<Class<?>, PreparedStatement> selectPSs;

    private Map<Class<?>, Map<String, PreparedStatement>> removePSs;

    private Map<CQLQueryType, PreparedStatement> counterQueryMap;

    private Map<Class<?>, Map<CQLQueryType, Map<String, PreparedStatement>>> clusteredCounterQueryMap;

    private Session session;

    private CacheManager cacheManager;

    private PreparedStatementBinder binder = new PreparedStatementBinder();

    private StatementGenerator statementGenerator = new StatementGenerator();

    private ConsistencyOverrider overrider = new ConsistencyOverrider();

    private static final Function<ResultSet, Row> RESULTSET_TO_ROW = new Function<ResultSet, Row>() {
        @Override
        public Row apply(ResultSet resultSet) {
            final List<Row> rows = resultSet.all();
            if (rows.isEmpty()) {
                return null;
            } else {
                return rows.get(0);
            }
        }
    };


    public void pushInsertStatement(DaoOperations context, List<PropertyMeta> pms) {
        log.debug("Push insert statement for PersistenceContext '{}' and properties '{}'", context, pms);

        PreparedStatement ps = cacheManager.getCacheForEntityInsert(session, dynamicPSCache, context, pms);
        BoundStatementWrapper bsWrapper = binder.bindForInsert(context, ps, pms);
        context.pushStatement(bsWrapper);
    }

    public void pushUpdateStatement(DaoOperations context, List<PropertyMeta> pms) {
        log.debug("Push update statement for PersistenceContext '{}' and properties '{}'", context, pms);

        PreparedStatement ps = cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms);
        BoundStatementWrapper bsWrapper = binder.bindForUpdate(context, ps, pms);
        context.pushStatement(bsWrapper);
    }

    public void pushCollectionAndMapUpdateStatement(DaoOperations context, DirtyCheckChangeSet changeSet) {

        final CollectionAndMapChangeType changeType = changeSet.getChangeType();
        final PropertyMeta propertyMeta = changeSet.getPropertyMeta();

        log.debug("Push update statement for PersistenceContext '{}' and collection/map property '{}' for change type '{}'", context, propertyMeta, changeType);

        if (changeType == SET_TO_LIST_AT_INDEX || changeType == REMOVE_FROM_LIST_AT_INDEX) {
            ConsistencyLevel writeLevel = overrider.getWriteLevel(context);
            final Pair<Update.Where, Object[]> pair = statementGenerator.generateCollectionAndMapUpdateOperation(context, changeSet);
            context.pushStatement(new RegularStatementWrapper(context.getEntityClass(), pair.left, pair.right, getCQLLevel(writeLevel), context.getCASResultListener()));
        } else {
            PreparedStatement ps = cacheManager.getCacheForCollectionAndMapOperation(session, dynamicPSCache, context, propertyMeta, changeSet);
            BoundStatementWrapper bsWrapper = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);
            context.pushStatement(bsWrapper);
        }
    }

    public Row loadProperty(DaoOperations context, PropertyMeta pm) {
        log.debug("Load property '{}' for PersistenceContext '{}'", pm, context);
        PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm);
        final ResultSetFutureWrapper wrapper = executeReadWithConsistency(context, ps);
        return new AchillesFuture<>(new WrapperToFuture<>(wrapper, RESULTSET_TO_ROW)).getImmediately();
    }

    public void bindForRemoval(DaoOperations context, String tableName) {
        log.debug("Push delete statement for PersistenceContext '{}'", context);
        Class<?> entityClass = context.getEntityClass();
        Map<String, PreparedStatement> psMap = removePSs.get(entityClass);

        if (psMap.containsKey(tableName)) {
            ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);
            BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(context, psMap.get(tableName), consistencyLevel);
            context.pushStatement(bsWrapper);
        } else {
            throw new AchillesException("Cannot find prepared statement for deletion for table '" + tableName + "'");
        }
    }

    // Simple counter
    public void bindForSimpleCounterIncrement(DaoOperations context, PropertyMeta counterMeta, Long increment) {
        log.debug("Push simple counter increment statement for PersistenceContext '{}' and value '{}'", context, increment);
        PreparedStatement ps = counterQueryMap.get(INCR);
        ConsistencyLevel writeLevel = overrider.getWriteLevel(context, counterMeta);
        BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(context, ps, counterMeta, increment, writeLevel);
        context.pushCounterStatement(bsWrapper);
    }

    public ResultSetFutureWrapper incrementSimpleCounter(DaoOperations context, PropertyMeta counterMeta, Long increment, ConsistencyLevel consistencyLevel) {
        log.debug("Increment immediately simple counter for PersistenceContext '{}' and value '{}'", context, increment);
        PreparedStatement ps = counterQueryMap.get(INCR);
        BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(context, ps, counterMeta, increment, consistencyLevel);
        return context.executeImmediate(bsWrapper);
    }

    public ResultSetFutureWrapper decrementSimpleCounter(DaoOperations context, PropertyMeta counterMeta, Long decrement, ConsistencyLevel consistencyLevel) {
        log.debug("Decrement immediately simple counter for PersistenceContext '{}' and value '{}'", context, decrement);
        PreparedStatement ps = counterQueryMap.get(DECR);
        BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(context, ps, counterMeta, decrement, consistencyLevel);
        return context.executeImmediate(bsWrapper);
    }

    public Long getSimpleCounter(DaoOperations context, PropertyMeta counterMeta, ConsistencyLevel consistencyLevel) {
        log.debug("Get simple counter value for counterMeta '{}' PersistenceContext '{}' using Consistency level '{}'", counterMeta, context, consistencyLevel);
        PreparedStatement ps = counterQueryMap.get(SELECT);
        BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterSelect(context, ps, counterMeta, consistencyLevel);
        final ResultSetFutureWrapper wrapper = context.executeImmediate(bsWrapper);
        final Row row = new AchillesFuture<>(new WrapperToFuture<>(wrapper, RESULTSET_TO_ROW)).getImmediately();
        return rowToLongFunction(CQL_COUNTER_VALUE).apply(row);
    }

    public void bindForSimpleCounterDelete(DaoOperations context, PropertyMeta counterMeta) {
        log.debug("Push simple counter deletion statement for counterMeta '{}' and PersistenceContext '{}'", counterMeta, context);
        PreparedStatement ps = counterQueryMap.get(DELETE);
        BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterDelete(context, ps, counterMeta);
        context.pushCounterStatement(bsWrapper);
    }

    // Clustered counter
    public void pushClusteredCounterIncrementStatement(DaoOperations context, PropertyMeta counterMeta, Long increment) {
        log.debug("Push clustered counter increment statement for counterMeta '{}' and PersistenceContext '{}' and value '{}'", counterMeta, context, increment);

        PreparedStatement ps = clusteredCounterQueryMap.get(context.getEntityClass()).get(INCR).get(counterMeta.getPropertyName());
        BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterIncrementDecrement(context, ps, increment);
        context.pushCounterStatement(bsWrapper);
    }

    public WrapperToFuture<Row> getClusteredCounter(DaoOperations context) {
        log.debug("Get clustered counter for PersistenceContext '{}'", context);
        EntityMeta entityMeta = context.getEntityMeta();
        PreparedStatement ps = clusteredCounterQueryMap.get(entityMeta.getEntityClass()).get(SELECT).get(SELECT_ALL.name());
        ConsistencyLevel consistencyLevel = overrider.getReadLevel(context);
        BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterSelect(context, ps, consistencyLevel);

        final ResultSetFutureWrapper wrapper = context.executeImmediate(bsWrapper);
        return new WrapperToFuture<>(wrapper, RESULTSET_TO_ROW);
    }

    public Long getClusteredCounterColumn(DaoOperations context, PropertyMeta counterMeta) {
        log.debug("Get clustered counter for PersistenceContext '{}'", context);
        EntityMeta entityMeta = context.getEntityMeta();
        final String counterColumnName = counterMeta.getPropertyName();
        PreparedStatement ps = clusteredCounterQueryMap.get(entityMeta.getEntityClass()).get(SELECT).get(counterColumnName);
        ConsistencyLevel readLevel = overrider.getReadLevel(context, counterMeta);
        BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterSelect(context, ps, readLevel);

        final ResultSetFutureWrapper wrapper = context.executeImmediate(bsWrapper);
        final Row row = new AchillesFuture<>(new WrapperToFuture<>(wrapper, RESULTSET_TO_ROW)).getImmediately();
        return rowToLongFunction(counterColumnName).apply(row);
    }

    public void bindForClusteredCounterDelete(DaoOperations context) {
        log.debug("Push clustered counter deletion statement for PersistenceContext '{}'", context);
        PreparedStatement ps = clusteredCounterQueryMap.get(context.getEntityClass()).get(DELETE).get(DELETE_ALL.name());
        BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterDelete(context, ps);
        context.pushCounterStatement(bsWrapper);
    }

    public WrapperToFuture<Row> loadEntity(DaoOperations context) {
        log.debug("Load entity for PersistenceContext '{}'", context);

        Class<?> entityClass = context.getEntityClass();
        PreparedStatement ps = selectPSs.get(entityClass);

        final ResultSetFutureWrapper wrapper = executeReadWithConsistency(context, ps);
        return new WrapperToFuture<>(wrapper, RESULTSET_TO_ROW);

    }

    private ResultSetFutureWrapper executeReadWithConsistency(DaoOperations context, PreparedStatement ps) {
        ConsistencyLevel readLevel = overrider.getReadLevel(context);
        BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(context, ps, readLevel);
        return context.executeImmediate(bsWrapper);
    }


    public ResultSetFutureWrapper execute(AbstractStatementWrapper statementWrapper) {
        return statementWrapper.executeAsync(session);
    }

    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement.getQueryString());
    }


    private Function<Row, Long> rowToLongFunction(final String counterColumnName) {
        return new Function<Row, Long>() {
            @Override
            public Long apply(Row row) {
                Long counterValue = null;
                if (row != null && !row.isNull(counterColumnName)) {
                    counterValue = row.getLong(counterColumnName);
                }
                return counterValue;
            }
        };
    }

    public Session getSession() {
        return session;
    }

    void setDynamicPSCache(Cache<StatementCacheKey, PreparedStatement> dynamicPSCache) {
        this.dynamicPSCache = dynamicPSCache;
    }

    void setSelectPSs(Map<Class<?>, PreparedStatement> selectPSs) {
        this.selectPSs = selectPSs;
    }

    void setRemovePSs(Map<Class<?>, Map<String, PreparedStatement>> removePSs) {
        this.removePSs = removePSs;
    }

    void setCounterQueryMap(Map<CQLQueryType, PreparedStatement> counterQueryMap) {
        this.counterQueryMap = counterQueryMap;
    }

    void setClusteredCounterQueryMap(Map<Class<?>, Map<CQLQueryType, Map<String,
            PreparedStatement>>> clusteredCounterQueryMap) {
        this.clusteredCounterQueryMap = clusteredCounterQueryMap;
    }

    void setSession(Session session) {
        this.session = session;
    }

    void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
}
