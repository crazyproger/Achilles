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
package info.archinnov.achilles.persistence;

import static info.archinnov.achilles.internal.async.AsyncUtils.maybeAddAsyncListeners;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.async.WrapperToFuture;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.utils.UUIDGen;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

public class BatchingPersistenceManager extends PersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(BatchingPersistenceManager.class);

    protected BatchingFlushContext flushContext;
    private final ConsistencyLevel defaultConsistencyLevel;
    private final boolean forceStatementsOrdering;

    BatchingPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory,
            DaoContext daoContext, ConfigurationContext configContext) {
        super(entityMetaMap, contextFactory, daoContext, configContext);
        this.defaultConsistencyLevel = configContext.getDefaultWriteConsistencyLevel();
        this.forceStatementsOrdering = configContext.isForceBatchStatementsOrdering();
        this.flushContext = new BatchingFlushContext(daoContext, defaultConsistencyLevel);
    }

    /**
     * Start a batch session.
     */
    public void startBatch() {
        log.debug("Starting batch mode");
        flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
    }

    /**
     * Start a batch session with WRITE consistency levels
     */
    public void startBatch(ConsistencyLevel consistencyLevel) {
        log.debug("Starting batch mode with consistency level {}", consistencyLevel.name());
        flushContext = flushContext.duplicateWithNoData(consistencyLevel);
    }

    /**
     * End an existing batch and flush all the pending statements.
     *
     * Do nothing if there is no pending statement
     *
     */
    public void flushBatch() {
        log.debug("Flushing batch");
        try {
            new AchillesFuture<>(flushContext.flushBatch()).getImmediately();
        } finally {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
        }
    }

    /**
     * End an existing batch and flush all the pending statements.
     *
     * Do nothing if there is no pending statement
     *
     */
    public AchillesFuture<Empty> asyncFlushBatch(FutureCallback<Object>... asyncListeners) {
        log.debug("Flushing batch asynchronously");
        try {
            final WrapperToFuture<Empty> wrapperToFuture = flushContext.flushBatch();
            maybeAddAsyncListeners(wrapperToFuture, asyncListeners, configContext.getExecutorService());
            return new AchillesFuture<>(wrapperToFuture);
        } finally {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
        }
    }

    /**
     * Cleaning all pending statements for the current batch session.
     */
    public void cleanBatch() {
        log.debug("Cleaning all pending statements");
        flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
    }


    @Override
    public <T> T persist(final T entity) {
        return this.asyncPersistInternal(entity, noOptions()).getImmediately();
    }

    @Override
    public <T> T persist(final T entity, Options options) {
        Options modifiedOptions = adaptOptionsForBatch(options);
        return this.asyncPersistInternal(entity, modifiedOptions).getImmediately();
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncPersist(T entity) {
        throw new UnsupportedOperationException("Cannot persist asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncPersist(T entity, Options options) {
        throw new UnsupportedOperationException("Cannot persist asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    public <T> AchillesFuture<T> asyncPersistInternal(final T entity, Options options) {
        Options modifiedOptions = adaptOptionsForBatch(options);
        log.debug("Persisting entity '{}' asynchronously with options {} ", entity, options);

        entityValidator.validateEntity(entity, entityMetaMap);

        optionsValidator.validateOptionsForInsert(entity, entityMetaMap, modifiedOptions);
        proxifier.ensureNotProxy(entity);
        PersistenceManagerOperations context = initPersistenceContext(entity, modifiedOptions);
        return context.batchPersist(entity);
    }

    @Override
    public void update(Object entity) {
        Options modifiedOptions = maybeAddTimestampToStatement(noOptions());
        super.asyncUpdate(entity, modifiedOptions).getImmediately();
    }

    @Override
    public void update(Object entity, Options options) {
        Options modifiedOptions = adaptOptionsForBatch(options);
        super.asyncUpdate(entity, modifiedOptions).getImmediately();
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncUpdate(T entity) {
        throw new UnsupportedOperationException("Cannot update asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncUpdate(T entity, Options options) {
        throw new UnsupportedOperationException("Cannot update asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Override
    public void remove(Object entity) {
        Options modifiedOptions = maybeAddTimestampToStatement(noOptions());
        super.asyncRemove(entity, modifiedOptions).getImmediately();
    }

    @Override
    public void remove(final Object entity, Options options) {
        Options modifiedOptions = adaptOptionsForBatch(options);
        super.asyncRemove(entity, modifiedOptions).getImmediately();
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncRemove(final T entity) {
        throw new UnsupportedOperationException("Cannot remove asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncRemove(final T entity, Options options) {
        throw new UnsupportedOperationException("Cannot remove asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Override
    public void removeById(Class<?> entityClass, Object primaryKey) {
        Options modifiedOptions = maybeAddTimestampToStatement(noOptions());
        super.asyncRemoveById(entityClass, primaryKey, modifiedOptions).getImmediately();
    }

    @Override
    public void removeById(Class<?> entityClass, Object primaryKey, ConsistencyLevel writeLevel) {
        Options modifiedOptions = maybeAddTimestampToStatement(withConsistency(writeLevel));
        super.asyncRemoveById(entityClass, primaryKey, modifiedOptions).getImmediately();
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncRemoveById(Class<T> entityClass, Object primaryKey) {
        throw new UnsupportedOperationException("Cannot remove by id asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Deprecated
    @Override
    public <T> AchillesFuture<T> asyncRemoveById(Class<T> entityClass, Object primaryKey, Options options) {
        throw new UnsupportedOperationException("Cannot remove by id asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");
    }

    @Override
    protected PersistenceManagerOperations initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
        log.trace("Initializing new persistence context for entity class {} and primary key {}",
                entityClass.getCanonicalName(), primaryKey);
        return contextFactory.newContextWithFlushContext(entityClass, primaryKey, options, flushContext).getPersistenceManagerFacade();
    }

    @Override
    protected PersistenceManagerOperations initPersistenceContext(Object entity, Options options) {
        log.trace("Initializing new persistence context for entity {}", entity);
        return contextFactory.newContextWithFlushContext(entity, options, flushContext).getPersistenceManagerFacade();
    }

    private Options adaptOptionsForBatch(Options options) {
        Options modifiedOptions = maybeAddTimestampToStatement(options);
        if (!optionsValidator.isOptionsValidForBatch(modifiedOptions)) {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
            throw new AchillesException("Runtime custom Consistency Level and/or async listeners cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)' and async listener using flushBatch(...)");
        }
        return modifiedOptions;
    }

    private Options maybeAddTimestampToStatement(Options options) {
        if (forceStatementsOrdering) {
            return options.duplicateWithNewTimestamp(UUIDGen.increasingMicroTimestamp());
        } else {
            return options;
        }
    }

}
