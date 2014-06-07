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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityValidator;
import info.archinnov.achilles.internal.persistence.operations.OptionsValidator;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class BatchingPersistenceManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private BatchingPersistenceManager manager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PersistenceContextFactory contextFactory;

    @Mock
    private PersistenceContext.PersistenceManagerFacade facade;

    @Mock
    private DaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private ExecutorService executorService;

    @Mock
    private BatchingFlushContext flushContext;

    @Mock
    private OptionsValidator optionsValidator;

    @Mock
    private EntityValidator entityValidator;

    @Mock
    private EntityProxifier proxifier;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private AsyncUtils asyncUtils;

    @Mock
    private ListenableFuture<List<ResultSet>> futureResultSets;

    @Mock
    private ListenableFuture<Empty> futureEmpty;

    @Mock
    private AchillesFuture<Empty> achillesFutureEmpty;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    private FutureCallback<Object>[] asyncListeners = new FutureCallback[] { };

    @Mock
    private PersistenceManagerFactory pmf;

    @Mock
    private Options options;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Before
    public void setUp() {
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);
        when(configContext.getExecutorService()).thenReturn(executorService);
        when(configContext.isForceBatchStatementsOrdering()).thenReturn(false);
        manager = new BatchingPersistenceManager(null, contextFactory, daoContext, configContext);
        manager.optionsValidator = optionsValidator;
        manager.entityValidator = entityValidator;
        manager.proxifier = proxifier;
        manager.entityMetaMap = entityMetaMap;
        manager.contextFactory = contextFactory;
        Whitebox.setInternalState(manager, BatchingFlushContext.class, flushContext);
        Whitebox.setInternalState(manager, AsyncUtils.class, asyncUtils);

    }

    @Test
    public void should_start_batch() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        manager.startBatch();

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);

    }

    @Test
    public void should_start_batch_with_consistency_level() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(EACH_QUORUM)).thenReturn(newFlushContext);

        //When
        manager.startBatch(EACH_QUORUM);

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);
    }

    @Test
    public void should_flush_batch() throws Exception {
        //Given
        when(flushContext.flushBatch()).thenReturn(futureResultSets);

        //When
        manager.flushBatch();

        //Then
        verify(asyncUtils).buildInterruptible(futureResultSets);
    }

    @Test
    public void should_flush_batch_async() throws Exception {
        //Given
        when(flushContext.flushBatch()).thenReturn(futureResultSets);
        when(asyncUtils.transformFutureToEmpty(futureResultSets)).thenReturn(futureEmpty);
        when(asyncUtils.buildInterruptible(futureEmpty)).thenReturn(achillesFutureEmpty);

        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        final AchillesFuture<Empty> actual = manager.asyncFlushBatch(asyncListeners);

        //Then
        assertThat(actual).isSameAs(achillesFutureEmpty);
        verify(asyncUtils).maybeAddAsyncListeners(futureEmpty, asyncListeners, executorService);
        assertThat(manager.flushContext).isSameAs(newFlushContext);
    }

    @Test
    public void should_clean_batch() throws Exception {
        //Given
        BatchingFlushContext newFlushContext = mock(BatchingFlushContext.class);
        when(flushContext.duplicateWithNoData(ONE)).thenReturn(newFlushContext);

        //When
        manager.cleanBatch();

        //Then
        assertThat(manager.flushContext).isSameAs(newFlushContext);
    }

    @Test
    public void should_persist_async() throws Exception {
        //Given
        CompleteBean entity = new CompleteBean();
        when(optionsValidator.isOptionsValidForBatch(options)).thenReturn(true);
        when(contextFactory.newContextWithFlushContext(entity, options, flushContext).getPersistenceManagerFacade()).thenReturn(facade);
        when(facade.batchPersist(entity)).thenReturn(achillesFutureEntity);

        //When
        final AchillesFuture<CompleteBean> actual = manager.asyncPersistInternal(entity, options);

        //Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        InOrder inOrder = inOrder(entityValidator, optionsValidator, proxifier);

        inOrder.verify(entityValidator).validateEntity(entity, entityMetaMap);
        inOrder.verify(optionsValidator).validateOptionsForInsert(entity, entityMetaMap, options);
        inOrder.verify(proxifier).ensureNotProxy(entity);
    }

    @Test
    public void should_add_timestamp_to_statement_if_ordered_batch() throws Exception {
        //Given
        Options options = noOptions();
        when(configContext.isForceBatchStatementsOrdering()).thenReturn(true);
        manager = new BatchingPersistenceManager(null, contextFactory, daoContext, configContext);

        //When
        final Options actual = manager.maybeAddTimestampToStatement(options);

        //Then
        assertThat(actual.getTimestamp()).isNotNull();
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_options_not_valid_for_batch() throws Exception {
        //Given
        when(optionsValidator.isOptionsValidForBatch(options)).thenReturn(false);

        //When
        manager.adaptOptionsForBatch(options);
    }

    @Test
    public void should_exception_when_persist_with_consistency() throws Exception {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot persist asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");

        manager.asyncPersist(new CompleteBean(), withConsistency(ONE));
    }

    @Test
    public void should_exception_when_update_with_consistency() throws Exception {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot update asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");

        manager.asyncUpdate(new CompleteBean(), withConsistency(ONE));
    }

    @Test
    public void should_exception_when_remove_with_consistency() throws Exception {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove asynchronously in a batch. Please use asyncFlushBatch(FutureCallback<Object>...asyncListeners) instead");

        manager.asyncRemove(new CompleteBean(), withConsistency(ONE));
    }

    @Test
    public void should_init_persistence_context_with_entity() throws Exception {
        // Given
        Object entity = new Object();
        Options options = OptionsBuilder.noOptions();
        PersistenceContext context = mock(PersistenceContext.class);
        PersistenceContext.PersistenceManagerFacade operations = mock(PersistenceContext.PersistenceManagerFacade.class);

        when(contextFactory.newContextWithFlushContext(entity, options, flushContext)).thenReturn(context);
        when(context.getPersistenceManagerFacade()).thenReturn(operations);

        // When
        PersistenceManagerOperations actual = manager.initPersistenceContext(entity, options);

        // Then
        assertThat(actual).isSameAs(operations);
    }

    @Test
    public void should_init_persistence_context_with_primary_key() throws Exception {
        // Given
        Object primaryKey = new Object();
        Options options = OptionsBuilder.noOptions();
        PersistenceContext context = mock(PersistenceContext.class);
        PersistenceContext.PersistenceManagerFacade operations = mock(PersistenceContext.PersistenceManagerFacade.class);

        when(contextFactory.newContextWithFlushContext(Object.class, primaryKey, options, flushContext)).thenReturn(context);
        when(context.getPersistenceManagerFacade()).thenReturn(operations);

        // When
        PersistenceManagerOperations actual = manager.initPersistenceContext(Object.class, primaryKey, options);

        // Then
        assertThat(actual).isSameAs(operations);
    }
}
