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

import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.statement.StatementGenerator;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.query.slice.SliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryExecutorTest {

    @InjectMocks
    private SliceQueryExecutor executor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext configContext;

    @Mock
    private StatementGenerator generator;

    @Mock
    private EntityMapper mapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private PersistenceContextFactory contextFactory;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private PersistenceContext context;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private Iterator<Row> iterator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    private SliceQuery<ClusteredEntity> sliceQuery;

    @Mock
    private ClusteredEntity entity;

    private Long partitionKey = RandomUtils.nextLong();

    private List<Object> partitionComponents = Arrays.<Object>asList(partitionKey);
    private List<Object> clusteringsFrom = Arrays.<Object>asList("name1");
    private List<Object> clusteringsTo = Arrays.<Object>asList("name2");
    private int limit = 98;
    private int batchSize = 99;
    private FutureCallback<Object>[] asyncListeners = new FutureCallback[] { };

    @Before
    public void setUp() {
        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(EACH_QUORUM);
        when(context.getEntityFacade()).thenReturn(entityFacade);
        when(meta.getIdMeta()).thenReturn(idMeta);

        when(idMeta.getComponentNames()).thenReturn(asList("id", "name"));
        when(idMeta.getComponentClasses()).thenReturn(Arrays.<Class<?>>asList(Long.class, String.class));

        sliceQuery = new SliceQuery<>(ClusteredEntity.class, meta, partitionComponents, clusteringsFrom, clusteringsTo,
                ASCENDING, EXCLUSIVE_BOUNDS, LOCAL_QUORUM, asyncListeners, limit, batchSize, true);

        Whitebox.setInternalState(executor, StatementGenerator.class, generator);
        Whitebox.setInternalState(executor, PersistenceContextFactory.class, contextFactory);
        Whitebox.setInternalState(executor, EntityProxifier.class, proxifier);
        Whitebox.setInternalState(executor, EntityMapper.class, mapper);
    }

    @Test
    public void should_get_clustered_entities() throws Exception {

        RegularStatementWrapper regularWrapper = mock(RegularStatementWrapper.class);
        ResultSetFuture resultSetFuture = mock(ResultSetFuture.class, RETURNS_DEEP_STUBS);
        Row row = mock(Row.class);
        List<Row> rows = asList(row);

        when(generator.generateSelectSliceQuery(anySliceQuery())).thenReturn(regularWrapper);

        when(resultSetFuture.getUninterruptibly().all()).thenReturn(asList(row));
        when(daoContext.execute(regularWrapper).getResultSetFutures()).thenReturn(asList(resultSetFuture));
        when(resultSetFuture.getUninterruptibly().all()).thenReturn(rows);

        when(meta.instanciate()).thenReturn(entity);
        when(contextFactory.newContext(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        List<ClusteredEntity> actual = executor.get(sliceQuery);

        assertThat(actual).containsOnly(entity);
        verify(meta).intercept(entity, Event.POST_LOAD);
        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);
    }

    @Test
    public void should_create_iterator_for_clustered_entities() throws Exception {
        RegularStatementWrapper regularWrapper = mock(RegularStatementWrapper.class);
        when(generator.generateSelectSliceQuery(anySliceQuery())).thenReturn(regularWrapper);
        ResultSetFuture resultSetFuture = mock(ResultSetFuture.class, RETURNS_DEEP_STUBS);

        when(daoContext.execute(regularWrapper).getResultSetFutures()).thenReturn(asList(resultSetFuture));
        when(resultSetFuture.getUninterruptibly().iterator()).thenReturn(iterator);

        when(contextFactory.newContextForSliceQuery(ClusteredEntity.class, partitionComponents, LOCAL_QUORUM))
                .thenReturn(context);

        when(idMeta.getCQLComponentNames()).thenReturn(asList("id", "comp1"));
        Iterator<ClusteredEntity> iter = executor.iterator(sliceQuery);

        assertThat(iter).isNotNull();
        assertThat(iter).isInstanceOf(SliceQueryIterator.class);
    }

    @Test
    public void should_remove_clustered_entities() throws Exception {
        sliceQuery = new SliceQuery<>(ClusteredEntity.class, meta, partitionComponents, asList(),
                Arrays.<Object>asList(), ASCENDING, EXCLUSIVE_BOUNDS, LOCAL_QUORUM, asyncListeners, limit, batchSize, false);

        RegularStatementWrapper regularWrapper = mock(RegularStatementWrapper.class);
        when(generator.generateRemoveSliceQuery(anySliceQuery())).thenReturn(regularWrapper);

        executor.remove(sliceQuery);

        verify(daoContext).execute(regularWrapper);

    }

    private CQLSliceQuery<ClusteredEntity> anySliceQuery() {
        return Mockito.any();
    }
}
