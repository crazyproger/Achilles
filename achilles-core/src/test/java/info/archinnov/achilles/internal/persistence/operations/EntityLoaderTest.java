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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest {

    @InjectMocks
    private EntityLoader loader;

    @Mock
    private EntityMapper mapper;

    @Mock
    private CounterLoader counterLoader;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private PersistenceContext.EntityFacade context;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private PropertyMeta pm;

    @Mock
    private ListenableFuture<Row> futureRow;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    @Captor
    private ArgumentCaptor<Function<Row, CompleteBean>> rowToEntityCaptor;

    private Long primaryKey = RandomUtils.nextLong();

    private CompleteBean entity = new CompleteBean();

    @Before
    public void setUp() throws Exception {

        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.getExecutorService()).thenReturn(executorService);
        when(meta.getIdMeta()).thenReturn(idMeta);
    }

    @Test
    public void should_create_empty_entity() throws Exception {
        when(meta.instanciate()).thenReturn(entity);

        CompleteBean actual = loader.createEmptyEntity(context, CompleteBean.class);

        assertThat(actual).isSameAs(entity);

        verify(idMeta).setValueToField(actual, primaryKey);
    }

    @Test
    public void should_load_simple_entity() throws Exception {
        // Given
        Row row = mock(Row.class);
        when(meta.isClusteredCounter()).thenReturn(false);
        when(context.loadEntity()).thenReturn(futureRow);
        when(meta.instanciate()).thenReturn(entity);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(row);
        assertThat(actualEntity).isSameAs(entity);

        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);

        verifyZeroInteractions(counterLoader);
    }

    @Test
    public void should_not_load_simple_entity_when_not_found() throws Exception {
        // Given
        when(meta.isClusteredCounter()).thenReturn(false);
        when(context.loadEntity()).thenReturn(futureRow);
        when(asyncUtils.transformFuture(eq(futureRow), rowToEntityCaptor.capture(), eq(executorService))).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        final CompleteBean actualEntity = rowToEntityCaptor.getValue().apply(null);
        assertThat(actualEntity).isNull();
        verify(meta, never()).instanciate();
        verifyZeroInteractions(counterLoader, mapper);
    }

    @Test
    public void should_load_clustered_counter_entity() throws Exception {
        // Given
        when(meta.isClusteredCounter()).thenReturn(true);
        when(counterLoader.<CompleteBean>loadClusteredCounters(context)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = loader.load(context, CompleteBean.class);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        verifyZeroInteractions(asyncUtils, mapper);
    }

    @Test
    public void should_load_properties_into_object() throws Exception {
        // Given
        when(pm.type()).thenReturn(PropertyType.SIMPLE);
        Row row = mock(Row.class);
        when(context.loadProperty(pm)).thenReturn(row);

        // When
        loader.loadPropertyIntoObject(context, entity, pm);

        // Then
        verify(mapper).setPropertyToEntity(row, pm, entity);
        verifyZeroInteractions(counterLoader);
    }

    @Test
    public void should_load_counter_properties_into_object() throws Exception {
        // Given
        when(pm.type()).thenReturn(PropertyType.COUNTER);

        // When
        loader.loadPropertyIntoObject(context, entity, pm);

        // Then
        verify(counterLoader).loadCounter(context, entity, pm);
        verifyZeroInteractions(mapper);
    }
}
