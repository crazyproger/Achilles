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
package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

@RunWith(MockitoJUnitRunner.class)
public class RootSliceQueryBuilderTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private RootSliceQueryBuilder<ClusteredEntity> builder;

    private Class<ClusteredEntity> entityClass = ClusteredEntity.class;

    @Mock
    private SliceQueryExecutor sliceQueryExecutor;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private List<ClusteredEntity> result;

    @Mock
    private AchillesFuture<Iterator<ClusteredEntity>> achillesIterator;

    @Mock
    private ListenableFuture<ClusteredEntity> futureEntity;

    @Mock
    private AchillesFuture<ClusteredEntity> achillesFutureEntity;

    @Mock
    private AchillesFuture<Empty> achillesFutureEmpty;

    @Mock
    private AchillesFuture<List<ClusteredEntity>> achillesFutureEntities;

    @Mock
    private ClusteredEntity entity;

    @Captor
    private ArgumentCaptor<Function<List<ClusteredEntity>, ClusteredEntity>> takeFirstCaptor;

    private List<ClusteredEntity> entities = Arrays.asList();


    @Before
    public void setUp() {
        builder.sliceQueryExecutor = sliceQueryExecutor;
        builder.asyncUtils = asyncUtils;
        builder.entityClass = entityClass;
        Whitebox.setInternalState(builder, "meta", meta);
        Whitebox.setInternalState(builder, "idMeta", idMeta);
        Whitebox.setInternalState(builder, "partitionComponents", new ArrayList<>());
        Whitebox.setInternalState(builder, "fromClusterings", new ArrayList<>());
        Whitebox.setInternalState(builder, "toClusterings", new ArrayList<>());

        when(achillesFutureEmpty.getImmediately()).thenReturn(Empty.INSTANCE);
        when(achillesFutureEntities.getImmediately()).thenReturn(entities);

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getClassName()).thenReturn("entityClass");
        doCallRealMethod().when(builder).partitionComponentsInternal(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_set_partition_components() throws Exception {
        builder.partitionComponentsInternal(11L);

        verify(idMeta).validatePartitionComponents(Arrays.<Object>asList(11L));

        assertThat((List<Object>) Whitebox.getInternalState(builder, "partitionComponents")).containsExactly(11L);
    }

    @Test
    public void should_exception_when_partition_components_do_not_match() throws Exception {
        Whitebox.setInternalState(builder, "partitionComponents", Arrays.<Object>asList(10L, "type"));

        exception.expect(AchillesException.class);
        exception.expectMessage("Partition components '[11, type]' do not match previously set values '[10, type]'");
        builder.partitionComponentsInternal(11L, "type");
    }

    @Test
    public void should_exception_when_partition_components_not_same_size_as_previously() throws Exception {
        Whitebox.setInternalState(builder, "partitionComponents", Arrays.<Object>asList(10L, "type"));

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Partition components '[11, type, 11]' do not match previously set values '[10, type]'");
        builder.partitionComponentsInternal(11L, "type", 11);
    }

    @Test
    public void should_set_clustering_from() throws Exception {

        when(idMeta.encodeToComponents(anyListOf(Object.class))).thenReturn(Arrays.<Object>asList(10L, 11L, "a", 12));
        builder.partitionComponentsInternal(10L).fromClusteringsInternal(11L, "a", 12);

        verify(idMeta).validateClusteringComponents(Arrays.<Object>asList(11L, "a", 12));
        assertThat(builder.buildClusterQuery().getClusteringsFrom()).containsExactly(10L, 11L, "a", 12);

    }

    @Test
    public void should_set_clustering_to() throws Exception {
        when(idMeta.encodeToComponents(anyListOf(Object.class))).thenReturn(Arrays.<Object>asList(10L, 11L, "a", 12));
        builder.partitionComponentsInternal(10L).toClusteringsInternal(11L, "a", 12);

        verify(idMeta).validateClusteringComponents(Arrays.<Object>asList(11L, "a", 12));

        assertThat(builder.buildClusterQuery().getClusteringsTo()).containsExactly(10L, 11L, "a", 12);
    }

    @Test
    public void should_set_ordering() throws Exception {
        builder.partitionComponentsInternal(10L).ordering(DESCENDING);

        assertThat(builder.buildClusterQuery().getOrdering()).isEqualTo(DESCENDING);
    }

    @Test
    public void should_exception_when_null_ordering() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage("Ordering mode for slice query for entity 'entityClass' should not be null");

        builder.partitionComponentsInternal(10L).ordering(null);
    }

    @Test
    public void should_set_bounding_mode() throws Exception {
        builder.partitionComponentsInternal(10L).bounding(EXCLUSIVE_BOUNDS);

        assertThat(builder.buildClusterQuery().getBounding()).isEqualTo(EXCLUSIVE_BOUNDS);
    }

    @Test
    public void should_exception_when_null_bounding() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage("Bounding mode for slice query for entity 'entityClass' should not be null");

        builder.partitionComponentsInternal(10L).bounding(null);
    }

    @Test
    public void should_set_consistency_level() throws Exception {
        builder.partitionComponentsInternal(10L).consistencyLevelInternal(EACH_QUORUM);

        assertThat(builder.buildClusterQuery().getConsistencyLevel()).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_exception_when_null_consistency_level() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage("ConsistencyLevel for slice query for entity 'entityClass' should not be null");

        builder.partitionComponentsInternal(10L).consistencyLevelInternal(null);
    }

    @Test
    public void should_set_limit() throws Exception {
        builder.partitionComponentsInternal(10L).limit(53);

        assertThat(builder.buildClusterQuery().getLimit()).isEqualTo(53);
    }

    @Test
    public void should_get() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);

        List<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).get();

        assertThat(actual).isSameAs(entities);
    }

    @Test
    public void should_get_n() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);

        List<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).get(5);

        assertThat(actual).isSameAs(entities);
        assertThat(Whitebox.<Integer>getInternalState(builder, "limit")).isEqualTo(5);
    }

    @Test
    public void should_get_first() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);
        when(asyncUtils.transformFuture(eq(achillesFutureEntities), takeFirstCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        final AchillesFuture<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).asyncGetFirstMatching();

        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(Whitebox.<Integer>getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_get_first_async_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);
        when(asyncUtils.transformFuture(eq(achillesFutureEntities), takeFirstCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        Object[] clusteringComponents = new Object[] { 1, "name" };
        final AchillesFuture<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).asyncGetFirstMatching(clusteringComponents);

        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_get_first_n_async() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);

        final AchillesFuture<List<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncGetFirstMatchingWithLimit(3);

        assertThat(actual).isSameAs(achillesFutureEntities);
        assertThat(Whitebox.<Integer>getInternalState(builder, "limit")).isEqualTo(3);
    }

    @Test
    public void should_get_first_n_async_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);

        Object[] clusteringComponents = new Object[] { 1, "name" };
        final AchillesFuture<List<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncGetFirstMatchingWithLimit(3, clusteringComponents);

        assertThat(actual).isSameAs(achillesFutureEntities);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(3);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_get_last_async() throws Exception {
        Long partitionKey = RandomUtils.nextLong();

        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);
        when(asyncUtils.transformFuture(eq(achillesFutureEntities), takeFirstCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        final AchillesFuture<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).asyncGetLastMatching();

        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_get_last_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();

        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);
        when(asyncUtils.transformFuture(eq(achillesFutureEntities), takeFirstCaptor.capture())).thenReturn(futureEntity);
        when(asyncUtils.buildInterruptible(futureEntity)).thenReturn(achillesFutureEntity);

        Object[] clusteringComponents = new Object[] { 1, "name" };

        final AchillesFuture<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).asyncGetLastMatching(clusteringComponents);

        assertThat(actual).isSameAs(achillesFutureEntity);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_get_last_n() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);

        final AchillesFuture<List<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncGetLastMatchingWithLimit(6);

        assertThat(actual).isSameAs(achillesFutureEntities);
        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
    }

    @Test
    public void should_get_last_n_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();

        when(sliceQueryExecutor.asyncGet(anySliceQuery())).thenReturn(achillesFutureEntities);
        Object[] clusteringComponents = new Object[] { 1, "name" };

        final AchillesFuture<List<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncGetLastMatchingWithLimit(6, clusteringComponents);

        assertThat(actual).isSameAs(achillesFutureEntities);
        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);

    }

    @Test
    public void should_get_iterator() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncIterator(anySliceQuery())).thenReturn(achillesIterator);

        final AchillesFuture<Iterator<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncIterator();

        assertThat(actual).isSameAs(achillesIterator);
    }

    @Test
    public void should_get_iterator_with_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };

        when(sliceQueryExecutor.asyncIterator(anySliceQuery())).thenReturn(achillesIterator);

        final AchillesFuture<Iterator<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncIteratorWithMatching(clusteringComponents);

        assertThat(actual).isSameAs(achillesIterator);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);

    }

    @Test
    public void should_get_iterator_with_batch_size() throws Exception {
        Long partitionKey = RandomUtils.nextLong();

        when(sliceQueryExecutor.asyncIterator(anySliceQuery())).thenReturn(achillesIterator);

        final AchillesFuture<Iterator<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncIterator(7);

        assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
        assertThat(actual).isSameAs(achillesIterator);
    }

    @Test
    public void should_get_iterator_with_batch_size_and_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };

        when(sliceQueryExecutor.asyncIterator(anySliceQuery())).thenReturn(achillesIterator);

        final AchillesFuture<Iterator<ClusteredEntity>> actual = builder.partitionComponentsInternal(partitionKey).asyncIteratorWithMatchingAndFetchSize(7, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
        assertThat(actual).isSameAs(achillesIterator);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);

    }

    @Test
    public void should_remove_async() throws Exception {
        // Given
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).remove();

        // When
        final AchillesFuture<Empty> actual = builder.asyncRemove();
        assertThat(actual).isSameAs(achillesFutureEmpty);
    }

    @Test
    public void should_remove_n() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);

        builder.partitionComponentsInternal(partitionKey).remove(8);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(8);
    }

    @Test
    public void should_remove_first() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);

        builder.partitionComponentsInternal(partitionKey).removeFirstMatching();

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_remove_first_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);

        Object[] clusteringComponents = new Object[] { 1, "name" };

        builder.partitionComponentsInternal(partitionKey).removeFirstMatching(clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_remove_first_n() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeFirstMatchingWithLimit(9);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
    }

    @Test
    public void should_remove_first_n_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeFirstMatchingWithLimit(9, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_remove_last() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeLastMatching();

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_remove_last_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeLastMatching(clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    @Test
    public void should_remove_last_n() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeLastMatchingWithLimit(10);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
    }

    @Test
    public void should_remove_last_n_with_clustering_components() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        when(sliceQueryExecutor.asyncRemove(anySliceQuery())).thenReturn(achillesFutureEmpty);
        builder.partitionComponentsInternal(partitionKey).removeLastMatchingWithLimit(10, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "fromClusterings")).containsExactly(
                clusteringComponents);
        assertThat(Whitebox.<List<Object>>getInternalState(builder, "toClusterings")).containsExactly(
                clusteringComponents);
    }

    private SliceQuery<ClusteredEntity> anySliceQuery() {
        return Mockito.any();
    }
}
