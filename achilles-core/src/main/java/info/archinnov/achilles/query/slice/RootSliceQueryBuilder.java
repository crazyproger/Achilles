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

import static info.archinnov.achilles.internal.validation.Validator.validateFalse;
import static info.archinnov.achilles.query.slice.SliceQuery.DEFAULT_BATCH_SIZE;
import static info.archinnov.achilles.query.slice.SliceQuery.DEFAULT_LIMIT;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;

public abstract class RootSliceQueryBuilder<T> {
    private static final Logger log = LoggerFactory.getLogger(RootSliceQueryBuilder.class);

    protected AsyncUtils asyncUtils = new AsyncUtils();
    protected SliceQueryExecutor sliceQueryExecutor;
    protected Class<T> entityClass;
    protected EntityMeta meta;

    protected List<Object> partitionComponents = new ArrayList<>();
    private PropertyMeta idMeta;
    private List<Object> fromClusterings = new ArrayList<>();
    private List<Object> toClusterings = new ArrayList<>();
    private OrderingMode ordering = OrderingMode.ASCENDING;
    private BoundingMode bounding = BoundingMode.INCLUSIVE_BOUNDS;
    private ConsistencyLevel consistencyLevel;
    private FutureCallback<Object>[] asyncListeners;
    private int limit = DEFAULT_LIMIT;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean limitHasBeenSet = false;
    private boolean orderingHasBeenSet = false;

    private Function<List<T>, T> takeFirstFunction = new Function<List<T>, T>() {
        @Override
        public T apply(List<T> result) {
            if (result.isEmpty()) {
                return null;
            } else {
                return result.get(0);
            }
        }
    };

    RootSliceQueryBuilder(SliceQueryExecutor sliceQueryExecutor, Class<T> entityClass, EntityMeta meta) {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
        this.idMeta = meta.getIdMeta();
    }

    protected RootSliceQueryBuilder<T> partitionComponentsInternal(List<Object> partitionComponents) {
        log.trace("Add partition key components {}", partitionComponents);
        idMeta.validatePartitionComponents(partitionComponents);
        if (this.partitionComponents.size() > 0) {
            Validator.validateTrue(this.partitionComponents.size() == partitionComponents.size(),
                    "Partition components '%s' do not match previously set values '%s'", partitionComponents,
                    this.partitionComponents);
            for (int i = 0; i < partitionComponents.size(); i++) {
                Validator.validateTrue(this.partitionComponents.get(i).equals(partitionComponents.get(i)),
                        "Partition components '%s' do not match previously set values '%s'", partitionComponents,
                        this.partitionComponents);
            }
        }
        this.partitionComponents = partitionComponents;
        return this;
    }

    protected RootSliceQueryBuilder<T> partitionComponentsInternal(Object... partitionComponents) {
        this.partitionComponentsInternal(Arrays.asList(partitionComponents));
        return this;
    }

    protected RootSliceQueryBuilder<T> fromClusteringsInternal(List<Object> clusteringComponents) {
        log.trace("Add clustering components {}", clusteringComponents);
        idMeta.validateClusteringComponents(clusteringComponents);
        fromClusterings = clusteringComponents;
        return this;
    }

    protected RootSliceQueryBuilder<T> fromClusteringsInternal(Object... clusteringComponents) {
        this.fromClusteringsInternal(Arrays.asList(clusteringComponents));
        return this;
    }

    protected RootSliceQueryBuilder<T> toClusteringsInternal(List<Object> clusteringComponents) {
        log.trace("Add clustering components {}", clusteringComponents);
        idMeta.validateClusteringComponents(clusteringComponents);
        toClusterings = clusteringComponents;
        return this;
    }

    protected RootSliceQueryBuilder<T> toClusteringsInternal(Object... clusteringComponents) {
        this.toClusteringsInternal(Arrays.asList(clusteringComponents));
        return this;
    }

    protected RootSliceQueryBuilder<T> ordering(OrderingMode ordering) {
        Validator.validateNotNull(ordering, "Ordering mode for slice query for entity '%s' should not be null",
                meta.getClassName());
        this.ordering = ordering;
        orderingHasBeenSet = true;
        return this;
    }

    protected RootSliceQueryBuilder<T> bounding(BoundingMode boundingMode) {
        Validator.validateNotNull(boundingMode, "Bounding mode for slice query for entity '%s' should not be null",
                meta.getClassName());
        bounding = boundingMode;

        return this;
    }

    protected RootSliceQueryBuilder<T> consistencyLevelInternal(ConsistencyLevel consistencyLevel) {
        Validator.validateNotNull(consistencyLevel,
                "ConsistencyLevel for slice query for entity '%s' should not be null", meta.getClassName());
        this.consistencyLevel = consistencyLevel;

        return this;
    }

    protected RootSliceQueryBuilder<T> asyncListenersInternal(FutureCallback<Object>... asyncListeners) {
        Validator.validateNotEmpty(asyncListeners, "Async listeners for slice query for entity '%s' should not be null", meta.getClassName());
        this.asyncListeners = asyncListeners;
        return this;
    }

    protected RootSliceQueryBuilder<T> limit(int limit) {
        this.limit = limit;
        limitHasBeenSet = true;
        return this;
    }

    protected List<T> get() {
        log.trace("Get {} results from slice query", limit);
        return asyncGet().getImmediately();
    }

    protected AchillesFuture<List<T>> asyncGet() {
        log.trace("Get {} results from slice query asynchronously", limit);
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncGet(clusteredQuery);
    }

    protected List<T> get(int limit) {
        log.trace("Get {} results from slice query", limit);
        return asyncGet(limit).getImmediately();
    }

    protected AchillesFuture<List<T>> asyncGet(int limit) {
        log.trace("Get {} results from slice query asynchronously", limit);
        this.limit = limit;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncGet(clusteredQuery);
    }

    protected T getFirstMatching(Object... clusteringComponents) {
        log.trace("Get first results from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        return asyncGetFirstMatching(clusteringComponents).getImmediately();
    }

    protected AchillesFuture<T> asyncGetFirstMatching(Object... clusteringComponents) {
        log.trace("Get first results asynchronously from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getFirstMatching/asyncGetFirstMatching(Object... clusteringComponents)");
        limit = 1;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        final AchillesFuture<List<T>> achillesFuture = sliceQueryExecutor.asyncGet(clusteredQuery);

        final ListenableFuture<T> futureEntity = asyncUtils.transformFuture(achillesFuture, takeFirstFunction);
        return asyncUtils.buildInterruptible(futureEntity);
    }

    protected List<T> getFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Get first results from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        return asyncGetFirstMatchingWithLimit(limit, clusteringComponents).getImmediately();
    }

    protected AchillesFuture<List<T>> asyncGetFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Get first results asynchronously from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getFirstMatchingWithLimit/asyncGetFirstMatchingWithLimit(int limit, Object... clusteringComponents)");
        this.limit = limit;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncGet(clusteredQuery);
    }

    protected T getLastMatching(Object... clusteringComponents) {
        log.trace("Get last results from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        return asyncGetLastMatching(clusteringComponents).getImmediately();
    }

    protected AchillesFuture<T> asyncGetLastMatching(Object... clusteringComponents) {
        log.trace("Get last results asynchronously from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling getLastMatching/asyncGetLastMatching(Object... clusteringComponents)");
        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getLastMatching/asyncGetLastMatching(Object... clusteringComponents)");
        limit = 1;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        final AchillesFuture<List<T>> achillesFuture = sliceQueryExecutor.asyncGet(clusteredQuery);

        final ListenableFuture<T> futureEntity = asyncUtils.transformFuture(achillesFuture, takeFirstFunction);
        return asyncUtils.buildInterruptible(futureEntity);
    }

    protected List<T> getLastMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Get last results from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        return asyncGetLastMatchingWithLimit(limit, clusteringComponents).getImmediately();
    }

    protected AchillesFuture<List<T>> asyncGetLastMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Get last results asynchronously from slice query having clustering values {} limit to {}", clusteringComponents, limit);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling getLastMatchingWithLimit/asyncGetLastMatchingWithLimit(int limit, Object... clusteringComponents)");
        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling getLastMatchingWithLimit/asyncGetLastMatchingWithLimit(int limit, Object... clusteringComponents)");
        this.limit = limit;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncGet(clusteredQuery);
    }

    protected Iterator<T> iterator() {
        log.trace("Result iterator from slice query with fetch size {}", limit);
        return asyncIterator().getImmediately();
    }

    protected AchillesFuture<Iterator<T>> asyncIterator() {
        log.trace("Asynchronous result iterator from slice query with fetch size {}", limit);
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncIterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithMatching(Object... clusteringComponents) {
        log.trace("Result iterator from slice query with clustering {} and fetch size {}", clusteringComponents, limit);
        return asyncIteratorWithMatching(clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Iterator<T>> asyncIteratorWithMatching(Object... clusteringComponents) {
        log.trace("Asynchronous result iterator from slice query with clustering {} and fetch size {}", clusteringComponents, limit);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncIterator(clusteredQuery);
    }

    protected Iterator<T> iterator(int fetchSize) {
        log.trace("Result iterator from slice query with fetch size {}", fetchSize);
        return asyncIterator(fetchSize).getImmediately();
    }

    protected AchillesFuture<Iterator<T>> asyncIterator(int fetchSize) {
        log.trace("Asynchronous result iterator from slice query with fetch size {}", fetchSize);
        this.batchSize = fetchSize;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncIterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithMatchingAndFetchSize(int fetchSize, Object... clusteringComponents) {
        log.trace("Result iterator from slice query with clustering {} and fetch size {}", clusteringComponents, fetchSize);
        return asyncIteratorWithMatchingAndFetchSize(fetchSize, clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Iterator<T>> asyncIteratorWithMatchingAndFetchSize(int fetchSize, Object... clusteringComponents) {
        log.trace("Asynchronous result iterator from slice query with clustering {} and fetch size {}", clusteringComponents, fetchSize);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);
        this.batchSize = fetchSize;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncIterator(clusteredQuery);
    }

    protected void remove() {
        log.trace("Remove {} results from slice query", limit);
        asyncRemove().getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemove() {
        log.trace("Remove asynchronously {} results from slice query", limit);
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected void remove(int limit) {
        log.trace("Remove {} results from slice query", limit);
        asyncRemove(limit).getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemove(int limit) {
        log.trace("Remove asynchronously {} results from slice query", this.limit);
        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling remove/asyncRemove(int limit)");
        this.limit = limit;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected void removeFirstMatching(Object... clusteringComponents) {
        log.trace("Remove first {} results with clustering {} from slice query", limit, clusteringComponents);
        asyncRemoveFirstMatching(clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemoveFirstMatching(Object... clusteringComponents) {
        log.trace("Remove asynchronously first {} results with clustering {} from slice query", limit, clusteringComponents);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeFirstMatching/asyncRemoveFirstMatching(Object... clusteringComponents)");
        limit = 1;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected void removeFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Remove first {} results with clustering {} from slice query", limit, clusteringComponents);
        asyncRemoveFirstMatchingWithLimit(limit, clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemoveFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Remove asynchronously first {} results with clustering {} from slice query", limit, clusteringComponents);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeFirstMatchingWithLimit/asyncRemoveFirstMatchingWithLimit(int limit, Object... clusteringComponents)");
        this.limit = limit;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected void removeLastMatching(Object... clusteringComponents) {
        log.trace("Remove last {} results with clustering {} from slice query", limit, clusteringComponents);
        asyncRemoveLastMatching(clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemoveLastMatching(Object... clusteringComponents) {
        log.trace("Remove asynchronously last {} results with clustering {} from slice query", limit, clusteringComponents);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling removeLastMatching/asyncRemoveLastMatching(Object... clusteringComponents)");
        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeLastMatching/asyncRemoveLastMatching(Object... clusteringComponents)");
        limit = 1;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected void removeLastMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Remove last {} results with clustering {} from slice query", limit, clusteringComponents);
        asyncRemoveLastMatchingWithLimit(limit, clusteringComponents).getImmediately();
    }

    protected AchillesFuture<Empty> asyncRemoveLastMatchingWithLimit(int limit, Object... clusteringComponents) {
        log.trace("Remove asynchronously last {} results with clustering {} from slice query", limit, clusteringComponents);
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        validateFalse(orderingHasBeenSet, "You should not set 'ordering' parameter when calling removeLastMatchingWithLimit/asyncRemoveLastMatchingWithLimit(int limit, Object... clusteringComponents)");
        validateFalse(limitHasBeenSet, "You should not set 'limit' parameter when calling removeLastMatchingWithLimit/asyncRemoveLastMatchingWithLimit(int limit, Object... clusteringComponents)");
        this.limit = limit;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.asyncRemove(clusteredQuery);
    }

    protected SliceQuery<T> buildClusterQuery() {
        return new SliceQuery<>(entityClass, meta, partitionComponents, fromClusterings, toClusterings, ordering,
                bounding, consistencyLevel, asyncListeners, limit, batchSize, limitHasBeenSet);
    }
}
