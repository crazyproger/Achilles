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

import java.util.Iterator;
import java.util.List;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;

public class SliceQueryBuilder<T> extends RootSliceQueryBuilder<T> {

    public SliceQueryBuilder(SliceQueryExecutor sliceQueryExecutor, Class<T> entityClass, EntityMeta meta) {
        super(sliceQueryExecutor, entityClass, meta);
    }

    /**
     * Query by partition key component(s) and clustering components<br/>
     * <br/>
     *
     * @param partitionComponents
     *            Partition key component(s)
     * @return SliceShortcutQueryBuilder
     */
    public SliceShortcutQueryBuilder partitionComponents(Object... partitionComponents) {
        super.partitionComponentsInternal(partitionComponents);
        return new SliceShortcutQueryBuilder();
    }

    /**
     * Query by 'from' & 'to' embeddedIds<br/>
     * <br/>
     *
     * @param fromEmbeddedId
     *            'from' embeddedId
     *
     * @return SliceFromEmbeddedIdBuilder
     */
    public SliceFromEmbeddedIdBuilder fromEmbeddedId(Object fromEmbeddedId) {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromEmbeddedId should be of type '%s'",
                embeddedIdClass.getCanonicalName());
        List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
        List<Object> partitionComponents = idMeta.extractPartitionComponents(components);
        List<Object> clusteringComponents = idMeta.extractClusteringComponents(components);

        super.partitionComponentsInternal(partitionComponents);
        this.fromClusteringsInternal(clusteringComponents);

        return new SliceFromEmbeddedIdBuilder();
    }

    /**
     * Query by 'from' & 'to' embeddedIds<br/>
     * <br/>
     *
     * @param toEmbeddedId
     *            'to' embeddedId
     *
     * @return SliceToEmbeddedIdBuilder
     */
    public SliceToEmbeddedIdBuilder toEmbeddedId(Object toEmbeddedId) {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "toEmbeddedId should be of type '%s'",
                embeddedIdClass.getCanonicalName());

        List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
        List<Object> partitionComponents = idMeta.extractPartitionComponents(components);
        List<Object> clusteringComponents = idMeta.extractClusteringComponents(components);

        super.partitionComponentsInternal(partitionComponents);
        this.toClusteringsInternal(clusteringComponents);

        return new SliceToEmbeddedIdBuilder();
    }

    public class SliceShortcutQueryBuilder extends DefaultQueryBuilder {

        protected SliceShortcutQueryBuilder() {
        }

        /**
         * Query using provided consistency level<br/>
         * <br/>
         *
         * @param consistencyLevel
         *            consistency level
         * @return SliceShortcutQueryBuilder
         */
        @Override
        public SliceShortcutQueryBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            SliceQueryBuilder.super.consistencyLevelInternal(consistencyLevel);
            return this;
        }

        /**
         * Set 'from' clustering component(s)<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            'from' clustering component(s)
         *
         * @return SliceFromClusteringsBuilder
         */
        public SliceFromClusteringsBuilder fromClusterings(Object... clusteringComponents) {
            SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
            return new SliceFromClusteringsBuilder();
        }

        /**
         * Set 'to' clustering component(s)<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            'to' clustering component(s)
         *
         * @return SliceToClusteringsBuilder
         */
        public SliceToClusteringsBuilder toClusterings(Object... clusteringComponents) {
            SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
            return new SliceToClusteringsBuilder();
        }

        /**
         * Set ordering<br/>
         * <br/>
         *
         * @param ordering
         *            ordering mode: ASCENDING or DESCENDING
         *
         * @return SliceShortcutQueryBuilder
         */
        @Override
        public SliceShortcutQueryBuilder ordering(OrderingMode ordering) {
            SliceQueryBuilder.super.ordering(ordering);
            return this;
        }

        /**
         * Get first n matching entities<br/>
         * <br/>
         *
         * @param n
         *            first n matching entities
         *
         * @return list of found entities or empty list
         */
        @Override
        public List<T> get(int n) {
            return SliceQueryBuilder.super.get(n);
        }


        /**
         * Get first n matching entities<br/>
         * <br/>
         *
         * @param limit
         *            first n matching entities
         *
         * @return list of found entities or empty list
         */
        @Override
        public AchillesFuture<List<T>> asyncGet(int limit) {
            return SliceQueryBuilder.super.asyncGet(limit);
        }

        /**
         * Get first matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return first matching entity, filtered by provided clustering
         *         components if any, or null if no matching entity is found
         */
        public T getFirstMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.getFirstMatching(clusteringComponents);
        }

        /**
         * Get first matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return first matching entity, filtered by provided clustering
         *         components if any, or null if no matching entity is found
         */
        public AchillesFuture<T> asyncGetFirstMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncGetFirstMatching(clusteringComponents);
        }

        /**
         * Get first n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return list of n first matching entities, filtered by provided
         *         clustering components if any, or empty list
         */
        public List<T> getFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
            return SliceQueryBuilder.super.getFirstMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Get first n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return list of n first matching entities, filtered by provided
         *         clustering components if any, or empty list
         */
        public AchillesFuture<List<T>> asyncGetFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncGetFirstMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Get last matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return last matching entity, filtered by provided clustering
         *         components if any, or null if no matching entity is found
         */
        public T getLastMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.getLastMatching(clusteringComponents);
        }

        /**
         * Get last matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return last matching entity, filtered by provided clustering
         *         components if any, or null if no matching entity is found
         */
        public AchillesFuture<T> asyncGetLastMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncGetLastMatching(clusteringComponents);
        }

        /**
         * Get last n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            last n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return list of last n matching entities, filtered by provided
         *         clustering components if any, or empty list
         */
        public List<T> getLastMatchingWithLimit(int limit, Object... clusteringComponents) {
            return SliceQueryBuilder.super.getLastMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Get last limit matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            last limit matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return list of last limit matching entities, filtered by provided
         *         clustering components if any, or empty list
         */
        public AchillesFuture<List<T>> asyncGetLastMatchingWithLimit(int limit, Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncGetLastMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return iterator on found entities
         */
        public Iterator<T> iterator(Object... clusteringComponents) {
            return SliceQueryBuilder.super.iteratorWithMatching(clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return iterator on found entities
         */
        public AchillesFuture<Iterator<T>> asyncIteratorWithMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncIteratorWithMatching(clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         *
         * @param fetchSize
         *            batch loading size for iterator
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return iterator on found entities
         */
        public Iterator<T> iterator(int fetchSize, Object... clusteringComponents) {
            return SliceQueryBuilder.super.iteratorWithMatchingAndFetchSize(fetchSize, clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         *
         * @param fetchSize
         *            batch loading size for iterator
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         *
         * @return iterator on found entities
         */
        public AchillesFuture<Iterator<T>> asyncIterator(int fetchSize, Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncIteratorWithMatchingAndFetchSize(fetchSize, clusteringComponents);
        }


        /**
         * Remove first n entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n entities
         */
        @Override
        public void remove(int limit) {
            SliceQueryBuilder.super.remove(limit);
        }

        /**
         * Remove first n entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n entities
         */
        @Override
        public AchillesFuture<Empty> asyncRemove(int limit) {
            return SliceQueryBuilder.super.asyncRemove(limit);
        }

        /**
         * Remove first matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public void removeFirstMatching(Object... clusteringComponents) {
            SliceQueryBuilder.super.removeFirstMatching(clusteringComponents);
        }

        /**
         * Remove first matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public AchillesFuture<Empty> asyncRemoveFirstMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncRemoveFirstMatching(clusteringComponents);
        }

        /**
         * Remove first n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public void removeFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
            SliceQueryBuilder.super.removeFirstMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Remove first n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param limit
         *            first n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public AchillesFuture<Empty> asyncRemoveFirstMatchingWithLimit(int limit, Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncRemoveFirstMatchingWithLimit(limit, clusteringComponents);
        }

        /**
         * Remove last matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public void removeLastMatching(Object... clusteringComponents) {
            SliceQueryBuilder.super.removeLastMatching(clusteringComponents);
        }

        /**
         * Remove last matching entity, using ASCENDING order<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public AchillesFuture<Empty> asyncRemoveLastMatching(Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncRemoveLastMatching(clusteringComponents);
        }


        /**
         * Remove last n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param n
         *            last n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public void removeLastMatchingWithLimit(int n, Object... clusteringComponents) {
            SliceQueryBuilder.super.removeLastMatchingWithLimit(n, clusteringComponents);
        }

        /**
         * Remove last n matching entities, using ASCENDING order<br/>
         * <br/>
         *
         * @param n
         *            last n matching entities
         *
         * @param clusteringComponents
         *            optional clustering component(s) for filtering
         */
        public AchillesFuture<Empty> asyncRemoveLastMatchingWithLimit(int n, Object... clusteringComponents) {
            return SliceQueryBuilder.super.asyncRemoveLastMatchingWithLimit(n, clusteringComponents);
        }
    }

    public class SliceFromEmbeddedIdBuilder {
        protected SliceFromEmbeddedIdBuilder() {
        }

        /**
         * Set 'to' embeddedId<br/>
         * <br/>
         *
         * @param toEmbeddedId
         *            'to' embeddedId
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder toEmbeddedId(Object toEmbeddedId) {
            SliceQueryBuilder.this.toEmbeddedId(toEmbeddedId);
            return new DefaultQueryBuilder();
        }
    }

    public class SliceToEmbeddedIdBuilder {
        protected SliceToEmbeddedIdBuilder() {
        }

        /**
         * Set 'from' embeddedId<br/>
         * <br/>
         *
         * @param fromEmbeddedId
         *            'from' embeddedId
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder fromEmbeddedId(Object fromEmbeddedId) {
            SliceQueryBuilder.this.fromEmbeddedId(fromEmbeddedId);
            return new DefaultQueryBuilder();
        }
    }

    public class SliceFromClusteringsBuilder extends DefaultQueryBuilder {

        public SliceFromClusteringsBuilder() {
        }

        /**
         * Set 'to' clustering component(s)<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            'to' clustering component(s)
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder toClusterings(Object... clusteringComponents) {
            SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
            return new DefaultQueryBuilder();
        }
    }

    public class SliceToClusteringsBuilder extends DefaultQueryBuilder {

        public SliceToClusteringsBuilder() {
        }

        /**
         * Set 'from' clustering component(s)<br/>
         * <br/>
         *
         * @param clusteringComponents
         *            'from' clustering component(s)
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder fromClusterings(Object... clusteringComponents) {
            SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
            return new DefaultQueryBuilder();
        }
    }

    public class DefaultQueryBuilder {

        protected DefaultQueryBuilder() {
        }

        /**
         * Set ordering<br/>
         * <br/>
         *
         * @param ordering
         *            ordering mode: ASCENDING or DESCENDING
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder ordering(OrderingMode ordering) {
            SliceQueryBuilder.super.ordering(ordering);
            return this;
        }

        /**
         * Set bounding mode<br/>
         * <br/>
         *
         * @param boundingMode
         *            bounding mode: ASCENDING or DESCENDING
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder bounding(BoundingMode boundingMode) {
            SliceQueryBuilder.super.bounding(boundingMode);
            return this;
        }

        /**
         * Set consistency level<br/>
         * <br/>
         *
         * @param consistencyLevel
         *            consistency level:
         *            ONE,TWO,THREE,QUORUM,LOCAL_QUORUM,EACH_QUORUM or ALL
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            SliceQueryBuilder.super.consistencyLevelInternal(consistencyLevel);
            return this;
        }

        /**
         * Set consistency level<br/>
         * <br/>
         *
         * @param asyncListeners
         *            listeners for asynchronous operations
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder asyncListeners(FutureCallback<Object>... asyncListeners) {
            SliceQueryBuilder.super.asyncListenersInternal(asyncListeners);
            return this;
        }


        /**
         * Set limit<br/>
         * <br/>
         *
         * @param limit
         *            limit to the number of returned rows
         *
         * @return DefaultQueryBuilder
         */
        public DefaultQueryBuilder limit(int limit) {
            SliceQueryBuilder.super.limit(limit);
            return this;
        }

        /**
         * Get entities<br/>
         * <br/>
         *
         *
         * @return List<T>
         */
        public List<T> get() {
            return SliceQueryBuilder.super.get();
        }

        /**
         * Get entities<br/>
         * <br/>
         *
         *
         * @return AchillesFuture<List<T>>
         */
        public AchillesFuture<List<T>> asyncGet() {
            return SliceQueryBuilder.super.asyncGet();
        }

        /**
         * Get first n entities<br/>
         * <br/>
         *
         *
         * @return List<T>
         */
        public List<T> get(int n) {
            return SliceQueryBuilder.super.get(n);
        }

        /**
         * Get first n entities<br/>
         * <br/>
         *
         *
         * @return AchillesFuture<List<T>>
         */
        public AchillesFuture<List<T>> asyncGet(int limit) {
            return SliceQueryBuilder.super.asyncGet(limit);
        }

        /**
         * Iterator on entities<br/>
         * <br/>
         *
         *
         * @return Iterator<T>
         */
        public Iterator<T> iterator() {
            return SliceQueryBuilder.super.iterator();
        }

        /**
         * Iterator on entities<br/>
         * <br/>
         *
         *
         * @return AchillesFuture<Iterator<T>>
         */
        public AchillesFuture<Iterator<T>> asyncIterator() {
            return SliceQueryBuilder.super.asyncIterator();
        }

        /**
         * Iterator on entities with fetchSize<br/>
         * <br/>
         *
         * @param fetchSize
         *            maximum number of rows to fetch on each batch
         *
         * @return Iterator<T>
         */
        public Iterator<T> iterator(int fetchSize) {
            return SliceQueryBuilder.super.iterator(fetchSize);
        }

        /**
         * Iterator on entities with fetchSize<br/>
         * <br/>
         *
         * @param fetchSize
         *            maximum number of rows to fetch on each batch
         *
         * @return AchillesFuture<Iterator<T>>
         */
        public AchillesFuture<Iterator<T>> asyncIterator(int fetchSize) {
            return SliceQueryBuilder.super.asyncIterator(fetchSize);
        }

        /**
         * Remove matched entities<br/>
         * <br/>
         *
         * @return Iterator<T>
         */
        public void remove() {
            SliceQueryBuilder.super.remove();
        }

        /**
         * Remove matched entities<br/>
         * <br/>
         *
         * @return AchillesFuture<Empty>
         */
        public AchillesFuture<Empty> asyncRemove() {
            return SliceQueryBuilder.super.asyncRemove();
        }

        /**
         * Remove first n matched entities<br/>
         * <br/>
         *
         * @param n
         *            first n matched entities
         *
         * @return Iterator<T>
         */
        public void remove(int n) {
            SliceQueryBuilder.super.remove(n);
        }

        /**
         * Remove first n matched entities<br/>
         * <br/>
         *
         * @param limit
         *            first n matched entities
         *
         * @return AchillesFuture<Empty>
         */
        public AchillesFuture<Empty> asyncRemove(int limit) {
            return SliceQueryBuilder.super.asyncRemove(limit);
        }
    }
}
