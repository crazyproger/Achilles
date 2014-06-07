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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.internal.async.Empty;
import info.archinnov.achilles.internal.proxy.EntityInterceptor;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTimeUUID;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import info.archinnov.achilles.type.TypedMap;
import net.sf.cglib.proxy.Factory;

public class AsyncQueryIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
            CompleteBean.class.getSimpleName(), TABLE_NAME, ClusteredEntityWithTimeUUID.TABLE_NAME,
            AchillesCounter.CQL_COUNTER_TABLE);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_return_rows_for_native_query_async() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(35L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(CounterBuilder.incr(17L)).buid();

        manager.persist(entity1);
        manager.persist(entity2);

        String nativeQuery = "SELECT name,age_in_years,friends,followers,preferences FROM CompleteBean WHERE id = :id";
        final AtomicReference<Object> successSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        final AchillesFuture<List<TypedMap>> future1 = manager.nativeQuery(nativeQuery, entity1.getId()).asyncGet(successCallBack);
        final AchillesFuture<List<TypedMap>> future2 = manager.nativeQuery(nativeQuery, entity2.getId()).asyncGet();

        final List<TypedMap> typedMaps1 = future1.get();
        assertThat(typedMaps1).hasSize(1);
        TypedMap typedMap1 = typedMaps1.get(0);

        final List<TypedMap> typedMaps2 = future2.get();
        assertThat(typedMaps2).hasSize(1);
        TypedMap typedMap2 = typedMaps2.get(0);

        assertThat(typedMap1.get("name")).isEqualTo("DuyHai");
        assertThat(typedMap1.get("age_in_years")).isEqualTo(35L);
        assertThat(typedMap1.<List<String>>getTyped("friends")).containsExactly("foo", "bar");
        assertThat(typedMap1.<Set<String>>getTyped("followers")).contains("George", "Paul");
        Map<Integer, String> preferences1 = typedMap1.getTyped("preferences");
        assertThat(preferences1.get(1)).isEqualTo("FR");
        assertThat(preferences1.get(2)).isEqualTo("Paris");
        assertThat(preferences1.get(3)).isEqualTo("75014");

        assertThat(typedMap2.get("name")).isEqualTo("John DOO");
        assertThat(typedMap2.get("age_in_years")).isEqualTo(35L);
        assertThat(typedMap2.<List<String>>getTyped("friends")).containsExactly("qux", "twix");
        assertThat(typedMap2.<Set<String>>getTyped("followers")).contains("Isaac", "Lara");
        Map<Integer, String> preferences2 = typedMap2.getTyped("preferences");
        assertThat(preferences2.get(1)).isEqualTo("US");
        assertThat(preferences2.get(2)).isEqualTo("NewYork");

        assertThat(successSpy.get()).isNotNull().isInstanceOf(List.class);
    }

    @Test
    public void should_excecute_DML_native_query_with_async_listeners() throws Exception {
        //Given
        Long id = RandomUtils.nextLong();
        final AtomicReference<Object> successSpy = new AtomicReference<>();
        final AtomicReference<Throwable> exceptionSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        FutureCallback<Object> exceptionCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionSpy.getAndSet(t);
            }
        };

        //When
        manager.nativeQuery("INSERT INTO completebean(id) VALUES (:id)", id).asyncExecute(successCallBack);
        manager.nativeQuery("DELETE FROM completebean WHERE name='test'").asyncExecute(exceptionCallBack);

        Thread.sleep(100);

        //Then
        assertThat(successSpy.get()).isNotNull().isSameAs(Empty.INSTANCE);
        assertThat(exceptionSpy.get()).isNotNull().isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void should_return_entities_for_typed_query_async() throws Exception {
        CompleteBean paul = builder().randomId().name("Paul").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Jack").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        CompleteBean john = builder().randomId().name("John").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").buid();

        manager.persist(paul);
        manager.persist(john);

        final AtomicReference<Object> successSpy1 = new AtomicReference<>();
        final AtomicReference<Object> successSpy2 = new AtomicReference<>();

        FutureCallback<Object> successCallBack1 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy1.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        FutureCallback<Object> successCallBack2 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy2.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        String queryString = "SELECT * FROM CompleteBean WHERE id = :id";
        final List<CompleteBean> list1 = manager.typedQuery(CompleteBean.class, queryString, paul.getId()).asyncGet(successCallBack1).get();
        final CompleteBean foundJohn = manager.typedQuery(CompleteBean.class, queryString, john.getId()).asyncGetFirst(successCallBack2).get();

        assertThat(list1).hasSize(1);

        CompleteBean foundPaul = list1.get(0);

        Factory factory1 = (Factory) foundPaul;
        @SuppressWarnings("unchecked")
        EntityInterceptor<CompleteBean> interceptor1 = (EntityInterceptor<CompleteBean>) factory1.getCallback(0);

        CompleteBean realPaul = (CompleteBean) interceptor1.getTarget();

        assertThat(realPaul.getLabel()).isNull();
        assertThat(realPaul.getWelcomeTweet()).isNull();

        assertThat(realPaul.getName()).isEqualTo(paul.getName());
        assertThat(realPaul.getAge()).isEqualTo(paul.getAge());
        assertThat(realPaul.getFriends()).containsAll(paul.getFriends());
        assertThat(realPaul.getFollowers()).containsAll(paul.getFollowers());
        assertThat(realPaul.getPreferences().get(1)).isEqualTo("FR");
        assertThat(realPaul.getPreferences().get(2)).isEqualTo("Paris");
        assertThat(realPaul.getPreferences().get(3)).isEqualTo("75014");

        Factory factory2 = (Factory) foundJohn;
        @SuppressWarnings("unchecked")
        EntityInterceptor<CompleteBean> interceptor2 = (EntityInterceptor<CompleteBean>) factory2.getCallback(0);

        CompleteBean realJohn = (CompleteBean) interceptor2.getTarget();

        assertThat(realJohn.getLabel()).isNull();
        assertThat(realJohn.getWelcomeTweet()).isNull();

        assertThat(realJohn.getName()).isEqualTo(john.getName());
        assertThat(realJohn.getAge()).isEqualTo(john.getAge());
        assertThat(realJohn.getFriends()).containsAll(john.getFriends());
        assertThat(realJohn.getFollowers()).containsAll(john.getFollowers());
        assertThat(realJohn.getPreferences().get(1)).isEqualTo("US");
        assertThat(realJohn.getPreferences().get(2)).isEqualTo("NewYork");

        assertThat(successSpy1.get()).isNotNull().isInstanceOf(List.class);
        assertThat(successSpy2.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_return_raw_entities_for_raw_typed_query_async() throws Exception {
        Counter counter1 = CounterBuilder.incr(15L);
        CompleteBean paul = builder().randomId().name("Paul").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Jack").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(counter1).buid();

        Counter counter2 = CounterBuilder.incr(17L);
        CompleteBean john = builder().randomId().name("John").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(counter2).buid();

        manager.persist(paul);
        manager.persist(john);

        final AtomicReference<Object> successSpy1 = new AtomicReference<>();
        final AtomicReference<Object> successSpy2 = new AtomicReference<>();

        FutureCallback<Object> successCallBack1 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy1.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        FutureCallback<Object> successCallBack2 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy2.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        String queryString = "SELECT * FROM CompleteBean WHERE id=:id";

        final AchillesFuture<CompleteBean> futurePaul = manager.rawTypedQuery(CompleteBean.class, queryString, paul.getId()).asyncGetFirst(successCallBack1);
        final AchillesFuture<CompleteBean> futureJohn = manager.rawTypedQuery(CompleteBean.class, queryString, john.getId()).asyncGetFirst(successCallBack2);

        CompleteBean foundPaul = futurePaul.get();

        assertThat(foundPaul.getName()).isEqualTo(paul.getName());
        assertThat(foundPaul.getAge()).isEqualTo(paul.getAge());
        assertThat(foundPaul.getFriends()).containsAll(paul.getFriends());
        assertThat(foundPaul.getFollowers()).containsAll(paul.getFollowers());
        assertThat(foundPaul.getPreferences().get(1)).isEqualTo("FR");
        assertThat(foundPaul.getPreferences().get(2)).isEqualTo("Paris");
        assertThat(foundPaul.getPreferences().get(3)).isEqualTo("75014");
        assertThat(foundPaul.getVersion()).isNull();

        CompleteBean foundJohn = futureJohn.get();
        assertThat(foundJohn.getName()).isEqualTo(john.getName());
        assertThat(foundJohn.getAge()).isEqualTo(john.getAge());
        assertThat(foundJohn.getFriends()).containsAll(john.getFriends());
        assertThat(foundJohn.getFollowers()).containsAll(john.getFollowers());
        assertThat(foundJohn.getPreferences().get(1)).isEqualTo("US");
        assertThat(foundJohn.getPreferences().get(2)).isEqualTo("NewYork");
        assertThat(foundJohn.getVersion()).isNull();

        assertThat(successSpy1.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
        assertThat(successSpy2.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_query_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4").asyncGet().get();

        assertThat(entities).isEmpty();

        final AtomicReference<Object> successSpy = new AtomicReference<>();
        final AtomicReference<Throwable> exceptionSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        FutureCallback<Object> exceptionCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionSpy.getAndSet(t);
            }
        };

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .partitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4")
                .asyncListeners(successCallBack).asyncGet().get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

        assertThat(successSpy.get()).isNotNull().isInstanceOf(List.class);
        List<CompleteBean> raws = (List<CompleteBean>) successSpy.get();
        assertThat(raws).hasSize(3);
        assertThat(raws.get(0)).isNotInstanceOf(Factory.class);

        manager.sliceQuery(ClusteredEntity.class)
                .fromEmbeddedId(new ClusteredEntity.ClusteredKey(partitionKey, 1, "name2"))
                .toEmbeddedId(new ClusteredEntity.ClusteredKey(partitionKey, 1, "name4"))
                .consistencyLevel(EACH_QUORUM).asyncListeners(exceptionCallBack)
                .asyncGet();

        Thread.sleep(100);
        assertThat(exceptionSpy.get()).isNotNull().isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void should_query_async_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .fromClusterings(1, "name4").toClusterings(1, "name1").bounding(INCLUSIVE_END_BOUND_ONLY)
                .ordering(DESCENDING).limit(2).asyncGet().get();

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

        entities = manager.sliceQuery(ClusteredEntity.class).fromEmbeddedId(new ClusteredEntity.ClusteredKey(partitionKey, 1, "name4"))
                .toEmbeddedId(new ClusteredEntity.ClusteredKey(partitionKey, 1, "name1")).bounding(INCLUSIVE_END_BOUND_ONLY)
                .ordering(DESCENDING).limit(4).get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);

    }

    @Test
    public void should_query_async_with_consistency_level() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 1, 5);

        final AchillesFuture<List<ClusteredEntity>> futures = manager.sliceQuery(ClusteredEntity.class)
                .partitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1, "name4")
                .consistencyLevel(EACH_QUORUM).asyncGet();

        exception.expect(ExecutionException.class);
        exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

        futures.get();
    }

    @Test
    public void should_query_async_with_getFirst() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncGetFirstMatching().get();

        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entity = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey).asyncGetFirstMatching().get();

        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 1);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .partitionComponents(partitionKey)
                .asyncGetFirstMatchingWithLimit(3).get();

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);

        entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncGetFirstMatchingWithLimit(3, 4).get();

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 41);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 42);

    }

    @Test
    public void should_query_async_with_getLast() throws Exception {
        long partitionKey = RandomUtils.nextLong();

        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncGetLastMatching().get();

        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entity = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey).asyncGetLastMatching().get();
        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 5);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncGetLastMatchingWithLimit(3).get();

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);
        insertClusteredEntity(partitionKey, 4, "name43", clusteredValuePrefix + 43);
        insertClusteredEntity(partitionKey, 4, "name44", clusteredValuePrefix + 44);

        entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncGetLastMatchingWithLimit(3, 4).get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 44);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 43);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 42);
    }

    @Test
    public void should_iterate_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .asyncIterator().get();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntity next = iter.next();
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(iter.hasNext()).isTrue();

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name2");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 2);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name3");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 3);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name4");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 4);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name5");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_async_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1).asyncIterator(2).get();

        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_async_over_clusterings_components() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong();
        insertClusteredEntity(partitionKey, 1, "name11", "val11");
        insertClusteredEntity(partitionKey, 1, "name12", "val12");
        insertClusteredEntity(partitionKey, 1, "name13", "val13");
        insertClusteredEntity(partitionKey, 2, "name21", "val21");
        insertClusteredEntity(partitionKey, 2, "name22", "val22");
        insertClusteredEntity(partitionKey, 3, "name31", "val31");
        insertClusteredEntity(partitionKey, 4, "name41", "val41");

        //When
        final Iterator<ClusteredEntity> iterator = manager.sliceQuery(ClusteredEntity.class)
                .partitionComponents(partitionKey).fromClusterings(1)
                .bounding(INCLUSIVE_START_BOUND_ONLY)
                .limit(6).asyncIterator(2).get();

        //Then
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val11");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val12");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val13");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val21");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val22");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val31");

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_remove_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong();
        String clusteredValuePrefix = insertValues(partitionKey, 1, 2);
        insertValues(partitionKey, 2, 3);
        insertValues(partitionKey, 3, 1);

        manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey).fromClusterings(2).toClusterings(2)
                .asyncRemove().get();

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionComponents(partitionKey).get(100);

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

        assertThat(entities.get(2).getId().getCount()).isEqualTo(3);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);
    }

    private String insertValues(long partitionKey, int countValue, int size) {
        String namePrefix = "name";
        String clusteredValuePrefix = "value";

        for (int i = 1; i <= size; i++) {
            insertClusteredEntity(partitionKey, countValue, namePrefix + i, clusteredValuePrefix + i);
        }
        return clusteredValuePrefix;
    }

    private void insertClusteredEntity(Long partitionKey, int count, String name, String clusteredValue) {
        ClusteredEntity.ClusteredKey embeddedId = new ClusteredEntity.ClusteredKey(partitionKey, count, name);
        ClusteredEntity entity = new ClusteredEntity(embeddedId, clusteredValue);
        manager.persist(entity);
    }
}
