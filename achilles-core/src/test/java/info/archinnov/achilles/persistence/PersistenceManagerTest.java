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
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityInitializer;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityValidator;
import info.archinnov.achilles.internal.persistence.operations.OptionsValidator;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.query.cql.NativeQuery;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private EntityInitializer initializer;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private EntityValidator entityValidator;

    @Mock
    private TypedQueryValidator typedQueryValidator;

    @Mock
    private SliceQueryExecutor sliceQueryExecutor;

    @Mock
    private OptionsValidator optionsValidator;

    @Mock
    private PersistenceManagerFactory pmf;

    @Mock
    private PersistenceContextFactory contextFactory;

    @Mock
    private DaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private PersistenceContext context;

    @Mock
    private PersistenceContext.PersistenceManagerFacade facade;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta idMeta;

    @Captor
    private ArgumentCaptor<Options> optionsCaptor;

    private PersistenceManager manager;

    private Long primaryKey = RandomUtils.nextLong();
    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

    @Before
    public void setUp() throws Exception {
        when(contextFactory.newContext(eq(entity), optionsCaptor.capture())).thenReturn(context);
        when(context.getPersistenceManagerFacade()).thenReturn(facade);
        when(context.getEntityFacade()).thenReturn(entityFacade);
        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);
        when(meta.getIdMeta()).thenReturn(idMeta);

        manager = new PersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
        manager = Mockito.spy(this.manager);
        Whitebox.setInternalState(manager, EntityProxifier.class, proxifier);
        Whitebox.setInternalState(manager, EntityValidator.class, entityValidator);
        Whitebox.setInternalState(manager, OptionsValidator.class, optionsValidator);
        Whitebox.setInternalState(manager, SliceQueryExecutor.class, sliceQueryExecutor);
        Whitebox.setInternalState(manager, TypedQueryValidator.class, typedQueryValidator);
        Whitebox.setInternalState(manager, PersistenceContextFactory.class, contextFactory);

        manager.setEntityMetaMap(entityMetaMap);
        entityMetaMap.put(CompleteBean.class, meta);
    }

    @Test
    public void should_persist() throws Exception {
        // Given
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);
        when(facade.persist(entity).getImmediately()).thenReturn(entity);

        // When
        CompleteBean actual = manager.persist(entity);

        // Then
        assertThat(actual).isSameAs(entity);
        verify(proxifier).ensureNotProxy(entity);
        verify(entityValidator).validateEntity(entity, entityMetaMap);
    }

    @Test
    public void should_persist_with_options() throws Exception {
        // Given
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);
        when(facade.persist(entity).getImmediately()).thenReturn(entity);

        // When
        CompleteBean actual = manager.persist(entity, withConsistency(EACH_QUORUM).withTtl(150)
                .withTimestamp(100L));

        // Then
        Options options = optionsCaptor.getValue();
        assertThat(actual).isSameAs(entity);
        verify(entityValidator).validateEntity(entity, entityMetaMap);
        verify(optionsValidator).validateOptionsForInsert(entity, entityMetaMap, options);
        verify(proxifier).ensureNotProxy(entity);
        verify(facade).persist(entity);

        assertThat(options.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
        assertThat(options.getTtl().get()).isEqualTo(150);
        assertThat(options.getTimestamp().get()).isEqualTo(100L);
    }

    @Test
    public void should_update() throws Exception {
        // Given
        when(proxifier.isProxy(entity)).thenReturn(true);
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.update(entity);

        // Then
        verify(proxifier).ensureProxy(entity);
        verify(entityValidator).validateEntity(entity, entityMetaMap);
        verify(facade).update(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_update_with_options() throws Exception {
        // Given
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.update(entity, withConsistency(EACH_QUORUM).withTtl(150).withTimestamp(100L));

        // Then
        Options options = optionsCaptor.getValue();
        verify(proxifier).ensureProxy(entity);
        verify(entityValidator).validateEntity(entity, entityMetaMap);
        verify(optionsValidator).validateOptionsForUpdate(entity, entityMetaMap, options);
        verify(facade).update(entity);

        assertThat(options.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
        assertThat(options.getTtl().get()).isEqualTo(150);
        assertThat(options.getTimestamp().get()).isEqualTo(100L);
    }

    @Test
    public void should_remove() throws Exception {
        // Given
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.remove(entity);

        // Then
        verify(entityValidator).validateEntity(entity, entityMetaMap);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_remove_with_consistency() throws Exception {
        // Given
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.remove(entity, withConsistency(EACH_QUORUM));

        // Then
        verify(entityValidator).validateEntity(entity, entityMetaMap);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_remove_by_id() throws Exception {
        // When
        when(contextFactory.newContext(CompleteBean.class, primaryKey, OptionsBuilder.noOptions())).thenReturn(context);
        when(facade.getIdMeta()).thenReturn(idMeta);

        manager.removeById(CompleteBean.class, primaryKey);

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        verify(facade).remove();
    }

    @Test
    public void should_remove_by_id_with_consistency() throws Exception {
        // When
        when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(context);

        when(facade.getIdMeta()).thenReturn(idMeta);

        manager.removeById(CompleteBean.class, primaryKey, LOCAL_QUORUM);

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        verify(facade).remove();

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().get()).isSameAs(LOCAL_QUORUM);
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_find() throws Exception {
        // When
        when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(context);
        when(facade.find(CompleteBean.class).getImmediately()).thenReturn(entity);

        PropertyMeta idMeta = new PropertyMeta();
        when(facade.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

        CompleteBean bean = manager.find(CompleteBean.class, primaryKey);

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        assertThat(bean).isSameAs(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_find_with_consistency() throws Exception {
        // When
        when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(context);
        when(facade.find(CompleteBean.class).getImmediately()).thenReturn(entity);
        when(facade.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

        CompleteBean bean = manager.find(CompleteBean.class, primaryKey, EACH_QUORUM);

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        assertThat(bean).isSameAs(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_get_reference() throws Exception {
        // When
        when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(context);
        when(facade.getProxy(CompleteBean.class).getImmediately()).thenReturn(entity);
        when(facade.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

        CompleteBean bean = manager.getProxy(CompleteBean.class, primaryKey);

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        assertThat(bean).isSameAs(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_get_reference_with_consistency() throws Exception {
        // When
        when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(context);
        when(facade.getProxy(CompleteBean.class).getImmediately()).thenReturn(entity);
        when(facade.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

        CompleteBean bean = manager.getProxy(CompleteBean.class, primaryKey, withConsistency(EACH_QUORUM));

        // Then
        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
        assertThat(bean).isSameAs(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_refresh() throws Exception {
        // Given
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.refresh(entity);

        // Then
        verify(entityValidator).validateEntity(entity, entityMetaMap);
        verify(proxifier).ensureProxy(entity);
        verify(facade).refresh(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_refresh_with_consistency() throws Exception {
        // Given
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        manager.refresh(entity, EACH_QUORUM);

        // Then
        verify(entityValidator).validateEntity(entity, entityMetaMap);
        verify(proxifier).ensureProxy(entity);
        verify(facade).refresh(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_initialize_entity() throws Exception {
        // Given
        when(facade.initialize(entity)).thenReturn(entity);
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        CompleteBean actual = manager.initialize(entity);

        // Then
        verify(proxifier).ensureProxy(entity);
        assertThat(actual).isSameAs(entity);

        Options options = optionsCaptor.getValue();
        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }

    @Test
    public void should_initialize_list_of_entities() throws Exception {
        // Given
        List<CompleteBean> entities = Arrays.asList(entity);
        when(facade.initialize(entity)).thenReturn(entity);
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        List<CompleteBean> actual = manager.initialize(entities);

        // Then
        assertThat(actual).containsExactly(entity);
    }

    @Test
    public void should_initialize_set_of_entities() throws Exception {
        // Given
        Set<CompleteBean> entities = Sets.newHashSet(entity);
        when(facade.initialize(entity)).thenReturn(entity);
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        // When
        Set<CompleteBean> actual = manager.initialize(entities);

        // Then
        assertThat(actual).containsExactly(entity);
    }

    @Test
    public void should_remove_proxy_from_entity() throws Exception {
        // Given
        when(proxifier.removeProxy(entity)).thenReturn(entity);

        // When
        CompleteBean actual = manager.removeProxy(entity);

        // Then
        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void should_remove_proxy_for_list_of_entities() throws Exception {
        // Given
        List<CompleteBean> proxies = new ArrayList<>();
        when(proxifier.removeProxy(proxies)).thenReturn(proxies);

        // When
        List<CompleteBean> actual = manager.removeProxy(proxies);

        // Then
        assertThat(actual).isSameAs(proxies);
    }

    @Test
    public void should_remove_proxy_for_set_of_entities() throws Exception {
        // Given
        Set<CompleteBean> proxies = new HashSet<>();

        // When
        when(proxifier.removeProxy(proxies)).thenReturn(proxies);

        Set<CompleteBean> actual = manager.removeProxy(proxies);

        // Then
        assertThat(actual).isSameAs(proxies);
    }

    @Test
    public void should_init_and_remove_proxy_for_entity() throws Exception {
        // Given
        when(facade.initialize(entity)).thenReturn(entity);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.removeProxy(entity)).thenReturn(entity);

        // When
        CompleteBean actual = manager.initAndRemoveProxy(entity);

        // Then
        assertThat(actual).isSameAs(entity);

    }

    @Test
    public void should_init_and_remove_proxy_for_list_of_entities() throws Exception {
        // Given
        List<CompleteBean> entities = Arrays.asList(entity);
        when(facade.initialize(entities)).thenReturn(entities);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.removeProxy(entities)).thenReturn(entities);

        // When
        List<CompleteBean> actual = manager.initAndRemoveProxy(entities);

        // Then
        assertThat(actual).isSameAs(entities);
    }

    @Test
    public void should_init_and_remove_proxy_for_set_of_entities() throws Exception {
        // Given
        Set<CompleteBean> entities = Sets.newHashSet(entity);
        when(facade.initialize(entities)).thenReturn(entities);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.removeProxy(entities)).thenReturn(entities);

        // When
        Set<CompleteBean> actual = manager.initAndRemoveProxy(entities);

        // Then
        assertThat(actual).isSameAs(entities);
    }

    @Test
    public void should_return_slice_query_builder() throws Exception {
        // When
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
        when(meta.isClusteredEntity()).thenReturn(true);

        SliceQueryBuilder<CompleteBean> builder = manager.sliceQuery(CompleteBean.class);

        // Then
        assertThat(Whitebox.getInternalState(builder, SliceQueryExecutor.class)).isSameAs(sliceQueryExecutor);
        assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
        assertThat(Whitebox.getInternalState(builder, PropertyMeta.class)).isSameAs(idMeta);
    }

    @Test
    public void should_return_native_query_builder() throws Exception {
        // When
        NativeQuery builder = manager.nativeQuery("queryString");

        assertThat(builder).isNotNull();

        // Then
        assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("queryString");
    }

    @Test
    public void should_return_typed_query_builder() throws Exception {
        // When
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
        when(meta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta>());

        TypedQuery<CompleteBean> builder = manager.typedQuery(CompleteBean.class, "queryString");

        // Then
        assertThat(builder).isNotNull();

        verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "queryString", meta);

        assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
        assertThat(Whitebox.getInternalState(builder, PersistenceContextFactory.class)).isSameAs(contextFactory);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
    }

    @Test
    public void should_return_raw_typed_query_builder() throws Exception {
        // When
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
        when(meta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta>());

        TypedQuery<CompleteBean> builder = manager.rawTypedQuery(CompleteBean.class, "queryString");

        // Then
        assertThat(builder).isNotNull();

        verify(typedQueryValidator).validateRawTypedQuery(CompleteBean.class, "queryString", meta);

        assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
        assertThat(Whitebox.getInternalState(builder, PersistenceContextFactory.class)).isSameAs(contextFactory);
        assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
    }

    @Test
    public void should_get_native_session() throws Exception {
        // Given
        Session session = mock(Session.class);

        // When
        when(daoContext.getSession()).thenReturn(session);

        Session actual = manager.getNativeSession();

        // Then
        assertThat(actual).isSameAs(session);
    }

    @Test
    public void should_get_indexed_query() throws Exception {
        // When
        final IndexCondition indexCondition = new IndexCondition("column", "value");
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
        when(meta.isClusteredEntity()).thenReturn(false);
        when(meta.getTableName()).thenReturn("table");
        when(meta.encodeBoundValuesForTypedQueries(any(Object[].class))).thenReturn(new Object[] { "value" });

        TypedQuery<CompleteBean> typedQuery = manager.indexedQuery(CompleteBean.class, indexCondition);

        // Then
        assertThat(Whitebox.<Object[]>getInternalState(typedQuery, "encodedBoundValues")).contains("value");
        verify(meta).encodeIndexConditionValue(indexCondition);
        verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "SELECT * FROM table WHERE column=:column;",
                meta);
    }

    @Test
    public void should_serialize_to_json() throws Exception {
        //Given
        manager.configContext = configContext;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        when(configContext.getMapperFor(CompleteBean.class)).thenReturn(mapper);
        CompleteBean entity = CompleteBeanTestBuilder.builder().id(10L).name("name").buid();

        //When
        final String serialized = manager.jsonSerialize(entity);

        //Then
        assertThat(serialized).isEqualTo("{\"id\":10,\"name\":\"name\",\"friends\":[],\"followers\":[],\"preferences\":{}}");
    }

    @Test
    public void should_deserialize_from_json() throws Exception {
        //Given
        manager.configContext = configContext;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        when(configContext.getMapperFor(CompleteBean.class)).thenReturn(mapper);

        //When
        final CompleteBean actual = manager.deserializeJson(CompleteBean.class, "{\"id\":10,\"name\":\"name\"}");

        //Then
        assertThat(actual.getId()).isEqualTo(10L);
        assertThat(actual.getName()).isEqualTo("name");
        assertThat(actual.getFriends()).isNull();
        assertThat(actual.getFollowers()).isNull();
        assertThat(actual.getPreferences()).isNull();
    }
}
