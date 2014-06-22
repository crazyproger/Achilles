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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityValidator;
import info.archinnov.achilles.internal.persistence.operations.OptionsValidator;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.query.cql.NativeQuery;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;

public class PersistenceManager {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);

    protected Map<Class<?>, EntityMeta> entityMetaMap;
    protected ConfigurationContext configContext;
    protected PersistenceContextFactory contextFactory;

    protected EntityProxifier proxifier = new EntityProxifier();
    protected EntityValidator entityValidator = new EntityValidator();
    protected OptionsValidator optionsValidator = new OptionsValidator();
    protected TypedQueryValidator typedQueryValidator = new TypedQueryValidator();

    protected SliceQueryExecutor sliceQueryExecutor;

    protected DaoContext daoContext;

    protected PersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, //
            PersistenceContextFactory contextFactory, DaoContext daoContext, ConfigurationContext configContext) {
        this.entityMetaMap = entityMetaMap;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.contextFactory = contextFactory;
        this.sliceQueryExecutor = new SliceQueryExecutor(contextFactory, configContext, daoContext);
    }

    /**
     * Persist an entity.
     *
     * @param entity
     *            Entity to be persisted
     * @return T
     */
    public <T> T persist(T entity) {
        log.debug("Persisting entity '{}'", entity);
        return persist(entity, noOptions());
    }

    /**
     * Persist an entity asynchronously.
     *
     * @param entity
     *            Entity to be persisted
     * @return AchillesFuture<T>
     */
    public <T> AchillesFuture<T> asyncPersist(T entity) {
        log.debug("Persisting entity '{}' asynchronously", entity);
        return asyncPersist(entity, noOptions());
    }

    /**
     * Persist an entity with the given options.
     *
     * @param entity
     *            Entity to be persisted
     * @param options
     *            options for consistency level, ttl and timestamp
     * @return T
     */
    public <T> T persist(final T entity, Options options) {
        log.debug("Persisting entity '{}' with options {}", entity, options);
        return this.asyncPersist(entity, options).getImmediately();
    }

    /**
     * Persist an entity with the given options, asynchronously.
     *
     * @param entity
     *            Entity to be persisted
     * @param options
     *            options for consistency level, ttl and timestamp
     * @return AchillesFuture<T>
     */
    public <T> AchillesFuture<T> asyncPersist(final T entity, Options options) {
        log.debug("Persisting entity '{}' asynchronously with options {} ", entity, options);

        entityValidator.validateEntity(entity, entityMetaMap);
        optionsValidator.validateOptionsForInsert(entity, entityMetaMap, options);
        proxifier.ensureNotProxy(entity);
        PersistenceManagerOperations context = initPersistenceContext(entity, options);
        return context.persist(entity);
    }


    /**
     * Update a "managed" entity
     *
     * @param entity
     *            Managed entity to be updated
     */
    public void update(Object entity) {
        log.debug("Updating entity '{}'", proxifier.getRealObject(entity));
        this.asyncUpdate(entity, noOptions()).getImmediately();
    }

    /**
     * Update a "managed" entity asynchronously
     *
     * @param entity
     *            Managed entity to be updated
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncUpdate(T entity) {
        log.debug("Updating entity '{}' asynchronously", proxifier.getRealObject(entity));
        return asyncUpdate(entity, noOptions());
    }

    /**
     * Update a "managed" entity
     *
     * @param entity
     *            Managed entity to be updated
     * @param options
     *            options for consistency level, ttl and timestamp
     */
    public void update(Object entity, Options options) {
        log.debug("Updating entity '{}' with options {}", proxifier.getRealObject(entity), options);
        asyncUpdate(entity, options).getImmediately();
    }

    /**
     * Update a "managed" entity asynchronously
     *
     * @param entity
     *            Managed entity to be updated
     * @param options
     *            options for consistency level, ttl and timestamp
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncUpdate(T entity, Options options) {
        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        log.debug("Updating entity '{}' asynchronously with options {} ", realObject, options);
        entityValidator.validateEntity(realObject, entityMetaMap);
        optionsValidator.validateOptionsForUpdate(entity, entityMetaMap, options);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.update(entity);
    }

    /**
     * Remove an entity.
     *
     * @param entity
     *            Entity to be removed
     */
    public void remove(Object entity) {
        log.debug("Removing entity '{}'", proxifier.getRealObject(entity));
        asyncRemove(entity, noOptions()).getImmediately();
    }

    /**
     * Remove an entity asynchronously.
     *
     * @param entity
     *            Entity to be removed
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRemove(T entity) {
        log.debug("Removing entity '{}' asynchronously", proxifier.getRealObject(entity));
        return asyncRemove(entity, noOptions());
    }


    /**
     * Remove an entity with the given options.
     *
     * @param entity
     *            Entity to be removed
     * @param options
     *            options for consistency level and timestamp
     */
    public void remove(final Object entity, Options options) {
        log.debug("Removing entity '{}' with options {}", proxifier.getRealObject(entity), options);
        asyncRemove(entity, options).getImmediately();
    }

    /**
     * Remove an entity with the given options, asynchronously.
     *
     * @param entity
     *            Entity to be removed
     * @param options
     *            options for consistency level and timestamp
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRemove(final T entity, Options options) {
        Object realObject = proxifier.getRealObject(entity);
        log.debug("Removing entity '{}' asynchronously with options {}", realObject, options);

        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.remove();
    }


    /**
     * Remove an entity by its id.
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void removeById(Class<?> entityClass, Object primaryKey) {
        log.debug("Removing entity of type '{}' by its id '{}'", entityClass, primaryKey);
        asyncRemoveById(entityClass, primaryKey).getImmediately();
    }

    /**
     * Remove an entity by its id, asynchronously.
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRemoveById(Class<T> entityClass, Object primaryKey) {
        log.debug("Removing entity of type '{}' asynchronously by its id '{}'", entityClass, primaryKey);
        Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
        Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, noOptions());
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.remove();
    }

    /**
     * Remove an entity by its id with the given Consistency Level for write.
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     */
    public void removeById(Class<?> entityClass, Object primaryKey, ConsistencyLevel writeLevel) {
        log.debug("Removing entity of type '{}' by its id '{}' with consistency level '{}'", entityClass, primaryKey, writeLevel);
        asyncRemoveById(entityClass, primaryKey, withConsistency(writeLevel)).getImmediately();
    }

    /**
     * Remove an entity by its id with the given Consistency Level for write, asynchronously.
     *
     * @param entityClass
     *            Entity class
     *
     * @param primaryKey
     *            Primary key
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRemoveById(Class<T> entityClass, Object primaryKey, Options options) {
        log.debug("Removing entity of type '{}' asynchronously by its id '{}' with options '{}'", entityClass, primaryKey, options);
        Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
        Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.remove();
    }


    /**
     * Find an entity.
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     *
     * @return T
     */
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        log.debug("Find entity class '{}' with primary key '{}'", entityClass, primaryKey);
        return asyncFind(entityClass, primaryKey, noOptions()).getImmediately();
    }

    /**
     * Find an entity, asynchronously.
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     *
     * @return AchillesFuture<T>
     */
    public <T> AchillesFuture<T> asyncFind(Class<T> entityClass, Object primaryKey) {
        log.debug("Find entity class '{}' with primary key '{}'", entityClass, primaryKey);
        return asyncFind(entityClass, primaryKey, noOptions());
    }

    /**
     * Find an entity with the given Consistency Level for read
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     * @param readLevel
     *            Consistency Level for read
     *
     * @return T
     */
    public <T> T find(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
        log.debug("Find entity class '{}' with primary key '{}' and consistency level '{}'", entityClass, primaryKey, readLevel);
        return asyncFind(entityClass, primaryKey, withConsistency(readLevel)).getImmediately();
    }

    /**
     * Find an entity with the given Consistency Level for read, asynchronously
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to load
     * @param options
     *            Options
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncFind(final Class<T> entityClass, final Object primaryKey, Options options) {
        log.debug("Find entity class '{}' with primary key '{}' and options '{}'", entityClass, primaryKey, options);
        Validator.validateNotNull(entityClass, "Entity class should not be null for find by id");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for find by id");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.find(entityClass);
    }

    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     *
     * @return T
     */
    public <T> T getProxy(Class<T> entityClass, Object primaryKey) {
        log.debug("Get reference for entity class '{}' with primary key {}", entityClass, primaryKey);
        return asyncGetProxy(entityClass, primaryKey, noOptions()).getImmediately();
    }

    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     *
     * @return AchillesFuture<T>
     */
    public <T> AchillesFuture<T> asyncGetProxy(Class<T> entityClass, Object primaryKey) {
        log.debug("Get reference asynchronously for entity class '{}' with primary key {}", entityClass, primaryKey);
        return asyncGetProxy(entityClass, primaryKey, noOptions());
    }


    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     * @param options
     *            Options
     *
     * @return T
     */
    public <T> T getProxy(final Class<T> entityClass, final Object primaryKey, Options options) {
        log.debug("Get reference for entity class '{}' with primary key {} and options {}", entityClass, primaryKey, options);
        return asyncGetProxy(entityClass, primaryKey, options).getImmediately();
    }

    /**
     * Create a proxy for the entity. An new empty entity will be created,
     * populated with the provided primary key and then proxified. This method
     * never returns null Use this method to perform direct update without
     * read-before-write
     *
     * @param entityClass
     *            Entity type
     * @param primaryKey
     *            Primary key (Cassandra row key) of the entity to initialize
     * @param options
     *            Options
     *
     * @return AchillesFuture<T>
     */
    public <T> AchillesFuture<T> asyncGetProxy(final Class<T> entityClass, final Object primaryKey, Options options) {
        log.debug("Get reference asynchronously for entity class '{}' with primary key {} and options {}", entityClass, primaryKey, options);

        Validator.validateNotNull(entityClass, "Entity class should not be null for get reference");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for get reference");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.getProxy(entityClass);
    }

    /**
     * Refresh an entity.
     *
     * @param entity
     *            Entity to be refreshed
     */
    public void refresh(Object entity) throws AchillesStaleObjectStateException {
        log.debug("Refreshing entity '{}'", proxifier.removeProxy(entity));
        asyncRefresh(entity, noOptions()).getImmediately();
    }


    /**
     * Refresh an entity asynchronously.
     *
     * @param entity
     *            Entity to be refreshed
     *
     * @return AchillesFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRefresh(T entity) throws AchillesStaleObjectStateException {
        log.debug("Refreshing entity '{}' asynchronously", proxifier.removeProxy(entity));
        return asyncRefresh(entity, noOptions());
    }

    /**
     * Refresh an entity with the given Consistency Level for read.
     *
     * @param entity
     *            Entity to be refreshed
     * @param readLevel
     *            Consistency Level for read
     */
    public void refresh(final Object entity, ConsistencyLevel readLevel) throws AchillesStaleObjectStateException {
        log.debug("Refreshing entity '{}' with consistency level '{}'", proxifier.removeProxy(entity));
        asyncRefresh(entity, withConsistency(readLevel)).getImmediately();
    }

    /**
     * Refresh an entity with the given Consistency Level for read, asynchronously.
     *
     * @param entity
     *            Entity to be refreshed
     * @param options
     *            Options
     *
     * @return ListenableFuture<Empty>
     */
    public <T> AchillesFuture<T> asyncRefresh(final T entity, Options options) throws AchillesStaleObjectStateException {
        log.debug("Refreshing entity '{}' asynchronously with options '{}'", proxifier.removeProxy(entity), options);

        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.refresh(entity);
    }

    /**
     * Initialize all lazy fields of a 'managed' entity, except WideMap/Counter
     * fields.
     *
     * Raise an <strong>IllegalStateException</strong> if the entity is not
     * 'managed'
     *
     */
    public <T> T initialize(final T entity) {
        log.debug("Force lazy fields initialization for entity {}", proxifier.removeProxy(entity));
        proxifier.ensureProxy(entity);
        T realObject = proxifier.getRealObject(entity);
        PersistenceManagerOperations context = initPersistenceContext(realObject, noOptions());
        return context.initialize(entity);
    }

    /**
     * Initialize all lazy fields of a set of 'managed' entities, except
     * WideMap/Counter fields.
     *
     * Raise an IllegalStateException if an entity is not 'managed'
     *
     */
    public <T> Set<T> initialize(final Set<T> entities) {
        log.debug("Force lazy fields initialization for entity set {}", entities);
        for (T entity : entities) {
            initialize(entity);
        }
        return entities;
    }

    /**
     * Initialize all lazy fields of a list of 'managed' entities, except
     * WideMap/Counter fields.
     *
     * Raise an IllegalStateException if an entity is not 'managed'
     *
     */
    public <T> List<T> initialize(final List<T> entities) {
        log.debug("Force lazy fields initialization for entity set {}", entities);
        for (T entity : entities) {
            initialize(entity);
        }
        return entities;
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(T entity))
     *
     */
    public <T> T initAndRemoveProxy(T entity) {
        return removeProxy(initialize(entity));
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(Set<T> entities))
     *
     */
    public <T> Set<T> initAndRemoveProxy(Set<T> entities) {
        return removeProxy(initialize(entities));
    }

    /**
     * Shorthand for manager.removeProxy(manager.initialize(List<T> entities))
     *
     */
    public <T> List<T> initAndRemoveProxy(List<T> entities) {
        return removeProxy(initialize(entities));
    }

    /**
     * Remove the proxy of a 'managed' entity and return the underlying "raw"
     * entity
     *
     * If the argument is not a proxy objet, return itself <br/>
     * Else, return the target object behind the proxy
     *
     * @param proxy
     * @return real object
     */
    public <T> T removeProxy(T proxy) {
        log.debug("Removing proxy for entity {}", proxy);

        T realObject = proxifier.removeProxy(proxy);

        return realObject;
    }

    /**
     * Remove the proxy of a list of 'managed' entities and return the
     * underlying "raw" entities
     *
     * See {@link #removeProxy}
     *
     * @param proxies
     *            list of proxified entity
     * @return real object list
     */
    public <T> List<T> removeProxy(List<T> proxies) {
        log.debug("Removing proxy for a list of entities {}", proxies);

        return proxifier.removeProxy(proxies);
    }

    /**
     * Remove the proxy of a set of 'managed' entities return the underlying
     * "raw" entities
     *
     * See {@link #removeProxy}
     *
     * @param proxies
     *            set of proxified entities
     * @return real object set
     */
    public <T> Set<T> removeProxy(Set<T> proxies) {
        log.debug("Removing proxy for a set of entities {}", proxies);

        return proxifier.removeProxy(proxies);
    }

    public <T> SliceQueryBuilder<T> sliceQuery(Class<T> entityClass) {
        log.debug("Execute slice query for entity class {}", entityClass);
        EntityMeta meta = entityMetaMap.get(entityClass);
        Validator.validateTrue(meta.isClusteredEntity(), "Cannot perform slice query on entity type '%s' because it is " + "not a clustered entity", meta.getClassName());
        return new SliceQueryBuilder<>(sliceQueryExecutor, entityClass, meta);
    }

    /**
     * Return a CQL native query builder
     *
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return NativeQuery
     */
    public NativeQuery nativeQuery(String queryString, Object... boundValues) {
        return this.nativeQuery(queryString, noOptions(), boundValues);
    }

    /**
     * Return a CQL native query builder
     *
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency
     *            options
     *
     * @param options
     *            options for the query. <strong>Only CAS Result listener passed as option is taken
     *            into account</strong>. For timestamp, TTL and CAS conditions you must specify them
     *            directly in the query string
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return NativeQuery
     */
    public NativeQuery nativeQuery(String queryString, Options options, Object... boundValues) {
        log.debug("Execute native query {}", queryString);
        Validator.validateNotBlank(queryString, "The query string for native query should not be blank");
        return new NativeQuery(daoContext, configContext, queryString, options, boundValues);
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be in 'managed' state
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> typedQuery(Class<T> entityClass, String queryString, Object... boundValues) {
        return typedQueryInternal(entityClass, queryString, true, boundValues);
    }

    private <T> TypedQuery<T> typedQueryInternal(Class<T> entityClass, String queryString,
            boolean normalizeQuery, Object... boundValues) {
        log.debug("Execute typed query for entity class {}", entityClass);
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "Cannot perform typed query because the entityClass '%s' is not managed by Achilles", entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateTypedQuery(entityClass, queryString, meta);
        return new TypedQuery<>(entityClass, daoContext, configContext, queryString, meta, contextFactory, true, normalizeQuery, boundValues);
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be in 'managed' state
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param indexCondition
     *            index condition
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> indexedQuery(Class<T> entityClass, IndexCondition indexCondition) {
        log.debug("Execute indexed query for entity class {}", entityClass);

        EntityMeta entityMeta = entityMetaMap.get(entityClass);

        Validator.validateFalse(entityMeta.isClusteredEntity(), "Index query is not supported for clustered entity. Please use typed query/native query");
        Validator.validateNotNull(indexCondition, "Index condition should not be null");

        entityMeta.encodeIndexConditionValue(indexCondition);

        String indexColumnName = indexCondition.getColumnName();
        final Select.Where query = select().from(entityMeta.getTableName()).where(eq(indexColumnName, bindMarker(indexColumnName)));
        return typedQueryInternal(entityClass, query.getQueryString(), false, indexCondition.getColumnValue());
    }

    /**
     * Return a CQL typed query builder
     *
     * All found entities will be returned as raw entities and not 'managed' by
     * Achilles
     *
     * @param entityClass
     *            type of entity to be returned
     *
     * @param queryString
     *            native CQL query string, including limit, ttl and consistency
     *            options
     *
     * @param boundValues
     *            values to be bind to the parameterized query, if any
     *
     * @return TypedQuery<T>
     */
    public <T> TypedQuery<T> rawTypedQuery(Class<T> entityClass, String queryString, Object... boundValues) {
        log.debug("Execute raw typed query for entity class {}", entityClass);
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "Cannot perform typed query because the entityClass '%s' is not managed by Achilles", entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateRawTypedQuery(entityClass, queryString, meta);
        return new TypedQuery<>(entityClass, daoContext, configContext, queryString, meta, contextFactory, false, true, boundValues);
    }

    /**
     * Serialize the entity in JSON using a registered Object Mapper or default Achilles Object Mapper
     * @param entity
     * @return serialized entity in JSON
     * @throws IOException
     */
    public String jsonSerialize(Object entity) throws IOException {
        Validator.validateNotNull(entity, "Cannot serialize to JSON null entity");
        final ObjectMapper objectMapper = configContext.getMapperFor(entity.getClass());
        return objectMapper.writeValueAsString(entity);
    }

    /**
     * Deserialize the given JSON into entity using a registered Object Mapper or default Achilles Object Mapper
     * @param type
     * @param serialized
     * @param <T>
     * @return deserialized entity from JSON
     * @throws IOException
     */
    public <T> T deserializeJson(Class<T> type, String serialized) throws IOException {
        Validator.validateNotNull(type, "Cannot deserialize from JSON if target type is null");
        final ObjectMapper objectMapper = configContext.getMapperFor(type);
        return objectMapper.readValue(serialized, type);
    }

    protected PersistenceManagerOperations initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
        return contextFactory.newContext(entityClass, primaryKey, options).getPersistenceManagerFacade();
    }

    protected PersistenceManagerOperations initPersistenceContext(Object entity, Options options) {
        return contextFactory.newContext(entity, options).getPersistenceManagerFacade();
    }

    public Session getNativeSession() {
        return daoContext.getSession();
    }

    protected Map<Class<?>, EntityMeta> getEntityMetaMap() {
        return entityMetaMap;
    }

    protected ConfigurationContext getConfigContext() {
        return configContext;
    }

    protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
        this.entityMetaMap = entityMetaMap;
    }

    protected void setConfigContext(ConfigurationContext configContext) {
        this.configContext = configContext;
    }
}
