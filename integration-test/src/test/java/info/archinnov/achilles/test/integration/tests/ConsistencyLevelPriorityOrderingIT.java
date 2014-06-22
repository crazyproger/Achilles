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
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.THREE;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnavailableException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.BatchingPersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.EntityWithConsistencyLevelOnClassAndField;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

public class ConsistencyLevelPriorityOrderingIT {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

    private PersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    // Normal type
    @Test
    public void should_override_mapping_on_class_by_runtime_value_on_batch_mode_for_normal_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        long id = RandomUtils.nextLong();
        entity.setId(id);
        entity.setName("name");

        manager.persist(entity);

        BatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
        batchEm.startBatch(ONE);
        logAsserter.prepareLogLevel();

        entity = batchEm.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());

        logAsserter.assertConsistencyLevels(ONE, ONE);
        batchEm.flushBatch();

        assertThatBatchContextHasBeenReset(batchEm);
        assertThat(entity.getName()).isEqualTo("name");

        expectedEx.expect(UnavailableException.class);
        expectedEx.expectMessage("Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
        manager.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());
    }

    @Test
    public void should_not_override_batch_mode_level_by_runtime_value_for_normal_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        entity.setId(RandomUtils.nextLong());
        entity.setName("name sdfsdf");
        manager.persist(entity);

        BatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();

        batchEm.startBatch(EACH_QUORUM);

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("Runtime custom Consistency Level and/or async listeners cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)' and async listener using flushBatch(...)");

        batchEm.update(entity.getId(), withConsistency(ONE));
    }

    // Counter type
    @Test
    public void should_override_mapping_on_class_by_mapping_on_field_for_counter_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        entity.setId(RandomUtils.nextLong());
        entity.setName("name");
        entity.setCount(CounterBuilder.incr());
        entity = manager.persist(entity);

        Counter counter = entity.getCount();
        counter.incr(10L);

        logAsserter.prepareLogLevel();
        assertThat(counter.get()).isEqualTo(11L);
        logAsserter.assertConsistencyLevels(ONE, ONE);
    }

    @Test
    public void should_override_mapping_on_field_by_batch_value_for_counter_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        entity.setId(RandomUtils.nextLong());
        entity.setName("name");
        entity.setCount(CounterBuilder.incr());

        BatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
        batchEm.startBatch(THREE);
        entity = batchEm.persist(entity);

        expectedEx.expect(UnavailableException.class);
        expectedEx.expectMessage("Not enough replica available for query at consistency THREE (3 required but only 1 alive)");

        entity.getCount();
    }

    @Test
    public void should_override_batch_level_by_runtime_value_for_slice_query() throws Exception {

        BatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
        batchEm.startBatch(ONE);

        expectedEx.expect(InvalidQueryException.class);
        expectedEx.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

        batchEm.sliceQuery(ClusteredEntity.class).partitionComponents(11L).consistencyLevel(EACH_QUORUM).get(10);
    }

    private void assertThatBatchContextHasBeenReset(BatchingPersistenceManager batchEm) {
        BatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, BatchingFlushContext.class);
        ConsistencyLevel consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
        List<AbstractStatementWrapper> statementWrappers = Whitebox.getInternalState(flushContext, "statementWrappers");

        assertThat(consistencyLevel).isEqualTo(ConsistencyLevel.ONE);
        assertThat(statementWrappers).isEmpty();
    }

}
