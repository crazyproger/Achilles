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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;

public class EntityPersister {
    private static final Logger log = LoggerFactory.getLogger(EntityPersister.class);

    private CounterPersister counterPersister = new CounterPersister();

    public void persist(EntityOperations context) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object entity = context.getEntity();

        log.debug("Persisting transient entity {}", entity);

        if (entityMeta.isClusteredCounter()) {
            counterPersister.persistClusteredCounters(context);
        } else {
            context.pushInsertStatement();
            counterPersister.persistCounters(context, entityMeta.getAllCounterMetas());
        }
    }

    public void remove(EntityOperations context) {
        log.trace("Removing entity using PersistenceContext {}", context);
        EntityMeta entityMeta = context.getEntityMeta();
        if (entityMeta.isClusteredCounter()) {
            context.bindForClusteredCounterRemoval();
        } else {
            context.bindForRemoval(entityMeta.getTableName());
            counterPersister.removeRelatedCounters(context);
        }
    }
}
