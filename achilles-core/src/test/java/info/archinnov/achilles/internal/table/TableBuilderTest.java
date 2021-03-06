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
package info.archinnov.achilles.internal.table;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.Counter;

import java.util.Collection;

import org.junit.Test;

public class TableBuilderTest {

	@Test
	public void should_build_table_with_all_types_and_comment() throws Exception {

		String ddlScript = TableBuilder.createTable("tableName").addColumn("longCol", Long.class)
				.addColumn("enumCol", PropertyType.class).addColumn("objectCol", UserBean.class)
				.addList("listCol", Long.class).addList("listEnumCol", PropertyType.class)
				.addList("listObjectCol", UserBean.class).addSet("setCol", Long.class)
				.addSet("setEnumCol", PropertyType.class).addSet("setObjectCol", UserBean.class)
				.addMap("mapCol", Integer.class, Long.class).addMap("mapEnumKeyCol", PropertyType.class, Long.class)
				.addMap("mapEnumValCol", Integer.class, PropertyType.class)
				.addMap("mapObjectValCol", Integer.class, UserBean.class).addPartitionComponent("longCol")
				.addClusteringComponent("enumCol").addComment("This is a comment for 'tableName'")
				.setReversedClusteredComponent("objectCol").generateDDLScript();

		assertThat(ddlScript).isEqualTo(
				"\n\tCREATE TABLE tableName(\n" + "\t\tlongCol bigint,\n" + "\t\tenumCol text,\n"
						+ "\t\tobjectCol text,\n" + "\t\tlistCol list<bigint>,\n" + "\t\tlistEnumCol list<text>,\n"
						+ "\t\tlistObjectCol list<text>,\n" + "\t\tsetCol set<bigint>,\n"
						+ "\t\tsetEnumCol set<text>,\n" + "\t\tsetObjectCol set<text>,\n"
						+ "\t\tmapCol map<int,bigint>,\n" + "\t\tmapEnumKeyCol map<text,bigint>,\n"
						+ "\t\tmapEnumValCol map<int,text>,\n" + "\t\tmapObjectValCol map<int,text>,\n"
						+ "\t\tPRIMARY KEY(longCol, enumCol)\n"
						+ "\t) WITH COMMENT = 'This is a comment for \"tableName\"'"
						+ " AND CLUSTERING ORDER BY (objectCol DESC)");

	}

	@Test
	public void should_generate_indices_scripts() throws Exception {
		Collection<String> indicesScript = TableBuilder.createTable("tableName").addColumn("longCol", Long.class)
				.addColumn("enumCol", PropertyType.class).addColumn("intCol", Integer.class)
				.addPartitionComponent("longCol").addClusteringComponent("enumCol").addIndex(new IndexProperties("", "intCol")).generateIndices();

		assertThat(indicesScript.iterator().next()).isEqualTo(
				"\nCREATE INDEX tableName_intCol\n" + "ON tableName (intCol);\n");
	}

	@Test
	public void should_build_counter_table_with_primary_keys() throws Exception {

		String ddlScript = TableBuilder.createCounterTable("tableName").addColumn("longCol", Long.class)
				.addColumn("enumCol", PropertyType.class).addColumn("counterColumn", Counter.class)
				.addPartitionComponent("longCol").addClusteringComponent("enumCol")
				.addComment("This is a comment for 'tableName'").generateDDLScript();

		assertThat(ddlScript).isEqualTo(
				"\n\tCREATE TABLE tableName(\n" + "\t\tlongCol bigint,\n" + "\t\tenumCol text,\n"
						+ "\t\tcounterColumn counter,\n" + "\t\tPRIMARY KEY(longCol, enumCol)\n"
						+ "\t) WITH COMMENT = 'This is a comment for \"tableName\"'");

	}
}
