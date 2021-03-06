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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;

public class DirtyCheckIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManager manager = resource.getPersistenceManager();

    private Session session = resource.getNativeSession();

    private CompleteBean bean;

    @Before
    public void setUp() {
        bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L).addFriends("foo", "bar")
                .addFollowers("George", "Paul").addPreference(1, "FR").addPreference(2, "Paris")
                .addPreference(3, "75014").buid();

        bean = manager.persist(bean);
    }

    /*
     *   SET
     */
    @Test
    public void should_dirty_check_assign_new_value_to_set() throws Exception {
        bean.setFollowers(newHashSet("Sylvain", "Jonathan"));
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> followers = row.getSet("followers", String.class);
        assertThat(followers).containsOnly("Sylvain", "Jonathan");
    }

    @Test
    public void should_dirty_check_set_element_add() throws Exception {
        bean.getFollowers().add("Jonathan");
        bean.getFollowers().addAll(asList("Sylvain", "Mickaël", "Jonathan"));
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> friends = row.getSet("followers", String.class);

        assertThat(friends).containsOnly("George", "Paul", "Jonathan", "Sylvain", "Mickaël");

    }

    @Test
    public void should_dirty_check_set_clear() throws Exception {
        bean.getFollowers().clear();
        bean.getFollowers().addAll(asList("Sylvain", "Jonathan"));
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> friends = row.getSet("followers", String.class);

        assertThat(friends).containsOnly("Jonathan", "Sylvain");

    }

    @Test
    public void should_dirty_check_set_remove() throws Exception {
        bean.getFollowers().remove("Sylvain");
        bean.getFollowers().removeAll(asList("George"));
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> friends = row.getSet("followers", String.class);

        assertThat(friends).containsOnly("Paul");
    }

    @Test
    public void should_dirty_check_set_retain_all() throws Exception {
        bean.getFollowers().retainAll(asList("Sylvain", "Paul"));
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> friends = row.getSet("followers", String.class);

        assertThat(friends).containsOnly("Paul");
    }

    @Test
    public void should_not_dirty_check_set_on_iterator_remove() throws Exception {
        final Iterator<String> iterator = bean.getFollowers().iterator();
        iterator.next();
        iterator.remove();
        manager.update(bean);

        Row row = session.execute("select followers from CompleteBean where id=" + bean.getId()).one();
        Set<String> friends = row.getSet("followers", String.class);

        assertThat(friends).containsOnly("Paul", "George");
    }

    /*
     *   LIST
     */
    @Test
    public void should_dirty_check_assign_new_value_to_list() throws Exception {
        bean.setFriends(asList("qux", "tux"));
        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);
        assertThat(friends).containsOnly("qux", "tux");
    }

    @Test
    public void should_dirty_check_list_element_add() throws Exception {
        bean.getFriends().add("qux");
        bean.getFriends().addAll(asList("qux", "qux"));
        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(5);
        assertThat(friends.get(2)).isEqualTo("qux");
        assertThat(friends.get(3)).isEqualTo("qux");
        assertThat(friends.get(4)).isEqualTo("qux");
    }

    @Test
    public void should_dirty_check_list_element_add_at_index() throws Exception {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Append, Prepend, Remove, RemoveAll and SetValueAtIndex are the only supported operations for CQL3 lists");
        bean.getFriends().add(1, "qux");
    }


    @Test
    public void should_dirty_check_list_element_clear() throws Exception {
        bean.getFriends().clear();
        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        assertThat(row.isNull("friends")).isTrue();
    }

    @Test
    public void should_dirty_check_list_element_clear_then_append() throws Exception {
        bean.getFriends().clear();
        bean.getFriends().add("qux");
        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        assertThat(row.getList("friends", String.class)).containsExactly("qux");
    }

    @Test
    public void should_dirty_check_list_element_prepend() throws Exception {
        bean.getFriends().add(0, "one");
        bean.getFriends().addAll(0, asList("two", "three"));
        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(5);
        assertThat(friends.get(0)).isEqualTo("three");
        assertThat(friends.get(1)).isEqualTo("two");
        assertThat(friends.get(2)).isEqualTo("one");
    }


    @Test
    public void should_dirty_check_list_element_remove_at_index() throws Exception {
        bean.getFriends().add("qux");
        bean.getFriends().remove(0);

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends.get(0)).isEqualTo("bar");
        assertThat(friends.get(1)).isEqualTo("qux");
    }

    /**
     * Ignore until JIRA
     * is solved
     */
    @Ignore
    @Test
    public void should_dirty_check_list_element_remove_at_same_index_twice() throws Exception {
        bean.getFriends().add("qux");
        bean.getFriends().remove(0);
        bean.getFriends().remove(0);

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("qux");
    }

    @Test
    public void should_dirty_check_list_element_remove_element_only_once() throws Exception {
        bean.getFriends().addAll(asList("qux", "tux", "foo", "bar"));
        bean.getFriends().remove("foo");
        bean.getFriends().removeAll(asList("bar"));

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(4);
        assertThat(friends.get(0)).isEqualTo("qux");
        assertThat(friends.get(1)).isEqualTo("tux");
        assertThat(friends.get(2)).isEqualTo("foo");
        assertThat(friends.get(3)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_remove_all() throws Exception {
        bean.getFriends().removeAll(asList("foo", "qux"));

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_retain_all() throws Exception {
        bean.getFriends().retainAll(asList("foo", "qux"));

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("foo");
    }

    @Test
    public void should_not_dirty_check_list_element_sub_list_remove() throws Exception {
        bean.getFriends().subList(0, 1).remove(0);

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends).contains("foo", "bar");
    }

    @Test
    public void should_dirty_check_list_element_set() throws Exception {
        bean.getFriends().set(1, "qux");

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends.get(1)).isEqualTo("qux");
    }

    @Test
    public void should_not_dirty_check_list_element_iterator_remove() throws Exception {
        Iterator<String> iter = bean.getFriends().iterator();

        iter.next();
        iter.remove();

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends).contains("foo", "bar");
    }

    @Test
    public void should_not_dirty_check_list_element_list_iterator_remove() throws Exception {
        Iterator<String> iter = bean.getFriends().listIterator();

        iter.next();
        iter.remove();

        manager.update(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends).contains("foo", "bar");
    }

    @Test
    public void should_not_dirty_check_list_element_list_iterator_set() throws Exception {
        ListIterator<String> iter = bean.getFriends().listIterator();

        iter.next();
        iter.set("qux");

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends).contains("foo", "bar");
    }

    /*
     *   Map
     */
    @Test
    public void should_dirty_check_assign_new_value_to_map() throws Exception {
        bean.setPreferences(ImmutableMap.of(4, "test", 5, "again"));
        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);
        assertThat(preferences).hasSize(2);
        assertThat(preferences).containsKey(4);
        assertThat(preferences.get(4)).isEqualTo("test");
        assertThat(preferences).containsKey(5);
        assertThat(preferences.get(5)).isEqualTo("again");
    }

    @Test
    public void should_dirty_check_map_put_element() throws Exception {
        bean.getPreferences().put(4, "test");

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(4);
        assertThat(preferences.get(4)).isEqualTo("test");
    }

    /**
     * Ignore because of https://datastax-oss.atlassian.net/browse/JAVA-271
     *
     */
    @Test
    public void should_dirty_check_map_remove_key() throws Exception {
        bean.getPreferences().remove(1);

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_map_put_all() throws Exception {
        Map<Integer, String> map = new HashMap<>();
        map.put(3, "75015");
        map.put(4, "test");
        bean.getPreferences().putAll(map);

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(4);
        assertThat(preferences.get(3)).isEqualTo("75015");
        assertThat(preferences.get(4)).isEqualTo("test");

    }

    @Test
    public void should_not_dirty_check_map_keyset_remove() throws Exception {
        bean.getPreferences().keySet().remove(1);

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_not_dirty_check_map_keyset_remove_all() throws Exception {
        bean.getPreferences().keySet().removeAll(asList(1, 2, 5));

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_not_dirty_check_map_keyset_retain_all() throws Exception {
        bean.getPreferences().keySet().retainAll(asList(1, 3));

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_keyset_iterator_remove() throws Exception {
        Iterator<Integer> iter = bean.getPreferences().keySet().iterator();

        iter.next();
        iter.remove();

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_valueset_remove() throws Exception {
        bean.getPreferences().values().remove("FR");

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_valueset_remove_all() throws Exception {
        bean.getPreferences().values().removeAll(asList("FR", "Paris", "test"));

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_map_valueset_retain_all() throws Exception {
        bean.getPreferences().values().retainAll(asList("FR", "Paris", "test"));

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_valueset_iterator_remove() throws Exception {
        Iterator<String> iter = bean.getPreferences().values().iterator();

        iter.next();
        iter.remove();

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_entrySet_remove_entry() throws Exception {

        Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

        Entry<Integer, String> entry = entrySet.iterator().next();

        entrySet.remove(entry);
        entry.setValue("test");

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_not_dirty_check_map_entrySet_remove_all_entry() throws Exception {

        Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

        Iterator<Entry<Integer, String>> iterator = entrySet.iterator();

        Entry<Integer, String> entry1 = iterator.next();
        Entry<Integer, String> entry2 = iterator.next();

        entrySet.removeAll(asList(entry1, entry2));

        manager.update(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(3);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_simple_property() throws Exception {
        bean.setName("another_name");

        manager.update(bean);

        Row row = session.execute("select name from CompleteBean where id=" + bean.getId()).one();
        Object reloadedName = row.getString("name");

        assertThat(reloadedName).isEqualTo("another_name");
    }
}
