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

package info.archinnov.achilles.internal.statement.wrapper;

import static com.datastax.driver.core.BatchStatement.Type.LOGGED;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;


@RunWith(MockitoJUnitRunner.class)
public class BatchStatementWrapperTest {
/*
    @Mock
    private AbstractStatementWrapper statementWrapper;

    @Mock
    private Session session;

    @Mock
    private ResultSetFuture resultSetFuture;

    @Captor
    private ArgumentCaptor<BatchStatement> batchCaptor;

    @Test
    public void should_get_query_string() throws Exception {
        //Given
        when(statementWrapper.getQueryString()).thenReturn("SELECT * FROM");

        //When
        BatchStatementWrapper wrapper = new BatchStatementWrapper(LOGGED, asList(statementWrapper));
        final String actual = wrapper.getQueryString();

        //Then
        assertThat(actual).isEqualTo("SELECT * FROM");
        verify(statementWrapper).activateQueryTracing();
    }

    @Test
    public void should_execute_async_and_get_statement() throws Exception {
        //Given
        when(session.executeAsync(batchCaptor.capture())).thenReturn(resultSetFuture);

        //When
        BatchStatementWrapper wrapper = new BatchStatementWrapper(LOGGED, asList(statementWrapper));
        final ResultSetFutureWrapper actual = wrapper.executeAsync(session);

        //Then
        assertThat(actual).isNotNull().isInstanceOf(ResultSetFutureWrapper.class);
        final BatchStatement batch = batchCaptor.getValue();
        assertThat(wrapper.getStatement()).isSameAs(batch);
    }

    @Test
    public void should_log_dml_statements() throws Exception {
        //Given
        when(statementWrapper.isTracingEnabled()).thenReturn(true);

        //When
        BatchStatementWrapper wrapper = new BatchStatementWrapper(LOGGED, asList(statementWrapper));
        wrapper.logDMLStatement("aa");

        //Then
        verify(statementWrapper).activateQueryTracing();
        verify(statementWrapper).logDMLStatement("aa");

    }
*/
}
