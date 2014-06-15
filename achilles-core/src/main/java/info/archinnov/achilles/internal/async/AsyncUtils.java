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

package info.archinnov.achilles.internal.async;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang.ArrayUtils;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.type.Options;

public class AsyncUtils {

    public static final Function<ResultSet, List<Row>> RESULTSET_TO_ROWS = new Function<ResultSet, List<Row>>() {
        @Override
        public List<Row> apply(ResultSet resultSet) {
            List<Row> rows = new ArrayList<>();
            if (resultSet != null) {
                rows = resultSet.all();
            }
            return rows;
        }
    };

    public static final Function<ResultSet, Row> RESULTSET_TO_ROW = new Function<ResultSet, Row>() {
        @Override
        public Row apply(ResultSet resultSet) {
            Row row = null;
            if (resultSet != null) {
                row = resultSet.one();
            }
            return row;
        }
    };

    public static final Function<ResultSet, Iterator<Row>> RESULTSET_TO_ITERATOR = new Function<ResultSet, Iterator<Row>>() {
        @Override
        public Iterator<Row> apply(ResultSet resultSet) {
            Iterator<Row> iterator = new ArrayList<Row>().iterator();
            if (resultSet != null) {
                iterator = resultSet.iterator();
            }
            return iterator;
        }
    };

    public static final Function<ResultSet, Empty> NO_OP = new Function<ResultSet, Empty>() {
        @Override
        public Empty apply(ResultSet resultSet) {
            return Empty.INSTANCE;
        }
    };

    public static void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, Options options, ExecutorService executorService) {
        if (options.hasAsyncListeners()) {
            for (FutureCallback<Object> callback : options.getAsyncListeners()) {
                Futures.addCallback(listenableFuture, callback, executorService);
            }
        }
    }


    public static void maybeAddAsyncListeners(ListenableFuture<?> listenableFuture, FutureCallback<Object>[] asyncListeners, ExecutorService executorService) {
        if (ArrayUtils.isNotEmpty(asyncListeners)) {
            for (FutureCallback<Object> callback : asyncListeners) {
                Futures.addCallback(listenableFuture, callback, executorService);
            }
        }
    }
}
