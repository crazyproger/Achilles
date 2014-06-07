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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;

public class ImmediateValue<V> implements ListenableFuture<V> {

    private final ResultSetFutureWrapper wrapper;
    private final V value;

    public ImmediateValue(V value) {
        this.value = value;
        this.wrapper = null;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        if (wrapper != null) {
            wrapper.addListener(listener, executor);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = true;
        if (wrapper != null) {
            result = false;
            for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
                result &= resultSetFuture.cancel(mayInterruptIfRunning);
            }
        }
        return result;
    }

    @Override
    public boolean isCancelled() {
        boolean result = false;
        if (wrapper != null) {
            for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
                result |= resultSetFuture.isCancelled();
            }
        }
        return result;
    }

    @Override
    public boolean isDone() {
        boolean result = true;
        if (wrapper != null) {
            result = false;
            for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
                result &= resultSetFuture.isDone();
            }
        }
        return result;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        if (wrapper != null) {
            for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
                resultSetFuture.get();
            }
        }
        return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (wrapper != null) {
            for (ResultSetFuture resultSetFuture : wrapper.getResultSetFutures()) {
                resultSetFuture.get(timeout, unit);
            }
        }
        return value;
    }
}
