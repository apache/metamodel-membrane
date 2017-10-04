package org.apache.metamodel.membrane.app.registry.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.membrane.app.exceptions.NoSuchTenantException;
import org.apache.metamodel.membrane.app.exceptions.TenantAlreadyExistException;
import org.apache.metamodel.membrane.app.registry.TenantContext;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.metamodel.membrane.app.registry.TenantRegistry;
import org.apache.metamodel.util.FileHelper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class CachedTenantRegistryWrapper implements TenantRegistry {

    /**
     * The default timeout (in seconds) before the cache evicts and closes the
     * created {@link TenantContext}s.
     */
    public static final int DEFAULT_TIMEOUT_SECONDS = 10 * 60;

    private final TenantRegistry delegate;
    private final LoadingCache<String, TenantContext> loadingCache;

    public CachedTenantRegistryWrapper(TenantRegistry delegate) {
        this(delegate, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public CachedTenantRegistryWrapper(TenantRegistry delegate, final long cacheTimeout,
            final TimeUnit cacheTimeoutUnit) {
        this.delegate = delegate;
        this.loadingCache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, cacheTimeoutUnit).removalListener(
                createRemovalListener()).build(createCacheLoader());
    }

    private RemovalListener<String, TenantContext> createRemovalListener() {
        return new RemovalListener<String, TenantContext>() {
            @Override
            public void onRemoval(final RemovalNotification<String, TenantContext> notification) {
                final TenantContext tenantContext = notification.getValue();
                // TenantContexts could be closeable - attempt closing it here
                FileHelper.safeClose(tenantContext);
            }
        };
    }

    private CacheLoader<String, TenantContext> createCacheLoader() {
        return new CacheLoader<String, TenantContext>() {
            @Override
            public TenantContext load(final String key) throws Exception {
                return delegate.getTenantContext(key);
            }
        };
    }

    @Override
    public List<String> getTenantIdentifiers() {
        return delegate.getTenantIdentifiers();
    }

    @Override
    public TenantContext getTenantContext(String tenantIdentifier) throws NoSuchTenantException {
        try {
            return loadingCache.getUnchecked(tenantIdentifier);
        } catch (UncheckedExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new MetaModelException("Unexpected error happened while getting TenantContext '" + tenantIdentifier
                    + "' from cache", e);
        }
    }

    @Override
    public TenantContext createTenantContext(String tenantIdentifier) throws IllegalArgumentException,
            TenantAlreadyExistException {
        final TenantContext tenantContext = delegate.createTenantContext(tenantIdentifier);
        loadingCache.put(tenantContext.getTenantName(), tenantContext);
        return tenantContext;
    }

    @Override
    public void deleteTenantContext(String tenantIdentifier) throws NoSuchTenantException {
        delegate.deleteTenantContext(tenantIdentifier);
        loadingCache.invalidate(tenantIdentifier);
    }

}
