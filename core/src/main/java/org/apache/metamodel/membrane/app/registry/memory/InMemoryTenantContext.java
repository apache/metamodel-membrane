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
package org.apache.metamodel.membrane.app.registry.memory;

import org.apache.metamodel.membrane.app.registry.DataSourceRegistry;
import org.apache.metamodel.membrane.app.registry.TenantContext;
import org.apache.metamodel.membrane.app.registry.cache.CachedDataSourceRegistryWrapper;

public class InMemoryTenantContext implements TenantContext {

    private final String tenantIdentifier;
    private final DataSourceRegistry dataContextRegistry;

    public InMemoryTenantContext(String tenantIdentifier) {
        this.tenantIdentifier = tenantIdentifier;
        this.dataContextRegistry = new CachedDataSourceRegistryWrapper(new InMemoryDataSourceRegistry(this));
    }

    @Override
    public String getTenantName() {
        return tenantIdentifier;
    }

    @Override
    public DataSourceRegistry getDataSourceRegistry() {
        return dataContextRegistry;
    }

    @Override
    public String toString() {
        return "InMemoryTenantContext[" + tenantIdentifier + "]";
    }
}
