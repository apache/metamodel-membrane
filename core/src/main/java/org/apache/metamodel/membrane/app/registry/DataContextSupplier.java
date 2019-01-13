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
package org.apache.metamodel.membrane.app.registry;

import java.util.function.Supplier;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactoryRegistry;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.ResourceFactoryRegistryImpl;
import org.apache.metamodel.membrane.app.federation.FederatedDataContextFactory;

public class DataContextSupplier implements Supplier<DataContext> {

    private final String dataSourceName;
    private final DataContextProperties dataContextProperties;
    private final TenantContext tenantContext;

    public DataContextSupplier(TenantContext tenantContext, String dataSourceName,
            DataContextProperties dataContextProperties) {
        this.tenantContext = tenantContext;
        this.dataSourceName = dataSourceName;
        this.dataContextProperties = dataContextProperties;
    }

    @Override
    public DataContext get() {
        final DataContextFactoryRegistry registry = getRegistryForTenant();
        final DataContext dataContext = registry.createDataContext(dataContextProperties);
        return dataContext;
    }

    private DataContextFactoryRegistry getRegistryForTenant() {
        final ResourceFactoryRegistry resourceFactoryRegistry = ResourceFactoryRegistryImpl.getDefaultInstance();
        final DataContextFactoryRegistry defaultRegistry = DataContextFactoryRegistryImpl.getDefaultInstance();

        // create a new registry with all the default factories in it
        final DataContextFactoryRegistry registry =
                new DataContextFactoryRegistryImpl(defaultRegistry.getFactories(), resourceFactoryRegistry);

        // add tenant-specific factory
        registry.addFactory(new FederatedDataContextFactory(tenantContext));
        return registry;
    }

    @Override
    public String toString() {
        return "DataContextSupplier[" + dataSourceName + "]";
    }
}
