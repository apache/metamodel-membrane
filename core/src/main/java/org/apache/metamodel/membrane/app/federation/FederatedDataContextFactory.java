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
package org.apache.metamodel.membrane.app.federation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.metamodel.CompositeDataContext;
import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.membrane.app.registry.DataSourceRegistry;
import org.apache.metamodel.membrane.app.registry.TenantContext;

public class FederatedDataContextFactory implements DataContextFactory {

    public static final String DATA_CONTEXT_TYPE = "federated";
    public static final String PROPERTY_DATA_SOURCES = "datasources";

    private final TenantContext tenant;

    public FederatedDataContextFactory(TenantContext tenant) {
        this.tenant = tenant;
    }

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        return DATA_CONTEXT_TYPE.equals(properties.getDataContextType());
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {

        final Object dataSourcesPropertyValue = properties.toMap().get(PROPERTY_DATA_SOURCES);
        final Collection<String> dataSourceNames = toDataSourceNames(dataSourcesPropertyValue);
        final DataSourceRegistry dataSourceRegistry = tenant.getDataSourceRegistry();
        final List<DataContext> dataContexts =
                dataSourceNames.stream().map(dataSourceRegistry::openDataContext).collect(Collectors.toList());
        return new CompositeDataContext(dataContexts);
    }

    @SuppressWarnings("unchecked")
    private Collection<String> toDataSourceNames(Object dataSourcesPropertyValue) {
        if (dataSourcesPropertyValue == null) {
            return Collections.emptyList();
        }
        if ("*".equals(dataSourcesPropertyValue)) {
            return tenant.getDataSourceRegistry().getDataSourceNames();
        }
        if (dataSourcesPropertyValue instanceof String) {
            final String str = (String) dataSourcesPropertyValue;
            return Collections.singleton(str);
        }
        if (dataSourcesPropertyValue instanceof Collection) {
            return new HashSet<>((Collection<String>)dataSourcesPropertyValue);
        }
        if (dataSourcesPropertyValue.getClass().isArray()) {
            final Set<String> result = new HashSet<>();
            final Object[] arr = (Object[]) dataSourcesPropertyValue;
            for (Object item : arr) {
                result.addAll(toDataSourceNames(item));
            }
            return result;
        }
        throw new IllegalArgumentException("Bad '" + PROPERTY_DATA_SOURCES + "' value: " + dataSourcesPropertyValue);
    }

}
