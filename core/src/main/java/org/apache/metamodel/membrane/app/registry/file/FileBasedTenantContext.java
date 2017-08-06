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
package org.apache.metamodel.membrane.app.registry.file;

import java.io.File;

import org.apache.metamodel.membrane.app.CachedDataSourceRegistryWrapper;
import org.apache.metamodel.membrane.app.DataSourceRegistry;
import org.apache.metamodel.membrane.app.TenantContext;

class FileBasedTenantContext implements TenantContext {

    private final File directory;
    private final DataSourceRegistry dataContextRegistry;

    public FileBasedTenantContext(File directory) {
        this.directory = directory;
        this.dataContextRegistry = new CachedDataSourceRegistryWrapper(new FileBasedDataSourceRegistry(directory));
    }

    @Override
    public String getTenantName() {
        return directory.getName();
    }

    @Override
    public DataSourceRegistry getDataSourceRegistry() {
        return dataContextRegistry;
    }

    @Override
    public String toString() {
        return "FileBasedTenantContext[" + directory.getName() + "]";
    }
}
