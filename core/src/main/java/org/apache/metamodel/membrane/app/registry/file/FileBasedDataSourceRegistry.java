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
import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.membrane.app.DataContextSupplier;
import org.apache.metamodel.membrane.app.DataSourceRegistry;
import org.apache.metamodel.membrane.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.membrane.app.exceptions.NoSuchDataSourceException;
import org.apache.metamodel.membrane.controllers.model.RestDataSourceDefinition;
import org.apache.metamodel.membrane.swagger.invoker.JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

public class FileBasedDataSourceRegistry implements DataSourceRegistry {

    private static final ObjectMapper OBJECT_MAPPER = new JSON().getContext(Object.class);
    private static final String DATASOURCE_FILE_SUFFIX = ".json";
    private static final String DATASOURCE_FILE_PREFIX = "ds_";

    private final File directory;

    public FileBasedDataSourceRegistry(File directory) {
        this.directory = directory;
    }

    @Override
    public List<String> getDataSourceNames() {
        final File[] files = directory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file.isDirectory()) {
                    final String filename = file.getName();
                    if (filename.startsWith(DATASOURCE_FILE_PREFIX) && filename.endsWith(DATASOURCE_FILE_SUFFIX)) {
                        return true;
                    }
                }
                return false;
            }
        });
        return Arrays.stream(files).map(f -> getDataSourceName(f)).collect(Collectors.toList());
    }

    private String getDataSourceName(File file) {
        final String filename = file.getName();
        return filename.substring(DATASOURCE_FILE_PREFIX.length(), filename.length() - DATASOURCE_FILE_SUFFIX.length());
    }

    private File getDataSourceFile(String name) {
        final String filename = DATASOURCE_FILE_PREFIX + name + DATASOURCE_FILE_SUFFIX;
        return new File(directory, filename);
    }

    @Override
    public String registerDataSource(String dataSourceName, DataContextProperties dataContextProperties)
            throws DataSourceAlreadyExistException {
        if (Strings.isNullOrEmpty(dataSourceName)) {
            throw new IllegalArgumentException("DataSource name cannot be null or empty");
        }
        final File file = getDataSourceFile(dataSourceName);
        if (file.exists()) {
            throw new DataSourceAlreadyExistException(dataSourceName);
        }

        final RestDataSourceDefinition dataSource = new RestDataSourceDefinition(dataContextProperties);
        try {
            OBJECT_MAPPER.writeValue(file, dataSource);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return dataSourceName;
    }

    @Override
    public DataContext openDataContext(String dataSourceName) throws NoSuchDataSourceException {
        if (Strings.isNullOrEmpty(dataSourceName)) {
            throw new IllegalArgumentException("DataSource name cannot be null or empty");
        }
        final File file = getDataSourceFile(dataSourceName);
        if (!file.exists()) {
            throw new NoSuchDataSourceException(dataSourceName);
        }

        final RestDataSourceDefinition dataSource;
        try {
            dataSource = OBJECT_MAPPER.readValue(file, RestDataSourceDefinition.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final DataContextSupplier supplier = new DataContextSupplier(dataSourceName, dataSource
                .toDataContextProperties());
        return supplier.get();
    }

}
