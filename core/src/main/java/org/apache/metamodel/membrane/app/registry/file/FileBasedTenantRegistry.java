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
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.app.exceptions.NoSuchTenantException;
import org.apache.metamodel.membrane.app.exceptions.TenantAlreadyExistException;

import com.google.common.base.Strings;

/**
 * A {@link TenantRegistry} that persists tenant and datasource information in
 * directories and files.
 */
public class FileBasedTenantRegistry implements TenantRegistry {

    private final File directory;

    public FileBasedTenantRegistry(File directory) {
        this.directory = directory;
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    @Override
    public List<String> getTenantIdentifiers() {
        final File[] files = directory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file.isDirectory() && !file.isHidden() && !file.getName().startsWith(".")) {
                    return true;
                }
                return false;
            }
        });
        if (files == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(files).map(File::getName).collect(Collectors.toList());
    }

    @Override
    public TenantContext getTenantContext(String tenantIdentifier) throws NoSuchTenantException {
        final File file = new File(directory, tenantIdentifier);
        if (!file.exists() || !file.isDirectory()) {
            throw new NoSuchTenantException(tenantIdentifier);
        }
        return new FileBasedTenantContext(file);
    }

    @Override
    public TenantContext createTenantContext(String tenantIdentifier) throws IllegalArgumentException,
            TenantAlreadyExistException {
        validateTenantIdentifier(tenantIdentifier);
        final File file = new File(directory, tenantIdentifier);
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new TenantAlreadyExistException(tenantIdentifier);
            } else {
                throw new IllegalArgumentException("Illegal tenant identifier string: " + tenantIdentifier
                        + ". String is reserved.");
            }
        }
        file.mkdirs();
        return getTenantContext(tenantIdentifier);
    }

    @Override
    public void deleteTenantContext(String tenantIdentifier) throws NoSuchTenantException {
        final File file = new File(directory, tenantIdentifier);
        if (!file.exists() || !file.isDirectory()) {
            throw new NoSuchTenantException(tenantIdentifier);
        }
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void validateTenantIdentifier(String tenantIdentifier) throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(tenantIdentifier)) {
            throw new IllegalArgumentException("Tenant identifier cannot be null or empty");
        }
        if (tenantIdentifier.startsWith(".")) {
            throw new IllegalArgumentException("Illegal tenant identifier string: " + tenantIdentifier
                    + ". Cannot start with dot ('.').");
        }
        try {
            Paths.get(tenantIdentifier);
        } catch (InvalidPathException ex) {
            throw new IllegalArgumentException("Illegal tenant identifier string: " + tenantIdentifier, ex);
        }
    }
}
