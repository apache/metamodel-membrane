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
package org.apache.metamodel.membrane.app;

import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.membrane.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.membrane.app.exceptions.DataSourceNotUpdateableException;
import org.apache.metamodel.membrane.app.exceptions.NoSuchDataSourceException;

/**
 * Represents a user's/tenant's registry of {@link DataContext}s.
 */
public interface DataSourceRegistry {

    public List<String> getDataSourceNames();

    /**
     * 
     * @param dataSourceName
     * @param dataContextProperties
     * @return the identifier/name for the data source.
     * @throws DataSourceAlreadyExistException
     */
    public String registerDataSource(String dataSourceName, DataContextProperties dataContextProperties)
            throws DataSourceAlreadyExistException;

    public void removeDataSource(String dataSourceName) throws NoSuchDataSourceException;
    
    /**
     * Opens a {@link DataContext} that exists in the registry.
     * 
     * @param dataSourceName
     * @return
     * @throws NoSuchDataSourceException
     */
    public DataContext openDataContext(String dataSourceName) throws NoSuchDataSourceException;

    /**
     * Opens a {@link DataContext} based on a set of {@link DataContextProperties}. This allows you to instantiate a
     * data source without necesarily having registered it (yet).
     * 
     * @param properties
     * @return
     */
    public DataContext openDataContext(DataContextProperties properties);

    /**
     * Opens a {@link UpdateableDataContext} that exists in the registry.
     * 
     * @param dataSourceName
     * @return
     * @throws DataSourceNotUpdateableException
     */
    public default UpdateableDataContext openDataContextForUpdate(String dataSourceName)
            throws DataSourceNotUpdateableException {
        final DataContext dataContext = openDataContext(dataSourceName);
        if (dataContext instanceof UpdateableDataContext) {
            return (UpdateableDataContext) dataContext;
        }
        throw new DataSourceNotUpdateableException(dataSourceName);
    }
}
