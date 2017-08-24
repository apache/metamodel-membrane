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
package org.apache.metamodel.membrane.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.Valid;
import javax.ws.rs.core.UriBuilder;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.membrane.app.DataSourceRegistry;
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.app.exceptions.InvalidDataSourceException;
import org.apache.metamodel.membrane.controllers.model.RestDataSourceDefinition;
import org.apache.metamodel.membrane.swagger.model.GetDatasourceResponse;
import org.apache.metamodel.membrane.swagger.model.GetDatasourceResponseSchemas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/{tenant}/{datasource}", produces = MediaType.APPLICATION_JSON_VALUE)
public class DataSourceController {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceController.class);

    private final TenantRegistry tenantRegistry;

    @Autowired
    public DataSourceController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public GetDatasourceResponse put(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceId,
            @RequestParam(value = "validate", required = false) Boolean validate,
            @Valid @RequestBody RestDataSourceDefinition dataSourceDefinition) {

        final Map<String, Object> map = new HashMap<>();
        map.putAll(dataSourceDefinition.getProperties());
        map.put(DataContextPropertiesImpl.PROPERTY_DATA_CONTEXT_TYPE, dataSourceDefinition.getType());

        final DataContextProperties properties = new DataContextPropertiesImpl(map);

        final DataSourceRegistry dataSourceRegistry = tenantRegistry.getTenantContext(tenantId).getDataSourceRegistry();
        if (validate != null && validate.booleanValue()) {
            // validate the data source by opening it and ensuring that a basic call such as getDefaultSchema() works.
            try {
                final DataContext dataContext = dataSourceRegistry.openDataContext(properties);
                dataContext.getDefaultSchema();
            } catch (Exception e) {
                logger.warn("Failed validation for PUT datasource '{}/{}'", tenantId, dataSourceId, e);
                throw new InvalidDataSourceException(e);
            }
        }

        final String dataContextIdentifier = dataSourceRegistry.registerDataSource(dataSourceId, properties);

        return get(tenantId, dataContextIdentifier);
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public GetDatasourceResponse get(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceName) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);

        final String tenantName = tenantContext.getTenantName();
        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{dataContext}/s/{schema}");

        List<GetDatasourceResponseSchemas> schemaLinks;
        Boolean updateable;
        try {
            final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);
            updateable = dataContext instanceof UpdateableDataContext;
            schemaLinks = dataContext.getSchemaNames().stream().map(s -> {
                final String uri = uriBuilder.build(tenantName, dataSourceName, s).toString();
                return new GetDatasourceResponseSchemas().name(s).uri(uri);
            }).collect(Collectors.toList());
        } catch (Exception e) {
            logger.warn("Failed to open for GET datasource '{}/{}'. No schemas will be listed.", tenantId,
                    dataSourceName, e);
            updateable = null;
            schemaLinks = null;
        }

        final GetDatasourceResponse resp = new GetDatasourceResponse();
        resp.type("datasource");
        resp.name(dataSourceName);
        resp.tenant(tenantName);
        resp.updateable(updateable);
        resp.queryUri(
                UriBuilder.fromPath("/{tenant}/{dataContext}/query").build(tenantName, dataSourceName).toString());
        resp.schemas(schemaLinks);
        return resp;
    }
}
