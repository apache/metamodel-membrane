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
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.controllers.model.RestDataSourceDefinition;
import org.apache.metamodel.membrane.swagger.model.GetDatasourceResponse;
import org.apache.metamodel.membrane.swagger.model.GetDatasourceResponseSchemas;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/{tenant}/{datasource}", produces = MediaType.APPLICATION_JSON_VALUE)
public class DataSourceController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public DataSourceController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public GetDatasourceResponse put(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceId,
            @Valid @RequestBody RestDataSourceDefinition dataContextDefinition) {

        final Map<String, Object> map = new HashMap<>();
        map.putAll(dataContextDefinition.getProperties());
        map.put(DataContextPropertiesImpl.PROPERTY_DATA_CONTEXT_TYPE, dataContextDefinition.getType());

        if (!map.containsKey(DataContextPropertiesImpl.PROPERTY_DATABASE)) {
            // add the data source ID as database name if it is not already set.
            map.put(DataContextPropertiesImpl.PROPERTY_DATABASE, dataSourceId);
        }

        final DataContextProperties properties = new DataContextPropertiesImpl(map);

        final String dataContextIdentifier = tenantRegistry.getTenantContext(tenantId).getDataSourceRegistry()
                .registerDataSource(dataSourceId, properties);

        return get(tenantId, dataContextIdentifier);
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public GetDatasourceResponse get(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceName) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final String tenantName = tenantContext.getTenantName();
        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{dataContext}/s/{schema}");

        final List<GetDatasourceResponseSchemas> schemaLinks = dataContext.getSchemaNames().stream().map(s -> {
            final String uri = uriBuilder.build(tenantName, dataSourceName, s).toString();
            return new GetDatasourceResponseSchemas().name(s).uri(uri);
        }).collect(Collectors.toList());

        final GetDatasourceResponse resp = new GetDatasourceResponse();
        resp.type("datasource");
        resp.name(dataSourceName);
        resp.tenant(tenantName);
        resp.updateable(dataContext instanceof UpdateableDataContext);
        resp.queryUri(UriBuilder.fromPath("/{tenant}/{dataContext}/query").build(tenantName, dataSourceName)
                .toString());
        resp.schemas(schemaLinks);
        return resp;
    }
}
