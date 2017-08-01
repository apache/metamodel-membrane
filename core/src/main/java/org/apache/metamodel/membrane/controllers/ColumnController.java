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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.membrane.app.DataContextTraverser;
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.swagger.model.GetColumnResponse;
import org.apache.metamodel.membrane.swagger.model.GetColumnResponseMetadata;
import org.apache.metamodel.schema.Column;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}/columns/{column}",
        "/{tenant}/{dataContext}/s/{schema}/t/{table}/c/{column}" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class ColumnController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public ColumnController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public GetColumnResponse get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @PathVariable("column") String columnId) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final DataContextTraverser traverser = new DataContextTraverser(dataContext);

        final Column column = traverser.getColumn(schemaId, tableId, columnId);

        final String tenantName = tenantContext.getTenantName();
        final String tableName = column.getTable().getName();
        final String schemaName = column.getTable().getSchema().getName();

        final GetColumnResponseMetadata metadata = new GetColumnResponseMetadata();
        metadata.number(column.getColumnNumber());
        metadata.size(column.getColumnSize());
        metadata.nullable(column.isNullable());
        metadata.primaryKey(column.isPrimaryKey());
        metadata.indexed(column.isIndexed());
        metadata.columnType(column.getType() == null ? null : column.getType().getName());
        metadata.nativeType(column.getNativeType());
        metadata.remarks(column.getRemarks());

        final GetColumnResponse response = new GetColumnResponse();
        response.type("column");
        response.name(column.getName());
        response.table(tableName);
        response.schema(schemaName);
        response.datasource(dataSourceName);
        response.tenant(tenantName);
        response.metadata(metadata);
        return response ;
    }
}
