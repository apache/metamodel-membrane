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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.swagger.model.QueryResponse;
import org.apache.metamodel.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/query",
        "/{tenant}/{dataContext}/q" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public QueryController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public QueryResponse get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName,
            @RequestParam(value = "sql", required = true) String queryString,
            @RequestParam(value = "offset", required = false) Integer offset,
            @RequestParam(value = "limit", required = false) Integer limit) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final Query query = dataContext.parseQuery(queryString);

        return executeQuery(dataContext, query, offset, limit);
    }

    public static QueryResponse executeQuery(DataContext dataContext, Query query, Integer offset, Integer limit) {

        if (offset != null) {
            query.setFirstRow(offset);
        }
        if (limit != null) {
            query.setMaxRows(limit);
        }

        final List<String> headers;
        final List<List<Object>> data = new ArrayList<>();

        try (final DataSet dataSet = dataContext.executeQuery(query)) {
            headers = dataSet.getSelectItems().stream().map((si) -> si.toString()).collect(Collectors.toList());
            while (dataSet.next()) {
                final Object[] values = dataSet.getRow().getValues();
                data.add(Arrays.asList(values));
            }
        }

        final QueryResponse resp = new QueryResponse();
        resp.type("dataset");
        resp.headers(headers);
        resp.data(data);
        return resp;
    }
}
