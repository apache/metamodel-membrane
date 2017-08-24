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

import static org.junit.Assert.assertEquals;

import org.apache.metamodel.membrane.app.InMemoryTenantRegistry;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.app.exceptions.InvalidDataSourceException;
import org.apache.metamodel.membrane.controllers.model.RestDataSourceDefinition;
import org.apache.metamodel.membrane.swagger.model.GetDatasourceResponse;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DataSourceControllerTest {

    private final TenantRegistry tenantRegistry = new InMemoryTenantRegistry();
    private final DataSourceController dataSourceController = new DataSourceController(tenantRegistry);

    @Rule
    public TestName name = new TestName();

    @Test
    public void testPutWithoutValidation() throws Exception {
        final String tenant = name.getMethodName();
        tenantRegistry.createTenantContext(tenant);

        final RestDataSourceDefinition dataSourceDefinition = new RestDataSourceDefinition();
        dataSourceDefinition.setType("foo bar");
        dataSourceDefinition.set("hello", "world");

        final GetDatasourceResponse resp = dataSourceController.put(tenant, "ds1", false, dataSourceDefinition);
        assertEquals("ds1", resp.getName());
        assertEquals(null, resp.getSchemas());
        assertEquals(null, resp.getUpdateable());
    }

    @Test
    public void testPutWithValidationFailing() throws Exception {
        final String tenant = name.getMethodName();
        tenantRegistry.createTenantContext(tenant);

        final RestDataSourceDefinition dataSourceDefinition = new RestDataSourceDefinition();
        dataSourceDefinition.setType("foo bar");
        dataSourceDefinition.set("hello", "world");

        try {
            dataSourceController.put(tenant, "ds1", true, dataSourceDefinition);
            Assert.fail("exception expected");
        } catch (InvalidDataSourceException e) {
            assertEquals("UnsupportedDataContextPropertiesException", e.getMessage());
        }
    }

    @Test
    public void testPutWithValidationPassing() throws Exception {
        final String tenant = name.getMethodName();
        tenantRegistry.createTenantContext(tenant);

        final RestDataSourceDefinition dataSourceDefinition = new RestDataSourceDefinition();
        dataSourceDefinition.setType("pojo");

        final GetDatasourceResponse resp = dataSourceController.put(tenant, "ds1", true, dataSourceDefinition);

        final ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(
                "[{'name':'information_schema','uri':'/testPutWithValidationPassing/ds1/s/information_schema'},"
                        + "{'name':'Schema','uri':'/testPutWithValidationPassing/ds1/s/Schema'}]",
                objectMapper.writeValueAsString(resp.getSchemas()).replace('\"', '\''));
    }

}
