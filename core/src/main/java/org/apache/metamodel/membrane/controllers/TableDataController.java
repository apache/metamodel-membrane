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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.RowBuilder;
import org.apache.metamodel.data.WhereClauseBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.membrane.app.DataContextTraverser;
import org.apache.metamodel.membrane.app.TenantContext;
import org.apache.metamodel.membrane.app.TenantRegistry;
import org.apache.metamodel.membrane.app.config.JacksonConfig;
import org.apache.metamodel.membrane.swagger.model.Operator;
import org.apache.metamodel.membrane.swagger.model.PostDataRequest;
import org.apache.metamodel.membrane.swagger.model.PostDataRequestDelete;
import org.apache.metamodel.membrane.swagger.model.PostDataRequestUpdate;
import org.apache.metamodel.membrane.swagger.model.PostDataResponse;
import org.apache.metamodel.membrane.swagger.model.QueryResponse;
import org.apache.metamodel.membrane.swagger.model.WhereCondition;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}/data",
        "/{tenant}/{dataContext}/s/{schema}/t/{table}/d" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class TableDataController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public TableDataController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public QueryResponse get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @RequestParam(value = "offset", required = false) Integer offset,
            @RequestParam(value = "limit", required = false) Integer limit) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final DataContextTraverser traverser = new DataContextTraverser(dataContext);

        final Table table = traverser.getTable(schemaId, tableId);

        final Query query = dataContext.query().from(table).selectAll().toQuery();

        return QueryController.executeQuery(dataContext, query, offset, limit);
    }

    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    public PostDataResponse post(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @RequestBody PostDataRequest postDataReq) {

        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final UpdateableDataContext dataContext =
                tenantContext.getDataSourceRegistry().openDataContextForUpdate(dataSourceName);

        final DataContextTraverser traverser = new DataContextTraverser(dataContext);

        final Table table = traverser.getTable(schemaId, tableId);

        final UpdateSummary result = dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                final List<PostDataRequestUpdate> updateItems = postDataReq.getUpdate();
                if (updateItems != null) {
                    for (PostDataRequestUpdate updateItem : updateItems) {
                        final RowUpdationBuilder updateBuilder = callback.update(table);
                        setWhere(updateBuilder, table, updateItem.getWhere());
                        setValues(updateBuilder, updateItem.getValues());
                        updateBuilder.execute();
                    }
                }

                final List<PostDataRequestDelete> deleteItems = postDataReq.getDelete();
                if (deleteItems != null) {
                    for (PostDataRequestDelete deleteItem : deleteItems) {
                        final RowDeletionBuilder deleteBuilder = callback.deleteFrom(table);
                        setWhere(deleteBuilder, table, deleteItem.getWhere());
                        deleteBuilder.execute();
                    }
                }

                final List<Object> insertItems = postDataReq.getInsert();
                if (insertItems != null) {
                    for (Object insertItem : insertItems) {
                        final RowInsertionBuilder insertBuild = callback.insertInto(table);
                        setValues(insertBuild, insertItem);
                        insertBuild.execute();
                    }
                }
            }
        });

        final PostDataResponse response = new PostDataResponse();
        response.status("ok");

        if (result.getDeletedRows().isPresent()) {
            final Integer deletedRecords = result.getDeletedRows().get();
            response.deletedRows(new BigDecimal(deletedRecords));
        }
        if (result.getUpdatedRows().isPresent()) {
            final Integer updatedRecords = result.getUpdatedRows().get();
            response.updatedRows(new BigDecimal(updatedRecords));
        }
        if (result.getInsertedRows().isPresent()) {
            final Integer insertedRecords = result.getInsertedRows().get();
            response.insertedRows(new BigDecimal(insertedRecords));
        }
        if (result.getGeneratedKeys().isPresent()) {
            final Iterable<Object> keys = result.getGeneratedKeys().get();
            response.generatedKeys(Lists.newArrayList(keys));
        }

        return response;
    }

    private void setWhere(WhereClauseBuilder<?> whereBuilder, Table table, List<WhereCondition> conditions) {
        for (WhereCondition condition : conditions) {
            final Column column = table.getColumnByName(condition.getColumn());
            if (column == null) {
                throw new IllegalArgumentException("No such column: " + condition.getColumn());
            }
            final OperatorType operator = toOperator(condition.getOperator());
            final FilterItem filterItem = new FilterItem(new SelectItem(column), operator, condition.getOperand());
            whereBuilder.where(filterItem);
        }
    }

    private OperatorType toOperator(Operator operator) {
        switch (operator) {
        case EQ:
            return OperatorType.EQUALS_TO;
        case NE:
            return OperatorType.DIFFERENT_FROM;
        case GT:
            return OperatorType.GREATER_THAN;
        case LT:
            return OperatorType.LESS_THAN;
        case LIKE:
            return OperatorType.LIKE;
        case NOT_LIKE:
            return OperatorType.NOT_LIKE;
        }
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }

    protected void setValues(RowBuilder<?> rowBuilder, Object values) {
        final ObjectMapper objectMapper = JacksonConfig.getObjectMapper();
        @SuppressWarnings("unchecked") final Map<String, ?> inputMap = objectMapper.convertValue(values, Map.class);

        for (Entry<String, ?> entry : inputMap.entrySet()) {
            rowBuilder.value(entry.getKey(), entry.getValue());
        }
    }
}
