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
package org.apache.metamodel.membrane.controllers.model;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.membrane.app.DataSourceDefinition;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestDataSourceDefinition implements DataSourceDefinition {

    private final Map<String, Object> properties = new HashMap<>();

    @JsonProperty(value = "type", required = true)
    @NotNull
    private String type;

    // default constructor
    public RestDataSourceDefinition() {
    }

    // specialized constructor for DataContextProperties conversion
    public RestDataSourceDefinition(DataContextProperties dataContextProperties) {
        properties.putAll(dataContextProperties.toMap());
        type = (String) properties.remove(DataContextPropertiesImpl.PROPERTY_DATA_CONTEXT_TYPE);
    }

    public DataContextProperties toDataContextProperties() {
        final DataContextPropertiesImpl dataContextPropertiesImpl = new DataContextPropertiesImpl(properties);
        dataContextPropertiesImpl.setDataContextType(type);
        return dataContextPropertiesImpl;
    }

    @Override
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }

    @JsonAnyGetter
    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void set(final String name, final Object value) {
        properties.put(name, value);
    }
}
