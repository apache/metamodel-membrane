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

import java.io.InputStream;
import java.net.InetAddress;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.metamodel.membrane.swagger.model.HelloResponse;
import org.apache.metamodel.membrane.swagger.model.HelloResponseOpenapi;
import org.apache.metamodel.membrane.swagger.model.HelloResponseServertime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RootInformationController {

    private static final Logger logger = LoggerFactory.getLogger(RootInformationController.class);

    @Autowired
    ServletContext servletContext;

    @RequestMapping(method = RequestMethod.GET, value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public HelloResponse index() {
        final HelloResponse resp = new HelloResponse();
        resp.ping("pong!");
        resp.application("Apache MetaModel Membrane");
        resp.version(getVersion());
        resp.serverTime(getServerTime());
        try {
            resp.canonicalHostname(InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            logger.info("Failed to get canonical-hostname", e);
        }
        resp.openApi(getOpenApi());
        return resp;
    }

    private HelloResponseOpenapi getOpenApi() {
        final String swaggerUri = servletContext.getContextPath() + "/swagger.json";
        return new HelloResponseOpenapi().spec(swaggerUri);
    }

    private HelloResponseServertime getServerTime() {
        final ZonedDateTime now = ZonedDateTime.now();
        final String dateFormatted = now.format(DateTimeFormatter.ISO_INSTANT);

        final HelloResponseServertime serverTime = new HelloResponseServertime();
        serverTime.timestamp(new Date().getTime());
        serverTime.iso8601(dateFormatted);
        return serverTime;
    }

    /**
     * Does the slightly tedious task of reading the software version from
     * META-INF based on maven metadata.
     * 
     * @return
     */
    private String getVersion() {
        final String groupId = "org.apache.metamodel.membrane";
        final String artifactId = "Membrane-core";
        final String resourcePath = "/META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties";
        final Properties properties = new Properties();
        try (final InputStream inputStream = RootInformationController.class.getResourceAsStream(resourcePath)) {
            properties.load(inputStream);
        } catch (Exception e) {
            logger.error("Failed to load version from manifest: " + e.getMessage());
        }

        final String version = properties.getProperty("version", "UNKNOWN");
        return version;
    }
}
