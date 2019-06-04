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
package org.apache.metamodel.membrane.server;

import java.io.File;

import javax.servlet.ServletException;

import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.request.RequestContextListener;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.DispatcherServlet;

import com.google.common.base.Strings;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.resource.FileResourceManager;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;

/**
 * Defines the main class that starts the Undertow-based web server.
 */
public class WebServer {

    private static final Logger logger = LoggerFactory.getLogger(WebServer.class);

    private static final int DEFAULT_PORT = 8080;

    public static void main(final String[] args) throws Exception {
        prepareDataDirectoryIfNeeded();

        final String portEnv = System.getenv("MEMBRANE_HTTP_PORT");
        final int port = Strings.isNullOrEmpty(portEnv) ? DEFAULT_PORT : Integer.parseInt(portEnv);
        final String enableCorsEnv = System.getenv("MEMBRANE_ENABLE_CORS");
        final boolean enableCors = !Strings.isNullOrEmpty(enableCorsEnv);

        logger.info("Apache MetaModel Membrane server initiating on port {}", port);

        startServer(port, enableCors);

        logger.info("Apache MetaModel Membrane server started on port {}", port);
    }

    private static void prepareDataDirectoryIfNeeded() {
        final String property = "DATA_DIRECTORY";
        if (System.getProperty(property) == null && System.getenv(property) == null) {
            final String tempDirectory = FileHelper.getTempDir().getAbsolutePath() + "/membrane";
            logger.warn("No DATA_DIRECTORY defined, using {}", tempDirectory);
            System.setProperty(property, tempDirectory);
        }
    }

    public static void startServer(int port, boolean enableCors) throws Exception {
        final DeploymentInfo deployment = Servlets.deployment().setClassLoader(WebServer.class.getClassLoader());
        deployment.setContextPath("");
        deployment.setDeploymentName("membrane");
        deployment.addInitParameter("contextConfigLocation", "classpath:context/application-context.xml");
        deployment.setResourceManager(new FileResourceManager(new File("."), 0));
        deployment.addListener(Servlets.listener(ContextLoaderListener.class));
        deployment.addListener(Servlets.listener(RequestContextListener.class));
        deployment.addServlet(Servlets.servlet("dispatcher", DispatcherServlet.class).addMapping("/*")
                .addInitParam("contextConfigLocation", "classpath:context/dispatcher-servlet.xml"));
        deployment.addFilter(Servlets.filter(CharacterEncodingFilter.class).addInitParam("forceEncoding", "true")
                .addInitParam("encoding", "UTF-8"));

        final DeploymentManager manager = Servlets.defaultContainer().addDeployment(deployment);
        manager.deploy();

        final HttpHandler handler;
        if (enableCors) {
            CorsHandlers corsHandlers = new CorsHandlers();
            handler = corsHandlers.allowOrigin(manager.start());
        } else {
            handler = manager.start();
        }

        final Undertow server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(handler).build();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // graceful shutdown of everything
                server.stop();
                try {
                    manager.stop();
                } catch (ServletException e) {
                }
                manager.undeploy();
            }
        });
    }
}
