// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// copied from https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/PluginManager.java

package org.apache.doris.connector;

import com.google.common.collect.ImmutableList;
import com.sun.jdi.connect.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectorManager implements ConnectorInstaller {
    private static final Logger LOG = LogManager.getLogger(ConnectorManager.class);

    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("org.apache.doris.connector.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("org.openjdk.jol.")
            .add("io.opentelemetry.api.")
            .add("io.opentelemetry.context.")
            .build();

    @Override
    public void loadConnectors() {

    }

    @Override
    public void installConnector(Connector connector) {

    }
}
