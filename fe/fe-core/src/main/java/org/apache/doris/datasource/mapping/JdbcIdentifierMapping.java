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

package org.apache.doris.datasource.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class JdbcIdentifierMapping implements IdentifierMapping {
    private static final Logger LOG = LogManager.getLogger(JdbcIdentifierMapping.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    public JdbcIdentifierMapping(boolean isLowerCaseMetaNames, String metaNamesMapping) {
        this.isLowerCaseMetaNames = isLowerCaseMetaNames;
        this.metaNamesMapping = metaNamesMapping;
    }

    private boolean isMappingInvalid() {
        return metaNamesMapping == null || metaNamesMapping.isEmpty();
    }

    @Override
    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteDatabaseName;
        }
        JsonNode databasesNode = readAndParseJson(metaNamesMapping, "databases");

        Map<String, String> databaseNameMapping = Maps.newTreeMap();
        if (databasesNode.isArray()) {
            for (JsonNode node : databasesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String mapping = node.path("mapping").asText();
                databaseNameMapping.put(remoteDatabase, mapping);
            }
        }

        return getMappedName(remoteDatabaseName, databaseNameMapping);
    }

    @Override
    public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteTableName;
        }
        JsonNode tablesNode = readAndParseJson(metaNamesMapping, "tables");

        Map<String, String> tableNameMapping = Maps.newTreeMap();
        if (tablesNode.isArray()) {
            for (JsonNode node : tablesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                if (remoteDatabaseName.equals(remoteDatabase)) {
                    String remoteTable = node.path("remoteTable").asText();
                    String mapping = node.path("mapping").asText();
                    tableNameMapping.put(remoteTable, mapping);
                }
            }
        }
        return getMappedName(remoteTableName, tableNameMapping);
    }

    @Override
    public String fromRemoteColumnName(String remoteDatabaseName, String remoteTableName, String remoteColumnName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteColumnName;
        }

        JsonNode columnsNode = readAndParseJson(metaNamesMapping, "columns");

        Map<String, String> columnNameMapping = Maps.newTreeMap();
        if (columnsNode.isArray()) {
            for (JsonNode node : columnsNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String remoteTable = node.path("remoteTable").asText();
                if (remoteDatabaseName.equals(remoteDatabase) && remoteTableName.equals(remoteTable)) {
                    String remoteColumn = node.path("remoteColumn").asText();
                    String mapping = node.path("mapping").asText();
                    columnNameMapping.put(remoteColumn, mapping);
                }
            }
        }
        return getMappedName(remoteColumnName, columnNameMapping);
    }

    @Override
    public String toRemoteDatabaseName(String localDatabaseName) {
        throw new UnsupportedOperationException("toRemoteDatabaseName is not supported");
    }

    @Override
    public String toRemoteTableName(String remoteDatabaseName, String localTableName) {
        throw new UnsupportedOperationException("toRemoteTableName is not supported");
    }

    @Override
    public String toRemoteColumnName(String remoteDatabaseName, String remoteTableName, String localColumnNames) {
        throw new UnsupportedOperationException("toRemoteColumnName is not supported");
    }

    private String getMappedName(String name, Map<String, String> nameMapping) {
        String mappedName = nameMapping.getOrDefault(name, name);
        return isLowerCaseMetaNames ? mappedName.toLowerCase() : mappedName;
    }

    private JsonNode readAndParseJson(String jsonPath, String nodeName) {
        JsonNode rootNode;
        try {
            rootNode = mapper.readTree(jsonPath);
            return rootNode.path(nodeName);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("parse meta_names_mapping property error", e);
        }
    }
}
