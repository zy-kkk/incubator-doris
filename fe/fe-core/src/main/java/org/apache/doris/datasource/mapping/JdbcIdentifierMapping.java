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

import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JdbcIdentifierMapping implements IdentifierMapping {
    private static final Logger LOG = LogManager.getLogger(JdbcIdentifierMapping.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, String> localDBToRemoteDB = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> localTableToRemoteTable
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>
            localColumnToRemoteColumn = new ConcurrentHashMap<>();

    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    private final JdbcExternalCatalog jdbcExternalCatalog;

    public JdbcIdentifierMapping(JdbcExternalCatalog jdbcExternalCatalog) {
        this.jdbcExternalCatalog = jdbcExternalCatalog;
        this.isLowerCaseMetaNames = Boolean.parseBoolean(jdbcExternalCatalog.getLowerCaseMetaNames());
        this.metaNamesMapping = jdbcExternalCatalog.getMetaNamesMapping();
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

        return getMappedName(remoteDatabaseName, localDBToRemoteDB, databaseNameMapping);
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

        localTableToRemoteTable.putIfAbsent(remoteDatabaseName, new ConcurrentHashMap<>());
        return getMappedName(remoteTableName, localTableToRemoteTable.get(remoteDatabaseName), tableNameMapping);
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

        localColumnToRemoteColumn.putIfAbsent(remoteDatabaseName, new ConcurrentHashMap<>());
        localColumnToRemoteColumn.get(remoteDatabaseName).putIfAbsent(remoteTableName, new ConcurrentHashMap<>());

        return getMappedName(remoteColumnName, localColumnToRemoteColumn.get(remoteDatabaseName).get(remoteTableName),
                columnNameMapping);
    }

    @Override
    public String toRemoteDatabaseName(String localDatabaseName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return localDatabaseName;
        }

        String remoteDbName = localDBToRemoteDB.get(localDatabaseName);
        if (remoteDbName != null) {
            return remoteDbName;
        }

        jdbcExternalCatalog.listDatabaseNames();
        remoteDbName = localDBToRemoteDB.get(localDatabaseName);
        if (remoteDbName != null) {
            return remoteDbName;
        }

        throw new RuntimeException("No remote database found for: " + localDatabaseName);
    }

    @Override
    public String toRemoteTableName(String remoteDatabaseName, String localTableName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return localTableName;
        }

        Map<String, String> tableMap = localTableToRemoteTable.get(remoteDatabaseName);
        String remoteTableName = (tableMap != null) ? tableMap.get(localTableName) : null;
        if (remoteTableName != null) {
            return remoteTableName;
        }

        jdbcExternalCatalog.listTableNames(null, remoteDatabaseName);
        tableMap = localTableToRemoteTable.get(remoteDatabaseName);
        remoteTableName = (tableMap != null) ? tableMap.get(localTableName) : null;
        if (remoteTableName != null) {
            return remoteTableName;
        }

        throw new RuntimeException("No remote table found for: " + localTableName + " in database: " + remoteDatabaseName);
    }

    @Override
    public String toRemoteColumnName(String remoteDatabaseName, String remoteTableName, String localColumnNames) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return localColumnNames;
        }
        Map<String, String> columnMap = localColumnToRemoteColumn.get(remoteDatabaseName).get(remoteTableName);
        String remoteColumnName = (columnMap != null) ? columnMap.get(localColumnNames) : null;
        if (remoteColumnName != null) {
            return remoteColumnName;
        }

        jdbcExternalCatalog.getSchema(remoteDatabaseName, remoteTableName);
    }

    private String getMappedName(String name, ConcurrentHashMap<String, String> localToRemoteMap,
            Map<String, String> nameMapping) {
        String mappedName = nameMapping.getOrDefault(name, name);
        String localName = isLowerCaseMetaNames ? mappedName.toLowerCase() : mappedName;
        localToRemoteMap.putIfAbsent(localName, name);
        return localName;
    }

    private <K, V> V getRequiredMapping(Map<K, V> map, K key, String typeName) {
        V value = map.get(key);
        if (value == null) {
            LOG.warn("No remote {} found for: {}. Please refresh this catalog.", typeName, key);
            throw new RuntimeException(
                    "No remote " + typeName + " found for: " + key + ". Please refresh this catalog.");
        }
        return value;
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
