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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class JdbcMySQLClient extends JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcMySQLClient.class);
    private boolean convertDateToNull = false;
    private boolean isDoris = false;

    protected JdbcMySQLClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        // Disable abandoned connection cleanup
        System.setProperty("com.mysql.cj.disableAbandonedConnectionCleanup", "true");
        convertDateToNull = isConvertDatetimeToNull(jdbcClientConfig);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'version_comment'");
            if (rs.next()) {
                String versionComment = rs.getString("Value");
                isDoris = versionComment.toLowerCase().contains("doris");
            }
        } catch (SQLException | JdbcClientException e) {
            closeClient();
            throw new JdbcClientException("Failed to initialize JdbcMySQLClient: %s", e.getMessage());
        } finally {
            close(rs, stmt, conn);
        }
    }

    protected JdbcMySQLClient(JdbcClientConfig jdbcClientConfig, String dbType) {
        super(jdbcClientConfig);
        convertDateToNull = isConvertDatetimeToNull(jdbcClientConfig);
        this.dbType = dbType;
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> remoteDatabaseNames = Lists.newArrayList();
        try {
            if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String currentDatabase = conn.getCatalog();
                remoteDatabaseNames.add(currentDatabase);
            } else {
                rs = conn.getMetaData().getCatalogs();
                while (rs.next()) {
                    remoteDatabaseNames.add(rs.getString("TABLE_CAT"));
                }
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, conn);
        }
        return filterDatabaseNames(remoteDatabaseNames);
    }

    @Override
    protected void processTable(String remoteDbName, String remoteTableName, String[] tableTypes,
            Consumer<ResultSet> resultSetConsumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getTables(remoteDbName, null, remoteTableName, tableTypes);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    @Override
    protected ResultSet getRemoteColumns(DatabaseMetaData databaseMetaData, String catalogName, String remoteDbName,
            String remoteTableName) throws SQLException {
        return databaseMetaData.getColumns(remoteDbName, null, remoteTableName, null);
    }

    /**
     * get all columns of one table
     */
    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String localDbName, String localTableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);

            Map<String, String> mapFieldtoType = Maps.newHashMap();
            if (isDoris) {
                mapFieldtoType = getColumnsDataTypeUseQuery(remoteDbName, remoteTableName);
            }

            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema(rs, mapFieldtoType);
                tableSchema.add(field);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get jdbc columns info for remote table `%s.%s`: %s",
                    remoteDbName, remoteTableName, Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    protected String getCatalogName(Connection conn) throws SQLException {
        return null;
    }

    protected Set<String> getFilterInternalDatabases() {
        return ImmutableSet.<String>builder()
                .add("information_schema")
                .add("performance_schema")
                .add("mysql")
                .add("sys")
                .build();
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        // For Doris type
        if (isDoris) {
            return dorisTypeToDoris(fieldSchema);
        }

        String jdbcTypeName = fieldSchema.getDataTypeName()
                .orElseThrow(() -> new JdbcClientException("Type name is missing: " + fieldSchema));

        switch (jdbcTypeName.toLowerCase(Locale.ENGLISH)) {
            case "tinyint unsigned":
                return Type.SMALLINT;
            case "smallint unsigned":
            case "mediumint unsigned":
                return Type.INT;
            case "int unsigned":
                return Type.BIGINT;
            case "bigint unsigned":
                return Type.LARGEINT;
            case "double unsigned":
                return Type.DOUBLE;
            case "float unsigned":
                return Type.FLOAT;
            case "decimal unsigned": {
                int precision = fieldSchema.requiredColumnSize() + 1;
                int scale = fieldSchema.requiredDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "json":
            case "enum":
                return Type.STRING;
            default: {
                switch (fieldSchema.getDataType()) {
                    case Types.BIT:
                        if (fieldSchema.getColumnSize().orElse(0) == 1) {
                            return Type.BOOLEAN;
                        } else {
                            return ScalarType.createStringType();
                        }
                    case Types.TINYINT:
                        return Type.TINYINT;
                    case Types.SMALLINT:
                        return Type.SMALLINT;
                    case Types.INTEGER:
                        return Type.INT;
                    case Types.BIGINT:
                        return Type.BIGINT;
                    case Types.REAL:
                        return Type.FLOAT;
                    case Types.DOUBLE:
                        return Type.DOUBLE;
                    case Types.NUMERIC:
                    case Types.DECIMAL: {
                        int precision = fieldSchema.requiredColumnSize();
                        int scale = fieldSchema.requiredDecimalDigits();
                        return createDecimalOrStringType(precision, scale);
                    }
                    case Types.DATE:
                        if (convertDateToNull) {
                            fieldSchema.setAllowNull(true);
                        }
                        return ScalarType.createDateV2Type();
                    case Types.TIMESTAMP:
                        if (convertDateToNull) {
                            fieldSchema.setAllowNull(true);
                        }
                        int columnSize = fieldSchema.requiredColumnSize();
                        int scale = columnSize > 19 ? columnSize - 20 : 0;
                        if (scale > 6) {
                            scale = 6;
                            LOG.warn("Datetime scale exceeds 6, setting to 6 for: {}", fieldSchema);
                        }
                        return ScalarType.createDatetimeV2Type(scale);
                    case Types.CHAR:
                        ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                        charType.setLength(fieldSchema.requiredColumnSize());
                        return charType;
                    case Types.VARCHAR:
                        return ScalarType.createVarcharType(fieldSchema.requiredColumnSize());
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.TIME:
                        return ScalarType.createStringType();
                    default:
                        return Type.UNSUPPORTED;
                }
            }
        }
    }

    private boolean isConvertDatetimeToNull(JdbcClientConfig jdbcClientConfig) {
        // Check if the JDBC URL contains "zeroDateTimeBehavior=convertToNull".
        return jdbcClientConfig.getJdbcUrl().contains("zeroDateTimeBehavior=convertToNull");
    }

    /**
     * get all columns like DatabaseMetaData.getColumns in mysql-jdbc-connector
     */
    private Map<String, String> getColumnsDataTypeUseQuery(String remoteDbName, String remoteTableName) {
        Connection conn = getConnection();
        ResultSet resultSet = null;
        Map<String, String> fieldtoType = Maps.newHashMap();

        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(remoteTableName);
        queryBuf.append(" FROM ");
        queryBuf.append(remoteDbName);
        try (Statement stmt = conn.createStatement()) {
            resultSet = stmt.executeQuery(queryBuf.toString());
            while (resultSet.next()) {
                // get column name
                String fieldName = resultSet.getString("Field");
                // get original type name
                String typeName = resultSet.getString("Type");
                fieldtoType.put(fieldName, typeName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get jdbc columns info for remote table `%s.%s`: %s",
                    remoteDbName, remoteTableName, Util.getRootCauseMessage(e));
        } finally {
            close(resultSet, conn);
        }
        return fieldtoType;
    }

    private Type dorisTypeToDoris(JdbcFieldSchema fieldSchema) {
        String type = fieldSchema.getDataTypeName().orElse("unknown").toUpperCase();
        String upperType = type.toUpperCase();

        // For ARRAY type
        if (upperType.startsWith("ARRAY")) {
            String innerType = upperType.substring(6, upperType.length() - 1).trim();
            JdbcFieldSchema innerFieldSchema = new JdbcFieldSchema(fieldSchema);
            innerFieldSchema.setDataTypeName(Optional.of(innerType));
            Type arrayInnerType = dorisTypeToDoris(innerFieldSchema);
            return ArrayType.create(arrayInnerType, true);
        }

        int openParen = upperType.indexOf("(");
        String baseType = (openParen == -1) ? upperType : upperType.substring(0, openParen);

        switch (baseType) {
            case "BOOL":
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "INT":
                return Type.INT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "BIGINT":
                return Type.BIGINT;
            case "LARGEINT":
                return Type.LARGEINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL":
            case "DECIMALV3": {
                int precision = fieldSchema.requiredColumnSize();
                int scale = fieldSchema.requiredDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "DATE":
            case "DATEV2":
                return ScalarType.createDateV2Type();
            case "DATETIME":
            case "DATETIMEV2": {
                int scale = (openParen == -1) ? 6
                        : Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "CHAR":
            case "CHARACTER":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.requiredColumnSize());
                return charType;
            case "VARCHAR":
                return ScalarType.createVarcharType(fieldSchema.requiredColumnSize());
            case "STRING":
            case "TEXT":
            case "JSON":
            case "JSONB":
                return ScalarType.createStringType();
            case "HLL":
                return ScalarType.createHllType();
            case "BITMAP":
                return Type.BITMAP;
            default:
                return Type.UNSUPPORTED;
        }
    }
}
