/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);
    final CachedConnectionProvider cachedConnectionProvider;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;

    JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = connectionProvider(
                config.connectionAttempts,
                config.connectionBackoffMs
        );
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    void write(final Collection<SinkRecord> records)
            throws SQLException, TableAlterOrCreateException {
        final Connection connection = cachedConnectionProvider.getConnection();
        // String schemaName = getSchemaSafe(connection).orElse(null);
        String catalogName = getCatalogSafe(connection).orElse(null);
        try {
            final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
            for (SinkRecord record : records) {
                if (!(record.value() instanceof Struct)) {
                    log.warn(
                            "Ignoring record with key {} and value {} because the value is not "
                                    + "an instance of Struct",
                            record.key().getClass().getCanonicalName(), record.value().getClass().getCanonicalName()
                    );
                    continue;
                }
                Struct recordValue = (Struct) record.value();
                Object sourceObj = recordValue.get("source");
                if (!(sourceObj instanceof Struct)) {
                    log.warn(
                            "Ignoring record with key {} and value {} because the value "
                                    + "does not have a source field",
                            record.key(), record.value()
                    );
                    continue;
                }
                Struct sourceRecord = (Struct) sourceObj;
                String tableName = sourceRecord.getString("table");
                String schemaName = sourceRecord.getString("schema");
                if (tableName == null || tableName.isEmpty() || schemaName == null || schemaName.isEmpty()) {
                    log.warn(
                            "Ignoring record with key {} and value {} because the value "
                                    + "does not have a table or schema field",
                            record.key(), record.value()
                    );
                    continue;
                }

                Struct after = (Struct) recordValue.get("after");
                if (after == null) {
                    log.warn(
                            "Ignoring record with key {} and value {} because the value "
                                    + "does not have an after field",
                            record.key(), record.value()
                    );
                    continue;
                }

                final TableId tableId = destinationTable(tableName, schemaName, catalogName);
                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
                    bufferByTable.put(tableId, buffer);
                }
                // modify record.value() to be the after struct
                SinkRecord newRecord = new SinkRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        after.schema(),
                        after,
                        record.kafkaOffset(),
                        record.timestamp(),
                        record.timestampType(),
                        record.headers()
                );

                buffer.add(newRecord);
            }
            for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
                TableId tableId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
                buffer.flush();
                buffer.close();
            }
            log.trace("Committing transaction");
            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
            log.error("Error during write operation. Attempting rollback.", e);
            try {
                connection.rollback();
                log.info("Successfully rolled back transaction");
            } catch (SQLException sqle) {
                log.error("Failed to rollback transaction", sqle);
                e.addSuppressed(sqle);
            } finally {
                throw e;
            }
        }
        log.info("Completed write operation for {} records to the database", records.size());
    }

    void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(String tableName, String schemaName, String catalogName) {
        if (tableName.isEmpty()) {
            throw new ConnectException("Destination table name is empty");
        }
        TableId parsedTableId = dbDialect.parseTableIdentifier(tableName);
        String finalCatalogName =
                (parsedTableId.catalogName() != null) ? parsedTableId.catalogName() : catalogName;
        String finalSchemaName =
                (parsedTableId.schemaName() != null) ? parsedTableId.schemaName() : schemaName;


        return new TableId(finalCatalogName, finalSchemaName, parsedTableId.tableName());
    }

    private Optional<String> getSchemaSafe(Connection connection) {
        try {
            return Optional.ofNullable(connection.getSchema());
        } catch (AbstractMethodError | SQLException e) {
            log.warn("Failed to get schema: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<String> getCatalogSafe(Connection connection) {
        try {
            return Optional.ofNullable(connection.getCatalog());
        } catch (AbstractMethodError | SQLException e) {
            log.warn("Failed to get catalog: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
