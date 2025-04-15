package com.github.senocak.sks.commands

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import org.springframework.shell.table.ArrayTableModel
import org.springframework.shell.table.BorderStyle
import org.springframework.shell.table.TableBuilder
import java.io.File
import java.io.FileWriter
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import javax.sql.DataSource

@ShellComponent
class PostgreSqlShellCommands {
    private var jdbcTemplate: JdbcTemplate? = null
    private var currentHost: String? = null
    private var currentPort: Int? = null
    private var currentDatabase: String? = null
    private var currentUsername: String? = null
    private var currentPassword: String? = null

    // db-connect localhost 54321 shelldb postgres senocak

    @ShellMethod(key = ["db-connect"], value = "Connect to PostgreSQL database", group = "PostgreSQL Operations")
    fun connect(
        @ShellOption(help = "Database host") host: String,
        @ShellOption(help = "Database port") port: Int,
        @ShellOption(help = "Database name") database: String,
        @ShellOption(help = "Database username") username: String,
        @ShellOption(help = "Database password") password: String
    ): String {
        try {
            val dataSource: DataSource = createDataSource(host = host, port = port, database = database,
                username = username, password = password)
            jdbcTemplate = JdbcTemplate(dataSource)
            jdbcTemplate!!.queryForObject("SELECT 1", Int::class.java)
            currentHost = host
            currentPort = port
            currentDatabase = database
            currentUsername = username
            currentPassword = password
            return "Successfully connected to PostgreSQL database at $host:$port/$database"
        } catch (e: Exception) {
            return "Failed to connect to database: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-list-tables"], value = "List all tables in the database", group = "PostgreSQL Operations")
    fun listTables(): String {
        checkConnection()
        val tables: MutableList<String> = mutableListOf()
        val connection: Connection = jdbcTemplate!!.dataSource!!.connection
        try {
            val metaData: DatabaseMetaData = connection.metaData
            val resultSet: ResultSet = metaData.getTables(null, "public", "%", arrayOf("TABLE"))
            while (resultSet.next())
                tables.add(resultSet.getString("TABLE_NAME"))
            if (tables.isEmpty())
                return "No tables found in the database."
            val data: Array<Array<String?>> = Array(size = tables.size + 1) { arrayOfNulls(size = 1) }
            data[0][0] = "Table Name"
            tables.forEachIndexed { index: Int, tableName: String ->
                data[index + 1][0] = tableName
            }
            val tableModel = ArrayTableModel(data)
            val tableBuilder = TableBuilder(tableModel)
            tableBuilder.addFullBorder(BorderStyle.fancy_light)
            return tableBuilder.build().render(80)
        } finally {
            connection.close()
        }
    }

    @ShellMethod(key = ["db-describe-table"], value = "Describe table structure", group = "PostgreSQL Operations")
    fun describeTable(@ShellOption(help = "Table name") tableName: String): String {
        checkConnection()
        try {
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData
                // Check if table exists
                val tables: ResultSet = metaData.getTables(null, "public", tableName, arrayOf("TABLE"))
                if (!tables.next())
                    return "Table '$tableName' does not exist."
                // Get column information
                val columns: ResultSet = metaData.getColumns(null, "public", tableName, "%")
                val columnList: MutableList<Array<String>> = mutableListOf()
                while (columns.next()) {
                    val columnName: String = columns.getString("COLUMN_NAME")
                    val dataType: String = columns.getString("TYPE_NAME")
                    val columnSize: Int = columns.getInt("COLUMN_SIZE")
                    val isNullable: String = if (columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable) "YES" else "NO"
                    columnList.add(element = arrayOf(columnName, dataType, columnSize.toString(), isNullable))
                }
                // Get primary key information
                val primaryKeys: ResultSet = metaData.getPrimaryKeys(null, "public", tableName)
                val pkColumns: MutableList<String> = mutableListOf()
                while (primaryKeys.next())
                    pkColumns.add(element = primaryKeys.getString("COLUMN_NAME"))
                // Create table for display
                val data: Array<Array<String?>> = Array(size = columnList.size + 1) { arrayOfNulls(size = 5) }
                data[0][0] = "Column Name"
                data[0][1] = "Data Type"
                data[0][2] = "Size"
                data[0][3] = "Nullable"
                data[0][4] = "Primary Key"
                columnList.forEachIndexed { index: Int, columnInfo: Array<String> ->
                    data[index + 1][0] = columnInfo[0]
                    data[index + 1][1] = columnInfo[1]
                    data[index + 1][2] = columnInfo[2]
                    data[index + 1][3] = columnInfo[3]
                    data[index + 1][4] = if (pkColumns.contains(columnInfo[0])) "YES" else "NO"
                }
                val tableModel = ArrayTableModel(data)
                val tableBuilder = TableBuilder(tableModel)
                tableBuilder.addFullBorder(BorderStyle.fancy_light)
                return tableBuilder.build().render(100)
            } finally {
                connection.close()
            }
        } catch (e: Exception) {
            return "Error describing table: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-query"], value = "Execute a SELECT query", group = "PostgreSQL Operations")
    fun executeQuery(
        @ShellOption(help = "SQL query") query: String,
        @ShellOption(help = "Maximum rows to display", defaultValue = "100") maxRows: Int
    ): String {
        checkConnection()
        if (!query.trim().uppercase().startsWith(prefix = "SELECT"))
            return "Only SELECT queries are allowed with this command. Use db-execute for other operations."
        try {
            val rows: SqlRowSet = jdbcTemplate!!.queryForRowSet(query)
            val metadata: SqlRowSetMetaData = rows.metaData
            val columnCount: Int = metadata.columnCount
            if (!rows.next())
                return "Query executed successfully. No results returned."
            // Reset the cursor
            rows.beforeFirst()
            // Prepare data for table display
            val resultList: MutableList<Array<String?>> = mutableListOf()
            val headers = Array<String?>(size = columnCount) { null }
            (1..columnCount).forEach { i: Int ->
                headers[i - 1] = metadata.getColumnName(i)
            }
            resultList.add(element = headers)
            var rowCount = 0
            while (rows.next() && rowCount < maxRows) {
                val rowData: Array<String?> = arrayOfNulls(size = columnCount)
                (1..columnCount).forEach { i: Int ->
                    rowData[i - 1] = rows.getObject(i)?.toString() ?: "NULL"
                }
                resultList.add(element = rowData)
                rowCount++
            }
            val data: Array<Array<String?>> = resultList.toTypedArray()
            val tableModel = ArrayTableModel(data)
            val tableBuilder = TableBuilder(tableModel)
            tableBuilder.addFullBorder(BorderStyle.fancy_light)
            val result: String = tableBuilder.build().render(120)
            return when {
                rowCount >= maxRows -> "$result\n\nShowing $maxRows rows. There may be more results. Use a LIMIT clause or increase maxRows parameter."
                else -> result
            }
        } catch (e: Exception) {
            return "Error executing query: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-execute"], value = "Execute a non-query SQL statement", group = "PostgreSQL Operations")
    fun executeStatement(@ShellOption(help = "SQL statement") sql: String): String {
        checkConnection()
        if (sql.trim().uppercase().startsWith(prefix = "SELECT"))
            return "Use db-query for SELECT operations."
        try {
            val rowsAffected: Int = jdbcTemplate!!.update(sql)
            return "Statement executed successfully. Rows affected: $rowsAffected"
        } catch (e: Exception) {
            return "Error executing statement: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-insert"], value = "Insert a record into a table", group = "PostgreSQL Operations")
    fun insertRecord(
        @ShellOption(help = "Table name") tableName: String,
        @ShellOption(help = "Column names (comma-separated)") columns: String,
        @ShellOption(help = "Values (comma-separated)") values: String
    ): String {
        checkConnection()
        val columnList: List<String> = columns.split(",").map { it.trim() }
        val valueList: List<String> = values.split(",").map { it.trim() }
        if (columnList.size != valueList.size)
            return "Error: Number of columns (${columnList.size}) does not match number of values (${valueList.size})"
        try {
            val sql: String = buildInsertStatement(tableName, columnList, valueList)
            val rowsAffected: Int = jdbcTemplate!!.update(sql)
            return "Record inserted successfully. Rows affected: $rowsAffected"
        } catch (e: Exception) {
            return "Error inserting record: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-update"], value = "Update records in a table", group = "PostgreSQL Operations")
    fun updateRecords(
        @ShellOption(help = "Table name") tableName: String,
        @ShellOption(help = "Set clause (column=value,...)") setClause: String,
        @ShellOption(help = "Where clause (without WHERE keyword)", defaultValue = "") whereClause: String
    ): String {
        checkConnection()
        try {
            val sql: String = buildUpdateStatement(tableName = tableName, setClause = setClause, whereClause = whereClause)
            val rowsAffected: Int = jdbcTemplate!!.update(sql)
            return "Update executed successfully. Rows affected: $rowsAffected"
        } catch (e: Exception) {
            return "Error updating records: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-delete"], value = "Delete records from a table", group = "PostgreSQL Operations")
    fun deleteRecords(
        @ShellOption(help = "Table name") tableName: String,
        @ShellOption(help = "Where clause (without WHERE keyword)", defaultValue = "") whereClause: String
    ): String {
        checkConnection()
        if (whereClause.isBlank())
            return "Warning: This will delete ALL records from the table. Use db-execute if you're sure."
        try {
            val rowsAffected: Int = jdbcTemplate!!.update("DELETE FROM $tableName WHERE $whereClause")
            return "Delete executed successfully. Rows affected: $rowsAffected"
        } catch (e: Exception) {
            return "Error deleting records: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-create-table"], value = "Create a new table", group = "PostgreSQL Operations")
    fun createTable(
        @ShellOption(help = "Table name") tableName: String,
        @ShellOption(help = "Column definitions (e.g., \"id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL\")") columnDefinitions: String
    ): String {
        checkConnection()
        try {
            // Check if table already exists
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData
                val tables: ResultSet = metaData.getTables(null, "public", tableName, arrayOf("TABLE"))
                if (tables.next())
                    return "Error: Table '$tableName' already exists."
            } finally {
                connection.close()
            }
            // Create the table
            jdbcTemplate!!.execute("CREATE TABLE $tableName ($columnDefinitions)")
            return "Table '$tableName' created successfully."
        } catch (e: Exception) {
            return "Error creating table: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-status"], value = "Show current database connection status", group = "PostgreSQL Operations")
    fun connectionStatus(): String =
        when {
            jdbcTemplate != null && currentHost != null ->
                "Connected to PostgreSQL database at $currentHost:$currentPort/$currentDatabase as $currentUsername"
            else ->
                "Not connected to any database. Use db-connect to establish a connection."
        }

    @ShellMethod(key = ["db-export-query"], value = "Execute a query and export results to a file", group = "PostgreSQL Operations")
    fun exportQuery(
        @ShellOption(help = "SQL query") query: String,
        @ShellOption(help = "Output file path") filePath: String,
        @ShellOption(help = "Export format (csv, json)", defaultValue = "csv") format: String
    ): String {
        checkConnection()
        if (!query.trim().uppercase().startsWith(prefix = "SELECT"))
            return "Only SELECT queries are allowed with this command."
        try {
            val rows: SqlRowSet = jdbcTemplate!!.queryForRowSet(query)
            val metadata: SqlRowSetMetaData = rows.metaData
            val columnCount: Int = metadata.columnCount
            if (!rows.next())
                return "Query executed successfully. No results to export."

            // Reset the cursor
            rows.beforeFirst()

            // Create output file
            val file = File(filePath)
            file.parentFile?.mkdirs() // Create parent directories if they don't exist

            when (format.lowercase()) {
                "csv" -> {
                    FileWriter(file).use { writer: FileWriter ->
                        // Write headers
                        val headers: List<String> = (1..columnCount).map { i: Int -> metadata.getColumnName(i) }
                        writer.write(headers.joinToString(separator = ","))
                        writer.write("\n")

                        // Write data
                        while (rows.next()) {
                            val rowData: List<String> = (1..columnCount).map { i: Int ->
                                val value: String = rows.getObject(i)?.toString() ?: ""
                                // Escape commas and quotes in CSV
                                "\"${value.replace(oldValue = "\"", newValue = "\"\"")}\""
                            }
                            writer.write(rowData.joinToString(separator = ","))
                            writer.write("\n")
                        }
                    }
                    return "Query results exported to CSV file: $filePath"
                }
                "json" -> {
                    FileWriter(file).use { writer: FileWriter ->
                        val headers: List<String> = (1..columnCount).map { i: Int -> metadata.getColumnName(i) }
                        writer.write("[\n")

                        var firstRow = true
                        while (rows.next()) {
                            if (!firstRow) writer.write(",\n") else firstRow = false

                            writer.write("  {")
                            val rowData: List<String> = (1..columnCount).map { i: Int ->
                                val columnName: String = metadata.getColumnName(i)
                                val value: String = rows.getObject(i)?.toString() ?: "null"
                                // Format value based on type
                                val formattedValue: String = when {
                                    value == "null" -> "null"
                                    // Try to parse as number
                                    value.toDoubleOrNull() != null -> value
                                    // Otherwise treat as string
                                    else -> "\"${value.replace(oldValue = "\"", newValue = "\\\"")}\""
                                }
                                "\"$columnName\": $formattedValue"
                            }
                            writer.write(rowData.joinToString(separator = ", "))
                            writer.write("}")
                        }
                        writer.write("\n]")
                    }
                    return "Query results exported to JSON file: $filePath"
                }
                else -> return "Unsupported export format: $format. Supported formats are: csv, json"
            }
        } catch (e: Exception) {
            return "Error exporting query results: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-show-indexes"], value = "Show indexes for a table", group = "PostgreSQL Operations")
    fun showIndexes(@ShellOption(help = "Table name") tableName: String): String {
        checkConnection()
        try {
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData

                // Check if table exists
                val tables: ResultSet = metaData.getTables(null, "public", tableName, arrayOf("TABLE"))
                if (!tables.next())
                    return "Table '$tableName' does not exist."

                // Get index information
                val indexes: ResultSet = metaData.getIndexInfo(null, "public", tableName, false, false)
                val indexList: MutableList<Array<String?>> = mutableListOf()

                while (indexes.next()) {
                    val indexName: String? = indexes.getString("INDEX_NAME")
                    if (indexName != null) {
                        val columnName: String = indexes.getString("COLUMN_NAME")
                        val nonUnique: Boolean = indexes.getBoolean("NON_UNIQUE")
                        val type: String = when(indexes.getShort("TYPE")) {
                            DatabaseMetaData.tableIndexStatistic -> "STATISTIC"
                            DatabaseMetaData.tableIndexClustered -> "CLUSTERED"
                            DatabaseMetaData.tableIndexHashed -> "HASHED"
                            DatabaseMetaData.tableIndexOther -> "OTHER"
                            else -> "UNKNOWN"
                        }
                        val ascOrDesc: String = indexes.getString("ASC_OR_DESC") ?: "N/A"

                        indexList.add(arrayOf(indexName, columnName, (!nonUnique).toString(), type, ascOrDesc))
                    }
                }
                if (indexList.isEmpty())
                    return "No indexes found for table '$tableName'."
                // Create table for display
                val data: Array<Array<String?>> = Array(size = indexList.size + 1) { arrayOfNulls(size = 5) }
                data[0][0] = "Index Name"
                data[0][1] = "Column Name"
                data[0][2] = "Unique"
                data[0][3] = "Type"
                data[0][4] = "Order"

                indexList.forEachIndexed { index: Int, indexInfo: Array<String?> ->
                    data[index + 1][0] = indexInfo[0]
                    data[index + 1][1] = indexInfo[1]
                    data[index + 1][2] = indexInfo[2]
                    data[index + 1][3] = indexInfo[3]
                    data[index + 1][4] = indexInfo[4]
                }

                val tableModel = ArrayTableModel(data)
                val tableBuilder = TableBuilder(tableModel)
                tableBuilder.addFullBorder(BorderStyle.fancy_light)
                return tableBuilder.build().render(100)
            } finally {
                connection.close()
            }
        } catch (e: Exception) {
            return "Error retrieving indexes: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-info"], value = "Show database server information", group = "PostgreSQL Operations")
    fun showDatabaseInfo(): String {
        checkConnection()
        try {
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData

                // Collect database information
                val info: MutableList<Pair<String, String>> = mutableListOf<Pair<String, String>>()
                info.add(element = Pair(first = "Database Product Name", second = metaData.databaseProductName))
                info.add(element = Pair(first = "Database Version", second = metaData.databaseProductVersion))
                info.add(element = Pair(first = "Driver Name", second = metaData.driverName))
                info.add(element = Pair(first = "Driver Version", second = metaData.driverVersion))
                info.add(element = Pair(first = "JDBC Major Version", second = metaData.jdbcMajorVersion.toString()))
                info.add(element = Pair(first = "JDBC Minor Version", second = metaData.jdbcMinorVersion.toString()))
                info.add(element = Pair(first = "Max Connections", second = metaData.maxConnections.toString()))
                info.add(element = Pair(first = "Username", second = metaData.userName))

                // Get database size
                try {
                    val sizeResult: Map<String?, Any?> = jdbcTemplate!!.queryForMap("SELECT pg_size_pretty(pg_database_size(current_database())) as db_size")
                    info.add(element = Pair(first = "Database Size", second = sizeResult["db_size"].toString()))
                } catch (e: Exception) {
                    // Ignore if this query fails
                }

                // Create table for display
                val data: Array<Array<String?>> = Array(size = info.size + 1) { arrayOfNulls(size = 2) }
                data[0][0] = "Property"
                data[0][1] = "Value"

                info.forEachIndexed { index: Int, (property: String, value: String): Pair<String, String> ->
                    data[index + 1][0] = property
                    data[index + 1][1] = value
                }

                val tableModel = ArrayTableModel(data)
                val tableBuilder = TableBuilder(tableModel)
                tableBuilder.addFullBorder(BorderStyle.fancy_light)
                return tableBuilder.build().render(100)
            } finally {
                connection.close()
            }
        } catch (e: Exception) {
            return "Error retrieving database information: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-activity"], value = "Show currently running queries", group = "PostgreSQL Operations")
    fun showDatabaseActivity(): String {
        checkConnection()
        try {
            // Query to get active queries in PostgreSQL
            val rows: SqlRowSet = jdbcTemplate!!.queryForRowSet("""
                SELECT pid, 
                       usename as username, 
                       application_name,
                       client_addr as client_address,
                       state,
                       query_start,
                       now() - query_start as duration,
                       query
                FROM pg_stat_activity
                WHERE state != 'idle'
                  AND pid != pg_backend_pid()
                ORDER BY query_start DESC
            """.trimIndent())
            val metadata: SqlRowSetMetaData = rows.metaData
            val columnCount: Int = metadata.columnCount

            if (!rows.next())
                return "No active queries found."

            // Reset the cursor
            rows.beforeFirst()

            // Prepare data for table display
            val resultList: MutableList<Array<String?>> = mutableListOf()
            val headers = Array<String?>(size = columnCount) { null }
            (1..columnCount).forEach { i: Int ->
                headers[i - 1] = metadata.getColumnName(i)
            }
            resultList.add(element = headers)

            while (rows.next()) {
                val rowData: Array<String?> = arrayOfNulls(size = columnCount)
                (1..columnCount).forEach { i: Int ->
                    rowData[i - 1] = rows.getObject(i)?.toString() ?: "NULL"
                }
                resultList.add(element = rowData)
            }

            val data: Array<Array<String?>> = resultList.toTypedArray()
            val tableModel = ArrayTableModel(data)
            val tableBuilder = TableBuilder(tableModel)
            tableBuilder.addFullBorder(BorderStyle.fancy_light)
            return tableBuilder.build().render(150)
        } catch (e: Exception) {
            return "Error retrieving database activity: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-truncate-table"], value = "Remove all data from a table", group = "PostgreSQL Operations")
    fun truncateTable(
        @ShellOption(help = "Table name") tableName: String,
        @ShellOption(help = "Restart identity (reset sequences)", defaultValue = "false") restartIdentity: Boolean = false,
        @ShellOption(help = "Cascade (also truncate dependent tables)", defaultValue = "false") cascade: Boolean = false
    ): String {
        checkConnection()
        try {
            // Check if table exists
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData
                val tables: ResultSet = metaData.getTables(null, "public", tableName, arrayOf("TABLE"))
                if (!tables.next())
                    return "Error: Table '$tableName' does not exist."
            } finally {
                connection.close()
            }

            // Build the TRUNCATE statement
            val sql = StringBuilder("TRUNCATE TABLE $tableName")

            // Add options if specified
            when {
                restartIdentity -> sql.append(" RESTART IDENTITY")
                else -> sql.append(" CONTINUE IDENTITY")
            }
            when {
                cascade -> sql.append(" CASCADE")
                else -> sql.append(" RESTRICT")
            }

            // Execute the TRUNCATE statement
            jdbcTemplate!!.execute(sql.toString())

            return "Table '$tableName' truncated successfully." +
                   (if (restartIdentity) " Identity columns reset." else "") +
                   (if (cascade) " Dependent tables also truncated." else "")
        } catch (e: Exception) {
            return "Error truncating table: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-table-stats"], value = "Show table statistics", group = "PostgreSQL Operations")
    fun tableStatistics(@ShellOption(help = "Table name") tableName: String): String {
        checkConnection()
        try {
            // Check if table exists
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData
                val tables: ResultSet = metaData.getTables(null, "public", tableName, arrayOf("TABLE"))
                if (!tables.next())
                    return "Error: Table '$tableName' does not exist."
            } finally {
                connection.close()
            }

            // Query for table statistics
            val statsQuery: String = """
                SELECT
                    pg_stat_user_tables.relname AS table_name,
                    pg_class.reltuples::bigint AS row_estimate,
                    pg_size_pretty(pg_total_relation_size(pg_class.oid)) AS total_size,
                    pg_size_pretty(pg_relation_size(pg_class.oid)) AS table_size,
                    pg_size_pretty(pg_total_relation_size(pg_class.oid) - pg_relation_size(pg_class.oid)) AS index_size,
                    pg_stat_user_tables.seq_scan AS sequential_scans,
                    pg_stat_user_tables.seq_tup_read AS sequential_rows_read,
                    pg_stat_user_tables.idx_scan AS index_scans,
                    pg_stat_user_tables.idx_tup_fetch AS index_rows_fetched,
                    pg_stat_user_tables.n_tup_ins AS rows_inserted,
                    pg_stat_user_tables.n_tup_upd AS rows_updated,
                    pg_stat_user_tables.n_tup_del AS rows_deleted,
                    pg_stat_user_tables.n_live_tup AS live_rows,
                    pg_stat_user_tables.n_dead_tup AS dead_rows,
                    pg_stat_user_tables.last_vacuum AS last_vacuum,
                    pg_stat_user_tables.last_autovacuum AS last_autovacuum,
                    pg_stat_user_tables.last_analyze AS last_analyze,
                    pg_stat_user_tables.last_autoanalyze AS last_autoanalyze
                FROM pg_stat_user_tables
                JOIN pg_class ON pg_class.relname = pg_stat_user_tables.relname
                WHERE pg_stat_user_tables.relname = '$tableName'
            """.trimIndent()

            val stats: Map<String?, Any?> = jdbcTemplate!!.queryForMap(statsQuery)

            // Create table for display
            val info: MutableList<Pair<String, String>> = mutableListOf()
            stats.forEach { (key: String?, value) ->
                info.add(element = Pair(first = key.toString(), second = value?.toString() ?: "NULL"))
            }

            val data: Array<Array<String?>> = Array(size = info.size + 1) { arrayOfNulls(size = 2) }
            data[0][0] = "Statistic"
            data[0][1] = "Value"

            info.forEachIndexed { index: Int, (property: String, value: String): Pair<String, String> ->
                data[index + 1][0] = property
                data[index + 1][1] = value
            }

            val tableModel = ArrayTableModel(data)
            val tableBuilder = TableBuilder(tableModel)
            tableBuilder.addFullBorder(BorderStyle.fancy_light)
            return tableBuilder.build().render(100)
        } catch (e: Exception) {
            return "Error retrieving table statistics: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-export-schema"], value = "Export database schema to a file", group = "PostgreSQL Operations")
    fun exportSchema(
        @ShellOption(help = "Output file path") filePath: String,
        @ShellOption(help = "Include data (true/false)", defaultValue = "false") includeData: Boolean = false,
        @ShellOption(help = "Tables to include (comma-separated, empty for all)", defaultValue = "") tables: String = ""
    ): String {
        checkConnection()
        try {
            // Create output file
            val file = File(filePath)
            file.parentFile?.mkdirs() // Create parent directories if they don't exist

            // Build pg_dump command
            val pgDumpCommand: MutableList<String> = mutableListOf(
                "pg_dump",
                "-h", currentHost!!,
                "-p", currentPort.toString(),
                "-U", currentUsername!!,
                "-d", currentDatabase!!,
                "-f", filePath,
                "--schema-only"  // Only schema by default
            )

            // Add data if requested
            if (includeData)
                pgDumpCommand.remove(element = "--schema-only")

            // Add specific tables if requested
            if (tables.isNotBlank())
                tables.split(",").map { it.trim() }.forEach { table: String ->
                    pgDumpCommand.add(element = "-t")
                    pgDumpCommand.add(element = table)
                }

            // Set PGPASSWORD environment variable
            val processBuilder = ProcessBuilder(pgDumpCommand)
            val env = processBuilder.environment()
            env["PGPASSWORD"] = currentPassword

            // Execute pg_dump
            val process: Process = processBuilder.start()
            val exitCode: Int = process.waitFor()

            // Check if pg_dump was successful
            return when (exitCode) {
                0 ->
                    "Database schema" + (if (includeData) " and data" else "") +
                            " exported successfully to: $filePath" +
                            (if (tables.isNotBlank()) " (Tables: $tables)" else "")
                else -> {
                    val errorOutput: String = process.errorStream.bufferedReader().readText()
                    "Error exporting schema: $errorOutput"
                }
            }
        } catch (e: Exception) {
            return "Error exporting schema: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-copy-table"], value = "Create a copy of a table", group = "PostgreSQL Operations")
    fun copyTable(
        @ShellOption(help = "Source table name") sourceTable: String,
        @ShellOption(help = "Destination table name") destinationTable: String,
        @ShellOption(help = "Include data (true/false)", defaultValue = "false") includeData: Boolean = false,
        @ShellOption(help = "Include indexes (true/false)", defaultValue = "true") includeIndexes: Boolean = true,
        @ShellOption(help = "Include constraints (true/false)", defaultValue = "true") includeConstraints: Boolean = true
    ): String {
        checkConnection()
        try {
            // Check if source table exists
            val connection: Connection = jdbcTemplate!!.dataSource!!.connection
            try {
                val metaData: DatabaseMetaData = connection.metaData
                val tables: ResultSet = metaData.getTables(null, "public", sourceTable, arrayOf("TABLE"))
                if (!tables.next())
                    return "Error: Source table '$sourceTable' does not exist."

                // Check if destination table already exists
                val destTables: ResultSet = metaData.getTables(null, "public", destinationTable, arrayOf("TABLE"))
                if (destTables.next())
                    return "Error: Destination table '$destinationTable' already exists."
            } finally {
                connection.close()
            }

            // Create the new table
            if (includeIndexes && includeConstraints) {
                // Use CREATE TABLE ... LIKE for full structure copy
                jdbcTemplate!!.execute("CREATE TABLE $destinationTable (LIKE $sourceTable INCLUDING ALL)")
            } else {
                // Selective inclusion
                val includeParts: MutableList<String> = mutableListOf()
                if (includeIndexes) includeParts.add(element = "INCLUDING INDEXES")
                if (includeConstraints) includeParts.add(element = "INCLUDING CONSTRAINTS")

                val includeClause: String = when {
                    includeParts.isEmpty() -> ""
                    else -> includeParts.joinToString(" ")
                }
                jdbcTemplate!!.execute("CREATE TABLE $destinationTable (LIKE $sourceTable $includeClause)")
            }

            // Copy data if requested
            if (includeData) {
                val rowsAffected: Int = jdbcTemplate!!.update("INSERT INTO $destinationTable SELECT * FROM $sourceTable")
                return "Table '$sourceTable' copied to '$destinationTable' with structure" + 
                       (if (includeIndexes) " and indexes" else "") +
                       (if (includeConstraints) " and constraints" else "") +
                       ". $rowsAffected rows copied."
            }

            return "Table '$sourceTable' structure copied to '$destinationTable'" +
                   (if (includeIndexes) " with indexes" else " without indexes") +
                   (if (includeConstraints) " with constraints" else " without constraints")
        } catch (e: Exception) {
            return "Error copying table: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-backup"], value = "Backup database to a file", group = "PostgreSQL Operations")
    fun backupDatabase(
        @ShellOption(help = "Output file path") filePath: String,
        @ShellOption(help = "Format (plain, custom, directory, tar)", defaultValue = "custom") format: String = "custom",
        @ShellOption(help = "Compression level (0-9)", defaultValue = "5") compressionLevel: Int = 5,
        @ShellOption(help = "Tables to include (comma-separated, empty for all)", defaultValue = "") tables: String = ""
    ): String {
        checkConnection()
        try {
            // Create output file directory if it doesn't exist
            val file = File(filePath)
            file.parentFile?.mkdirs()

            // Build pg_dump command
            val pgDumpCommand: MutableList<String> = mutableListOf(
                "pg_dump",
                "-h", currentHost!!,
                "-p", currentPort.toString(),
                "-U", currentUsername!!,
                "-d", currentDatabase!!,
                "-f", filePath,
                "-F", format.first().toString(), // Format (c=custom, p=plain, d=directory, t=tar)
                "-Z", compressionLevel.toString() // Compression level
            )

            // Add specific tables if requested
            if (tables.isNotBlank()) {
                tables.split(",").map { it.trim() }.forEach { table: String ->
                    pgDumpCommand.add(element = "-t")
                    pgDumpCommand.add(element = table)
                }
            }

            // Set PGPASSWORD environment variable
            val processBuilder = ProcessBuilder(pgDumpCommand)
            val env  = processBuilder.environment()
            env["PGPASSWORD"] = currentPassword

            // Execute pg_dump
            val process: Process = processBuilder.start()
            val exitCode: Int = process.waitFor()

            // Check if pg_dump was successful
            return when (exitCode) {
                0 ->
                    "Database backup created successfully at: $filePath" +
                            (if (tables.isNotBlank()) " (Tables: $tables)" else "") +
                            " using $format format with compression level $compressionLevel."
                else -> {
                    val errorOutput: String = process.errorStream.bufferedReader().readText()
                    "Error creating backup: $errorOutput"
                }
            }
        } catch (e: Exception) {
            return "Error creating backup: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-restore"], value = "Restore database from a backup file", group = "PostgreSQL Operations")
    fun restoreDatabase(
        @ShellOption(help = "Input backup file path") filePath: String,
        @ShellOption(help = "Clean before restore (drop existing objects)", defaultValue = "false") clean: Boolean = false,
        @ShellOption(help = "Single transaction (all or nothing)", defaultValue = "true") singleTransaction: Boolean = true
    ): String {
        checkConnection()
        try {
            // Check if file exists
            val file = File(filePath)
            if (!file.exists())
                return "Error: Backup file '$filePath' does not exist."

            // Build pg_restore command for custom, directory, or tar formats
            val isPlainFormat: Boolean = filePath.endsWith(suffix = ".sql")

            val restoreCommand: MutableList<String> = mutableListOf<String>()

            if (isPlainFormat) {
                // For plain SQL files, use psql
                restoreCommand.addAll(elements = listOf(
                    "psql",
                    "-h", currentHost!!,
                    "-p", currentPort.toString(),
                    "-U", currentUsername!!,
                    "-d", currentDatabase!!,
                    "-f", filePath
                ))

                if (clean) restoreCommand.add(element = "-c")
                if (singleTransaction) restoreCommand.add(element = "--single-transaction")
            } else {
                // For other formats, use pg_restore
                restoreCommand.addAll(elements = listOf(
                    "pg_restore",
                    "-h", currentHost!!,
                    "-p", currentPort.toString(),
                    "-U", currentUsername!!,
                    "-d", currentDatabase!!,
                    filePath
                ))

                if (clean) restoreCommand.add(element = "--clean")
                if (singleTransaction) restoreCommand.add(element = "--single-transaction")
            }

            // Set PGPASSWORD environment variable
            val processBuilder = ProcessBuilder(restoreCommand)
            val env = processBuilder.environment()
            env["PGPASSWORD"] = currentPassword
            // Execute restore
            val process: Process = processBuilder.start()
            // Check if restore was successful
            return when (process.waitFor()) {
                0 -> "Database restored successfully from: $filePath"
                else -> {
                    val errorOutput: String = process.errorStream.bufferedReader().readText()
                    "Error restoring database: $errorOutput"
                }
            }
        } catch (e: Exception) {
            return "Error restoring database: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-list-users"], value = "List database users", group = "PostgreSQL Operations")
    fun listUsers(): String {
        checkConnection()
        try {
            val rows: SqlRowSet = jdbcTemplate!!.queryForRowSet("""
                SELECT 
                    rolname AS username,
                    rolcreatedb AS can_create_db,
                    rolsuper AS is_superuser,
                    rolvaliduntil AS valid_until
                FROM pg_roles
                WHERE rolcanlogin
                ORDER BY rolname
            """.trimIndent())
            val metadata: SqlRowSetMetaData = rows.metaData
            val columnCount: Int = metadata.columnCount

            if (!rows.next())
                return "No database users found."

            // Reset the cursor
            rows.beforeFirst()

            // Prepare data for table display
            val resultList: MutableList<Array<String?>> = mutableListOf()
            val headers = Array<String?>(size = columnCount) { null }
            (1..columnCount).forEach { i: Int ->
                headers[i - 1] = metadata.getColumnName(i)
            }
            resultList.add(element = headers)

            while (rows.next()) {
                val rowData: Array<String?> = arrayOfNulls(size = columnCount)
                (1..columnCount).forEach { i: Int ->
                    rowData[i - 1] = rows.getObject(i)?.toString() ?: "NULL"
                }
                resultList.add(element = rowData)
            }

            val data: Array<Array<String?>> = resultList.toTypedArray()
            val tableModel = ArrayTableModel(data)
            val tableBuilder = TableBuilder(tableModel)
            tableBuilder.addFullBorder(BorderStyle.fancy_light)
            return tableBuilder.build().render(120)
        } catch (e: Exception) {
            return "Error listing database users: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-create-user"], value = "Create a new database user", group = "PostgreSQL Operations")
    fun createUser(
        @ShellOption(help = "Username") username: String,
        @ShellOption(help = "Password") password: String,
        @ShellOption(help = "Can create databases", defaultValue = "false") canCreateDb: Boolean = false,
        @ShellOption(help = "Is superuser", defaultValue = "false") isSuperuser: Boolean = false,
        @ShellOption(help = "Valid until (date in YYYY-MM-DD format, empty for no expiration)", defaultValue = "") validUntil: String = ""
    ): String {
        checkConnection()
        try {
            // Build CREATE USER statement
            val sql = StringBuilder("CREATE USER $username WITH PASSWORD '$password'")

            when {
                canCreateDb -> sql.append(" CREATEDB")
                else -> sql.append(" NOCREATEDB")
            }
            when {
                isSuperuser -> sql.append(" SUPERUSER")
                else -> sql.append(" NOSUPERUSER")
            }
            if (validUntil.isNotBlank())
                sql.append(" VALID UNTIL '$validUntil'")
            // Execute the CREATE USER statement
            jdbcTemplate!!.execute(sql.toString())

            return "User '$username' created successfully with" +
                   (if (canCreateDb) " CREATEDB" else " NOCREATEDB") +
                   (if (isSuperuser) " SUPERUSER" else " NOSUPERUSER") +
                   (if (validUntil.isNotBlank()) " and valid until $validUntil" else "")
        } catch (e: Exception) {
            return "Error creating user: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-alter-user"], value = "Modify a database user", group = "PostgreSQL Operations")
    fun alterUser(
        @ShellOption(help = "Username") username: String,
        @ShellOption(help = "New password (leave empty to not change)", defaultValue = "") newPassword: String = "",
        @ShellOption(help = "Can create databases (true/false, leave empty to not change)", defaultValue = "") canCreateDb: String = "",
        @ShellOption(help = "Is superuser (true/false, leave empty to not change)", defaultValue = "") isSuperuser: String = "",
        @ShellOption(help = "Valid until (date in YYYY-MM-DD format, 'none' for no expiration, empty to not change)", defaultValue = "") validUntil: String = ""
    ): String {
        checkConnection()
        try {
            // Build ALTER USER statement
            val sql = StringBuilder("ALTER USER $username")
            val changes: MutableList<String> = mutableListOf()
            if (newPassword.isNotBlank())
                changes.add(element = "PASSWORD '$newPassword'")
            if (canCreateDb.isNotBlank())
                changes.add(element = if (canCreateDb.toBoolean()) "CREATEDB" else "NOCREATEDB")
            if (isSuperuser.isNotBlank())
                changes.add(element = if (isSuperuser.toBoolean()) "SUPERUSER" else "NOSUPERUSER")
            if (validUntil.isNotBlank())
                changes.add(element = when {
                    validUntil.equals("none", ignoreCase = true) -> "VALID UNTIL 'infinity'"
                    else -> "VALID UNTIL '$validUntil'"
                })
            if (changes.isEmpty())
                return "No changes specified for user '$username'"
            sql.append(" WITH ${changes.joinToString(separator = " ")}")

            // Execute the ALTER USER statement
            jdbcTemplate!!.execute(sql.toString())

            return "User '$username' modified successfully with changes: ${changes.joinToString(separator = ", ")}"
        } catch (e: Exception) {
            return "Error modifying user: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-drop-user"], value = "Delete a database user", group = "PostgreSQL Operations")
    fun dropUser(
        @ShellOption(help = "Username") username: String,
        @ShellOption(help = "If exists (don't error if user doesn't exist)", defaultValue = "false") ifExists: Boolean = false
    ): String {
        checkConnection()
        try {
            // Execute the DROP USER statement
            jdbcTemplate!!.execute("DROP USER ${if (ifExists) "IF EXISTS " else ""}$username")
            return "User '$username' dropped successfully"
        } catch (e: Exception) {
            return "Error dropping user: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-grant"], value = "Grant privileges to a user", group = "PostgreSQL Operations")
    fun grantPrivileges(
        @ShellOption(help = "Privileges (ALL, SELECT, INSERT, UPDATE, DELETE, etc. or comma-separated list)") privileges: String,
        @ShellOption(help = "Object type (TABLE, SEQUENCE, DATABASE, etc.)") objectType: String,
        @ShellOption(help = "Object name (table name, sequence name, etc.)") objectName: String,
        @ShellOption(help = "Username to grant privileges to") username: String
    ): String {
        checkConnection()
        try {
            // Format privileges
            val formattedPrivileges: String = when {
                privileges.equals(other = "ALL", ignoreCase = true) -> "ALL PRIVILEGES"
                else -> privileges
            }
            // Execute the GRANT statement
            jdbcTemplate!!.execute("GRANT $formattedPrivileges ON $objectType $objectName TO $username")
            return "Granted $formattedPrivileges on $objectType $objectName to user '$username'"
        } catch (e: Exception) {
            return "Error granting privileges: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-revoke"], value = "Revoke privileges from a user", group = "PostgreSQL Operations")
    fun revokePrivileges(
        @ShellOption(help = "Privileges (ALL, SELECT, INSERT, UPDATE, DELETE, etc. or comma-separated list)") privileges: String,
        @ShellOption(help = "Object type (TABLE, SEQUENCE, DATABASE, etc.)") objectType: String,
        @ShellOption(help = "Object name (table name, sequence name, etc.)") objectName: String,
        @ShellOption(help = "Username to revoke privileges from") username: String
    ): String {
        checkConnection()
        try {
            // Format privileges
            val formattedPrivileges: String = when {
                privileges.equals(other = "ALL", ignoreCase = true) -> "ALL PRIVILEGES"
                else -> privileges
            }
            // Execute the REVOKE statement
            jdbcTemplate!!.execute("REVOKE $formattedPrivileges ON $objectType $objectName FROM $username")
            return "Revoked $formattedPrivileges on $objectType $objectName from user '$username'"
        } catch (e: Exception) {
            return "Error revoking privileges: ${e.localizedMessage}"
        }
    }

    @ShellMethod(key = ["db-help"], value = "Show available PostgreSQL commands", group = "PostgreSQL Operations")
    fun help(): String {
        return """
            PostgreSQL Shell Commands:

            Connection:
              db-connect <host> <port> <database> <username> <password> - Connect to PostgreSQL database
              db-status - Show current database connection status
              db-info - Show database server information

            Schema Operations:
              db-list-tables - List all tables in the database
              db-describe-table <tableName> - Show column properties for a table
              db-create-table <tableName> <columnDefinitions> - Create a new table
              db-show-indexes <tableName> - Show indexes for a table
              db-export-schema <filePath> [includeData] [tables] - Export database schema to a file
              db-copy-table <sourceTable> <destinationTable> [includeData] [includeIndexes] [includeConstraints] - Create a copy of a table

            Data Operations:
              db-query <query> [maxRows] - Execute a SELECT query
              db-execute <sql> - Execute a non-query SQL statement
              db-insert <tableName> <columns> <values> - Insert a record into a table
              db-update <tableName> <setClause> <whereClause> - Update records in a table
              db-delete <tableName> <whereClause> - Delete records from a table
              db-truncate-table <tableName> [restartIdentity] [cascade] - Remove all data from a table
              db-export-query <query> <filePath> [format] - Export query results to a file (csv, json)
              db-activity - Show currently running queries
              db-table-stats <tableName> - Show table statistics

            Backup and Restore:
              db-backup <filePath> [format] [compressionLevel] [tables] - Backup database to a file
              db-restore <filePath> [clean] [singleTransaction] - Restore database from a backup file

            User Management:
              db-list-users - List database users
              db-create-user <username> <password> [canCreateDb] [isSuperuser] [validUntil] - Create a new database user
              db-alter-user <username> [newPassword] [canCreateDb] [isSuperuser] [validUntil] - Modify a database user
              db-drop-user <username> [ifExists] - Delete a database user
              db-grant <privileges> <objectType> <objectName> <username> - Grant privileges to a user
              db-revoke <privileges> <objectType> <objectName> <username> - Revoke privileges from a user

            Help:
              db-help - Show this help message

            Examples:
              db-connect localhost 5432 mydb postgres password
              db-list-tables
              db-describe-table users
              db-create-table employees "id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100) UNIQUE, hire_date DATE"
              db-query "SELECT * FROM users WHERE id > 10" 50
              db-insert users "username,email" "john_doe,john@example.com"
              db-update users "email=new@example.com" "id=1"
              db-delete users "id=5"
              db-truncate-table users true false
              db-export-query "SELECT * FROM users" "/tmp/users.csv" "csv"
              db-export-schema "/tmp/schema.sql" false "users,products"
              db-copy-table users users_backup true true true
              db-backup "/tmp/mydb_backup.dump" "custom" 5
              db-restore "/tmp/mydb_backup.dump" false true
              db-table-stats users
              db-show-indexes users
              db-list-users
              db-create-user new_user password123 true false "2023-12-31"
              db-grant "SELECT,INSERT,UPDATE" TABLE users new_user
              db-info
              db-activity
        """.trimIndent()
    }

    private fun createDataSource(host: String, port: Int, database: String, username: String, password: String): DataSource {
        val dataSource = DriverManagerDataSource()
        dataSource.setDriverClassName("org.postgresql.Driver")
        dataSource.url = "jdbc:postgresql://$host:$port/$database"
        dataSource.username = username
        dataSource.password = password
        return dataSource
    }

    val dataSourceString: String?
        get() =
            when(jdbcTemplate) {
                null -> null
                else -> "jdbc:postgresql://$currentUsername:$currentPassword@$currentHost:$currentPort/$currentDatabase"
            }

    private fun checkConnection() {
        if (jdbcTemplate == null)
            throw IllegalStateException("Not connected to a database. Use db-connect first.")
    }

    private fun buildInsertStatement(tableName: String, columns: List<String>, values: List<String>): String {
        val quotedValues: List<String> = values.map { it: String ->
            // Simple SQL injection prevention - not comprehensive
            when {
                it.equals(other = "null", ignoreCase = true) -> "NULL"
                else -> "'${it.replace(oldValue = "'", newValue = "''")}'"
            }
        }
        return "INSERT INTO $tableName (${columns.joinToString(separator = ", ")}) VALUES (${quotedValues.joinToString(separator = ", ")})"
    }

    private fun buildUpdateStatement(tableName: String, setClause: String, whereClause: String): String {
        val setItems: List<String> = setClause.split(",").map { it.trim() }
        val sql = StringBuilder("UPDATE $tableName SET ")
        sql.append(setItems.joinToString(separator = ", ") { item: String ->
            val parts: List<String> = item.split("=", limit = 2)
            if (parts.size != 2)
                throw IllegalArgumentException("Invalid set clause format: $item")
            val column: String = parts[0].trim()
            val value: String = parts[1].trim()
            when {
                value.equals(other = "null", ignoreCase = true) ->
                    "$column = NULL"
                else ->
                    "$column = '${value.replace(oldValue = "'", newValue = "''")}'"
            }
        })

        if (whereClause.isNotBlank())
            sql.append(" WHERE $whereClause")
        return sql.toString()
    }
}
