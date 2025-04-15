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
            val file = java.io.File(filePath)
            file.parentFile?.mkdirs() // Create parent directories if they don't exist

            when (format.lowercase()) {
                "csv" -> {
                    java.io.FileWriter(file).use { writer ->
                        // Write headers
                        val headers = (1..columnCount).map { i -> metadata.getColumnName(i) }
                        writer.write(headers.joinToString(","))
                        writer.write("\n")

                        // Write data
                        while (rows.next()) {
                            val rowData = (1..columnCount).map { i -> 
                                val value = rows.getObject(i)?.toString() ?: ""
                                // Escape commas and quotes in CSV
                                "\"${value.replace("\"", "\"\"")}\""
                            }
                            writer.write(rowData.joinToString(","))
                            writer.write("\n")
                        }
                    }
                    return "Query results exported to CSV file: $filePath"
                }
                "json" -> {
                    java.io.FileWriter(file).use { writer ->
                        val headers = (1..columnCount).map { i -> metadata.getColumnName(i) }
                        writer.write("[\n")

                        var firstRow = true
                        while (rows.next()) {
                            if (!firstRow) writer.write(",\n") else firstRow = false

                            writer.write("  {")
                            val rowData = (1..columnCount).map { i -> 
                                val columnName = metadata.getColumnName(i)
                                val value = rows.getObject(i)?.toString() ?: "null"
                                // Format value based on type
                                val formattedValue = when {
                                    value == "null" -> "null"
                                    // Try to parse as number
                                    value.toDoubleOrNull() != null -> value
                                    // Otherwise treat as string
                                    else -> "\"${value.replace("\"", "\\\"")}\""
                                }
                                "\"$columnName\": $formattedValue"
                            }
                            writer.write(rowData.joinToString(", "))
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
                val info = mutableListOf<Pair<String, String>>()
                info.add(Pair("Database Product Name", metaData.databaseProductName))
                info.add(Pair("Database Version", metaData.databaseProductVersion))
                info.add(Pair("Driver Name", metaData.driverName))
                info.add(Pair("Driver Version", metaData.driverVersion))
                info.add(Pair("JDBC Major Version", metaData.jdbcMajorVersion.toString()))
                info.add(Pair("JDBC Minor Version", metaData.jdbcMinorVersion.toString()))
                info.add(Pair("Max Connections", metaData.maxConnections.toString()))
                info.add(Pair("Username", metaData.userName))

                // Get database size
                try {
                    val sizeQuery = "SELECT pg_size_pretty(pg_database_size(current_database())) as db_size"
                    val sizeResult = jdbcTemplate!!.queryForMap(sizeQuery)
                    info.add(Pair("Database Size", sizeResult["db_size"].toString()))
                } catch (e: Exception) {
                    // Ignore if this query fails
                }

                // Create table for display
                val data: Array<Array<String?>> = Array(size = info.size + 1) { arrayOfNulls(size = 2) }
                data[0][0] = "Property"
                data[0][1] = "Value"

                info.forEachIndexed { index: Int, (property, value): Pair<String, String> ->
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
            val query = """
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
            """.trimIndent()

            val rows: SqlRowSet = jdbcTemplate!!.queryForRowSet(query)
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

            Data Operations:
              db-query <query> [maxRows] - Execute a SELECT query
              db-execute <sql> - Execute a non-query SQL statement
              db-insert <tableName> <columns> <values> - Insert a record into a table
              db-update <tableName> <setClause> <whereClause> - Update records in a table
              db-delete <tableName> <whereClause> - Delete records from a table
              db-export-query <query> <filePath> [format] - Export query results to a file (csv, json)
              db-activity - Show currently running queries

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
              db-export-query "SELECT * FROM users" "/tmp/users.csv" "csv"
              db-show-indexes users
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
