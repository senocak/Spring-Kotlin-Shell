# Spring Kotlin Shell Application

A command-line shell application built with Spring Shell and Kotlin that provides a powerful interface for PostgreSQL database operations.

## Features

- Interactive command-line interface with PostgreSQL database integration
- Comprehensive PostgreSQL operations (connect, query, table management, data manipulation)
- Dynamic colored prompt based on time of day
- Tab completion for commands and options
- Command history and help system

## Getting Started

### Prerequisites

- Java 21 or higher
- Gradle
- PostgreSQL database

### Running the Application

```bash
./gradlew bootRun
```

Or build and run the JAR:

```bash
./gradlew build
java -jar build/libs/Spring-Kolin-Shell-0.0.1.jar
```

## PostgreSQL Shell Commands

The application provides a comprehensive set of commands for interacting with PostgreSQL databases:

### Connection

- `db-connect <host> <port> <database> <username> <password>` - Connect to PostgreSQL database
- `db-status` - Show current database connection status
- `db-info` - Show database server information

### Schema Operations

- `db-list-tables` - List all tables in the database
- `db-describe-table <tableName>` - Show column properties for a table
- `db-create-table <tableName> <columnDefinitions>` - Create a new table
- `db-show-indexes <tableName>` - Show indexes for a table
- `db-export-schema <filePath> [includeData] [tables]` - Export database schema to a file
- `db-copy-table <sourceTable> <destinationTable> [includeData] [includeIndexes] [includeConstraints]` - Create a copy of a table

### Data Operations

- `db-query <query> [maxRows]` - Execute a SELECT query
- `db-execute <sql>` - Execute a non-query SQL statement
- `db-insert <tableName> <columns> <values>` - Insert a record into a table
- `db-update <tableName> <setClause> <whereClause>` - Update records in a table
- `db-delete <tableName> <whereClause>` - Delete records from a table
- `db-truncate-table <tableName> [restartIdentity] [cascade]` - Remove all data from a table
- `db-export-query <query> <filePath> [format]` - Export query results to a file (csv, json)
- `db-activity` - Show currently running queries
- `db-table-stats <tableName>` - Show table statistics

### Backup and Restore

- `db-backup <filePath> [format] [compressionLevel] [tables]` - Backup database to a file
- `db-restore <filePath> [clean] [singleTransaction]` - Restore database from a backup file

### User Management

- `db-list-users` - List database users
- `db-create-user <username> <password> [canCreateDb] [isSuperuser] [validUntil]` - Create a new database user
- `db-alter-user <username> [newPassword] [canCreateDb] [isSuperuser] [validUntil]` - Modify a database user
- `db-drop-user <username> [ifExists]` - Delete a database user
- `db-grant <privileges> <objectType> <objectName> <username>` - Grant privileges to a user
- `db-revoke <privileges> <objectType> <objectName> <username>` - Revoke privileges from a user

### Help

- `db-help` - Show available PostgreSQL commands

## Shell Features

- Tab completion for commands and options
- Command history (use up/down arrows)
- Help system (use `help` command)
- Dynamic prompt that changes color based on time of day

## Examples

```
shell> db-connect localhost 54321 shelldb postgres senocak
Successfully connected to PostgreSQL database at localhost:5432/mydb

shell> db-list-tables
┌────────────┐
│Table Name  │
├────────────┤
│users       │
│products    │
└────────────┘

shell> db-describe-table users
┌────────────┬───────────┬──────┬──────────┬─────────────┐
│Column Name │Data Type  │Size  │Nullable  │Primary Key  │
├────────────┼───────────┼──────┼──────────┼─────────────┤
│id          │SERIAL     │10    │NO        │YES          │
│username    │VARCHAR    │255   │NO        │NO           │
│email       │VARCHAR    │255   │NO        │NO           │
│created_at  │TIMESTAMP  │29    │NO        │NO           │
└────────────┴───────────┴──────┴──────────┴─────────────┘

shell> db-create-table employees "id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100) UNIQUE, hire_date DATE"
Table 'employees' created successfully.

shell> db-query "SELECT * FROM users WHERE id > 10" 50
┌────┬──────────┬─────────────────┬─────────────────────┐
│id  │username  │email            │created_at           │
├────┼──────────┼─────────────────┼─────────────────────┤
│11  │john      │john@example.com │2023-06-15 10:30:45  │
│12  │jane      │jane@example.com │2023-06-16 14:22:33  │
└────┴──────────┴─────────────────┴─────────────────────┘

shell> db-insert users "username,email" "john_doe,john@example.com"
Record inserted successfully. Rows affected: 1

shell> db-update users "email=new@example.com" "id=1"
Update executed successfully. Rows affected: 1

shell> db-delete users "id=5"
Delete executed successfully. Rows affected: 1

shell> db-truncate-table users true false
Table 'users' truncated successfully. Identity columns reset.

shell> db-export-query "SELECT * FROM users" "/tmp/users.csv" "csv"
Query results exported to CSV file: /tmp/users.csv

shell> db-show-indexes users
┌─────────────┬─────────────┬────────┬───────┬───────┐
│Index Name   │Column Name  │Unique  │Type   │Order  │
├─────────────┼─────────────┼────────┼───────┼───────┤
│users_pkey   │id           │true    │OTHER  │A      │
│idx_username │username     │false   │OTHER  │A      │
└─────────────┴─────────────┴────────┴───────┴───────┘

shell> db-table-stats users
┌─────────────┬─────────┐
│Statistic    │Value    │
├─────────────┼─────────┤
│table_name   │users    │
│row_estimate │1000     │
│total_size   │1 MB     │
│table_size   │800 KB   │
│index_size   │200 KB   │
│live_rows    │990      │
│dead_rows    │10       │
└─────────────┴─────────┘

shell> db-copy-table users users_backup true true true
Table 'users' copied to 'users_backup' with structure and indexes and constraints. 990 rows copied.

shell> db-export-schema "/tmp/schema.sql" false "users,products"
Database schema exported successfully to: /tmp/schema.sql (Tables: users,products)

shell> db-backup "/tmp/mydb_backup.dump" "custom" 5
Database backup created successfully at: /tmp/mydb_backup.dump using custom format with compression level 5.

shell> db-restore "/tmp/mydb_backup.dump" false true
Database restored successfully from: /tmp/mydb_backup.dump

shell> db-list-users
┌──────────┬──────────────┬─────────────┬─────────────┐
│username  │can_create_db │is_superuser │valid_until  │
├──────────┼──────────────┼─────────────┼─────────────┤
│postgres  │true          │true         │NULL         │
│testuser  │true          │false        │2023-12-31   │
└──────────┴──────────────┴─────────────┴─────────────┘

shell> db-create-user new_user password123 true false "2023-12-31"
User 'new_user' created successfully with CREATEDB NOSUPERUSER and valid until 2023-12-31

shell> db-grant "SELECT,INSERT,UPDATE" TABLE users new_user
Granted SELECT,INSERT,UPDATE on TABLE users to user 'new_user'

shell> db-info
┌─────────────────────┬─────────────────────────┐
│Property             │Value                    │
├─────────────────────┼─────────────────────────┤
│Database Product Name│PostgreSQL               │
│Database Version     │14.5                     │
│Driver Name          │PostgreSQL JDBC Driver   │
│Driver Version       │42.5.0                   │
│Max Connections      │100                      │
│Database Size        │24 MB                    │
└─────────────────────┴─────────────────────────┘

shell> db-activity
┌──────┬──────────┬─────────────────────────────────────────┐
│pid   │username  │query                                    │
├──────┼──────────┼─────────────────────────────────────────┤
│12345 │postgres  │SELECT * FROM users WHERE active = true  │
└──────┴──────────┴─────────────────────────────────────────┘
```

## Project Structure

- `SpringKolinShellApplication.kt` - Main application class
- `PostgreSqlShellCommands.kt` - PostgreSQL database operations commands
- `ShellConfig.kt` - Custom prompt configuration
- `schema.sql` - Database schema for the users table

## License

This project is licensed under the MIT License - see the LICENSE file for details.
