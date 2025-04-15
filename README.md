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

### Schema Operations

- `db-list-tables` - List all tables in the database
- `db-describe-table <tableName>` - Show column properties for a table
- `db-create-table <tableName> <columnDefinitions>` - Create a new table

### Data Operations

- `db-query <query> [maxRows]` - Execute a SELECT query
- `db-execute <sql>` - Execute a non-query SQL statement
- `db-insert <tableName> <columns> <values>` - Insert a record into a table
- `db-update <tableName> <setClause> <whereClause>` - Update records in a table
- `db-delete <tableName> <whereClause>` - Delete records from a table

### Help

- `db-help` - Show available PostgreSQL commands

## Shell Features

- Tab completion for commands and options
- Command history (use up/down arrows)
- Help system (use `help` command)
- Dynamic prompt that changes color based on time of day

## Examples

```
shell> db-connect localhost 5432 mydb postgres password
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
```

## Project Structure

- `SpringKolinShellApplication.kt` - Main application class
- `PostgreSqlShellCommands.kt` - PostgreSQL database operations commands
- `ShellConfig.kt` - Custom prompt configuration
- `schema.sql` - Database schema for the users table

## License

This project is licensed under the MIT License - see the LICENSE file for details.
