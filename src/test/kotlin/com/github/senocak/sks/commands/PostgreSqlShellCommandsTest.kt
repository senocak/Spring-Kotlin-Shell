package com.github.senocak.sks.commands

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.Mockito.lenient
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData
import org.springframework.test.util.ReflectionTestUtils
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(MockitoExtension::class)
class PostgreSqlShellCommandsTest {

    @InjectMocks
    private lateinit var postgreSqlShellCommands: PostgreSqlShellCommands

    @Mock
    private lateinit var jdbcTemplate: JdbcTemplate

    @Mock
    private lateinit var dataSource: DataSource

    @Mock
    private lateinit var connection: Connection

    @Mock
    private lateinit var databaseMetaData: DatabaseMetaData

    @Mock
    private lateinit var resultSet: ResultSet

    @Mock
    private lateinit var sqlRowSet: SqlRowSet

    @Mock
    private lateinit var sqlRowSetMetaData: SqlRowSetMetaData

    @BeforeEach
    fun setup() {
        ReflectionTestUtils.setField(postgreSqlShellCommands, "jdbcTemplate", jdbcTemplate)
        // Use lenient() to avoid "unnecessary stubbing" errors
        lenient().`when`(jdbcTemplate.dataSource).thenReturn(dataSource)
        lenient().`when`(dataSource.connection).thenReturn(connection)
        lenient().`when`(connection.metaData).thenReturn(databaseMetaData)
    }

    @Test
    fun `test connect success`() {
        // Arrange
        val host = "localhost"
        val port = 5432
        val database = "testdb"
        val username = "testuser"
        val password = "testpass"

        // Use mockito-kotlin's any() for Kotlin compatibility
        `when`(jdbcTemplate.queryForObject(anyString(), eq(Int::class.java))).thenReturn(1)

        // Act
        val result = postgreSqlShellCommands.connect(host, port, database, username, password)

        // Assert
        assertTrue(result.contains("Successfully connected"))
        assertEquals(username, postgreSqlShellCommands.currentUsername)
    }

    @Test
    fun `test listTables success`() {
        // Arrange
        `when`(databaseMetaData.getTables(isNull(), eq("public"), eq("%"), any())).thenReturn(resultSet)
        `when`(resultSet.next()).thenReturn(true, true, false) // Two tables
        `when`(resultSet.getString("TABLE_NAME")).thenReturn("users", "products")

        // Act
        val result = postgreSqlShellCommands.listTables()

        // Assert
        assertTrue(result.contains("Table Name"))
        assertTrue(result.contains("users"))
        assertTrue(result.contains("products"))
    }

    @Test
    fun `test describeTable success`() {
        // Arrange
        val tableName = "users"

        // Mock tables result set
        val tablesResultSet = mock(ResultSet::class.java)
        `when`(databaseMetaData.getTables(isNull(), eq("public"), eq(tableName), any())).thenReturn(tablesResultSet)
        `when`(tablesResultSet.next()).thenReturn(true)

        // Mock columns result set
        val columnsResultSet = mock(ResultSet::class.java)
        `when`(databaseMetaData.getColumns(isNull(), eq("public"), eq(tableName), eq("%"))).thenReturn(columnsResultSet)
        `when`(columnsResultSet.next()).thenReturn(true, true, false) // Two columns
        `when`(columnsResultSet.getString("COLUMN_NAME")).thenReturn("id", "username")
        `when`(columnsResultSet.getString("TYPE_NAME")).thenReturn("SERIAL", "VARCHAR")
        `when`(columnsResultSet.getInt("COLUMN_SIZE")).thenReturn(10, 255)
        `when`(columnsResultSet.getInt("NULLABLE")).thenReturn(0, 0) // Not nullable

        // Mock primary keys result set
        val primaryKeysResultSet = mock(ResultSet::class.java)
        `when`(databaseMetaData.getPrimaryKeys(isNull(), eq("public"), eq(tableName))).thenReturn(primaryKeysResultSet)
        `when`(primaryKeysResultSet.next()).thenReturn(true, false) // One primary key
        `when`(primaryKeysResultSet.getString("COLUMN_NAME")).thenReturn("id")

        // Act
        val result = postgreSqlShellCommands.describeTable(tableName)

        // Assert
        assertTrue(result.contains("Column Name"))
        assertTrue(result.contains("Data Type"))
        assertTrue(result.contains("id"))
        assertTrue(result.contains("SERIAL"))
        assertTrue(result.contains("username"))
        assertTrue(result.contains("VARCHAR"))
        assertTrue(result.contains("YES")) // Primary key for id
    }

    @Test
    fun `test executeQuery success`() {
        // Arrange
        val query = "SELECT * FROM users"
        `when`(jdbcTemplate.queryForRowSet(query)).thenReturn(sqlRowSet)
        `when`(sqlRowSet.next()).thenReturn(true, true, false) // Two rows
        `when`(sqlRowSet.metaData).thenReturn(sqlRowSetMetaData)
        `when`(sqlRowSetMetaData.columnCount).thenReturn(2)
        `when`(sqlRowSetMetaData.getColumnName(1)).thenReturn("id")
        `when`(sqlRowSetMetaData.getColumnName(2)).thenReturn("username")
        `when`(sqlRowSet.getObject(1)).thenReturn(1, 2)
        `when`(sqlRowSet.getObject(2)).thenReturn("user1", "user2")

        // Act
        val result = postgreSqlShellCommands.executeQuery(query, 100)

        // Assert
        assertTrue(result.contains("id"))
        assertTrue(result.contains("username"))
        assertTrue(result.contains("1"))
        assertTrue(result.contains("user1"))
        assertTrue(result.contains("2"))
        assertTrue(result.contains("user2"))
    }

    @Test
    fun `test executeStatement success`() {
        // Arrange
        val sql = "UPDATE users SET username = 'newname' WHERE id = 1"
        `when`(jdbcTemplate.update(sql)).thenReturn(1)

        // Act
        val result = postgreSqlShellCommands.executeStatement(sql)

        // Assert
        assertTrue(result.contains("Statement executed successfully"))
        assertTrue(result.contains("Rows affected: 1"))
    }

    @Test
    fun `test insertRecord success`() {
        // Arrange
        val tableName = "users"
        val columns = "username,email"
        val values = "john_doe,john@example.com"
        val expectedSql = "INSERT INTO users (username, email) VALUES ('john_doe', 'john@example.com')"

        `when`(jdbcTemplate.update(argThat<String> { sql -> 
            sql.startsWith("INSERT INTO users") && 
            sql.contains("username") && 
            sql.contains("email") && 
            sql.contains("'john_doe'") && 
            sql.contains("'john@example.com'")
        })).thenReturn(1)

        // Act
        val result = postgreSqlShellCommands.insertRecord(tableName, columns, values)

        // Assert
        assertTrue(result.contains("Record inserted successfully"))
        assertTrue(result.contains("Rows affected: 1"))
    }

    @Test
    fun `test updateRecords success`() {
        // Arrange
        val tableName = "users"
        val setClause = "email=new@example.com"
        val whereClause = "id=1"

        `when`(jdbcTemplate.update(argThat<String> { sql -> 
            sql.startsWith("UPDATE users SET") && 
            sql.contains("email = 'new@example.com'") && 
            sql.contains("WHERE id=1")
        })).thenReturn(1)

        // Act
        val result = postgreSqlShellCommands.updateRecords(tableName, setClause, whereClause)

        // Assert
        assertTrue(result.contains("Update executed successfully"))
        assertTrue(result.contains("Rows affected: 1"))
    }

    @Test
    fun `test deleteRecords success`() {
        // Arrange
        val tableName = "users"
        val whereClause = "id=5"
        val expectedSql = "DELETE FROM users WHERE id=5"

        `when`(jdbcTemplate.update(expectedSql)).thenReturn(1)

        // Act
        val result = postgreSqlShellCommands.deleteRecords(tableName, whereClause)

        // Assert
        assertTrue(result.contains("Delete executed successfully"))
        assertTrue(result.contains("Rows affected: 1"))
    }

    @Test
    fun `test createTable success`() {
        // Arrange
        val tableName = "employees"
        val columnDefinitions = "id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL"

        // Mock tables result set to indicate table doesn't exist
        val tablesResultSet = mock(ResultSet::class.java)
        `when`(databaseMetaData.getTables(isNull(), eq("public"), eq(tableName), any())).thenReturn(tablesResultSet)
        `when`(tablesResultSet.next()).thenReturn(false)

        // Act
        val result = postgreSqlShellCommands.createTable(tableName, columnDefinitions)

        // Assert
        assertTrue(result.contains("Table 'employees' created successfully"))
        verify(jdbcTemplate).execute("CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL)")
    }

    @Test
    fun `test connectionStatus when connected`() {
        // Arrange
        ReflectionTestUtils.setField(postgreSqlShellCommands, "currentHost", "localhost")
        ReflectionTestUtils.setField(postgreSqlShellCommands, "currentPort", 5432)
        ReflectionTestUtils.setField(postgreSqlShellCommands, "currentDatabase", "testdb")
        ReflectionTestUtils.setField(postgreSqlShellCommands, "currentUsername", "testuser")

        // Act
        val result = postgreSqlShellCommands.connectionStatus()

        // Assert
        assertTrue(result.contains("Connected to PostgreSQL database"))
        assertTrue(result.contains("localhost:5432/testdb"))
        assertTrue(result.contains("testuser"))
    }

    @Test
    fun `test connectionStatus when not connected`() {
        // Arrange
        ReflectionTestUtils.setField(postgreSqlShellCommands, "currentHost", null)

        // Act
        val result = postgreSqlShellCommands.connectionStatus()

        // Assert
        assertTrue(result.contains("Not connected to any database"))
    }

    @Test
    fun `test help returns command list`() {
        // Act
        val result = postgreSqlShellCommands.help()

        // Assert
        assertTrue(result.contains("PostgreSQL Shell Commands"))
        assertTrue(result.contains("db-connect"))
        assertTrue(result.contains("db-list-tables"))
        assertTrue(result.contains("db-query"))
    }
}
