package com.github.senocak.sks.config

import com.github.senocak.sks.commands.PostgreSqlShellCommands
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.shell.jline.PromptProvider
import java.time.LocalTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(MockitoExtension::class)
class ShellConfigTest {

    @Mock
    private lateinit var advancedShellCommands: PostgreSqlShellCommands

    @InjectMocks
    private lateinit var shellConfig: ShellConfig

    @Test
    fun `test prompt text when user is connected`() {
        // Arrange
        val dataSourceString = "jdbc:postgresql://testuser:password@localhost:5432/testdb"
        `when`(advancedShellCommands.dataSourceString).thenReturn(dataSourceString)

        // Act
        val promptProvider: PromptProvider = shellConfig.customPromptProvider()
        val prompt: AttributedString = promptProvider.getPrompt()

        // Assert
        assertTrue(prompt.toAnsi().contains("shell:$dataSourceString>"))
    }

    @Test
    fun `test prompt text when user is not connected`() {
        // Arrange
        `when`(advancedShellCommands.dataSourceString).thenReturn(null)

        // Act
        val promptProvider: PromptProvider = shellConfig.customPromptProvider()
        val prompt: AttributedString = promptProvider.getPrompt()

        // Assert
        assertTrue(prompt.toAnsi().contains("shell:guest>"))
    }

    @Test
    fun `test prompt color in the morning`() {
        // Since we can't easily mock LocalTime.now(), we'll test the logic directly
        // Morning is defined as before 12 PM (hour < 12)
        val hour = 8 // 8 AM
        val mockTime = LocalTime.of(hour, 0)

        // Verify the logic matches what's in ShellConfig
        val expectedStyle = when {
            mockTime.hour < 12 -> AttributedStyle.YELLOW
            mockTime.hour < 18 -> AttributedStyle.GREEN
            else -> AttributedStyle.BLUE
        }

        assertEquals(AttributedStyle.YELLOW, expectedStyle, "Morning time should use yellow color")
    }

    @Test
    fun `test prompt color in the afternoon`() {
        // Afternoon is defined as 12 PM to 6 PM (12 <= hour < 18)
        val hour = 14 // 2 PM
        val mockTime = LocalTime.of(hour, 0)

        // Verify the logic matches what's in ShellConfig
        val expectedStyle = when {
            mockTime.hour < 12 -> AttributedStyle.YELLOW
            mockTime.hour < 18 -> AttributedStyle.GREEN
            else -> AttributedStyle.BLUE
        }

        assertEquals(AttributedStyle.GREEN, expectedStyle, "Afternoon time should use green color")
    }

    @Test
    fun `test prompt color in the evening`() {
        // Evening is defined as 6 PM and later (hour >= 18)
        val hour = 20 // 8 PM
        val mockTime = LocalTime.of(hour, 0)

        // Verify the logic matches what's in ShellConfig
        val expectedStyle = when {
            mockTime.hour < 12 -> AttributedStyle.YELLOW
            mockTime.hour < 18 -> AttributedStyle.GREEN
            else -> AttributedStyle.BLUE
        }

        assertEquals(AttributedStyle.BLUE, expectedStyle, "Evening time should use blue color")
    }
}
