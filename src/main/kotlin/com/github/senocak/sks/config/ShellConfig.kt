package com.github.senocak.sks.config

import com.github.senocak.sks.commands.PostgreSqlShellCommands
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.shell.jline.PromptProvider
import java.time.LocalTime

@Configuration
class ShellConfig(private val advancedShellCommands: PostgreSqlShellCommands) {

    @Bean
    fun customPromptProvider(): PromptProvider {
        return PromptProvider {
            val now: LocalTime = LocalTime.now()
            val baseStyle: AttributedStyle = when {
                now.hour < 12 -> AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW) // Morning
                now.hour < 18 -> AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)  // Afternoon
                else -> AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE)           // Evening
            }
            val promptText: String = when {
                advancedShellCommands.currentUsername != null -> "shell:${advancedShellCommands.currentUsername}> "
                else -> "shell:guest> "
            }
            AttributedString(promptText, baseStyle)
        }
    }
}
