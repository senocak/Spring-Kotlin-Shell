package com.github.senocak.sks

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration
import org.springframework.boot.runApplication

@SpringBootApplication(exclude = [HibernateJpaAutoConfiguration::class, DataSourceAutoConfiguration::class])
class SpringKolinShellApplication

fun main(args: Array<String>) {
    runApplication<SpringKolinShellApplication>(*args)
}
