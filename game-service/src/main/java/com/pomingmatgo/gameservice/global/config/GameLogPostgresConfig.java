package com.pomingmatgo.gameservice.global.config;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.r2dbc.core.DatabaseClient;

// R2DBC 강제 — JDBC blocking은 BlockHound가 런타임에 잡는다
@Configuration
@ConditionalOnProperty(name = "game.log.store", havingValue = "postgres")
@EnableConfigurationProperties(GameLogPostgresProperties.class)
public class GameLogPostgresConfig {

    @Bean(destroyMethod = "dispose")
    public ConnectionPool gameLogConnectionPool(GameLogPostgresProperties props) {
        ConnectionFactoryOptions.Builder options = ConnectionFactoryOptions.parse(props.url()).mutate();
        if (props.username() != null) {
            options.option(ConnectionFactoryOptions.USER, props.username());
        }
        if (props.password() != null) {
            options.option(ConnectionFactoryOptions.PASSWORD, props.password());
        }
        return new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(options.build()))
                .initialSize(props.poolInitialSize())
                .maxSize(props.poolMaxSize())
                .build());
    }

    @Bean
    public DatabaseClient gameLogDatabaseClient(ConnectionPool gameLogConnectionPool) {
        return DatabaseClient.create(gameLogConnectionPool);
    }

    @Bean
    @ConditionalOnProperty(name = "game.log.postgres.init-schema", havingValue = "true", matchIfMissing = true)
    public ConnectionFactoryInitializer gameLogSchemaInitializer(ConnectionPool gameLogConnectionPool) {
        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(gameLogConnectionPool);
        initializer.setDatabasePopulator(new ResourceDatabasePopulator(new ClassPathResource("db/game-log-schema.sql")));
        return initializer;
    }
}
