package com.pomingmatgo.gameservice.cluster;

import com.pomingmatgo.gameservice.domain.cluster.NodeIdentity;
import com.pomingmatgo.gameservice.domain.cluster.NodeRegistry;
import com.pomingmatgo.gameservice.domain.repository.NodeRegistryRepository;
import com.pomingmatgo.gameservice.domain.repository.PostgresNodeRegistryRepository;
import com.pomingmatgo.gameservice.global.config.NodeRegistryProperties;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.r2dbc.core.DatabaseClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 노드 레지스트리 membership 통합 테스트 — 실물 Postgres 대상, 미기동이면 전체 skip.
 * 기동 예: docker run -d --name gostop-pg-test -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16-alpine
 */
class PostgresNodeRegistryTest {

    private static final String URL = System.getenv().getOrDefault(
            "GAME_LOG_PG_URL", "r2dbc:postgresql://postgres:postgres@localhost:15432/postgres");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final NodeRegistryProperties PROPS =
            new NodeRegistryProperties(Duration.ofSeconds(30), Duration.ofSeconds(5), 128, "");

    private static ConnectionPool pool;
    private static DatabaseClient db;
    private static PostgresNodeRegistryRepository repository;

    @BeforeAll
    static void setUp() {
        assumeTrue(reachable(), "Postgres 미기동 — skip: " + URL);
        pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(URL))
                .initialSize(1).maxSize(4).build());
        db = DatabaseClient.create(pool);
        new ResourceDatabasePopulator(new ClassPathResource("db/game-log-schema.sql")).populate(pool).block(TIMEOUT);
        repository = new PostgresNodeRegistryRepository(db);
    }

    @AfterAll
    static void tearDown() {
        if (pool != null) {
            pool.dispose();
        }
    }

    private static boolean reachable() {
        URI uri = URI.create(URL.substring("r2dbc:".length()));
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? 5432 : uri.getPort()), 1500);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static String newNodeId() {
        return "node-" + UUID.randomUUID();
    }

    private List<String> activeNodes(Duration ttl) {
        return repository.findActiveNodes(ttl)
                .map(NodeRegistryRepository.ActiveNode::instanceId)
                .collectList().block(TIMEOUT);
    }

    private String addressOf(String nodeId, Duration ttl) {
        return repository.findActiveNodes(ttl)
                .filter(node -> node.instanceId().equals(nodeId))
                .blockFirst(TIMEOUT)
                .address();
    }

    // heartbeat 정체를 기다리지 않고 강제 — 죽은 노드 상황 재현
    private void forceStale(String nodeId, Duration age) {
        db.sql("UPDATE node_registry SET last_heartbeat = now() - make_interval(secs => :age) WHERE instance_id = :id")
                .bind("age", age.toMillis() / 1000.0)
                .bind("id", nodeId)
                .then().block(TIMEOUT);
    }

    private String stateOf(String nodeId) {
        return db.sql("SELECT state FROM node_registry WHERE instance_id = :id")
                .bind("id", nodeId)
                .map(row -> row.get("state", String.class))
                .one().block(TIMEOUT);
    }

    @Test
    @DisplayName("register: 멤버십에 편입되고, active 상태의 재등록은 멱등하다(행 1개 유지)")
    void registerJoinsMembership() {
        String node = newNodeId();
        repository.register(node, null).block(TIMEOUT);
        repository.register(node, null).block(TIMEOUT);

        assertTrue(activeNodes(PROPS.ttl()).contains(node));
        Long rows = db.sql("SELECT count(*) AS cnt FROM node_registry WHERE instance_id = :id")
                .bind("id", node).map(row -> row.get("cnt", Long.class)).one().block(TIMEOUT);
        assertEquals(1L, rows);
    }

    @Test
    @DisplayName("멤버십 판정은 DB 시계 기준 ttl — heartbeat가 정체된 노드는 leave 없이도 빠지고, heartbeat로 복귀한다")
    void staleNodeDropsOutAndHeartbeatRevives() {
        String node = newNodeId();
        repository.register(node, null).block(TIMEOUT);
        forceStale(node, Duration.ofSeconds(10));

        // 같은 행이 ttl에 따라 갈린다 — 판정이 등록 여부가 아니라 시계라는 증거
        assertFalse(activeNodes(Duration.ofSeconds(5)).contains(node));
        assertTrue(activeNodes(Duration.ofSeconds(30)).contains(node));

        assertEquals(1L, repository.heartbeat(node).block(TIMEOUT));
        assertTrue(activeNodes(Duration.ofSeconds(5)).contains(node));
    }

    @Test
    @DisplayName("leave: heartbeat가 신선해도 멤버십에서 빠지고, 이후 heartbeat(0행)·register 모두 되살리지 못한다(최종 상태)")
    void leaveIsFinal() {
        String node = newNodeId();
        repository.register(node, null).block(TIMEOUT);
        repository.leave(node).block(TIMEOUT);

        assertFalse(activeNodes(PROPS.ttl()).contains(node));
        // 종료 경합 가드 — leave 직후 늦게 도착한 heartbeat/register(자기 치유)가 유령 멤버를 만들지 못한다
        assertEquals(0L, repository.heartbeat(node).block(TIMEOUT));
        repository.register(node, null).block(TIMEOUT);
        assertEquals("left", stateOf(node));
        assertFalse(activeNodes(PROPS.ttl()).contains(node));
    }

    @Test
    @DisplayName("NodeRegistry.beat: 행이 없으면 등록(최초 등록 = 자기 치유의 특수형), 있으면 heartbeat 갱신")
    void beatRegistersThenRefreshes() {
        NodeRegistry registry = new NodeRegistry(repository, PROPS, new NodeIdentity());

        registry.beat().block(TIMEOUT);
        assertTrue(registryNodeIds(registry).contains(registry.instanceId()));

        forceStale(registry.instanceId(), Duration.ofMinutes(10));
        assertFalse(registryNodeIds(registry).contains(registry.instanceId()));
        registry.beat().block(TIMEOUT);
        assertTrue(registryNodeIds(registry).contains(registry.instanceId()));
        assertEquals("active", stateOf(registry.instanceId()));
    }

    private List<String> registryNodeIds(NodeRegistry registry) {
        return registry.activeNodes()
                .map(NodeRegistryRepository.ActiveNode::instanceId)
                .collectList().block(TIMEOUT);
    }

    @Test
    @DisplayName("advertise_address: 등록 시 광고한 주소가 멤버 목록에 실려 오고, 재등록(자기 치유)이 갱신하며, 미광고면 null")
    void advertiseAddressRoundTrip() {
        String node = newNodeId();
        repository.register(node, "host-a:8084").block(TIMEOUT);
        assertEquals("host-a:8084", addressOf(node, PROPS.ttl()));

        // 같은 프로세스의 재등록 = 자기 치유 — 주소 변경(설정 변경 후 재기동은 새 행이지만, upsert 계약 자체를 검증)
        repository.register(node, "host-a:9000").block(TIMEOUT);
        assertEquals("host-a:9000", addressOf(node, PROPS.ttl()));

        String silent = newNodeId();
        repository.register(silent, null).block(TIMEOUT);
        assertTrue(activeNodes(PROPS.ttl()).contains(silent));
        assertNull(addressOf(silent, PROPS.ttl()));
    }
}
