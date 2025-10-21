package com.pomingmatgo.gameservice.domain.repository;
import com.pomingmatgo.gameservice.domain.GameState;
import com.pomingmatgo.gameservice.global.exception.BusinessException;
import com.pomingmatgo.gameservice.global.exception.ErrorCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
//@RequiredArgsConstructor
public class GameStateRepository {
    @Qualifier("gameStateRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, GameState> redisOps;

    private static final String GAME_STATE_KEY_PREFIX = "gameState:";

    public Mono<GameState> findById(long roomId) {
        String redisKey = GAME_STATE_KEY_PREFIX + roomId;

        return redisOps.opsForValue().get(redisKey);
    }

    public Mono<Long> create(GameState gameState) {
        String redisKey = GAME_STATE_KEY_PREFIX + gameState.getRoomId();

        return redisOps.opsForValue()
                .setIfAbsent(redisKey, gameState)
                .flatMap(wasSet -> {
                    if (Boolean.TRUE.equals(wasSet)) {
                        return Mono.just(gameState.getRoomId());
                    } else {
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM));
                    }
                });
    }

    public Mono<Long> delete(long roomId) {
        String redisKey = GAME_STATE_KEY_PREFIX + roomId;
        return redisOps.hasKey(redisKey)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return redisOps.delete(redisKey);
                    }
                    return Mono.just(roomId);
                });
    }

    public Mono<Long> save(GameState gameState) {
        String redisKey = GAME_STATE_KEY_PREFIX + gameState.getRoomId();

        return saveState(gameState, redisKey)
                .filter(isSaved -> isSaved) // true 값만 통과시킴
                .map(isSaved -> gameState.getRoomId()) // roomId로 변환
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR)));
    }
    private Mono<Boolean> checkKeyExists(String redisKey) {
        return redisOps.hasKey(redisKey);
    }

    public Mono<Boolean> saveState(GameState gameState, String redisKey) {
        return redisOps.opsForValue().set(redisKey, gameState);
    }
}

/*@Repository
public class GameStateRepository {
    @Qualifier("cardRedisTemplate")
    @Autowired
    private ReactiveRedisOperations<String, String> redisOps; // String 기반으로 변경

    private static final String GAME_STATE_PREFIX = "gameState:"; // 개별 필드 저장을 위한 Key Prefix

    public Mono<GameState> findById(long roomId) {
        String key = GAME_STATE_PREFIX + roomId;
        return redisOps.opsForHash()
                .entries(key) // 모든 필드 조회
                .collectMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString()) // Map 변환
                .flatMap(this::mapToGameState); // GameState로 변환
    }

    public Mono<Long> create(GameState gameState) {
        String key = GAME_STATE_PREFIX + gameState.getRoomId();
        return checkKeyExists(key)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return Mono.error(new BusinessException(ErrorCode.ALREADY_EXISTED_ROOM));
                    }
                    return saveState(gameState);
                })
                .flatMap(saved -> saved ? Mono.just(gameState.getRoomId()) : Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR)));
    }

    public Mono<Long> delete(long roomId) {
        String key = GAME_STATE_PREFIX + roomId;
        return redisOps.opsForValue()
                .delete(key)
                .thenReturn(roomId);
    }


    public Mono<Long> save(GameState gameState) {
        return saveState(gameState)
                .flatMap(saved -> saved ? Mono.just(gameState.getRoomId()) : Mono.error(new BusinessException(ErrorCode.SYSTEM_ERROR)));
    }

    private Mono<Boolean> checkKeyExists(String key) {
        return redisOps.opsForHash().hasKey(key, "roomId"); // 특정 필드가 존재하는지 확인
    }

    private Mono<Boolean> saveState(GameState gameState) {
        String key = GAME_STATE_PREFIX + gameState.getRoomId();
        return redisOps.opsForHash()
                .putAll(key, mapFromGameState(gameState)) // 모든 필드를 저장
                .map(result -> true);
    }

    private Mono<GameState> mapToGameState(Map<String, String> map) {
        if (map.isEmpty()) {
            return Mono.empty();
        }
        GameState gameState = new GameState(
                Long.parseLong(map.get("roomId")),
                Long.parseLong(map.get("player1Id")),
                Long.parseLong(map.get("player2Id")),
                Boolean.parseBoolean(map.get("player1Ready")),
                Boolean.parseBoolean(map.get("player2Ready")),
                Integer.parseInt(map.get("leadingPlayer")),
                Integer.parseInt(map.get("round")),
                Integer.parseInt(map.get("currentTurn"))
        );
        return Mono.just(gameState);
    }

    private Map<String, String> mapFromGameState(GameState gameState) {
        Map<String, String> map = new HashMap<>();
        map.put("roomId", String.valueOf(gameState.getRoomId()));
        map.put("player1Id", String.valueOf(gameState.getPlayer1Id()));
        map.put("player2Id", String.valueOf(gameState.getPlayer2Id()));
        map.put("player1Ready", String.valueOf(gameState.isPlayer1Ready()));
        map.put("player2Ready", String.valueOf(gameState.isPlayer2Ready()));
        map.put("leadingPlayer", String.valueOf(gameState.getLeadingPlayer()));
        map.put("round", String.valueOf(gameState.getRound()));
        map.put("currentTurn", String.valueOf(gameState.getCurrentTurn()));
        return map;
    }
}*/
