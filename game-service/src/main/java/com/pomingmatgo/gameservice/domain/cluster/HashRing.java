package com.pomingmatgo.gameservice.domain.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

// 가상 노드 기반 consistent hash ring — 배치 힌트일 뿐 권위가 아니다 (방 소유의 권위는 lease + fencing token).
// 멤버 정렬 + 고정 해시(MD5)로 빌드가 결정적 — 같은 멤버십이면 모든 노드가 같은 링을 각자 계산한다 (링 합의 프로토콜 불필요의 근거)
public final class HashRing {

    public static final HashRing EMPTY = new HashRing(new long[0], new String[0], Set.of());

    private final long[] points;   // 정렬된 가상 노드 위치 (signed long 전순서 — 어떤 전순서든 일관되면 링은 성립)
    private final String[] owners; // points[i]의 주인 노드
    private final Set<String> nodes;

    private HashRing(long[] points, String[] owners, Set<String> nodes) {
        this.points = points;
        this.owners = owners;
        this.nodes = nodes;
    }

    public static HashRing build(Collection<String> nodeIds, int virtualNodes) {
        List<String> members = nodeIds.stream().distinct().sorted().toList();
        if (members.isEmpty() || virtualNodes <= 0) {
            return EMPTY;
        }
        NavigableMap<Long, String> ring = new TreeMap<>();
        for (String node : members) {
            for (int i = 0; i < virtualNodes; i++) {
                // 위치 충돌(64bit, 사실상 0)은 사전순 작은 노드가 이긴다 — 입력 순서와 무관한 결정성
                ring.merge(hash64(node + "#" + i), node, (a, b) -> a.compareTo(b) <= 0 ? a : b);
            }
        }
        long[] points = new long[ring.size()];
        String[] owners = new String[ring.size()];
        int idx = 0;
        for (Map.Entry<Long, String> entry : ring.entrySet()) {
            points[idx] = entry.getKey();
            owners[idx++] = entry.getValue();
        }
        return new HashRing(points, owners, Set.copyOf(members));
    }

    /** 배치 힌트 — 시계 방향 첫 가상 노드의 주인. 빈 링이면 null */
    public String route(long roomId) {
        if (points.length == 0) {
            return null;
        }
        int idx = Arrays.binarySearch(points, hash64(Long.toString(roomId)));
        if (idx < 0) {
            idx = -idx - 1;
        }
        return owners[idx == points.length ? 0 : idx];
    }

    public Set<String> nodes() {
        return nodes;
    }

    public boolean isEmpty() {
        return points.length == 0;
    }

    // MD5는 암호 목적이 아니라 결정성+분포 목적 — JVM·플랫폼 무관 동일 값이어야 노드들이 같은 링을 계산한다
    private static long hash64(String key) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 미지원 JVM", e);
        }
    }
}
