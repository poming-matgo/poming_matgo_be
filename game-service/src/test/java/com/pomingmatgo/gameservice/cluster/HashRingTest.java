package com.pomingmatgo.gameservice.cluster;

import com.pomingmatgo.gameservice.domain.cluster.HashRing;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 해시 링 (3-B) — 결정성·consistent hashing 성질 검증 + 가상 노드 개수별 분포 편차 측정(로드맵 산출물).
 * 노드 id는 UUID.nameUUIDFromBytes로 고정 — 측정이 재현 가능한 결정적 수치다.
 */
class HashRingTest {

    private static final int ROOMS = 100_000;
    private static final int VNODES = 128;

    private static List<String> nodes(int count) {
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(UUID.nameUUIDFromBytes(("node-" + i).getBytes(StandardCharsets.UTF_8)).toString());
        }
        return nodes;
    }

    private static Map<String, Integer> countByNode(HashRing ring, int rooms) {
        Map<String, Integer> counts = new HashMap<>();
        for (long roomId = 0; roomId < rooms; roomId++) {
            counts.merge(ring.route(roomId), 1, Integer::sum);
        }
        return counts;
    }

    // 최대 편차% = max|count - mean| / mean — "가장 몰린/빈 노드가 이상적 배분에서 얼마나 벗어나는가"
    private static double maxDeviationPct(Map<String, Integer> counts, int nodeCount, int rooms) {
        double mean = (double) rooms / nodeCount;
        return counts.values().stream()
                .mapToDouble(c -> Math.abs(c - mean) / mean * 100)
                .max().orElse(0);
    }

    private static double cvPct(Map<String, Integer> counts, int nodeCount, int rooms) {
        double mean = (double) rooms / nodeCount;
        double variance = counts.values().stream()
                .mapToDouble(c -> (c - mean) * (c - mean))
                .sum() / nodeCount;
        return Math.sqrt(variance) / mean * 100;
    }

    @Test
    @DisplayName("빌드는 멤버 입력 순서와 무관하게 결정적 — 같은 멤버십이면 모든 노드가 같은 링을 계산한다")
    void buildIsDeterministicRegardlessOfInputOrder() {
        List<String> members = nodes(5);
        List<String> shuffled = new ArrayList<>(members);
        Collections.shuffle(shuffled);

        HashRing ring = HashRing.build(members, VNODES);
        HashRing reordered = HashRing.build(shuffled, VNODES);
        for (long roomId = 0; roomId < 10_000; roomId++) {
            assertEquals(ring.route(roomId), reordered.route(roomId));
        }
    }

    @Test
    @DisplayName("빈 링은 null 라우팅(단일 노드 폴백은 호출자 몫), 노드 1개면 전 방이 그 노드")
    void emptyAndSingleNode() {
        assertTrue(HashRing.build(List.of(), VNODES).isEmpty());
        assertNull(HashRing.build(List.of(), VNODES).route(1L));

        HashRing single = HashRing.build(nodes(1), VNODES);
        for (long roomId = 0; roomId < 1_000; roomId++) {
            assertEquals(nodes(1).get(0), single.route(roomId));
        }
    }

    @Test
    @DisplayName("노드 추가: 이동은 새 노드로 가는 방뿐(기존 노드 간 재셔플 0) + 이동량은 K/(N+1)에 수렴")
    void addNodeStealsOnly() {
        List<String> five = nodes(5);
        List<String> six = nodes(6);
        String added = six.get(5);
        HashRing before = HashRing.build(five, VNODES);
        HashRing after = HashRing.build(six, VNODES);

        int moved = 0;
        for (long roomId = 0; roomId < ROOMS; roomId++) {
            String from = before.route(roomId);
            String to = after.route(roomId);
            if (!from.equals(to)) {
                moved++;
                assertEquals(added, to, "기존 노드 간 재셔플 발생 — consistent hashing 성질 위반");
            }
        }
        double idealPct = 100.0 / 6;
        double movedPct = moved * 100.0 / ROOMS;
        System.out.printf("[3-B] 노드 5→6 추가: 이동 %d/%d (%.2f%%, 이상값 %.2f%%)%n", moved, ROOMS, movedPct, idealPct);
        assertTrue(Math.abs(movedPct - idealPct) < idealPct * 0.25, "이동량이 K/N에서 25% 이상 벗어남: " + movedPct);
    }

    @Test
    @DisplayName("노드 제거: 제거된 노드의 방만 이동하고 나머지는 배치 불변 + 이동량은 K/N에 수렴")
    void removeNodeMovesOnlyItsRooms() {
        List<String> five = nodes(5);
        String removed = five.get(2);
        List<String> four = new ArrayList<>(five);
        four.remove(removed);
        HashRing before = HashRing.build(five, VNODES);
        HashRing after = HashRing.build(four, VNODES);

        int moved = 0;
        for (long roomId = 0; roomId < ROOMS; roomId++) {
            String from = before.route(roomId);
            if (from.equals(removed)) {
                moved++;
                assertNotEquals(removed, after.route(roomId));
            } else {
                assertEquals(from, after.route(roomId), "제거와 무관한 방이 이동 — consistent hashing 성질 위반");
            }
        }
        double movedPct = moved * 100.0 / ROOMS;
        System.out.printf("[3-B] 노드 5→4 제거: 이동 %d/%d (%.2f%%, 이상값 %.2f%%)%n", moved, ROOMS, movedPct, 100.0 / 5);
        assertTrue(Math.abs(movedPct - 20.0) < 5.0);
    }

    @Test
    @DisplayName("측정: 가상 노드 개수별 분포 편차 (5노드 × 10만 방) — 기본값 128의 선택 근거")
    void measureDeviationByVirtualNodeCount() {
        List<String> members = nodes(5);
        Map<Integer, Double> devByVnodes = new HashMap<>();
        System.out.println("[3-B] 가상 노드 개수별 분포 (5노드, 방 100,000, 이상적 20,000/노드)");
        System.out.println("vnodes | maxDev% |   cv% | min..max");
        for (int vnodes : new int[]{1, 4, 16, 64, 128, 256, 512}) {
            Map<String, Integer> counts = countByNode(HashRing.build(members, vnodes), ROOMS);
            double maxDev = maxDeviationPct(counts, 5, ROOMS);
            devByVnodes.put(vnodes, maxDev);
            System.out.printf("%6d | %6.2f%% | %5.2f%% | %d..%d%n",
                    vnodes, maxDev, cvPct(counts, 5, ROOMS),
                    Collections.min(counts.values()), Collections.max(counts.values()));
        }
        assertTrue(devByVnodes.get(128) < devByVnodes.get(1), "가상 노드가 편차를 줄이지 못함");
        assertTrue(devByVnodes.get(128) < 15.0, "기본값 128의 편차가 15% 초과: " + devByVnodes.get(128));
    }

    @Test
    @DisplayName("측정: 노드 수별 분포 편차 (vnodes=128 고정) — 스케일아웃 시 편차 추이")
    void measureDeviationByNodeCount() {
        System.out.println("[3-B] 노드 수별 분포 (vnodes=128, 방 100,000)");
        System.out.println("nodes | maxDev% |   cv%");
        for (int nodeCount : new int[]{2, 3, 5, 10}) {
            Map<String, Integer> counts = countByNode(HashRing.build(nodes(nodeCount), 128), ROOMS);
            System.out.printf("%5d | %6.2f%% | %5.2f%%%n",
                    nodeCount, maxDeviationPct(counts, nodeCount, ROOMS), cvPct(counts, nodeCount, ROOMS));
            assertEquals(nodeCount, counts.size(), "배치받지 못한 노드 존재");
        }
    }
}
