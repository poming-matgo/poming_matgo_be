#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/in.h>
#include <linux/types.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

#define NANO_TO_SEC 1000000000ULL

struct policy_config {
    __u64 ip_rate;
    __u64 ip_capacity;
    __u64 svc_rate;
    __u64 svc_capacity;
};

struct service_key {
    __u32 daddr;
    __u16 dport;
    __u16 _pad;          // 4바이트 정렬을 위한 패딩
};

struct token_bucket {
    struct bpf_spin_lock lock;
    __u32 _pad;          // 8바이트 정렬을 맞추기 위한 패딩
    __u64 last_time;     // 마지막 토큰 갱신 시간 (ns)
    __u64 tokens;        // 현재 보유 토큰 수
};

// 1. 설정값 저장
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct policy_config);
} config_map SEC(".maps");

// 2. Source IP 기준 Limiter (LRU Hash Map)
// 메모리가 부족하면 가장 오래된 IP 정보를 삭제함
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 200000); // 약 20만 개의 동시 접속 IP 추적
    __type(key, __u32);          // Source IP
    __type(value, struct token_bucket);
} ip_limiter_map SEC(".maps");

// 3. Service(Destination) 기준 Limiter (Hash Map)
// 내 서버의 포트 기준이므로 항목 수가 적음 (LRU 불필요)
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);     // 최대 64개 포트 관리
    __type(key, struct service_key);
    __type(value, struct token_bucket);
} svc_limiter_map SEC(".maps");


// 토큰 버킷 알고리즘
static __always_inline int apply_token_bucket(struct token_bucket *bucket,
                                              __u64 rate, __u64 capacity, __u64 now) {
    if (rate == 0) return 1; // 제한 없음

    int verdict = 0;

    bpf_spin_lock(&bucket->lock);

    // 1. 첫 패킷인 경우 초기화
    if (bucket->last_time == 0) {
        bucket->last_time = now;
        bucket->tokens = capacity; // 처음엔 꽉 채워줌
        // 토큰 하나 사용
        if (bucket->tokens >= 1) {
            bucket->tokens -= 1;
            verdict = 1;
        }
        bpf_spin_unlock(&bucket->lock);
        return verdict;
    }

    __u64 delta = now - bucket->last_time;

    // 2. 시간 역전 방어
    if ((__s64)delta < 0) {
        bucket->last_time = now;
        delta = 0;
    }

    // 3. 너무 오랜만에 온 경우
    if (delta > NANO_TO_SEC * 10) {
        bucket->tokens = capacity;
        bucket->last_time = now;
    } else {
        // 4. 토큰 리필 계산
        // (delta * rate) 연산 시 u64 오버플로우 주의해야 하나,
        // 10초 제한을 두었으므로 안전함.
        __u64 tokens_to_add = (delta * rate) / NANO_TO_SEC;

        if (tokens_to_add > 0) {
            bucket->tokens += tokens_to_add;
            if (bucket->tokens > capacity) {
                bucket->tokens = capacity;
            }
            // 마지막 갱신 시간 업데이트 (정밀도 보정)
            bucket->last_time += (tokens_to_add * NANO_TO_SEC) / rate;
        }
    }

    // 5. 토큰 소비
    if (bucket->tokens >= 1) {
        bucket->tokens -= 1;
        verdict = 1; // PASS
    } else {
        verdict = 0; // DROP
    }

    bpf_spin_unlock(&bucket->lock);
    return verdict;
}

SEC("xdp")
int xdp_ddos_mitigator(struct xdp_md *ctx) {
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

    // --- L2 Parsing ---
    struct ethhdr *eth = data;
    if (unlikely((void *)(eth + 1) > data_end)) return XDP_PASS;

    // IP 패킷만 처리
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return XDP_PASS;

    // --- L3 Parsing ---
    struct iphdr *iph = (void *)(eth + 1);
    if (unlikely((void *)(iph + 1) > data_end)) return XDP_PASS;

    // TCP 패킷만 처리
    if (iph->protocol != IPPROTO_TCP) return XDP_PASS;

    __u32 ip_hlen = (iph->ihl & 0x0F) * 4;
    // IP 헤더 길이 검증
    if (unlikely(ip_hlen < sizeof(struct iphdr) || ip_hlen > 60)) return XDP_PASS;

    // --- L4 Parsing ---
    struct tcphdr *tcph = (void *)((__u8 *)iph + ip_hlen);
    if (unlikely((void *)(tcph + 1) > data_end)) return XDP_PASS;

    // --- Filtering Logic ---
    // 오직 SYN 패킷만 Rate Limit 적용
    // (이미 연결된 세션의 데이터 패킷은 건드리지 않음 -> 성능 보장)
    if (!tcph->syn || tcph->ack) {
        return XDP_PASS;
    }

    // 설정값 조회
    __u32 cfg_key = 0;
    struct policy_config *cfg = bpf_map_lookup_elem(&config_map, &cfg_key);
    if (unlikely(!cfg)) return XDP_PASS; // 설정 없으면 통과

    __u64 now = bpf_ktime_get_ns();


    // === Tier 1: 개별 IP 차단 ===
    if (cfg->ip_rate > 0) {
        __u32 ip_key = iph->saddr;
        struct token_bucket *ip_bucket = bpf_map_lookup_elem(&ip_limiter_map, &ip_key);

        if (!ip_bucket) {
            struct token_bucket zero_bucket = {0};
            bpf_map_update_elem(&ip_limiter_map, &ip_key, &zero_bucket, BPF_NOEXIST);
            ip_bucket = bpf_map_lookup_elem(&ip_limiter_map, &ip_key);
        }

        if (ip_bucket) {
            if (!apply_token_bucket(ip_bucket, cfg->ip_rate, cfg->ip_capacity, now)) {
                return XDP_DROP; // IP 기준 초과 시 드랍
            }
        }
    }


    // === Tier 2: 전체 총량 제어 ===
    if (cfg->svc_rate > 0) {
        struct service_key svc_k;
        // Key 해시값 일관성
        __builtin_memset(&svc_k, 0, sizeof(svc_k));
        svc_k.daddr = iph->daddr;
        svc_k.dport = tcph->dest;

        struct token_bucket *svc_bucket = bpf_map_lookup_elem(&svc_limiter_map, &svc_k);

        if (!svc_bucket) {
            struct token_bucket zero_bucket = {0};
            bpf_map_update_elem(&svc_limiter_map, &svc_k, &zero_bucket, BPF_NOEXIST);
            svc_bucket = bpf_map_lookup_elem(&svc_limiter_map, &svc_k);
        }

        if (svc_bucket) {
            if (!apply_token_bucket(svc_bucket, cfg->svc_rate, cfg->svc_capacity, now)) {
                return XDP_DROP; // 서비스 총량 초과 시 드랍
            }
        }
    }

    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";