import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom Prometheus/k6 metrics
const status202 = new Counter('http_202_accepted_total');
const status4xx = new Counter('http_4xx_client_err_total');
const status5xx = new Counter('http_5xx_server_err_total');
const cacheHits = new Counter('idempotency_cache_hits_total');
const cacheMisses = new Counter('idempotency_cache_misses_total');
const cacheHitRate = new Rate('idempotency_cache_hit_rate');
const latencyTrend = new Trend('job_latency_ms', true);

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const SELECTED_SCENARIO = __ENV.SCENARIO || 'all';

// ── KEY POOLS ───────────────────────────────────────────────────
// Scenario B: Aggressive collision storm with only 20 keys
const HOT_POOL_20 = [];
for (let i = 0; i < 20; i++) {
  HOT_POOL_20.push(`storm-key-${i}`);
}

// ── SCENARIO DEFINITIONS ─────────────────────────────────────────
function getScenarios() {
  if (SELECTED_SCENARIO === 'burst') {
    return {
      pool_saturation_burst: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '30s', target: 1500 },
          { duration: '60s', target: 1500 },
          { duration: '10s', target: 0 },
        ],
        exec: 'poolSaturationBurst',
        tags: { scenario: 'burst_1500vu' },
      },
    };
  }

  if (SELECTED_SCENARIO === 'collision') {
    return {
      lock_cache_storm: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '10s', target: 1000 },
          { duration: '45s', target: 1000 },
          { duration: '5s', target: 0 },
        ],
        exec: 'lockCacheStorm',
        tags: { scenario: 'collision_1000vu' },
      },
    };
  }

  if (SELECTED_SCENARIO === 'payload') {
    return {
      payload_edge_cases: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '15s', target: 500 },
          { duration: '30s', target: 500 },
          { duration: '5s', target: 0 },
        ],
        exec: 'payloadEdgeCases',
        tags: { scenario: 'payload_edge' },
      },
    };
  }

  // Default: Full multi-scenario battery (A, then B, then C)
  return {
    scenario_a_burst: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 1500 },
        { duration: '60s', target: 1500 },
        { duration: '10s', target: 0 },
      ],
      exec: 'poolSaturationBurst',
      tags: { scenario: 'scenario_a_burst' },
    },
    scenario_b_collision: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 1000 },
        { duration: '45s', target: 1000 },
        { duration: '5s', target: 0 },
      ],
      startTime: '105s', // Begins after Scenario A finishes
      exec: 'lockCacheStorm',
      tags: { scenario: 'scenario_b_collision' },
    },
    scenario_c_payload: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 500 },
        { duration: '30s', target: 500 },
        { duration: '5s', target: 0 },
      ],
      startTime: '170s', // Begins after Scenario B finishes
      exec: 'payloadEdgeCases',
      tags: { scenario: 'scenario_c_payload' },
    },
  };
}

export const options = {
  scenarios: getScenarios(),
  thresholds: {
    http_req_failed: ['rate<0.05'], // Allow up to 5% failure during extreme 1500-VU pool saturation / fault injection
    'http_req_duration{scenario:scenario_b_collision}': ['p(95)<1500'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)'],
};

// ── SCENARIO A: POOL SATURATION & BURST STRESS (1,500 VUs) ─────────
// Forces pgxpool contention (150 max conns) with continuous unique outbox inserts
export function poolSaturationBurst() {
  const uniqueKey = `burst-user-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`;
  const payload = JSON.stringify({
    job_type: 'stress_burst',
    handler_name: 'compute_handler',
    payload: {
      vu: __VU,
      iter: __ITER,
      ts: Date.now(),
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': uniqueKey,
    },
    tags: { scenario: 'burst_1500vu' },
    timeout: '10s',
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/jobs`, payload, params);
  latencyTrend.add(Date.now() - start);

  handleResponse(res);
  sleep(0.01);
}

// ── SCENARIO B: AGGRESSIVE COLLISION & LOCK CAORM (1,000 VUs) ─────
// 98% collision on only 20 keys, hitting Redis fast-path and ON CONFLICT simultaneously
export function lockCacheStorm() {
  let key;
  if (Math.random() < 0.98) {
    // 98% hit tight hot-pool of 20 keys
    const idx = Math.floor(Math.random() * HOT_POOL_20.length);
    key = HOT_POOL_20[idx];
  } else {
    key = `storm-novel-${__VU}-${Date.now()}`;
  }

  const payload = JSON.stringify({
    job_type: 'storm_collision',
    handler_name: 'payment_handler',
    payload: {
      amount: 42.50,
      currency: 'USD',
      vu: __VU,
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': key,
    },
    tags: { scenario: 'collision_1000vu' },
    timeout: '10s',
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/jobs`, payload, params);
  latencyTrend.add(Date.now() - start);

  handleResponse(res);
  sleep(0.005);
}

// ── SCENARIO C: PAYLOAD EDGE CASES (500 VUs, 1 KB to 500 KB) ───────
// Dynamic nested JSON + 1% intentionally malformed requests to test resource leaks
export function payloadEdgeCases() {
  const dice = Math.random();

  // 1% missing header or malformed JSON
  if (dice < 0.005) {
    // Missing Idempotency-Key
    const res = http.post(`${BASE_URL}/v1/jobs`, '{"job_type":"invalid"}', {
      headers: { 'Content-Type': 'application/json' },
      tags: { scenario: 'payload_edge_missing_header' },
      timeout: '5s',
    });
    handleResponse(res);
    return;
  }
  if (dice < 0.01) {
    // Malformed JSON body
    const res = http.post(`${BASE_URL}/v1/jobs`, '{"bad_json": incomplete...', {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `malformed-${__VU}-${__ITER}`,
      },
      tags: { scenario: 'payload_edge_malformed' },
      timeout: '5s',
    });
    handleResponse(res);
    return;
  }

  // Normal payload: variable sizes (1 KB to 50 KB) with nested tree structures
  const sizeKb = Math.floor(Math.random() * 50) + 1; // 1 to 50 KB
  const dummyData = 'x'.repeat(sizeKb * 1024);

  const payload = JSON.stringify({
    job_type: 'large_payload',
    handler_name: 'compute_handler',
    payload: {
      size_kb: sizeKb,
      nested: {
        level1: { level2: { level3: [1, 2, 3, 4, 5] } },
        blob: dummyData,
      },
    },
  });

  const key = `payload-${sizeKb}kb-${__VU}-${__ITER}`;
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': key,
    },
    tags: { scenario: 'payload_edge' },
    timeout: '10s',
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/jobs`, payload, params);
  latencyTrend.add(Date.now() - start);

  handleResponse(res);
  sleep(0.02);
}

// ── RESPONSE DISPATCHER & METRICS RECORDER ────────────────────────
function handleResponse(res) {
  if (res.status === 202 || res.status === 200) {
    status202.add(1);
    try {
      const data = JSON.parse(res.body);
      const isCached = data && data.cached === true;
      cacheHitRate.add(isCached);
      if (isCached) {
        cacheHits.add(1);
      } else {
        cacheMisses.add(1);
      }
    } catch (e) {
      // Body not JSON or empty
    }
  } else if (res.status >= 400 && res.status < 500) {
    status4xx.add(1);
  } else if (res.status >= 500 || res.status === 0) {
    status5xx.add(1);
  }
}
