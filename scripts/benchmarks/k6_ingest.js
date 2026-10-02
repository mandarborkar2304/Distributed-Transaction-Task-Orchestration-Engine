import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom Prometheus/k6 metrics
const cachedRate = new Rate('idempotency_cached_rate');
const jobAccepted = new Counter('jobs_accepted_total');
const cacheHits = new Counter('idempotency_cache_hits_total');
const cacheMisses = new Counter('idempotency_cache_misses_total');
const ingestionDuration = new Trend('job_ingestion_duration_ms');

export const options = {
  stages: [
    { duration: '5s', target: 50 },    // Ramp-up to 50 VUs
    { duration: '10s', target: 200 },  // Ramp-up to 200 VUs
    { duration: '15s', target: 500 },  // Peak at 500 VUs
    { duration: '15s', target: 500 },  // Sustain 500 VUs
    { duration: '5s', target: 0 },     // Ramp-down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'], // Less than 1% error rate
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8000';

// Pool of 500 shared idempotency keys to force heavy duplicate collisions under 500 VUs
const KEY_POOL_SIZE = 500;
const KEY_POOL = [];
for (let i = 0; i < KEY_POOL_SIZE; i++) {
  KEY_POOL.push(`bench-idemp-key-${i}`);
}

export default function () {
  // 70% chance to reuse an existing key (testing Redis cache fast-path)
  // 30% chance to generate a fresh unique key (testing PostgreSQL atomic outbox insertion)
  let idempotencyKey;
  if (Math.random() < 0.70) {
    const idx = Math.floor(Math.random() * KEY_POOL_SIZE);
    idempotencyKey = KEY_POOL[idx];
  } else {
    idempotencyKey = `bench-fresh-key-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`;
  }

  const payload = JSON.stringify({
    job_type: 'payment',
    handler_name: 'payment_handler',
    payload: {
      amount: Math.floor(Math.random() * 1000) + 1,
      currency: 'USD',
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
    tags: { name: 'PostJobIngest' },
  };

  const startTime = Date.now();
  const res = http.post(`${BASE_URL}/v1/jobs`, payload, params);
  const duration = Date.now() - startTime;
  ingestionDuration.add(duration);

  const isOk = check(res, {
    'status is 202': (r) => r.status === 202,
    'has valid job_id': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body && body.job_id && body.job_id.length > 0;
      } catch (e) {
        return false;
      }
    },
  });

  if (isOk) {
    jobAccepted.add(1);
    try {
      const data = JSON.parse(res.body);
      const isCached = data.cached === true;
      cachedRate.add(isCached);
      if (isCached) {
        cacheHits.add(1);
      } else {
        cacheMisses.add(1);
      }
    } catch (e) {
      // Ignore JSON parse error in metric tracking
    }
  }

  // Small pacing between requests per VU
  sleep(0.01);
}
