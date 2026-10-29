import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const count = Number(__ENV.BENCH_COUNT);
if (![5, 10, 50, 100, 500].includes(count)) throw new Error('bounded workflow count required');
if (__ENV.BENCH_URL !== 'https://127.0.0.1:8443' || !__ENV.BENCH_HOST || !__ENV.BENCH_TOKEN) throw new Error('owned local TLS target and operator authentication required');
const accepted = new Counter('admission_accepted');
const rejected = new Counter('admission_rejected');
const durations = new Trend('admission_all_ms', true);
export const options = {
  scenarios: { submissions: { executor: 'shared-iterations', vus: Math.min(count, 10), iterations: count, maxDuration: '60s', gracefulStop: '2s' } },
  maxRedirects: 0,
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'min', 'max', 'p(50)', 'p(95)', 'p(99)'],
};
const body = JSON.stringify({name: 'bounded infrastructure diamond', tasks: [
  {key: 'root', kind: 'delay', delay_ms: 20},
  {key: 'left', kind: 'delay', delay_ms: 20, depends_on: ['root']},
  {key: 'right', kind: 'delay', delay_ms: 20, depends_on: ['root']},
  {key: 'join', kind: 'noop', depends_on: ['left', 'right']},
]});
export default function () {
  const iteration = exec.scenario.iterationInTest;
  const started_at_ms = Date.now();
  const response = http.post(__ENV.BENCH_URL + '/v1/workflows', body, {
    headers: {Host: __ENV.BENCH_HOST, Authorization: 'Bearer ' + __ENV.BENCH_TOKEN, 'Content-Type': 'application/json', 'Idempotency-Key': 'bench:' + __ENV.BENCH_RUN + ':' + iteration},
    timeout: '10s',
    tags: {operation: 'admission'},
  });
  durations.add(response.timings.duration, {status: String(response.status)});
  let workflow = null;
  if (response.status === 201) { workflow = response.json('id'); accepted.add(1); } else { rejected.add(1, {status: String(response.status)}); }
  console.log(JSON.stringify({schema_version: 1, iteration, status: response.status, duration_ms: response.timings.duration, workflow_id: workflow, payload_bytes: body.length, started_at_ms, ended_at_ms: Date.now(), error_code: response.error_code || null}));
}
export function handleSummary(data) { return {'/results/k6-summary.json': JSON.stringify(data, null, 2)}; }
