import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const repoRoot = __ENV.RIDEMATCH_REPO_ROOT || '.';

const driverProtoPath = `${repoRoot}/proto/driver.proto`;
const riderProtoPath = `${repoRoot}/proto/rider.proto`;

const driverCli = new grpc.Client();
const riderCli = new grpc.Client();

export const options = {
  scenarios: {
    drivers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 25 },
        { duration: '2m', target: 50 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '30s',
      exec: 'driverScenario',
      startTime: '0s',
    },
    riders: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '2m', target: 25 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '30s',
      exec: 'riderScenario',
      startTime: '15s',
    },
  },
  thresholds: {
    grpc_req_duration: ['p(95)<2500'],
  },
};

export function setup() {
  driverCli.load([], driverProtoPath);
  riderCli.load([], riderProtoPath);

  driverCli.connect(__ENV.DRIVER_GRPC_ADDR || '127.0.0.1:50051', {
    plaintext: true,
    timeout: '60s',
  });
  riderCli.connect(__ENV.RIDER_GRPC_ADDR || '127.0.0.1:50052', {
    plaintext: true,
    timeout: '60s',
  });
}

export function driverScenario() {
  const id = `d-${__VU}-${__ITER}`;

  check(
    driverCli.invoke('ridematch.driver.v1.DriverService/RegisterDriver', { driver_id: id }),
    {
      driver_register_ok: (r) => r && r.status === grpc.StatusOK,
    },
  );

  const lat = 40.7831 + (__VU % 10) / 8000;
  const lng = -73.9712 + (__ITER % 10) / 8000;

  check(
    driverCli.invoke('ridematch.driver.v1.DriverService/UpdateLocation', {
      driver_id: id,
      lat,
      lng,
    }),
    {
      driver_update_ok: (r) => r && r.status === grpc.StatusOK,
    },
  );

  sleep(0.03);
}

export function riderScenario() {
  const res = riderCli.invoke('ridematch.rider.v1.RiderService/RequestRide', {
    rider_id: `r-${__VU}-${__ITER}`,
    pickup_lat: 40.7831,
    pickup_lng: -73.9712,
    dropoff_lat: 40.791,
    dropoff_lng: -73.981,
  });

  check(res, {
    rider_request_ok: (r) => r && r.status === grpc.StatusOK,
  });

  sleep(0.03);
}

export function teardown() {
  driverCli.close();
  riderCli.close();
}

export function handleSummary(data) {
  const gd = data.metrics.grpc_req_duration;
  const it = data.metrics.iterations;

  const lines = [];
  lines.push('');
  lines.push('--- RideMatch aggregate summary helpers (grpc_req_duration units are documented by k6) ---');
  if (gd && gd.values && gd.values.avg != null) {
    lines.push(`grpc_req_duration avg=${gd.values.avg}`);
  }
  if (gd && gd.values && gd.values['p(50)'] != null) {
    lines.push(`grpc_req_duration p50=${gd.values['p(50)']}`);
  }
  if (gd && gd.values && gd.values['p(95)'] != null) {
    lines.push(`grpc_req_duration p95=${gd.values['p(95)']}`);
  }
  if (gd && gd.values && gd.values['p(99)'] != null) {
    lines.push(`grpc_req_duration p99=${gd.values['p(99)']}`);
  }

  lines.push('');
  lines.push('--- iterations throughput (scenario-wide) ---');
  if (it && it.values && it.values.rate != null) {
    lines.push(`iterations rate=${it.values.rate}/s`);
  }

  lines.push('');
  lines.push('Full JSON grpc_req_duration:');
  lines.push(JSON.stringify(gd ?? null, null, 2));

  return { stdout: lines.join('\n') + '\n' };
}
