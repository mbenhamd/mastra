import { readFileSync } from 'node:fs';

import { describe, expect, it, vi } from 'vitest';

import {
  createInngestTestRuntimeConfig,
  createLocalTestEndpoints,
  InngestTestRuntimeManager,
  INNGEST_TEST_CONTAINER_LABEL,
  INNGEST_TEST_CONTAINER_NAME,
  INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY,
  INNGEST_TEST_DOCKER_IMAGE,
} from './inngest-test-runtime';

type RuntimeSignal = 'SIGHUP' | 'SIGINT' | 'SIGTERM';

const DEFAULT_OWNER_TOKEN = 'test-owner-token';

interface MockDockerContainer {
  id: string;
  running: boolean;
  image: string;
  runtimeLabel: string;
  ownerToken: string;
}

interface MockDockerState {
  container: MockDockerContainer | null;
}

function jsonResponse(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    headers: { 'content-type': 'application/json' },
  });
}

function createRuntimeHarness(
  fetchImplementation: (input: string, init?: RequestInit) => Promise<Response>,
  options: {
    readonly ownerToken?: string;
    readonly dockerState?: MockDockerState;
    readonly beforeFirstDockerInspect?: () => Promise<void>;
  } = {},
) {
  const ownerToken = options.ownerToken ?? DEFAULT_OWNER_TOKEN;
  const dockerState = options.dockerState ?? { container: null };
  let now = 0;
  let processExited = false;
  let firstDockerInspect = true;
  const signalListeners = new Map<RuntimeSignal, () => void>();
  const processWait = vi.fn(async () => undefined);
  const processKill = vi.fn(() => {
    processExited = true;
    if (dockerState.container?.ownerToken === ownerToken) {
      dockerState.container.running = false;
    }
    return true;
  });
  const runCommand = vi.fn(async (_file: string, args: readonly string[]) => {
    if (args[0] === 'container' && args[1] === 'inspect') {
      const inspectedContainer = dockerState.container ? { ...dockerState.container } : null;
      if (firstDockerInspect) {
        firstDockerInspect = false;
        await options.beforeFirstDockerInspect?.();
      }
      if (!inspectedContainer) {
        return {
          exitCode: 1,
          stdout: '',
          stderr: `No such object: ${INNGEST_TEST_CONTAINER_NAME}`,
        };
      }
      return {
        exitCode: 0,
        stdout:
          `${inspectedContainer.id}\t${inspectedContainer.running ? 'true' : 'false'}\t` +
          `${inspectedContainer.image}\t` +
          `${inspectedContainer.runtimeLabel}\t${inspectedContainer.ownerToken}`,
        stderr: '',
      };
    }
    if (args[0] === 'rm') {
      if (!dockerState.container || args[2] !== dockerState.container.id) {
        return {
          exitCode: 1,
          stdout: '',
          stderr: `No such container: ${args[2] ?? INNGEST_TEST_CONTAINER_NAME}`,
        };
      }
      dockerState.container = null;
      return {
        exitCode: 0,
        stdout: INNGEST_TEST_CONTAINER_NAME,
        stderr: '',
      };
    }
    return {
      exitCode: 0,
      stdout: '',
      stderr: '',
    };
  });
  const spawnCommand = vi.fn((file: string, args: readonly string[]) => {
    processExited = false;
    if (file === 'docker') {
      if (dockerState.container?.running) {
        processExited = true;
      } else {
        const ownerLabelIndex = args.indexOf(`${INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY}=${ownerToken}`);
        if (ownerLabelIndex === -1) {
          throw new Error('Docker run did not include the expected owner-token label.');
        }
        dockerState.container = {
          id: `container-${ownerToken}`,
          running: true,
          image: INNGEST_TEST_DOCKER_IMAGE,
          runtimeLabel: 'inngest',
          ownerToken,
        };
      }
    }
    return {
      kill: processKill,
      wait: processWait,
      hasExited: () => processExited,
    };
  });
  const exit = vi.fn();
  const sleep = vi.fn(async (ms: number) => {
    now += ms;
  });

  return {
    dependencies: {
      fetch: vi.fn(fetchImplementation) as unknown as typeof globalThis.fetch,
      runCommand,
      spawnCommand,
      sleep,
      now: () => now,
      createOwnerToken: () => ownerToken,
      signals: {
        on: (signal: RuntimeSignal, listener: () => void) => {
          signalListeners.set(signal, listener);
        },
        off: (signal: RuntimeSignal, listener: () => void) => {
          if (signalListeners.get(signal) === listener) {
            signalListeners.delete(signal);
          }
        },
        exit,
      },
    },
    exit,
    markProcessExited: () => {
      processExited = true;
      if (dockerState.container?.ownerToken === ownerToken) {
        dockerState.container.running = false;
      }
    },
    processKill,
    processWait,
    runCommand,
    signalListeners,
    sleep,
    spawnCommand,
  };
}

function createSuccessfulFetch(expectedFunctionIds: readonly string[] = []) {
  return async (input: string, init?: RequestInit): Promise<Response> => {
    if (input.endsWith('/dev')) {
      return jsonResponse({
        functions: expectedFunctionIds.map(id => ({ id })),
      });
    }
    if (init?.method === 'GET' || init?.method === 'PUT') {
      return new Response();
    }
    throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${input}`);
  };
}

describe('InngestTestRuntimeManager', () => {
  it('uses one immutable digest-pinned Docker topology and removes its container', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 43123, handlerPort: 43124 });
    const config = createInngestTestRuntimeConfig(endpoints, {}, 'linux');
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.ready']));
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    expect(Object.isFrozen(config)).toBe(true);
    expect(Object.isFrozen(config.endpoints)).toBe(true);
    expect(Object.isFrozen(config.endpoints.ports)).toBe(true);
    expect(config.dockerImage).toBe(INNGEST_TEST_DOCKER_IMAGE);
    const composeSource = readFileSync(new URL('../../docker-compose.yaml', import.meta.url), 'utf8');
    expect(composeSource).toContain(`image: ${INNGEST_TEST_DOCKER_IMAGE}`);
    expect(composeSource).toContain('io.mastra.test-runtime: inngest');
    expect(composeSource).toContain('network_mode: host');
    expect(composeSource).not.toContain('host-gateway');

    const packageJson = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as {
      scripts?: Record<string, string>;
    };
    expect(packageJson.scripts).not.toHaveProperty('test:docker');

    const workflowSource = readFileSync(
      new URL('../../../../.github/workflows/papersflow-fork-pr.yml', import.meta.url),
      'utf8',
    );
    expect(workflowSource).not.toContain('Start pinned Inngest dev server');
    expect(workflowSource).not.toContain('Stop pinned Inngest dev server');
    expect(workflowSource).not.toContain('MASTRA_INNGEST_TEST_DOCKER=1');
    expect(workflowSource).not.toContain('docker run --detach --name mastra-inngest-test');

    const indexSource = readFileSync(new URL('../index.test.ts', import.meta.url), 'utf8');
    expect(indexSource.match(/new InngestTestRuntimeManager/g)).toHaveLength(1);
    expect(indexSource).not.toContain('npx inngest-cli');
    expect(config.containerName).toBe(INNGEST_TEST_CONTAINER_NAME);
    expect(manager.registerOptions).toEqual({
      registerOptions: {
        serveOrigin: 'http://127.0.0.1:43124',
        servePath: '/inngest/api',
      },
    });

    await manager.ensureReady(['workflow.ready']);
    await manager.stop();

    const dockerRun = harness.spawnCommand.mock.calls.find(([file]) => file === 'docker');
    expect(dockerRun).toEqual([
      'docker',
      [
        'run',
        '--rm',
        '--sig-proxy=true',
        '--name',
        INNGEST_TEST_CONTAINER_NAME,
        '--label',
        INNGEST_TEST_CONTAINER_LABEL,
        '--label',
        `${INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY}=${DEFAULT_OWNER_TOKEN}`,
        '--network',
        'host',
        INNGEST_TEST_DOCKER_IMAGE,
        'inngest',
        'dev',
        '-p',
        '43123',
        '-u',
        'http://127.0.0.1:43124/inngest/api',
        '--poll-interval=1',
      ],
    ]);
    expect(harness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
    expect(
      harness.runCommand.mock.calls.some(
        ([, args]) => args[0] === 'container' && args[1] === 'inspect' && args.includes('--format'),
      ),
    ).toBe(true);
    expect(harness.processKill).toHaveBeenCalledWith('SIGTERM');
  });

  it('requires explicit endpoint and binary configuration outside Docker mode', () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });

    expect(() =>
      createInngestTestRuntimeConfig(endpoints, {
        MASTRA_INNGEST_TEST_DOCKER: '0',
      }),
    ).toThrow(/no longer selects an implicit host CLI/);
    expect(() =>
      createInngestTestRuntimeConfig(endpoints, {
        MASTRA_INNGEST_TEST_MODE: 'external',
      }),
    ).toThrow(/requires MASTRA_INNGEST_TEST_EXTERNAL_URL/);
    expect(() =>
      createInngestTestRuntimeConfig(endpoints, {
        MASTRA_INNGEST_TEST_MODE: 'host',
        MASTRA_INNGEST_TEST_HOST_BINARY: 'inngest',
      }),
    ).toThrow(/absolute MASTRA_INNGEST_TEST_HOST_BINARY/);
    expect(() =>
      createInngestTestRuntimeConfig(endpoints, {
        MASTRA_INNGEST_TEST_DOCKER_URL: 'http://mastra-inngest-test:4200',
      }),
    ).toThrow(/Nested Docker routing requires/);
  });

  it('never spawns or cleans an explicitly supplied external daemon', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_MODE: 'external',
      MASTRA_INNGEST_TEST_EXTERNAL_URL: 'http://inngest.example.test:8288/',
      MASTRA_INNGEST_TEST_EXTERNAL_SERVE_ORIGIN: 'http://runner.example.test:4201/',
    });
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.external']));
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.external']);
    await manager.stop();

    expect(config.endpoints.clientBaseUrl).toBe('http://inngest.example.test:8288');
    expect(config.endpoints.devServerUrl).toBe('http://inngest.example.test:8288/dev');
    expect(manager.registerOptions).toEqual({
      registerOptions: {
        serveOrigin: 'http://runner.example.test:4201',
        servePath: '/inngest/api',
      },
    });
    expect(harness.runCommand).not.toHaveBeenCalled();
    expect(harness.spawnCommand).not.toHaveBeenCalled();
  });

  it('supports explicit nested-daemon routing without embedding a machine address', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_DOCKER_NETWORK: 'mastra-tests',
      MASTRA_INNGEST_TEST_DOCKER_URL: 'http://mastra-inngest-test:4200',
      MASTRA_INNGEST_TEST_DOCKER_SERVE_ORIGIN: 'http://test-runner:4201',
    });
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.nested']));
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.nested']);
    await manager.stop();

    expect(config.endpoints.devServerUrl).toBe('http://mastra-inngest-test:4200/dev');
    const dockerRun = harness.spawnCommand.mock.calls.find(([file]) => file === 'docker');
    expect(dockerRun?.[1]).toContain('mastra-tests');
    expect(dockerRun?.[1]).toContain('http://test-runner:4201/inngest/api');
    expect(dockerRun?.[1]).not.toContain('host.docker.internal:host-gateway');
    expect(dockerRun?.[1]).not.toContain('--publish');
  });

  it('uses Docker Desktop host routing without injecting a host-gateway address', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {}, 'darwin');
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.desktop']));
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.desktop']);
    await manager.stop();

    expect(config.dockerNetwork).toBeUndefined();
    expect(config.registrationServeOrigin).toBe('http://host.docker.internal:4201');
    const dockerRun = harness.spawnCommand.mock.calls.find(([file]) => file === 'docker');
    expect(dockerRun?.[1]).toContain('4200:4200');
    expect(dockerRun?.[1]).toContain('http://host.docker.internal:4201/inngest/api');
    expect(dockerRun?.[1]).not.toContain('host.docker.internal:host-gateway');
    expect(dockerRun?.[1]).not.toContain('--network');
  });

  it('serializes concurrent consumers behind one Docker lifecycle', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.one', 'workflow.two']));
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await Promise.all([manager.ensureReady(['workflow.one']), manager.ensureReady(['workflow.two'])]);
    await manager.stop();

    expect(harness.spawnCommand.mock.calls.filter(([file]) => file === 'docker')).toHaveLength(1);
  });

  it('fences concurrent managers that both inspect the fixed container name before either spawn', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    const dockerState: MockDockerState = { container: null };
    let inspectArrivals = 0;
    let releaseInspects!: () => void;
    const bothManagersInspected = new Promise<void>(resolve => {
      releaseInspects = resolve;
    });
    const beforeFirstDockerInspect = async () => {
      inspectArrivals += 1;
      if (inspectArrivals === 2) releaseInspects();
      await bothManagersInspected;
    };
    const firstHarness = createRuntimeHarness(createSuccessfulFetch(['workflow.first']), {
      ownerToken: 'manager-one',
      dockerState,
      beforeFirstDockerInspect,
    });
    const secondHarness = createRuntimeHarness(createSuccessfulFetch(['workflow.second']), {
      ownerToken: 'manager-two',
      dockerState,
      beforeFirstDockerInspect,
    });
    const firstManager = new InngestTestRuntimeManager(config, firstHarness.dependencies);
    const secondManager = new InngestTestRuntimeManager(config, secondHarness.dependencies);

    const results = await Promise.allSettled([
      firstManager.ensureReady(['workflow.first']),
      secondManager.ensureReady(['workflow.second']),
    ]);
    expect(results.map(result => result.status).sort()).toEqual(['fulfilled', 'rejected']);

    const winnerIndex = results.findIndex(result => result.status === 'fulfilled');
    const failedResult = results.find(result => result.status === 'rejected');
    expect(String(failedResult && 'reason' in failedResult ? failedResult.reason : '')).toMatch(
      /Owned Inngest Docker process exited before the test runtime became ready/,
    );

    const winnerManager = winnerIndex === 0 ? firstManager : secondManager;
    const winnerToken = winnerIndex === 0 ? 'manager-one' : 'manager-two';
    const losingHarness = winnerIndex === 0 ? secondHarness : firstHarness;
    expect(dockerState.container?.ownerToken).toBe(winnerToken);
    expect(losingHarness.spawnCommand).toHaveBeenCalledTimes(1);
    expect(losingHarness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(0);
    expect(losingHarness.signalListeners.size).toBe(0);

    await winnerManager.stop();
    expect(dockerState.container).toBeNull();
  });

  it('removes a stopped stale Inngest container before starting its uniquely owned container', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    const dockerState: MockDockerState = {
      container: {
        id: 'container-stale-manager',
        running: false,
        image: INNGEST_TEST_DOCKER_IMAGE,
        runtimeLabel: 'inngest',
        ownerToken: 'stale-manager',
      },
    };
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.replacement']), {
      ownerToken: 'replacement-manager',
      dockerState,
    });
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.replacement']);
    expect(dockerState.container).toMatchObject({
      running: true,
      ownerToken: 'replacement-manager',
    });
    await manager.stop();

    expect(harness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(2);
    expect(dockerState.container).toBeNull();
  });

  it('does not adopt a reachable daemon after the owned Docker process exits', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    let markProcessExited = () => undefined;
    const harness = createRuntimeHarness(async input => {
      if (input.endsWith('/dev')) {
        markProcessExited();
        return jsonResponse({ functions: [{ id: 'workflow.unrelated' }] });
      }
      return new Response();
    });
    markProcessExited = harness.markProcessExited;
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await expect(manager.ensureReady(['workflow.unrelated'])).rejects.toThrow(
      /Owned Inngest Docker process exited before the test runtime became ready/,
    );

    expect(harness.dependencies.fetch).toHaveBeenCalledTimes(1);
    expect(harness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
  });

  it('rejects a reachable daemon when the named container identity does not match', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.unrelated']));
    let inspectAttempts = 0;
    harness.runCommand.mockImplementation(async (_file, args) => {
      if (args[0] === 'container' && args[1] === 'inspect') {
        inspectAttempts += 1;
        if (inspectAttempts === 1 || inspectAttempts >= 3) {
          return { exitCode: 1, stdout: '', stderr: `No such object: ${config.containerName}` };
        }
        return {
          exitCode: 0,
          stdout: `container-foreign\ttrue\tunrelated/image:latest\tinngest\tforeign-owner`,
          stderr: '',
        };
      }
      return { exitCode: 0, stdout: '', stderr: '' };
    });
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await expect(manager.ensureReady(['workflow.unrelated'])).rejects.toThrow(
      /does not match the Inngest test runtime identity/,
    );

    expect(harness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(0);
  });

  it('waits beyond 30 seconds for a 219-workflow registry to become complete', async () => {
    const expectedFunctionIds = Array.from({ length: 219 }, (_, index) => `workflow.shared-${index}`);
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_POLL_INTERVAL_MS: '10000',
      MASTRA_INNGEST_TEST_REGISTRATION_TIMEOUT_MS: '60000',
    });
    let devRequests = 0;
    const harness = createRuntimeHarness(async (input, init) => {
      if (input.endsWith('/dev')) {
        devRequests += 1;
        const functions = devRequests >= 6 ? expectedFunctionIds.map(id => ({ id })) : [];
        return jsonResponse({ functions });
      }
      if (init?.method === 'GET' || init?.method === 'PUT') {
        return new Response();
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${input}`);
    });
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(expectedFunctionIds);
    const registrationElapsedMs = harness.dependencies.sleep.mock.calls.reduce((total, [ms]) => total + ms, 0);
    await manager.stop();

    expect(devRequests).toBe(6);
    expect(registrationElapsedMs).toBe(40_000);
  });

  it('removes the Docker container when startup or registration times out', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const startupConfig = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_STARTUP_TIMEOUT_MS: '20',
      MASTRA_INNGEST_TEST_POLL_INTERVAL_MS: '10',
    });
    const startupHarness = createRuntimeHarness(async () => {
      throw new Error('connection refused');
    });
    const startupManager = new InngestTestRuntimeManager(startupConfig, startupHarness.dependencies);

    await expect(startupManager.ensureReady()).rejects.toThrow(/did not become ready/);
    await startupManager.stop();
    expect(startupHarness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
    expect(startupHarness.signalListeners.size).toBe(0);

    const registrationConfig = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_REGISTRATION_TIMEOUT_MS: '20',
      MASTRA_INNGEST_TEST_POLL_INTERVAL_MS: '10',
    });
    const registrationHarness = createRuntimeHarness(async (input, init) => {
      if (input.endsWith('/dev')) return jsonResponse({ functions: [] });
      if (init?.method === 'GET' || init?.method === 'PUT') return new Response();
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${input}`);
    });
    const registrationManager = new InngestTestRuntimeManager(registrationConfig, registrationHarness.dependencies);

    await expect(registrationManager.ensureReady(['workflow.missing'])).rejects.toThrow(/registered 0\/1/);
    expect(registrationHarness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
    expect(registrationHarness.signalListeners.size).toBe(0);
  });

  it('surfaces owned-container cleanup failures and permits an explicit retry', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {});
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.cleanup']));
    let inspectAttempts = 0;
    let removeAttempts = 0;
    harness.runCommand.mockImplementation(async (_file, args) => {
      if (args[0] === 'container' && args[1] === 'inspect') {
        inspectAttempts += 1;
        if (inspectAttempts === 1) {
          return { exitCode: 1, stdout: '', stderr: `No such object: ${config.containerName}` };
        }
        return {
          exitCode: 0,
          stdout: `container-owned\ttrue\t${config.dockerImage}\tinngest\t${DEFAULT_OWNER_TOKEN}`,
          stderr: '',
        };
      }

      removeAttempts += 1;
      if (removeAttempts === 1) {
        return { exitCode: 1, stdout: '', stderr: 'daemon cleanup refused' };
      }
      return { exitCode: 0, stdout: config.containerName, stderr: '' };
    });
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.cleanup']);
    await expect(manager.stop()).rejects.toThrow(/daemon cleanup refused/);
    await manager.stop();

    expect(removeAttempts).toBe(2);
  });

  it('retains an explicit host process when cleanup fails so stop can be retried', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_MODE: 'host',
      MASTRA_INNGEST_TEST_HOST_BINARY: '/opt/inngest',
    });
    const harness = createRuntimeHarness(createSuccessfulFetch(['workflow.host-retry']));
    harness.processWait.mockRejectedValueOnce(new Error('host wait failed')).mockResolvedValue(undefined);
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);

    await manager.ensureReady(['workflow.host-retry']);
    await expect(manager.stop()).rejects.toThrow(/host wait failed/);
    expect(harness.signalListeners.size).toBe(3);
    await manager.stop();

    expect(harness.processKill).toHaveBeenCalledTimes(2);
    expect(harness.signalListeners.size).toBe(0);
  });

  it('interrupts startup promptly and cleans its owned Docker runtime', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const config = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_STARTUP_TIMEOUT_MS: '60000',
      MASTRA_INNGEST_TEST_POLL_INTERVAL_MS: '10000',
    });
    let announceFetchStarted = () => undefined;
    const fetchStarted = new Promise<void>(resolve => {
      announceFetchStarted = resolve;
    });
    const harness = createRuntimeHarness(async (_input, init) => {
      announceFetchStarted();
      return new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal;
        if (signal?.aborted) {
          reject(new Error('request aborted'));
          return;
        }
        signal?.addEventListener('abort', () => reject(new Error('request aborted')), { once: true });
      });
    });
    const manager = new InngestTestRuntimeManager(config, harness.dependencies);
    const readiness = manager.ensureReady();
    const readinessFailure = expect(readiness).rejects.toThrow(/startup was interrupted/);

    await fetchStarted;
    const signalListener = harness.signalListeners.get('SIGINT');
    expect(signalListener).toBeDefined();
    signalListener?.();

    await readinessFailure;
    await vi.waitFor(() => {
      expect(harness.exit).toHaveBeenCalledWith(130);
    });
    expect(harness.sleep).not.toHaveBeenCalledWith(config.pollIntervalMs);
    expect(harness.processKill).toHaveBeenCalledWith('SIGTERM');
    expect(harness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
  });

  it('terminates an explicit host binary and cleans Docker on interruption', async () => {
    const endpoints = createLocalTestEndpoints({ inngestPort: 4200, handlerPort: 4201 });
    const hostConfig = createInngestTestRuntimeConfig(endpoints, {
      MASTRA_INNGEST_TEST_MODE: 'host',
      MASTRA_INNGEST_TEST_HOST_BINARY: '/opt/inngest',
    });
    const hostHarness = createRuntimeHarness(createSuccessfulFetch(['workflow.host']));
    const hostManager = new InngestTestRuntimeManager(hostConfig, hostHarness.dependencies);

    await hostManager.ensureReady(['workflow.host']);
    await hostManager.stop();

    expect(hostHarness.spawnCommand).toHaveBeenCalledWith('/opt/inngest', [
      'dev',
      '-p',
      '4200',
      '-u',
      'http://localhost:4201/inngest/api',
      '--poll-interval=1',
      '--retry-interval=1',
    ]);
    expect(hostHarness.processKill).toHaveBeenCalledWith('SIGTERM');

    const dockerConfig = createInngestTestRuntimeConfig(endpoints, {});
    const dockerHarness = createRuntimeHarness(createSuccessfulFetch(['workflow.signal']));
    const dockerManager = new InngestTestRuntimeManager(dockerConfig, dockerHarness.dependencies);
    await dockerManager.ensureReady(['workflow.signal']);

    const signalListener = dockerHarness.signalListeners.get('SIGTERM');
    expect(signalListener).toBeDefined();
    signalListener?.();
    await vi.waitFor(() => {
      expect(dockerHarness.exit).toHaveBeenCalledWith(143);
    });
    expect(dockerHarness.runCommand.mock.calls.filter(([, args]) => args[0] === 'rm')).toHaveLength(1);
  });
});
