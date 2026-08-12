import { randomUUID } from 'node:crypto';
import path from 'node:path';

import { execa } from 'execa';

export const INNGEST_TEST_DOCKER_IMAGE =
  'inngest/inngest:v1.34.0@sha256:4e1a3ab68036114bbdf2208d3a3ab8a135a106bb39199bdef031794294e60852';
export const INNGEST_TEST_CONTAINER_NAME = 'mastra-inngest-test';
export const INNGEST_TEST_CONTAINER_LABEL = 'io.mastra.test-runtime=inngest';
export const INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY = 'io.mastra.test-runtime-owner';

const INNGEST_TEST_CONTAINER_LABEL_KEY = 'io.mastra.test-runtime';
const INNGEST_TEST_CONTAINER_LABEL_VALUE = 'inngest';
const DOCKER_INSPECT_FORMAT =
  `{{.Id}}\t{{.State.Running}}\t{{.Config.Image}}\t` +
  `{{index .Config.Labels "${INNGEST_TEST_CONTAINER_LABEL_KEY}"}}\t` +
  `{{index .Config.Labels "${INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY}"}}`;

export interface LocalTestPorts {
  readonly inngestPort: number;
  readonly handlerPort: number;
}

export interface LocalTestEndpoints {
  readonly ports: LocalTestPorts;
  readonly clientBaseUrl: string;
  readonly devServerUrl: string;
  readonly handlerUrl: string;
  readonly dockerServeOrigin: string;
  readonly servePath: string;
}

export type InngestTestRuntimeMode = 'docker' | 'external' | 'host';

export interface InngestTestRuntimeConfig {
  readonly mode: InngestTestRuntimeMode;
  readonly endpoints: LocalTestEndpoints;
  readonly registrationServeOrigin?: string;
  readonly dockerImage: string;
  readonly containerName: string;
  readonly dockerNetwork?: string;
  readonly hostBinary?: string;
  readonly startupTimeoutMs: number;
  readonly registrationTimeoutMs: number;
  readonly pollIntervalMs: number;
  readonly requestTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
}

interface CommandResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

interface ManagedProcess {
  kill(signal: NodeJS.Signals): boolean;
  wait(): Promise<void>;
  hasExited(): boolean;
}

type RuntimeSignal = 'SIGHUP' | 'SIGINT' | 'SIGTERM';
type RuntimeSignalListener = () => void;

interface RuntimeSignalHost {
  on(signal: RuntimeSignal, listener: RuntimeSignalListener): void;
  off(signal: RuntimeSignal, listener: RuntimeSignalListener): void;
  exit(code: number): void;
}

interface DockerContainerIdentity {
  readonly id: string;
  readonly running: boolean;
  readonly image: string;
  readonly runtimeLabel: string;
  readonly ownerToken: string;
}

export interface InngestTestRuntimeDependencies {
  readonly fetch: typeof globalThis.fetch;
  readonly runCommand: (file: string, args: readonly string[]) => Promise<CommandResult>;
  readonly spawnCommand: (file: string, args: readonly string[]) => ManagedProcess;
  readonly sleep: (ms: number) => Promise<void>;
  readonly now: () => number;
  readonly createOwnerToken: () => string;
  readonly signals: RuntimeSignalHost;
}

const DEFAULT_STARTUP_TIMEOUT_MS = 60_000;
const DEFAULT_REGISTRATION_TIMEOUT_MS = 120_000;
const DEFAULT_POLL_INTERVAL_MS = 500;
const DEFAULT_REQUEST_TIMEOUT_MS = 5_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 2_000;
const REGISTRATION_RETRY_INTERVAL_MS = 5_000;
const SIGNAL_EXIT_CODES: Record<RuntimeSignal, number> = {
  SIGHUP: 129,
  SIGINT: 130,
  SIGTERM: 143,
};

function normalizeOrigin(value: string, variableName: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${variableName} must be an absolute HTTP(S) URL.`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${variableName} must use HTTP or HTTPS.`);
  }
  return value.replace(/\/+$/, '');
}

function readPositiveInteger(env: NodeJS.ProcessEnv, variableName: string, fallback: number): number {
  const rawValue = env[variableName];
  if (rawValue === undefined) return fallback;

  const value = Number(rawValue);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${variableName} must be a positive integer.`);
  }
  return value;
}

function resolveMode(env: NodeJS.ProcessEnv): InngestTestRuntimeMode {
  const configuredMode = env.MASTRA_INNGEST_TEST_MODE;
  if (configuredMode === undefined || configuredMode === '') {
    if (env.MASTRA_INNGEST_TEST_DOCKER === '0') {
      throw new Error(
        'MASTRA_INNGEST_TEST_DOCKER=0 no longer selects an implicit host CLI. ' +
          'Set MASTRA_INNGEST_TEST_MODE=host or external with an explicit binary or endpoint.',
      );
    }
    return 'docker';
  }
  if (configuredMode === 'docker' || configuredMode === 'external' || configuredMode === 'host') {
    return configuredMode;
  }
  throw new Error('MASTRA_INNGEST_TEST_MODE must be docker, external, or host.');
}

export function createLocalTestEndpoints(ports: LocalTestPorts, servePath = '/inngest/api'): LocalTestEndpoints {
  const immutablePorts = Object.freeze({
    inngestPort: ports.inngestPort,
    handlerPort: ports.handlerPort,
  });
  const clientBaseUrl = `http://localhost:${immutablePorts.inngestPort}`;

  return Object.freeze({
    ports: immutablePorts,
    clientBaseUrl,
    devServerUrl: `${clientBaseUrl}/dev`,
    handlerUrl: `http://localhost:${immutablePorts.handlerPort}${servePath}`,
    dockerServeOrigin: `http://host.docker.internal:${immutablePorts.handlerPort}`,
    servePath,
  });
}

export function createInngestTestRuntimeConfig(
  endpoints: LocalTestEndpoints,
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): InngestTestRuntimeConfig {
  const mode = resolveMode(env);
  let runtimeEndpoints = endpoints;
  let registrationServeOrigin: string | undefined;
  let dockerNetwork: string | undefined;
  let hostBinary: string | undefined;

  if (mode === 'external') {
    const externalUrl = env.MASTRA_INNGEST_TEST_EXTERNAL_URL;
    const externalServeOrigin = env.MASTRA_INNGEST_TEST_EXTERNAL_SERVE_ORIGIN;
    if (!externalUrl || !externalServeOrigin) {
      throw new Error(
        'External Inngest mode requires MASTRA_INNGEST_TEST_EXTERNAL_URL and ' +
          'MASTRA_INNGEST_TEST_EXTERNAL_SERVE_ORIGIN.',
      );
    }
    const clientBaseUrl = normalizeOrigin(externalUrl, 'MASTRA_INNGEST_TEST_EXTERNAL_URL');
    registrationServeOrigin = normalizeOrigin(externalServeOrigin, 'MASTRA_INNGEST_TEST_EXTERNAL_SERVE_ORIGIN');
    runtimeEndpoints = Object.freeze({
      ...endpoints,
      clientBaseUrl,
      devServerUrl: `${clientBaseUrl}/dev`,
    });
  } else if (mode === 'host') {
    hostBinary = env.MASTRA_INNGEST_TEST_HOST_BINARY;
    if (!hostBinary || !path.isAbsolute(hostBinary)) {
      throw new Error('Host Inngest mode requires an absolute MASTRA_INNGEST_TEST_HOST_BINARY path.');
    }
  } else {
    const configuredDockerNetwork = env.MASTRA_INNGEST_TEST_DOCKER_NETWORK;
    const configuredServeOrigin = env.MASTRA_INNGEST_TEST_DOCKER_SERVE_ORIGIN;
    const configuredClientUrl = env.MASTRA_INNGEST_TEST_DOCKER_URL;
    const hasNestedDockerConfig = Boolean(configuredDockerNetwork || configuredServeOrigin || configuredClientUrl);
    if (hasNestedDockerConfig && (!configuredDockerNetwork || !configuredServeOrigin || !configuredClientUrl)) {
      throw new Error(
        'Nested Docker routing requires MASTRA_INNGEST_TEST_DOCKER_NETWORK, ' +
          'MASTRA_INNGEST_TEST_DOCKER_URL, and MASTRA_INNGEST_TEST_DOCKER_SERVE_ORIGIN.',
      );
    }
    if (configuredClientUrl) {
      const clientBaseUrl = normalizeOrigin(configuredClientUrl, 'MASTRA_INNGEST_TEST_DOCKER_URL');
      runtimeEndpoints = Object.freeze({
        ...endpoints,
        clientBaseUrl,
        devServerUrl: `${clientBaseUrl}/dev`,
      });
    }
    dockerNetwork = configuredDockerNetwork ?? (platform === 'linux' ? 'host' : undefined);
    registrationServeOrigin = configuredServeOrigin
      ? normalizeOrigin(configuredServeOrigin, 'MASTRA_INNGEST_TEST_DOCKER_SERVE_ORIGIN')
      : dockerNetwork === 'host'
        ? `http://127.0.0.1:${endpoints.ports.handlerPort}`
        : endpoints.dockerServeOrigin;
  }

  return Object.freeze({
    mode,
    endpoints: runtimeEndpoints,
    registrationServeOrigin,
    dockerImage: INNGEST_TEST_DOCKER_IMAGE,
    containerName: INNGEST_TEST_CONTAINER_NAME,
    dockerNetwork,
    hostBinary,
    startupTimeoutMs: readPositiveInteger(env, 'MASTRA_INNGEST_TEST_STARTUP_TIMEOUT_MS', DEFAULT_STARTUP_TIMEOUT_MS),
    registrationTimeoutMs: readPositiveInteger(
      env,
      'MASTRA_INNGEST_TEST_REGISTRATION_TIMEOUT_MS',
      DEFAULT_REGISTRATION_TIMEOUT_MS,
    ),
    pollIntervalMs: readPositiveInteger(env, 'MASTRA_INNGEST_TEST_POLL_INTERVAL_MS', DEFAULT_POLL_INTERVAL_MS),
    requestTimeoutMs: readPositiveInteger(env, 'MASTRA_INNGEST_TEST_REQUEST_TIMEOUT_MS', DEFAULT_REQUEST_TIMEOUT_MS),
    shutdownTimeoutMs: readPositiveInteger(env, 'MASTRA_INNGEST_TEST_SHUTDOWN_TIMEOUT_MS', DEFAULT_SHUTDOWN_TIMEOUT_MS),
  });
}

function createDefaultDependencies(): InngestTestRuntimeDependencies {
  return {
    fetch: globalThis.fetch,
    runCommand: async (file, args) => {
      const result = await execa(file, [...args], { reject: false });
      return {
        exitCode: result.exitCode ?? 1,
        stdout: String(result.stdout ?? ''),
        stderr: String(result.stderr ?? ''),
      };
    },
    spawnCommand: (file, args) => {
      const child = execa(file, [...args], {
        reject: false,
        stdio: 'ignore',
      });
      let exited = false;
      void child.then(
        () => {
          exited = true;
        },
        () => {
          exited = true;
        },
      );
      return {
        kill: signal => child.kill(signal),
        wait: async () => {
          await child;
        },
        hasExited: () => exited,
      };
    },
    sleep: ms => new Promise(resolve => setTimeout(resolve, ms)),
    now: () => Date.now(),
    createOwnerToken: () => randomUUID(),
    signals: {
      on: (signal, listener) => process.on(signal, listener),
      off: (signal, listener) => process.off(signal, listener),
      exit: code => process.exit(code),
    },
  };
}

function mergeDependencies(
  dependencies: Partial<InngestTestRuntimeDependencies> | undefined,
): InngestTestRuntimeDependencies {
  const defaults = createDefaultDependencies();
  return {
    ...defaults,
    ...dependencies,
    signals: {
      ...defaults.signals,
      ...dependencies?.signals,
    },
  };
}

function getFunctionCandidates(data: unknown): string[] {
  if (typeof data !== 'object' || data === null || !('functions' in data)) return [];
  const functions = (data as { functions?: unknown }).functions;
  if (!Array.isArray(functions)) return [];

  return functions.flatMap(entry => {
    if (typeof entry !== 'object' || entry === null) return [];
    const fn = entry as { slug?: unknown; id?: unknown; name?: unknown };
    return [fn.slug, fn.id, fn.name].filter((value): value is string => typeof value === 'string');
  });
}

function functionIdMatches(expectedId: string, candidate: string): boolean {
  return candidate === expectedId || candidate.endsWith(`-${expectedId}`) || candidate.endsWith(`.${expectedId}`);
}

export class InngestTestRuntimeManager {
  readonly config: InngestTestRuntimeConfig;

  private readonly dependencies: InngestTestRuntimeDependencies;
  private readonly dockerOwnerToken: string;
  private readonly signalListeners = new Map<RuntimeSignal, RuntimeSignalListener>();
  private startPromise: Promise<void> | null = null;
  private stopPromise: Promise<void> | null = null;
  private registrationTail: Promise<void> = Promise.resolve();
  private readonly stopController = new AbortController();
  private hostProcess: ManagedProcess | null = null;
  private dockerProcess: ManagedProcess | null = null;
  private ownsDockerContainer = false;
  private ready = false;
  private stopRequested = false;

  constructor(config: InngestTestRuntimeConfig, dependencies?: Partial<InngestTestRuntimeDependencies>) {
    this.config = config;
    this.dependencies = mergeDependencies(dependencies);
    this.dockerOwnerToken = this.dependencies.createOwnerToken();
    if (!this.dockerOwnerToken) {
      throw new Error('Inngest test runtime owner token must not be empty.');
    }
    this.installSignalHandlers();
  }

  get registerOptions():
    | { readonly registerOptions: { readonly serveOrigin: string; readonly servePath: string } }
    | Record<string, never> {
    if (!this.config.registrationServeOrigin) return {};
    return {
      registerOptions: {
        serveOrigin: this.config.registrationServeOrigin,
        servePath: this.config.endpoints.servePath,
      },
    };
  }

  /** Start the policy-owned runtime without HTTP function registration.
   * Used by connect-worker tests whose functions register over the gateway.
   */
  async start(): Promise<void> {
    try {
      await this.ensureRuntimeStarted();
    } catch (error) {
      try {
        await this.stop();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Inngest runtime startup and cleanup both failed.');
      }
      throw error;
    }
  }

  async ensureReady(expectedFunctionIds: readonly string[] = []): Promise<void> {
    try {
      await this.ensureRuntimeStarted();

      const registration = this.registrationTail.then(() => this.waitForFunctionRegistration(expectedFunctionIds));
      this.registrationTail = registration.catch(() => undefined);
      await registration;
    } catch (error) {
      try {
        await this.stop();
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          'Inngest function registration failed and owned-runtime cleanup also failed.',
        );
      }
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;

    this.stopRequested = true;
    this.stopController.abort();
    this.stopPromise = (async () => {
      if (this.startPromise) {
        await this.startPromise.catch(() => undefined);
      }
      try {
        await this.cleanupOwnedRuntime();
        this.removeSignalHandlers();
      } finally {
        this.ready = false;
      }
    })();

    try {
      await this.stopPromise;
    } finally {
      this.stopPromise = null;
    }
  }

  private installSignalHandlers(): void {
    for (const signal of Object.keys(SIGNAL_EXIT_CODES) as RuntimeSignal[]) {
      const listener = () => {
        void this.stop().then(
          () => {
            this.dependencies.signals.exit(SIGNAL_EXIT_CODES[signal]);
          },
          () => {
            this.dependencies.signals.exit(SIGNAL_EXIT_CODES[signal]);
          },
        );
      };
      this.signalListeners.set(signal, listener);
      this.dependencies.signals.on(signal, listener);
    }
  }

  private removeSignalHandlers(): void {
    for (const [signal, listener] of this.signalListeners) {
      this.dependencies.signals.off(signal, listener);
    }
    this.signalListeners.clear();
  }

  private async ensureRuntimeStarted(): Promise<void> {
    if (this.ready) {
      await this.assertRuntimeOwnership();
      return;
    }
    if (this.stopRequested) {
      throw new Error('Inngest test runtime manager has already been stopped.');
    }
    if (this.startPromise) return this.startPromise;

    this.startPromise = (async () => {
      try {
        if (this.config.mode === 'docker') {
          await this.startDockerRuntime();
        } else if (this.config.mode === 'host') {
          this.startHostRuntime();
        }
        await this.waitForDevServer();
        if (this.stopRequested) {
          throw new Error('Inngest test runtime startup was interrupted.');
        }
        this.ready = true;
      } catch (error) {
        this.stopRequested = true;
        this.stopController.abort();
        try {
          await this.cleanupOwnedRuntime();
          this.removeSignalHandlers();
        } catch (cleanupError) {
          throw new AggregateError(
            [error, cleanupError],
            'Inngest test runtime startup failed and owned-runtime cleanup also failed.',
          );
        }
        throw error;
      }
    })();

    try {
      await this.startPromise;
    } finally {
      this.startPromise = null;
    }
  }

  private async startDockerRuntime(): Promise<void> {
    await this.removeStoppedStaleDockerContainer();

    const registrationServeOrigin = this.config.registrationServeOrigin;
    if (!registrationServeOrigin) {
      throw new Error('Docker Inngest mode is missing its registration serve origin.');
    }

    const args = [
      'run',
      '--rm',
      '--sig-proxy=true',
      '--name',
      this.config.containerName,
      '--label',
      INNGEST_TEST_CONTAINER_LABEL,
      '--label',
      `${INNGEST_TEST_CONTAINER_OWNER_LABEL_KEY}=${this.dockerOwnerToken}`,
    ];
    if (this.config.dockerNetwork) {
      args.push('--network', this.config.dockerNetwork);
    }
    if (!this.config.dockerNetwork) {
      args.push('--publish', `${this.config.endpoints.ports.inngestPort}:${this.config.endpoints.ports.inngestPort}`);
    }
    args.push(
      this.config.dockerImage,
      'inngest',
      'dev',
      '-p',
      String(this.config.endpoints.ports.inngestPort),
      '-u',
      `${registrationServeOrigin}${this.config.endpoints.servePath}`,
      '--poll-interval=1',
    );

    this.dockerProcess = this.dependencies.spawnCommand('docker', args);
    this.ownsDockerContainer = true;
  }

  private startHostRuntime(): void {
    const hostBinary = this.config.hostBinary;
    if (!hostBinary) {
      throw new Error('Host Inngest mode is missing its configured binary.');
    }
    this.hostProcess = this.dependencies.spawnCommand(hostBinary, [
      'dev',
      '-p',
      String(this.config.endpoints.ports.inngestPort),
      '-u',
      this.config.endpoints.handlerUrl,
      '--poll-interval=1',
      '--retry-interval=1',
    ]);
  }

  private async waitForDevServer(): Promise<void> {
    const deadline = this.dependencies.now() + this.config.startupTimeoutMs;
    while (this.dependencies.now() < deadline) {
      this.throwIfStopRequested();
      this.assertManagedProcessRunning();

      let response: Response | undefined;
      try {
        response = await this.fetchWithTimeout(this.config.endpoints.devServerUrl);
      } catch {
        this.throwIfStopRequested();
        this.assertManagedProcessRunning();
        // The daemon is still starting.
      }

      if (response?.ok) {
        this.assertManagedProcessRunning();
        if (this.config.mode === 'docker') {
          await this.assertOwnedDockerContainerReady();
          this.assertManagedProcessRunning();
        }
        return;
      }

      await this.sleepUntilNextPoll();
    }
    this.throwIfStopRequested();
    throw new Error(
      `Inngest test runtime did not become ready within ${this.config.startupTimeoutMs}ms ` +
        `at ${this.config.endpoints.devServerUrl}.`,
    );
  }

  private async waitForHandler(): Promise<void> {
    const deadline = this.dependencies.now() + this.config.startupTimeoutMs;
    while (this.dependencies.now() < deadline) {
      this.throwIfStopRequested();
      this.assertManagedProcessRunning();
      try {
        const response = await this.fetchWithTimeout(this.config.endpoints.handlerUrl, {
          method: 'GET',
        });
        this.assertManagedProcessRunning();
        if (response.ok || response.status === 405) return;
      } catch {
        this.throwIfStopRequested();
        this.assertManagedProcessRunning();
        // The handler is still starting.
      }
      await this.sleepUntilNextPoll();
    }
    this.throwIfStopRequested();
    throw new Error(
      `Inngest test handler did not become ready within ${this.config.startupTimeoutMs}ms ` +
        `at ${this.config.endpoints.handlerUrl}.`,
    );
  }

  private async waitForFunctionRegistration(expectedFunctionIds: readonly string[]): Promise<void> {
    await this.waitForHandler();
    await this.triggerRegistration();

    // A successful handler PUT means this exact handler completed its
    // out-of-band registration request. When no concrete IDs are available,
    // that is stronger evidence than accepting any function left in the dev
    // server's registry from an earlier test.
    if (expectedFunctionIds.length === 0) {
      return;
    }

    const registrationTimeoutMs = this.config.registrationTimeoutMs;
    const deadline = this.dependencies.now() + registrationTimeoutMs;
    let nextRegistrationRetry = this.dependencies.now() + REGISTRATION_RETRY_INTERVAL_MS;
    let bestMatchCount = 0;
    let bestMissingFunctionIds = [...expectedFunctionIds];

    while (this.dependencies.now() < deadline) {
      this.assertManagedProcessRunning();
      try {
        const response = await this.fetchWithTimeout(this.config.endpoints.devServerUrl);
        this.assertManagedProcessRunning();
        if (response.ok) {
          const candidates = getFunctionCandidates(await response.json());
          this.assertManagedProcessRunning();

          const missingFunctionIds = expectedFunctionIds.filter(
            expectedId => !candidates.some(candidate => functionIdMatches(expectedId, candidate)),
          );
          const matchCount = expectedFunctionIds.length - missingFunctionIds.length;
          if (matchCount > bestMatchCount) {
            bestMatchCount = matchCount;
            bestMissingFunctionIds = missingFunctionIds;
          }
          if (missingFunctionIds.length === 0) {
            await this.assertRuntimeOwnership();
            return;
          }
        }
      } catch {
        this.throwIfStopRequested();
        this.assertManagedProcessRunning();
        // The registry is not ready yet.
      }

      if (this.dependencies.now() >= nextRegistrationRetry) {
        await this.triggerRegistration();
        nextRegistrationRetry = this.dependencies.now() + REGISTRATION_RETRY_INTERVAL_MS;
      }
      await this.sleepUntilNextPoll();
    }

    throw new Error(
      `Inngest registered ${bestMatchCount}/${expectedFunctionIds.length || 1} expected functions ` +
        `within ${registrationTimeoutMs}ms.` +
        (bestMissingFunctionIds.length > 0 ? ` Missing: ${bestMissingFunctionIds.slice(0, 10).join(', ')}.` : ''),
    );
  }

  private async triggerRegistration(): Promise<void> {
    await this.assertRuntimeOwnership();
    const response = await this.fetchWithTimeout(this.config.endpoints.handlerUrl, {
      method: 'PUT',
    });
    await this.assertRuntimeOwnership();
    if (!response.ok) {
      throw new Error(`Inngest handler registration returned HTTP ${response.status}.`);
    }
  }

  private fetchWithTimeout(input: string, init: RequestInit = {}): Promise<Response> {
    const signals = [AbortSignal.timeout(this.config.requestTimeoutMs), this.stopController.signal];
    if (init.signal) signals.push(init.signal);

    return this.dependencies.fetch(input, {
      ...init,
      signal: AbortSignal.any(signals),
    });
  }

  private throwIfStopRequested(): void {
    if (this.stopRequested) {
      throw new Error('Inngest test runtime startup was interrupted.');
    }
  }

  private assertManagedProcessRunning(): void {
    if (this.config.mode === 'docker' && (!this.dockerProcess || this.dockerProcess.hasExited())) {
      throw new Error('Owned Inngest Docker process is not running.');
    }
    if (this.config.mode === 'host' && (!this.hostProcess || this.hostProcess.hasExited())) {
      throw new Error('Owned Inngest host process is not running.');
    }
  }

  private async assertRuntimeOwnership(): Promise<void> {
    this.throwIfStopRequested();
    this.assertManagedProcessRunning();
    if (this.config.mode === 'docker') {
      await this.assertOwnedDockerContainerReady();
      this.assertManagedProcessRunning();
    }
  }

  private async sleepUntilNextPoll(): Promise<void> {
    this.throwIfStopRequested();

    let rejectOnStop: (() => void) | undefined;
    const stopped = new Promise<never>((_, reject) => {
      rejectOnStop = () => {
        reject(new Error('Inngest test runtime startup was interrupted.'));
      };
      this.stopController.signal.addEventListener('abort', rejectOnStop, { once: true });
      if (this.stopController.signal.aborted) {
        rejectOnStop();
      }
    });

    try {
      await Promise.race([this.dependencies.sleep(this.config.pollIntervalMs), stopped]);
    } finally {
      if (rejectOnStop) {
        this.stopController.signal.removeEventListener('abort', rejectOnStop);
      }
    }
  }

  private async cleanupOwnedRuntime(): Promise<void> {
    const cleanupErrors: unknown[] = [];

    if (this.hostProcess) {
      const hostProcess = this.hostProcess;
      try {
        await this.stopManagedProcess(hostProcess);
        this.hostProcess = null;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    if (this.dockerProcess) {
      const dockerProcess = this.dockerProcess;
      try {
        await this.stopManagedProcess(dockerProcess);
        this.dockerProcess = null;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    if (this.ownsDockerContainer) {
      try {
        await this.ensureOwnedDockerContainerRemoved();
        this.ownsDockerContainer = false;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }

    if (cleanupErrors.length === 1) {
      throw cleanupErrors[0];
    }
    if (cleanupErrors.length > 1) {
      throw new AggregateError(cleanupErrors, 'Multiple Inngest owned-runtime cleanup operations failed.');
    }
  }

  private async stopManagedProcess(managedProcess: ManagedProcess): Promise<void> {
    managedProcess.kill('SIGTERM');

    const exited = await Promise.race([
      managedProcess.wait().then(() => true),
      this.dependencies.sleep(this.config.shutdownTimeoutMs).then(() => false),
    ]);
    if (!exited) {
      managedProcess.kill('SIGKILL');
      await managedProcess.wait();
    }
  }

  private async ensureOwnedDockerContainerRemoved(): Promise<void> {
    const identity = await this.inspectDockerContainer();
    if (!identity) return;
    if (identity.ownerToken !== this.dockerOwnerToken) return;

    this.assertInngestDockerContainerIdentity(identity);
    await this.removeDockerContainer(identity.id);
  }

  private async removeStoppedStaleDockerContainer(): Promise<void> {
    const identity = await this.inspectDockerContainer();
    if (!identity) return;

    if (identity.running) {
      throw new Error(
        `Docker container ${this.config.containerName} is already running and will not be adopted or removed.`,
      );
    }

    this.assertInngestDockerContainerIdentity(identity);
    await this.removeDockerContainer(identity.id);
  }

  private async assertOwnedDockerContainerReady(): Promise<void> {
    const identity = await this.inspectDockerContainer();
    if (!identity) {
      throw new Error(`Owned Inngest Docker container ${this.config.containerName} is not present.`);
    }

    this.assertOwnedDockerContainerIdentity(identity);
    if (!identity.running) {
      throw new Error(`Owned Inngest Docker container ${this.config.containerName} is not running.`);
    }
  }

  private assertOwnedDockerContainerIdentity(identity: DockerContainerIdentity): void {
    this.assertInngestDockerContainerIdentity(identity);
    if (identity.ownerToken !== this.dockerOwnerToken) {
      throw new Error(
        `Docker container ${this.config.containerName} is not owned by this Inngest test runtime manager.`,
      );
    }
  }

  private assertInngestDockerContainerIdentity(identity: DockerContainerIdentity): void {
    if (identity.image !== this.config.dockerImage || identity.runtimeLabel !== INNGEST_TEST_CONTAINER_LABEL_VALUE) {
      throw new Error(
        `Docker container ${this.config.containerName} does not match the Inngest test runtime identity.`,
      );
    }
  }

  private async inspectDockerContainer(): Promise<DockerContainerIdentity | null> {
    const inspect = await this.dependencies.runCommand('docker', [
      'container',
      'inspect',
      '--format',
      DOCKER_INSPECT_FORMAT,
      this.config.containerName,
    ]);
    if (inspect.exitCode !== 0) {
      const detail = inspect.stderr || inspect.stdout || `exit code ${inspect.exitCode}`;
      if (/no such (?:object|container)/i.test(detail)) return null;
      throw new Error(`Failed to inspect Inngest Docker container ${this.config.containerName}: ${detail}`);
    }

    const [id, running, image, runtimeLabel, ownerToken, ...unexpectedFields] = inspect.stdout
      .replace(/\r?\n$/, '')
      .split('\t');
    if (
      !id ||
      (running !== 'true' && running !== 'false') ||
      !image ||
      runtimeLabel === undefined ||
      ownerToken === undefined ||
      unexpectedFields.length > 0
    ) {
      throw new Error(`Docker returned an invalid identity for Inngest container ${this.config.containerName}.`);
    }

    return {
      id,
      running: running === 'true',
      image,
      runtimeLabel,
      ownerToken,
    };
  }

  private async removeDockerContainer(containerId: string): Promise<void> {
    const result = await this.dependencies.runCommand('docker', ['rm', '--force', containerId]);
    if (result.exitCode === 0) return;

    const detail = result.stderr || result.stdout || `exit code ${result.exitCode}`;
    if (/no such (?:object|container)/i.test(detail)) return;
    throw new Error(`Failed to remove Inngest Docker container ${containerId}: ${detail}`);
  }
}
