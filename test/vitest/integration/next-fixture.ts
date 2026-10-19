import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { join } from 'path';
import fetch from 'node-fetch';

const STOP_TIMEOUT_MS = 5_000;

export type NextFixture = {
  urls: string[];
  /** Kills every server process group, or keeps them if KEEP_SERVER_RUNNING=true. */
  stop(): Promise<void>;
};

export type NextFixtureOptions = {
  appDir: string;
  ports: number[];
  /** Run `pnpm i` and `pnpm build` first. Default: SKIP_BUILD !== 'true'. */
  build?: boolean;
  /** Extra environment for `next start`, on top of process.env. */
  env?: NodeJS.ProcessEnv;
  /** Path that must answer 2xx before the server counts as ready. */
  readyPath?: string;
  readyTimeoutMs?: number;
};

/**
 * Builds (optionally) and starts one `next start` per port. Each server runs
 * in its own process group, so stop() kills npx and the next server it spawns,
 * and nothing else. If any server fails to start, the ones already started
 * are stopped before the error is thrown.
 */
export async function startNextFixture({
  appDir,
  ports,
  build = process.env.SKIP_BUILD !== 'true',
  env,
  readyPath = '/',
  readyTimeoutMs = 20_000,
}: NextFixtureOptions): Promise<NextFixture> {
  await Promise.all(ports.map((port) => stopLeftoverServer(appDir, port)));
  await Promise.all(ports.map(assertPortIsFree));

  if (build) {
    await runCommand('pnpm', ['i'], appDir);
    await runCommand('pnpm', ['build'], appDir);
  }

  const servers = ports.map((port) => startServer(appDir, port, env));
  const stop = async () => {
    if (process.env.KEEP_SERVER_RUNNING === 'true') {
      for (const { proc } of servers) proc.unref();
      console.log('keeping next server(s) running');
      return;
    }
    await Promise.all(servers.map((server) => stopServer(appDir, server)));
  };

  try {
    await Promise.all(
      servers.map((server) =>
        waitForServer(server, server.url + readyPath, readyTimeoutMs),
      ),
    );
  } catch (error) {
    await Promise.all(servers.map((server) => stopServer(appDir, server)));
    throw error;
  }

  return { urls: servers.map(({ url }) => url), stop };
}

export async function runCommand(cmd: string, args: string[], cwd: string) {
  const proc = spawn(cmd, args, { cwd, stdio: 'pipe' });
  const output = collectOutput(proc, `${cmd} ${args.join(' ')}`);
  const code = await new Promise<number | null>((resolve) =>
    proc.on('close', resolve),
  );
  if (code !== 0) {
    throw new Error(
      `${cmd} ${args.join(' ')} failed with code ${code}\n${output()}`,
    );
  }
}

type Server = {
  port: number;
  url: string;
  proc: ChildProcessWithoutNullStreams;
  output: () => string;
};

function startServer(
  appDir: string,
  port: number,
  env: NodeJS.ProcessEnv | undefined,
): Server {
  const proc = spawn('npx', ['next', 'start', '-p', String(port)], {
    cwd: appDir,
    env: { ...process.env, ...env },
    stdio: 'pipe',
    detached: true,
  });
  if (proc.pid !== undefined) {
    mkdirSync(join(appDir, '.next'), { recursive: true });
    writeFileSync(serverPidFile(appDir, port), String(proc.pid));
  }
  const output = collectOutput(proc, `next :${port}`, true);
  return { port, url: `http://localhost:${port}`, proc, output };
}

function collectOutput(
  proc: ChildProcessWithoutNullStreams,
  label: string,
  alwaysLogStderr = false,
) {
  let output = '';
  proc.stdout.on('data', (data) => {
    output += data;
    if (process.env.DEBUG_INTEGRATION) console.log(`[${label}] ${data}`);
  });
  proc.stderr.on('data', (data) => {
    output += data;
    if (alwaysLogStderr || process.env.DEBUG_INTEGRATION) {
      console.error(`[${label}] ${data}`);
    }
  });
  return () => output;
}

async function waitForServer(server: Server, url: string, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (server.proc.exitCode !== null || server.proc.signalCode !== null) {
      throw new Error(
        `next start on port ${server.port} exited before it was ready\n${server.output()}`,
      );
    }
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(
    `next start on port ${server.port} was not ready within ${timeoutMs} ms\n${server.output()}`,
  );
}

// A server this fixture does not own would answer the readiness check, so
// fail instead of testing against it.
async function assertPortIsFree(port: number) {
  const inUse = await fetch(`http://localhost:${port}`).then(
    () => true,
    () => false,
  );
  if (inUse) {
    throw new Error(
      `Port ${port} is already in use. Stop the process that listens on it.`,
    );
  }
}

function groupIsAlive(pgid: number) {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

async function killProcessGroup(pgid: number) {
  if (!groupIsAlive(pgid)) return;
  process.kill(-pgid, 'SIGTERM');
  const deadline = Date.now() + STOP_TIMEOUT_MS;
  while (groupIsAlive(pgid) && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  if (groupIsAlive(pgid)) process.kill(-pgid, 'SIGKILL');
}

async function stopServer(appDir: string, server: Server) {
  if (server.proc.pid !== undefined) await killProcessGroup(server.proc.pid);
  rmSync(serverPidFile(appDir, server.port), { force: true });
}

// Each server records its process group until stop() kills it. A file that
// is still there belongs to a run that was interrupted or that used
// KEEP_SERVER_RUNNING, so the next run stops exactly that server before it
// reuses the port.
function serverPidFile(appDir: string, port: number) {
  return join(appDir, '.next', `next-fixture-${port}.pid`);
}

async function stopLeftoverServer(appDir: string, port: number) {
  const file = serverPidFile(appDir, port);
  if (!existsSync(file)) return;
  const pgid = Number(readFileSync(file, 'utf8'));
  rmSync(file);
  if (Number.isInteger(pgid) && pgid > 0) await killProcessGroup(pgid);
}
