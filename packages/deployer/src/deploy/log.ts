import { Writable } from 'node:stream';
import type { IMastraLogger } from '@mastra/core/logger';
import { execa } from 'execa';

export const createPinoStream = (logger: IMastraLogger) => {
  return new Writable({
    write(chunk, _encoding, callback) {
      // Convert Buffer/string to string and trim whitespace
      const line = chunk.toString().trim();

      if (line) {
        console.info(line);
        // Log each line through Pino
        logger.info(line);
      }

      callback();
    },
  });
};

export function createChildProcessLogger({ logger, root }: { logger: IMastraLogger; root: string }) {
  const pinoStream = createPinoStream(logger);
  return async ({ cmd, args, env }: { cmd: string; args: string[]; env: Record<string, string> }) => {
    try {
      const subprocess = execa(cmd, args, {
        ...(root ? { cwd: root } : {}),
        env,
        buffer: false,
        extendEnv: false,
        shell: false,
        stdin: 'ignore',
        stdout: 'pipe',
        stderr: 'pipe',
      });

      subprocess.stdout?.pipe(pinoStream, { end: false });
      subprocess.stderr?.pipe(pinoStream, { end: false });

      await subprocess;
      pinoStream.end();
      return { success: true };
    } catch (error) {
      const processError = error as {
        name?: unknown;
        exitCode?: unknown;
        signal?: unknown;
        timedOut?: unknown;
        isCanceled?: unknown;
      };
      logger.error('Process failed', {
        error: {
          name: typeof processError?.name === 'string' ? processError.name : 'Error',
          exitCode: typeof processError?.exitCode === 'number' ? processError.exitCode : undefined,
          signal: typeof processError?.signal === 'string' ? processError.signal : undefined,
          timedOut: processError?.timedOut === true,
          isCanceled: processError?.isCanceled === true,
        },
      });
      pinoStream.end();
      throw error;
    }
  };
}
