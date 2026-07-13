import fsSync from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import * as p from '@clack/prompts';
import { execa } from 'execa';
import color from 'picocolors';

import { DepsService } from '../../services/service.deps.js';
import type { PackageManager } from '../../utils/package-manager.js';
import { interactivePrompt } from '../init/utils.js';
import type { LLMProvider } from '../init/utils.js';
import { getPackageManager, isGitInitialized } from '../utils.js';

function getInitArgs(pm: PackageManager): string[] {
  switch (pm) {
    case 'npm':
      return ['init', '-y'];
    case 'pnpm':
      return ['init'];
    case 'yarn':
      return ['init', '-y'];
    case 'bun':
      return ['init', '-y'];
    default:
      return ['init', '-y'];
  }
}

async function initializePackageJson(pm: PackageManager): Promise<void> {
  await execa(pm, getInitArgs(pm));

  // Read and update package.json directly (more reliable than pkg set)
  const packageJsonPath = path.join(process.cwd(), 'package.json');
  const packageJson = JSON.parse(await fs.readFile(packageJsonPath, 'utf-8'));

  packageJson.type = 'module';
  packageJson.engines = {
    ...packageJson.engines,
    node: '>=22.13.0',
  };

  // pnpm v11+ writes devEngines.packageManager with a semver range (e.g.
  // "^11.3.0"). Corepack ≤0.35.0 (bundled with Node 22) reads this field
  // too and rejects ranges, so we must remove both the legacy packageManager
  // field and devEngines.packageManager to avoid the error:
  //   "Invalid package manager specification in package.json (pnpm@^11.3.0)"
  delete packageJson.packageManager;
  if (packageJson.devEngines?.packageManager) {
    delete packageJson.devEngines.packageManager;
    if (Object.keys(packageJson.devEngines).length === 0) {
      delete packageJson.devEngines;
    }
  }

  await fs.writeFile(packageJsonPath, JSON.stringify(packageJson, null, 2));
}

const writeReadmeFile = async ({ dirPath, projectName }: { dirPath: string; projectName: string }) => {
  const packageManager = getPackageManager();
  const readmePath = path.join(dirPath, 'README.md');

  const content = `# ${projectName}

Welcome to your new [Mastra](https://mastra.ai/) project! We're excited to see what you'll build.

## Getting Started

Start the development server:

\`\`\`shell
${packageManager} run dev
\`\`\`

Open [http://localhost:4111](http://localhost:4111) in your browser to access [Mastra Studio](https://mastra.ai/docs/studio/overview). It provides an interactive UI for building and testing your agents, along with a REST API that exposes your Mastra application as a local service. This lets you start building without worrying about integration right away.

You can start editing files inside the \`src/mastra\` directory. The development server will automatically reload whenever you make changes.

## Learn more

To learn more about Mastra, visit our [documentation](https://mastra.ai/docs/). Your bootstrapped project includes example code for [agents](https://mastra.ai/docs/agents/overview), [tools](https://mastra.ai/docs/agents/using-tools), [workflows](https://mastra.ai/docs/workflows/overview), [scorers](https://mastra.ai/docs/evals/overview), and [observability](https://mastra.ai/docs/observability/overview).

If you're new to AI agents, check out our [course](https://mastra.ai/learn) and [YouTube videos](https://youtube.com/@mastra-ai). You can also join our [Discord](https://discord.gg/BTYqqHKUrf) community to get help and share your projects.

## Deploy to the Mastra platform

The [Mastra platform](https://projects.mastra.ai) provides two products for deploying and managing AI applications built with the Mastra framework:

- **Studio**: A hosted visual environment for testing agents, running workflows, and inspecting traces
- **Server**: A production deployment target that runs your Mastra application as an API server

Learn more in the [Mastra platform documentation](https://mastra.ai/docs/mastra-platform/overview).`;

  await fs.writeFile(readmePath, content);
};

async function installMastraDependencies(
  depsService: DepsService,
  dependencies: string[],
  versionTag: string,
  isDev: boolean,
  timeout?: number,
) {
  const dependenciesWithVersion = dependencies.map(dependency => `${dependency}${versionTag}`);

  try {
    await depsService.installPackages(dependenciesWithVersion, { dev: isDev, timeout });
  } catch (err) {
    if (versionTag === '@latest') {
      throw new Error(
        `Failed to install ${dependenciesWithVersion.join(' ')}: ${err instanceof Error ? err.message : 'Unknown error'}`,
      );
    }

    const latestDependencies = dependencies.map(dependency => `${dependency}@latest`);
    try {
      await depsService.installPackages(latestDependencies, { dev: isDev, timeout });
    } catch (fallbackErr) {
      throw new Error(
        `Failed to install ${dependencies.join(', ')} (tried ${versionTag} and @latest): ${fallbackErr instanceof Error ? fallbackErr.message : 'Unknown error'}`,
      );
    }
  }
}

export const createMastraProject = async ({
  projectName: name,
  createVersionTag,
  timeout,
  llmProvider,
  llmApiKey,
  skills,
  mcpServer,
  observability,
  needsInteractive,
  onObservabilitySelected,
}: {
  projectName?: string;
  createVersionTag?: string;
  timeout?: number;
  llmProvider?: LLMProvider;
  llmApiKey?: string;
  skills?: string[];
  mcpServer?: string;
  observability?: boolean;
  needsInteractive?: boolean;
  onObservabilitySelected?: (event: {
    command?: 'create' | 'init';
    enabled: boolean;
    answer: 'yes' | 'no';
    selection_method: 'interactive';
  }) => void;
}) => {
  p.intro(color.inverse(' Mastra Create '));

  const projectName =
    name ??
    (await p.text({
      message: 'What do you want to name your project?',
      placeholder: 'my-mastra-app',
      validate: value => {
        if (!value || value.length === 0) return 'Project name cannot be empty';
        if (fsSync.existsSync(value)) {
          return `A directory named "${value}" already exists. Please choose a different name.`;
        }
      },
    }));

  if (p.isCancel(projectName)) {
    p.cancel('Operation cancelled');
    process.exit(0);
  }

  let result: Awaited<ReturnType<typeof interactivePrompt>> | undefined = undefined;

  if (needsInteractive) {
    const skipGitInit = await isGitInitialized({ cwd: process.cwd() });

    result = await interactivePrompt({
      options: { command: 'create', showBanner: false, onObservabilitySelected },
      skip: {
        llmProvider: llmProvider !== undefined,
        llmApiKey: llmApiKey !== undefined,
        skills: skills !== undefined && skills.length > 0,
        mcpServer: mcpServer !== undefined,
        observability: observability !== undefined,
        directory: true,
        gitInit: skipGitInit,
      },
    });
  }
  const s = p.spinner();
  const originalCwd = process.cwd();
  let projectPath: string | null = null;

  try {
    s.start('Creating project');
    try {
      await fs.mkdir(projectName);
      projectPath = path.resolve(originalCwd, projectName);
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'EEXIST') {
        s.stop(`A directory named "${projectName}" already exists. Please choose a different name.`);
        process.exit(1);
      }
      throw new Error(
        `Failed to create project directory: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }

    process.chdir(projectName);
    const pm = getPackageManager();
    const depsService = new DepsService(pm);

    s.message('Initializing project structure');
    try {
      await initializePackageJson(pm);
      await depsService.addScriptsToPackageJson({
        dev: 'mastra dev',
        build: 'mastra build',
        start: 'mastra start',
      });
      await writeReadmeFile({ dirPath: process.cwd(), projectName });
    } catch (error) {
      throw new Error(
        `Failed to initialize project structure: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }

    // Write pnpm workspace config for pnpm v11
    if (pm === 'pnpm') {
      await fs.writeFile(
        'pnpm-workspace.yaml',
        `packages:
  - '.'
allowBuilds:
  esbuild: true
  sharp: true
onlyBuiltDependencies:
  - esbuild
  - sharp
`,
      );
    }

    s.stop('Project structure created');

    s.start(`Installing ${pm} dependencies`);
    try {
      await depsService.installPackages(['zod@^4'], { timeout });
      await depsService.installPackages(['typescript', '@types/node'], { dev: true, timeout });
      await fs.writeFile(
        'tsconfig.json',
        `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true,
    "outDir": "dist"
  },
  "include": [
    "src/**/*"
  ]
}`,
      );
    } catch (error) {
      throw new Error(
        `Failed to install basic dependencies: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }

    s.stop(`${pm} dependencies installed`);

    s.start('Installing Mastra CLI');
    const versionTag = createVersionTag ? `@${createVersionTag}` : '@latest';

    try {
      await installMastraDependencies(depsService, ['mastra'], versionTag, true, timeout);
    } catch (error) {
      throw new Error(`Failed to install Mastra CLI: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
    s.stop('Mastra CLI installed');

    s.start('Installing Mastra dependencies');
    try {
      await installMastraDependencies(
        depsService,
        ['@mastra/core', '@mastra/libsql', '@mastra/memory'],
        versionTag,
        false,
        timeout,
      );
    } catch (error) {
      throw new Error(
        `Failed to install Mastra dependencies: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
    s.stop('Mastra dependencies installed');

    s.start('Adding .gitignore');
    try {
      await fs.writeFile(
        '.gitignore',
        [
          'output.txt',
          'node_modules',
          'dist',
          '.mastra',
          '.env.development',
          '.env',
          '*.db',
          '*.db-*',
          '.netlify',
          '.vercel',
          '',
        ].join('\n'),
      );
    } catch (error) {
      throw new Error(`Failed to create .gitignore: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
    s.stop('.gitignore added');

    p.outro('Project created successfully');
    console.info('');

    return { projectName, result };
  } catch (error) {
    s.stop();

    const errorMessage = error instanceof Error ? error.message : 'An unexpected error occurred';
    p.cancel(`Project creation failed: ${errorMessage}`);

    // Clean up: remove the created directory on failure
    if (projectPath && fsSync.existsSync(projectPath)) {
      try {
        // Change back to original directory before cleanup
        process.chdir(originalCwd);
        await fs.rm(projectPath, { recursive: true, force: true });
      } catch (cleanupError) {
        // Log but don't throw - we want to exit with the original error
        console.error(
          `Warning: Failed to clean up project directory: ${cleanupError instanceof Error ? cleanupError.message : 'Unknown error'}`,
        );
      }
    }

    process.exit(1);
  }
};
