import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const canonicalRepository = 'CuddlerAI/n8n-nodes-cuddler';
const packageName = 'n8n-nodes-cuddler';
const releaseEnvironment = 'npm-release';

export function validateReleaseContext(environment, manifest, head) {
  assert.equal(environment.GITHUB_ACTIONS, 'true', 'release requires GitHub Actions');
  assert.equal(environment.GITHUB_EVENT_NAME, 'workflow_dispatch', 'release requires manual dispatch');
  assert.equal(environment.GITHUB_REPOSITORY, canonicalRepository, 'release requires the canonical repository');
  assert.equal(environment.RELEASE_REPOSITORY_VISIBILITY, 'public', 'provenance requires public source');
  assert.equal(environment.GITHUB_REF, 'refs/heads/main', 'release requires main');
  assert.equal(environment.GITHUB_WORKFLOW_REF,
    `${canonicalRepository}/.github/workflows/publish.yml@refs/heads/main`, 'unexpected release workflow');
  assert.equal(environment.RELEASE_RUNNER_ENVIRONMENT, 'github-hosted', 'release requires a GitHub-hosted runner');
  assert.match(environment.RELEASE_EXPECTED_SHA ?? '', /^[a-f0-9]{40}$/, 'expected SHA must be 40 lowercase hex characters');
  assert.equal(environment.GITHUB_SHA, environment.RELEASE_EXPECTED_SHA, 'dispatch SHA differs from the reviewed SHA');
  assert.equal(head, environment.RELEASE_EXPECTED_SHA, 'checkout differs from the reviewed SHA');
  assert.equal(manifest.name, packageName, 'unexpected release package');
  assert.equal(manifest.version, environment.RELEASE_EXPECTED_VERSION, 'release version differs from the reviewed version');
  assert.match(manifest.version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, 'unexpected release version');
  assert.equal(manifest.private ?? false, false, 'release package must be public');
  assert.equal(manifest.repository?.url, `git+https://github.com/${canonicalRepository}.git`, 'unexpected source repository');
  assert.equal(manifest.publishConfig?.access, 'public', 'release package must explicitly select public access');
  assert.notEqual(manifest.publishConfig?.provenance, false, 'provenance cannot be disabled');
  assert.match(environment.RELEASE_ACCEPTANCE_URL ?? '',
    /^https:\/\/github\.com\/CuddlerAI\/(?:Cuddler|n8n-nodes-cuddler)\/(?:issues\/\d+(?:#issuecomment-\d+)?|actions\/runs\/\d+)$/,
    'acceptance evidence must be a redacted canonical GitHub issue/comment or Actions run');
  assert.match(environment.RELEASE_ACCEPTANCE_SHA256 ?? '', /^[a-f0-9]{64}$/, 'acceptance receipt requires a SHA-256 digest');
  for (const [key, value] of Object.entries(environment)) {
    if (/^(?:NODE_AUTH_TOKEN|NPM_TOKEN|NPM_CONFIG_.*(?:_AUTH|TOKEN|PASSWORD))$/i.test(key)) {
      assert.ok(!value, 'npm token or password fallback is forbidden');
    }
  }
}

export function validateReleaseEnvironment(environment) {
  assert.equal(environment.name, releaseEnvironment, 'unexpected release environment');
  assert.ok(environment.protection_rules?.some((rule) =>
    rule.type === 'required_reviewers' && Array.isArray(rule.reviewers) && rule.reviewers.some((entry) =>
      ['User', 'Team'].includes(entry.type) && Number.isSafeInteger(entry.reviewer?.id) && entry.reviewer.id > 0)),
  'npm-release must have configured required reviewers');
}

async function githubPublicJson(path) {
  const response = await fetch(`https://api.github.com/repos/${canonicalRepository}/${path}`, {
    headers: {
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...(process.env.GH_TOKEN ? { Authorization: `Bearer ${process.env.GH_TOKEN}` } : {}),
    },
    redirect: 'error',
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(response.status, 200, 'public release protection could not be verified');
  return response.json();
}

async function checkRelease() {
  const head = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  const manifest = JSON.parse(readFileSync('package.json', 'utf8'));
  validateReleaseContext(process.env, manifest, head);
  assert.equal(existsSync('.npmrc'), false, 'project npm configuration is forbidden during release');
  for (const key of ['NPM_CONFIG_USERCONFIG', 'NPM_CONFIG_GLOBALCONFIG']) {
    assert.ok(process.env[key], 'release requires explicit empty npm configuration');
    assert.equal(readFileSync(process.env[key], 'utf8'), '', 'release npm configuration must be empty');
  }
  assert.equal(execFileSync('git', ['status', '--porcelain', '--untracked-files=normal'], { encoding: 'utf8' }).trim(),
    '', 'release checkout must remain clean');
  validateReleaseEnvironment(await githubPublicJson(`environments/${releaseEnvironment}`));
  const branch = await githubPublicJson('branches/main');
  assert.equal(branch.commit?.sha, head, 'main moved after the reviewed dispatch; review a new release');
  const response = await fetch(`https://registry.npmjs.org/${packageName}`, {
    redirect: 'error', signal: AbortSignal.timeout(15000),
  });
  assert.equal(response.status, 200, 'existing npm package control must be established before release');
  const published = await response.json();
  assert.equal(published.name, packageName, 'unexpected npm package');
  assert.ok(Array.isArray(published.maintainers) && published.maintainers.length > 0, 'npm package has no established maintainer');
  assert.equal(Object.hasOwn(published.versions ?? {}, manifest.version), false, 'release version is already published');
  if (process.argv.includes('--publishing')) {
    const [major, minor, patch] = execFileSync('npm', ['--version'], { encoding: 'utf8' }).trim().split('.').map(Number);
    assert.ok(major > 11 || (major === 11 && (minor > 5 || (minor === 5 && patch >= 1))),
      'trusted publishing requires npm >=11.5.1');
    assert.ok(process.env.ACTIONS_ID_TOKEN_REQUEST_URL && process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN,
      'trusted publishing requires the GitHub OIDC environment');
  }
  console.log('Release identity, current main and configured reviewers verified; the guard does not evaluate acceptance receipt contents.');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  checkRelease().catch(() => {
    console.error('Release guard failed; verify the reviewed identity, redacted receipt, protected reviewers, npm configuration and publisher prerequisites.');
    process.exitCode = 1;
  });
}
