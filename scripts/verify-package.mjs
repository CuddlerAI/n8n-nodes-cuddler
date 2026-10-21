import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstatSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const packageName = 'n8n-nodes-cuddler';
const repositoryUrl = 'https://github.com/CuddlerAI/n8n-nodes-cuddler';
const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*)?$/;
const allowedPath = /^(?:package\.json|README\.md|LICENSE|dist\/(?:nodes|credentials)\/[A-Za-z0-9_./-]+\.(?:js|d\.ts|js\.map|svg))$/;
const temporaryDirectory = mkdtempSync(join(tmpdir(), 'n8n-cuddler-package-'));

function validateMetadata(manifest) {
  assert.equal(manifest.name, packageName, 'unexpected public package name');
  assert.ok(semver.test(manifest.version), 'package version must be valid semver');
  assert.notEqual(manifest.private, true, 'public package must not be private');
  assert.equal(manifest.license, 'MIT', 'package license must be MIT');
  assert.ok(manifest.keywords?.includes('n8n-community-node-package'), 'missing n8n package keyword');
  const repository = typeof manifest.repository === 'string' ? manifest.repository : manifest.repository?.url;
  assert.equal(repository?.replace(/^git\+/, '').replace(/\.git$/, ''), repositoryUrl, 'repository must be canonical');
  for (const field of ['dependencies', 'optionalDependencies']) {
    assert.equal(Object.keys(manifest[field] ?? {}).length, 0, `${field} must remain empty`);
  }
  for (const field of ['bundledDependencies', 'bundleDependencies']) {
    assert.ok(manifest[field] === undefined || (Array.isArray(manifest[field]) && manifest[field].length === 0), `${field} must remain empty`);
  }
  assert.deepEqual(manifest.peerDependencies, { 'n8n-workflow': '*' }, 'n8n-workflow must remain the host peer');
  assert.equal(manifest.n8n?.n8nNodesApiVersion, 1, 'unexpected n8n node API version');
  assert.equal(manifest.n8n?.strict, true, 'n8n strict mode must remain enabled');
  const entries = [];
  for (const kind of ['nodes', 'credentials']) {
    const paths = manifest.n8n[kind];
    assert.ok(Array.isArray(paths) && paths.length > 0, `missing n8n ${kind} entries`);
    for (const path of paths) {
      assert.ok(typeof path === 'string' && path.startsWith(`dist/${kind}/`) && path.endsWith('.js'), `invalid n8n ${kind} entry`);
      validatePath(path);
      entries.push(path);
    }
  }
  assert.equal(new Set(entries).size, entries.length, 'duplicate n8n entries');
  return entries;
}

function validatePath(path) {
  assert.ok(typeof path === 'string' && allowedPath.test(path), `unexpected packed path: ${path}`);
  assert.ok(path.split('/').every((part) => part !== '.' && part !== '..' && part !== ''), `unsafe packed path: ${path}`);
}

function tar(args) {
  return execFileSync('tar', args, { encoding: null, maxBuffer: 32 * 1024 * 1024 });
}

try {
  const sourceManifest = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8'));
  const entries = validateMetadata(sourceManifest);
  const emptyUserConfig = join(temporaryDirectory, 'user.npmrc');
  const emptyGlobalConfig = join(temporaryDirectory, 'global.npmrc');
  writeFileSync(emptyUserConfig, '');
  writeFileSync(emptyGlobalConfig, '');

  // Run from the owned directory so a project/user .npmrc cannot supply registry credentials.
  // An absolute local package path, offline mode and ignored scripts keep packing local.
  const environment = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(npm_|node_auth_token$)/i.test(key)));
  const npmArguments = [
    'pack', packageRoot, '--ignore-scripts', '--offline', '--json', '--workspaces=false',
    '--audit=false', '--fund=false', '--update-notifier=false',
    `--pack-destination=${temporaryDirectory}`, `--cache=${join(temporaryDirectory, 'cache')}`,
    `--userconfig=${emptyUserConfig}`, `--globalconfig=${emptyGlobalConfig}`,
  ];
  const npmCli = process.env.npm_execpath;
  const [command, args] = npmCli ? [process.execPath, [npmCli, ...npmArguments]] : ['npm', npmArguments];
  const reports = JSON.parse(execFileSync(command, args, {
    cwd: temporaryDirectory,
    env: environment,
    encoding: 'utf8',
    maxBuffer: 4 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
  }));
  assert.ok(Array.isArray(reports) && reports.length === 1, 'npm must create exactly one archive');
  const report = reports[0];
  assert.equal(report.name, sourceManifest.name, 'npm report package name mismatch');
  assert.equal(report.version, sourceManifest.version, 'npm report package version mismatch');
  assert.equal(report.filename, `${packageName}-${sourceManifest.version}.tgz`, 'unexpected archive filename');
  assert.ok(Array.isArray(report.files) && report.files.length > 0, 'npm report has no files');
  assert.deepEqual(report.bundled, [], 'archive must not bundle dependencies');
  const archive = join(temporaryDirectory, report.filename);
  assert.ok(lstatSync(archive).isFile(), 'archive must be a regular file');
  const archiveBytes = readFileSync(archive);
  assert.equal(report.size, archiveBytes.length, 'archive size differs from npm report');
  assert.equal(report.shasum, createHash('sha1').update(archiveBytes).digest('hex'), 'archive SHA-1 differs from npm report');
  assert.equal(report.integrity, `sha512-${createHash('sha512').update(archiveBytes).digest('base64')}`, 'archive integrity differs from npm report');

  const members = tar(['-tf', archive]).toString('utf8').trimEnd().split('\n');
  const verboseMembers = tar(['-tvf', archive]).toString('utf8').trimEnd().split('\n');
  assert.equal(members.length, verboseMembers.length, 'tar listings disagree');
  assert.ok(verboseMembers.every((line) => line.startsWith('-')), 'archive must contain only regular files, without links or directories');
  assert.equal(new Set(members).size, members.length, 'duplicate tar archive members');
  const packedPaths = members.map((member) => {
    assert.ok(member.startsWith('package/'), `unexpected archive root: ${member}`);
    const path = member.slice('package/'.length);
    validatePath(path);
    return path;
  });
  const reportPaths = report.files.map((file) => {
    validatePath(file.path);
    assert.ok(Number.isSafeInteger(file.size) && file.size >= 0, `invalid npm file size: ${file.path}`);
    return file.path;
  });
  assert.equal(new Set(reportPaths).size, reportPaths.length, 'duplicate npm manifest paths');
  assert.deepEqual([...packedPaths].sort(), [...reportPaths].sort(), 'actual tar members differ from npm manifest');
  assert.equal(report.entryCount, packedPaths.length, 'archive member count differs from npm report');
  for (const required of ['package.json', 'README.md', 'LICENSE', ...entries]) {
    assert.ok(packedPaths.includes(required), `required package file missing: ${required}`);
  }

  let unpackedSize = 0;
  for (const file of report.files) {
    const source = join(packageRoot, file.path);
    assert.ok(lstatSync(source).isFile(), `source must be a regular file: ${file.path}`);
    // Read a validated member to stdout; never extract paths into the filesystem.
    const actual = tar(['-xOf', archive, `package/${file.path}`]);
    assert.equal(actual.length, file.size, `packed size differs from npm manifest: ${file.path}`);
    assert.ok(actual.equals(readFileSync(source)), `packed bytes differ from source: ${file.path}`);
    if (file.path === 'package.json') {
      const packedManifest = JSON.parse(actual.toString('utf8'));
      validateMetadata(packedManifest);
      assert.deepEqual(packedManifest, sourceManifest, 'packed metadata differs from source');
    }
    unpackedSize += actual.length;
  }
  assert.equal(report.unpackedSize, unpackedSize, 'unpacked size differs from npm report');
  const sha256 = createHash('sha256').update(archiveBytes).digest('hex');
  console.log(`Package audit passed: ${report.filename}; ${members.length} regular files; ${entries.length} n8n entries; ${archiveBytes.length} bytes; sha256=${sha256}`);
} catch (error) {
  console.error(`Package audit failed: ${error.message}`);
  process.exitCode = 1;
} finally {
  rmSync(temporaryDirectory, { recursive: true, force: true });
}
