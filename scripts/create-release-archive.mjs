/* global process, URL */

import { createHash } from 'node:crypto'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { spawnSync } from 'node:child_process'

const tag = process.argv[2]
const packageMetadata = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
)
const expectedTag = `v${packageMetadata.version}`
if (tag !== expectedTag) {
  throw new Error(`Release tag ${tag ?? '<missing>'} does not match package version ${expectedTag}`)
}

const artifacts = new URL('../artifacts/', import.meta.url)
mkdirSync(artifacts, { recursive: true })
const archiveName = `node-reliability-lab-${tag}.tar.gz`
const archive = new URL(archiveName, artifacts)
const tar = spawnSync(
  'tar',
  [
    '-czf',
    archive.pathname,
    'dist/src',
    'fixtures',
    'package.json',
    'README.md',
    'CHANGELOG.md',
    'LICENSE',
  ],
  { cwd: new URL('..', import.meta.url), encoding: 'utf8' },
)
if (tar.status !== 0) throw new Error(tar.stderr || 'tar failed')

const checksum = createHash('sha256').update(readFileSync(archive)).digest('hex')
writeFileSync(new URL(`${archiveName}.sha256`, artifacts), `${checksum}  ${archiveName}\n`)
process.stdout.write(`${archive.pathname}\n`)
