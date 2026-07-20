import type { ServerResponse } from 'node:http'

export interface ProblemDetails {
  readonly type: string
  readonly title: string
  readonly status: number
  readonly detail: string
  readonly requestId: string
  readonly lineNumber?: number
}

export function sendJson(
  response: ServerResponse,
  status: number,
  value: unknown,
  headers: Readonly<Record<string, string>> = {},
): void {
  if (response.headersSent || response.destroyed) return
  const body = `${JSON.stringify(value)}\n`
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(Buffer.byteLength(body)),
    ...headers,
  })
  response.end(body)
}

export function sendProblem(
  response: ServerResponse,
  problem: ProblemDetails,
  headers: Readonly<Record<string, string>> = {},
): void {
  if (response.headersSent || response.destroyed) return
  const body = `${JSON.stringify(problem)}\n`
  response.writeHead(problem.status, {
    'content-type': 'application/problem+json; charset=utf-8',
    'content-length': String(Buffer.byteLength(body)),
    ...headers,
  })
  response.end(body)
}
