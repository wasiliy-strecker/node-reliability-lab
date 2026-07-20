export type ControlWorkerInput =
  | { readonly kind: 'echo'; readonly value: string }
  | { readonly kind: 'context' }
  | { readonly kind: 'fail' }
  | { readonly kind: 'crash' }
  | { readonly kind: 'hold'; readonly gate: SharedArrayBuffer; readonly value: string }
  | { readonly kind: 'spin'; readonly iterations: number }

export interface ControlWorkerOutput {
  readonly value: string
  readonly requestId?: string
}
