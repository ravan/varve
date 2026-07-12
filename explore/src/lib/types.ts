export type ExecutionMode = 'read' | 'write';

export type JsonScalar = null | boolean | number | string | { $bytes: string };

export type QueryParameters = Record<string, JsonScalar>;

export type Basis = number | `at:${number}`;

export interface QueryRequest {
  gql: string;
  params?: QueryParameters;
  basis?: Basis;
  basis_timeout_ms?: number;
}

export interface QueryResponse {
  rows: Record<string, unknown>[];
}

export interface TxReceipt {
  tx_id: number;
  system_time: string;
  system_time_us: number;
  basis: number;
  side_effects: Record<string, number>;
}

export type ExplorerErrorCode =
  | 'unauthorized'
  | 'invalid_request'
  | 'not_acceptable'
  | 'basis_timeout'
  | 'backpressure'
  | 'misdirected_request'
  | 'writer_unavailable'
  | 'writer_fenced'
  | 'follower_failed'
  | 'internal'
  | 'network'
  | 'timeout'
  | 'cancelled'
  | 'malformed_response';

export interface ExplorerError {
  code: ExplorerErrorCode;
  message: string;
  status?: number;
  retryAfterMs?: number;
}
