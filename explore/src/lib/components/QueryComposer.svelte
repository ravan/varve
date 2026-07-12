<script lang="ts">
  import ParametersPanel from '$lib/components/ParametersPanel.svelte';
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import { Label } from '$lib/components/ui/label';
  import * as Tabs from '$lib/components/ui/tabs';
  import { normalizeQueryResponse, normalizeTxReceipt } from '$lib/logic/results';
  import { parseBasis, parsePositiveInteger, validateParameters } from '$lib/logic/validation';
  import type { ExecutionFrame, HistoryOutcome } from '$lib/logic/workspace';
  import type { ConnectionStore, FetchLike } from '$lib/stores/connection.svelte';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import type { ExplorerErrorCode, QueryRequest } from '$lib/types';
  import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
  import { bracketMatching } from '@codemirror/language';
  import { EditorState } from '@codemirror/state';
  import {
    Decoration,
    EditorView,
    keymap,
    lineNumbers,
    MatchDecorator,
    type DecorationSet,
    type ViewUpdate,
    ViewPlugin,
  } from '@codemirror/view';
  import Play from '@lucide/svelte/icons/play';
  import Square from '@lucide/svelte/icons/square';
  import { onMount, tick } from 'svelte';

  let {
    connection,
    workspace,
    fetcher = fetch,
    active = $bindable(false),
  }: {
    connection: ConnectionStore;
    workspace: WorkspaceStore;
    fetcher?: FetchLike;
    active?: boolean;
  } = $props();

  let editorHost = $state<HTMLDivElement | null>(null);
  let editor = $state<EditorView | null>(null);
  let parametersInput = $state('{}');
  let basisInput = $state('');
  let timeoutInput = $state('30000');
  let activeController = $state<AbortController | null>(null);
  let requestError = $state<ErrorCopy | null>(null);
  let componentMounted = false;
  let finalizeActiveExecution: (() => void) | null = null;

  let parametersValidation = $derived(validateParameters(parametersInput));
  let basisValidation = $derived(parseBasis(basisInput));
  let timeoutValidation = $derived(
    timeoutInput.trim() === ''
      ? ({ ok: true, value: undefined } as const)
      : parsePositiveInteger(timeoutInput, 'Basis timeout'),
  );
  let mode = $derived(workspace.queryMode);
  let canRun = $derived(
    workspace.queryDraft.trim().length > 0 &&
      parametersValidation.ok &&
      (mode === 'write' || (basisValidation.ok && timeoutValidation.ok)) &&
      activeController === null,
  );

  const keywordMatcher = new MatchDecorator({
    regexp:
      /\b(?:ALTER|AND|AS|BY|CALL|CASE|CREATE|DELETE|DENY|DETACH|DISTINCT|DROP|ELSE|END|EXISTS|FALSE|FINISH|GRANT|INSERT|LIMIT|MATCH|MERGE|NOT|NULL|OFFSET|OPTIONAL|OR|ORDER|REMOVE|REPLACE|RETURN|REVOKE|SET|SKIP|THEN|TRUE|UNION|UNWIND|WHEN|WHERE|WITH|YIELD)\b/gi,
    decoration: Decoration.mark({ class: 'cm-gql-keyword' }),
  });

  class GqlKeywordHighlighter {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = keywordMatcher.createDeco(view);
    }

    update(update: ViewUpdate): void {
      this.decorations = keywordMatcher.updateDeco(update, this.decorations);
    }
  }

  const gqlKeywordHighlighting = ViewPlugin.fromClass(GqlKeywordHighlighter, {
    decorations: (plugin) => plugin.decorations,
  });

  onMount(() => {
    if (editorHost === null) return;
    componentMounted = true;
    editor = new EditorView({
      parent: editorHost,
      state: EditorState.create({
        doc: workspace.queryDraft,
        extensions: [
          lineNumbers(),
          bracketMatching(),
          history(),
          gqlKeywordHighlighting,
          EditorView.lineWrapping,
          EditorView.contentAttributes.of({
            'aria-label': 'GQL query',
            autocapitalize: 'off',
            autocomplete: 'off',
            spellcheck: 'false',
          }),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) workspace.setQueryDraft(update.state.doc.toString());
          }),
          keymap.of([
            ...defaultKeymap,
            ...historyKeymap,
            {
              key: 'Mod-Enter',
              preventDefault: true,
              run: () => {
                void runQuery();
                return true;
              },
            },
          ]),
        ],
      }),
    });

    return () => {
      const controller = activeController;
      try {
        finalizeActiveExecution?.();
      } finally {
        componentMounted = false;
        controller?.abort();
        activeController = null;
        editor?.destroy();
        editor = null;
      }
    };
  });

  $effect(() => {
    const draft = workspace.queryDraft;
    if (editor !== null && editor.state.doc.toString() !== draft) {
      editor.dispatch({
        changes: { from: 0, to: editor.state.doc.length, insert: draft },
      });
    }
  });

  $effect(() => {
    const nextBasis = workspace.defaultReadBasis;
    if (nextBasis !== undefined) basisInput = String(nextBasis);
  });

  $effect(() => {
    active = activeController !== null;
  });

  $effect(() => {
    const validation = parametersValidation;
    workspace.setQueryParametersDraft(validation.ok ? validation.value : null);
  });

  function cancelQuery(): void {
    activeController?.abort();
  }

  export async function rerunQuery(frame: ExecutionFrame): Promise<void> {
    if (activeController !== null) return;
    parametersInput = JSON.stringify(frame.params, null, 2);
    basisInput = frame.mode === 'read' && frame.readBasis !== undefined ? String(frame.readBasis) : '';
    timeoutInput =
      frame.mode === 'read' && frame.basisTimeoutMs !== undefined
        ? String(frame.basisTimeoutMs)
        : '';
    workspace.setQueryMode(frame.mode);
    workspace.setQueryDraft(frame.gql);
    requestError = null;
    await tick();
    await runQuery();
  }

  async function runQuery(): Promise<void> {
    if (!canRun || !parametersValidation.ok) return;
    if (mode === 'read' && (!basisValidation.ok || !timeoutValidation.ok)) return;

    const gql = workspace.queryDraft;
    const params = parametersValidation.value;
    const readBasis = mode === 'read' && basisValidation.ok ? basisValidation.value : undefined;
    const basisTimeoutMs =
      mode === 'read' && timeoutValidation.ok ? timeoutValidation.value : undefined;
    const startedAt = Date.now();
    const frameId = crypto.randomUUID();
    const controller = new AbortController();
    const runningFrame: ExecutionFrame = {
      id: frameId,
      gql,
      mode,
      params,
      ...(readBasis === undefined ? {} : { readBasis }),
      ...(basisTimeoutMs === undefined ? {} : { basisTimeoutMs }),
      parameterSummary: summarizeParameters(params),
      state: 'running',
      startedAt,
      pinned: false,
    };

    requestError = null;
    activeController = controller;
    workspace.addFrame(runningFrame);

    let outcome: HistoryOutcome = 'error';
    let rowCount = 0;
    let effectCount = 0;
    let responseValue: unknown;
    let rawResponseValue: unknown;
    let finalized = false;

    const finalizeOnCleanup = () => {
      outcome = 'cancelled';
      responseValue = { code: 'cancelled' satisfies ExplorerErrorCode };
      rawResponseValue = undefined;
      finalizeExecution();
    };
    finalizeActiveExecution = finalizeOnCleanup;

    function finalizeExecution(): void {
      if (finalized) return;
      finalized = true;
      const finishedAt = Date.now();
      const durationMs = Math.max(0, finishedAt - startedAt);
      const currentFrame = workspace.frames.find(({ id }) => id === frameId);
      if (currentFrame !== undefined) {
        workspace.replaceFrame({
          ...runningFrame,
          pinned: currentFrame.pinned,
          state: outcome,
          finishedAt,
          durationMs,
          response: responseValue,
          ...(rawResponseValue === undefined ? {} : { rawResponse: rawResponseValue }),
        });
      }
      workspace.recordHistory({
        gql,
        mode,
        params,
        finishedAt,
        durationMs,
        rowCount,
        effectCount,
        outcome,
        runCount: 1,
      });
      workspace.observeExecution(gql, outcome, finishedAt);
      if (activeController === controller) activeController = null;
      if (finalizeActiveExecution === finalizeOnCleanup) finalizeActiveExecution = null;
    }

    try {
      const request: QueryRequest = { gql, params };
      if (runningFrame.mode === 'read') {
        if (runningFrame.readBasis !== undefined) request.basis = runningFrame.readBasis;
        if (runningFrame.basisTimeoutMs !== undefined) {
          request.basis_timeout_ms = runningFrame.basisTimeoutMs;
        }
      }

      const response = await fetcher(runningFrame.mode === 'read' ? '/api/varve/query' : '/api/varve/tx', {
        method: 'POST',
        headers: { accept: 'application/json', 'content-type': 'application/json' },
        body: JSON.stringify(request),
        signal: controller.signal,
      });
      const raw = await readJson(response);
      rawResponseValue = raw;
      if (!componentMounted) return;
      if (!response.ok) throw responseFailure(raw, response.status);

      if (mode === 'read') {
        const normalized = normalizeQueryResponse(raw);
        responseValue = normalized;
        rawResponseValue = normalized.raw;
        rowCount = normalized.rows.length;
      } else {
        const receipt = normalizeTxReceipt(raw);
        responseValue = receipt;
        rawResponseValue = receipt.raw;
        effectCount = Object.values(receipt.side_effects).reduce((total, count) => total + count, 0);
        workspace.setDefaultReadBasis(receipt.basis);
      }
      outcome = 'success';
    } catch (cause) {
      if (componentMounted) {
        const failure = normalizeExecutionFailure(cause, controller.signal.aborted);
        outcome = failure.code === 'cancelled' ? 'cancelled' : 'error';
        responseValue = failure;
        requestError = executionErrorCopy(failure);
        if (failure.code === 'unauthorized') void connection.refresh();
      }
    } finally {
      if (componentMounted) finalizeExecution();
    }
  }

  async function readJson(response: Response): Promise<unknown> {
    try {
      return await response.json();
    } catch {
      throw { code: 'malformed_response' satisfies ExplorerErrorCode, status: response.status };
    }
  }

  function responseFailure(value: unknown, status: number): ExecutionFailure {
    if (isRecord(value) && isExplorerErrorCode(value.code)) {
      return {
        code: value.code,
        status,
        ...(typeof value.message === 'string' ? { message: value.message } : {}),
        ...(typeof value.retryAfterMs === 'number' ? { retryAfterMs: value.retryAfterMs } : {}),
      };
    }
    return { code: status === 401 ? 'unauthorized' : 'malformed_response', status };
  }

  function normalizeExecutionFailure(value: unknown, aborted: boolean): ExecutionFailure {
    if (aborted) return { code: 'cancelled' };
    if (isRecord(value) && isExplorerErrorCode(value.code)) {
      return {
        code: value.code,
        ...(typeof value.status === 'number' ? { status: value.status } : {}),
        ...(typeof value.message === 'string' ? { message: value.message } : {}),
        ...(typeof value.retryAfterMs === 'number' ? { retryAfterMs: value.retryAfterMs } : {}),
      };
    }
    if (value instanceof TypeError) {
      return value.message.includes('response') || value.message.includes('receipt')
        ? { code: 'malformed_response' }
        : { code: 'network' };
    }
    return { code: 'network' };
  }

  function executionErrorCopy(failure: ExecutionFailure): ErrorCopy {
    const descriptions: Partial<Record<ExplorerErrorCode, ErrorCopy>> = {
      unauthorized: {
        title: 'Authentication required',
        description: 'Reconnect, then rerun this query. The bearer token was not retained.',
      },
      invalid_request: {
        title: 'Invalid request',
        description: 'Varve rejected the request. The query and parameters remain available to edit.',
      },
      not_acceptable: {
        title: 'Compatibility issue',
        description: 'The target cannot return a response format this Explorer supports.',
      },
      malformed_response: {
        title: 'Compatibility issue',
        description: 'The target returned a malformed or unsupported response.',
      },
      basis_timeout: {
        title: 'Basis timeout',
        description: 'The requested basis was not available before the timeout. Adjust it and rerun.',
      },
      backpressure: {
        title: 'Server busy',
        description:
          failure.retryAfterMs === undefined
            ? 'Varve is applying backpressure. Retry manually; writes are never retried automatically.'
            : `Varve is applying backpressure. Retry manually after ${failure.retryAfterMs} ms.`,
      },
      misdirected_request: {
        title: 'Writer unavailable',
        description: 'This target cannot accept the request. Reconnect to the configured writer and retry.',
      },
      writer_unavailable: {
        title: 'Writer unavailable',
        description: 'The writer is unavailable. Your query remains ready for a manual retry.',
      },
      writer_fenced: {
        title: 'Writer unavailable',
        description: 'The writer was fenced. Confirm the target status before retrying.',
      },
      follower_failed: {
        title: 'Writer unavailable',
        description: 'A follower failed while applying the write. Inspect service health before retrying.',
      },
      internal: {
        title: 'Server error',
        description: 'Varve could not complete the request. The query remains available to retry.',
      },
      network: {
        title: 'Network error',
        description: 'Explorer could not reach Varve. Check the connection and retry.',
      },
      timeout: {
        title: 'Network error',
        description: 'The request timed out before Varve returned a response.',
      },
      cancelled: {
        title: 'Request cancelled',
        description: 'The client stopped waiting. This does not roll back work already accepted by Varve.',
      },
    };
    return descriptions[failure.code] ?? {
      title: 'Server error',
      description: 'The request could not be completed. The query remains available to retry.',
    };
  }

  function summarizeParameters(params: Record<string, unknown>): string {
    const keys = Object.keys(params);
    if (keys.length === 0) return 'No parameters';
    return `${keys.length} ${keys.length === 1 ? 'parameter' : 'parameters'}: ${keys.join(', ')}`;
  }

  function isExplorerErrorCode(value: unknown): value is ExplorerErrorCode {
    return (
      typeof value === 'string' &&
      [
        'unauthorized',
        'invalid_request',
        'not_acceptable',
        'basis_timeout',
        'backpressure',
        'misdirected_request',
        'writer_unavailable',
        'writer_fenced',
        'follower_failed',
        'internal',
        'network',
        'timeout',
        'cancelled',
        'malformed_response',
      ].includes(value)
    );
  }

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }

  interface ExecutionFailure {
    readonly code: ExplorerErrorCode;
    readonly status?: number;
    readonly message?: string;
    readonly retryAfterMs?: number;
  }

  interface ErrorCopy {
    readonly title: string;
    readonly description: string;
  }
</script>

<section class="mx-auto grid w-full max-w-5xl gap-4" aria-labelledby="composer-heading">
  <div class="flex flex-wrap items-end justify-between gap-3">
    <div>
      <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Workspace</p>
      <h1 id="composer-heading" class="text-2xl font-semibold tracking-tight">New query</h1>
    </div>
    <div class="flex items-center gap-2">
      {#if !workspace.queryModeOverridden}
        <Badge variant="secondary">Auto-detected</Badge>
      {/if}
      <Tabs.Root value={mode} class="w-44">
        <Tabs.List class="grid w-full grid-cols-2">
          <Tabs.Trigger value="read" onclick={() => workspace.setQueryMode('read')}>Read</Tabs.Trigger>
          <Tabs.Trigger value="write" onclick={() => workspace.setQueryMode('write')}>Write</Tabs.Trigger>
        </Tabs.List>
      </Tabs.Root>
    </div>
  </div>

  <div class="overflow-hidden rounded-xl border bg-card shadow-sm">
    <div class="flex items-center justify-between border-b px-3 py-2">
      <Label id="gql-query-label">GQL query</Label>
      <span class="text-muted-foreground font-mono text-xs">⌘/Ctrl + Enter</span>
    </div>
    <div bind:this={editorHost} class="query-editor min-h-56" aria-labelledby="gql-query-label"></div>
  </div>

  <ParametersPanel
    {mode}
    bind:parameters={parametersInput}
    bind:basis={basisInput}
    bind:timeout={timeoutInput}
    parametersError={parametersValidation.ok ? undefined : parametersValidation.error}
    basisError={mode === 'read' && !basisValidation.ok ? basisValidation.error : undefined}
    timeoutError={mode === 'read' && !timeoutValidation.ok ? timeoutValidation.error : undefined}
  />

  {#if workspace.queryDraft.length > 0 && workspace.queryDraft.trim().length === 0}
    <Alert variant="destructive">
      <AlertTitle>Invalid request</AlertTitle>
      <AlertDescription>GQL query must contain a statement.</AlertDescription>
    </Alert>
  {/if}

  {#if requestError !== null}
    <Alert variant="destructive">
      <AlertTitle>{requestError.title}</AlertTitle>
      <AlertDescription>{requestError.description}</AlertDescription>
    </Alert>
  {/if}

  <div class="flex justify-end">
    {#if activeController !== null}
      <Button variant="destructive" onclick={cancelQuery}>
        <Square aria-hidden="true" />
        Cancel query
      </Button>
    {:else}
      <Button disabled={!canRun} onclick={() => void runQuery()}>
        <Play aria-hidden="true" />
        Run query
      </Button>
    {/if}
  </div>
</section>
