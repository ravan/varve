<script lang="ts">
  import RawResult from '$lib/components/RawResult.svelte';
  import GraphResult from '$lib/components/GraphResult.svelte';
  import ResultTable from '$lib/components/ResultTable.svelte';
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Card from '$lib/components/ui/card';
  import * as Collapsible from '$lib/components/ui/collapsible';
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import * as Tabs from '$lib/components/ui/tabs';
  import type { NormalizedQueryResponse, NormalizedTxReceipt } from '$lib/logic/results';
  import { extractGraph, type GraphExtraction } from '$lib/logic/graph';
  import { extractQueryShape } from '$lib/logic/gql';
  import type { ExecutionFrame } from '$lib/logic/workspace';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import type { ExplorerErrorCode } from '$lib/types';
  import ChevronDown from '@lucide/svelte/icons/chevron-down';
  import CircleX from '@lucide/svelte/icons/circle-x';
  import Copy from '@lucide/svelte/icons/copy';
  import Ellipsis from '@lucide/svelte/icons/ellipsis';
  import Heart from '@lucide/svelte/icons/heart';
  import Pin from '@lucide/svelte/icons/pin';
  import PinOff from '@lucide/svelte/icons/pin-off';
  import RotateCcw from '@lucide/svelte/icons/rotate-ccw';

  let {
    frame,
    workspace,
    onRerun,
    rerunDisabled,
  }: {
    frame: ExecutionFrame;
    workspace: WorkspaceStore;
    onRerun: (frame: ExecutionFrame) => void;
    rerunDisabled: boolean;
  } = $props();

  let open = $state(true);
  let copyStatus = $state<'idle' | 'copied' | 'failed'>('idle');

  let queryResult = $derived(asQueryResult(frame.response));
  let receipt = $derived(asTxReceipt(frame.response));
  let rowCount = $derived(queryResult?.rows.length ?? 0);
  let sideEffects = $derived(
    receipt === null
      ? []
      : Object.entries(receipt.side_effects).filter(([, count]) => count !== 0),
  );
  let sideEffectCount = $derived(sideEffects.reduce((total, [, count]) => total + count, 0));
  let rawValue = $derived(frame.rawResponse ?? queryResult?.raw ?? receipt?.raw ?? frame.response);
  let graphExtraction = $derived(extractFrameGraph(frame.gql, queryResult));

  async function copyGql(): Promise<void> {
    try {
      await navigator.clipboard.writeText(frame.gql);
      copyStatus = 'copied';
    } catch {
      copyStatus = 'failed';
    }
  }

  function addToFavorites(): void {
    const now = Date.now();
    const firstLine = frame.gql.trim().split(/\r?\n/, 1)[0] || 'Saved query';
    workspace.addFavorite({
      id: crypto.randomUUID(),
      name: firstLine.slice(0, 80),
      gql: frame.gql,
      mode: frame.mode,
      params: frame.params,
      createdAt: now,
      updatedAt: now,
    });
  }

  function togglePinned(): void {
    workspace.replaceFrame({ ...frame, pinned: !frame.pinned });
  }

  function formatTimestamp(timestamp: number): string {
    return `${new Date(timestamp).toISOString().replace('T', ' ').replace('.000Z', 'Z')}`;
  }

  function formatDuration(durationMs: number | undefined): string {
    return durationMs === undefined ? 'In progress' : `${durationMs} ms`;
  }

  function effectLabel(key: string): string {
    return key.replaceAll('_', ' ');
  }

  function asQueryResult(value: unknown): NormalizedQueryResponse | null {
    if (!isRecord(value) || !Array.isArray(value.columns) || !Array.isArray(value.rows)) return null;
    if (!value.columns.every((column) => typeof column === 'string')) return null;
    return value as unknown as NormalizedQueryResponse;
  }

  function asTxReceipt(value: unknown): NormalizedTxReceipt | null {
    if (
      !isRecord(value) ||
      !Number.isSafeInteger(value.tx_id) ||
      !Number.isSafeInteger(value.basis) ||
      typeof value.system_time !== 'string' ||
      !isRecord(value.side_effects)
    ) {
      return null;
    }
    return value as unknown as NormalizedTxReceipt;
  }

  function errorCopy(value: unknown, cancelled: boolean): { title: string; description: string } {
    if (cancelled) {
      return {
        title: 'Request cancelled',
        description:
          'The client stopped waiting. This does not roll back work already accepted by Varve.',
      };
    }

    const failure = isRecord(value) ? value : {};
    const code = typeof failure.code === 'string' ? (failure.code as ExplorerErrorCode) : undefined;
    switch (code) {
      case 'unauthorized':
        return {
          title: 'Authentication required',
          description: 'Reconnect, then rerun this query. The bearer token was not retained.',
        };
      case 'invalid_request':
        return {
          title: 'Invalid request',
          description: 'Varve rejected the request. The original GQL and parameters remain available.',
        };
      case 'basis_timeout':
        return {
          title: 'Basis timeout',
          description: 'The requested basis was not available before the timeout. Rerun manually.',
        };
      case 'backpressure': {
        const delay =
          typeof failure.retryAfterMs === 'number' ? ` after ${failure.retryAfterMs} ms` : '';
        return {
          title: 'Server busy',
          description:
            frame.mode === 'write'
              ? `Varve is applying backpressure. Retry manually${delay}; writes are never retried automatically.`
              : `Varve is applying backpressure. Retry manually${delay}.`,
        };
      }
      case 'misdirected_request':
      case 'writer_unavailable':
      case 'writer_fenced':
      case 'follower_failed':
        return {
          title: 'Writer unavailable',
          description: 'The configured writer could not accept this request. Check the target and retry manually.',
        };
      case 'network':
      case 'timeout':
        return {
          title: 'Network error',
          description:
            code === 'timeout'
              ? 'The request timed out before Varve returned a response.'
              : 'Explorer could not reach Varve. Check the connection and retry.',
        };
      case 'not_acceptable':
      case 'malformed_response':
        return {
          title: 'Server error',
          description: 'The target returned a malformed or unsupported response format.',
        };
      case 'internal':
      default:
        return {
          title: 'Server error',
          description: 'Varve could not complete the request. The original GQL remains available.',
        };
    }
  }

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }

  function extractFrameGraph(gql: string, result: NormalizedQueryResponse | null): GraphExtraction {
    if (result === null) {
      return {
        available: false,
        reason: 'Graph topology is unavailable because this result has no normalized rows.',
        nodes: [],
        edges: [],
        truncated: false,
      };
    }
    try {
      return extractGraph(extractQueryShape(gql), result.rows, 1_000);
    } catch {
      return {
        available: false,
        reason: 'Graph topology cannot be proven from the returned query values.',
        nodes: [],
        edges: [],
        truncated: false,
      };
    }
  }
</script>

<Card.Root class="result-frame overflow-hidden" data-state={frame.state}>
  <Collapsible.Root bind:open>
    <Card.Header class="border-b py-4">
      <div class="grid min-w-0 gap-2">
        <div class="flex flex-wrap items-center gap-2">
          <Badge variant={frame.state === 'error' || frame.state === 'cancelled' ? 'destructive' : 'secondary'}>
            {frame.state}
          </Badge>
          <Badge variant="outline">{frame.mode}</Badge>
          {#if frame.pinned}<Badge variant="outline">Pinned</Badge>{/if}
        </div>
        <Card.Title class="truncate font-mono text-sm">{frame.gql.split(/\r?\n/, 1)[0]}</Card.Title>
        <Card.Description class="flex flex-wrap gap-x-4 gap-y-1">
          <span>{formatDuration(frame.durationMs)}</span>
          <time datetime={new Date(frame.finishedAt ?? frame.startedAt).toISOString()}>
            {formatTimestamp(frame.finishedAt ?? frame.startedAt)}
          </time>
          <span>{frame.parameterSummary}</span>
          {#if frame.state === 'success' && frame.mode === 'read'}
            <span>{rowCount} {rowCount === 1 ? 'row' : 'rows'}</span>
          {:else if frame.state === 'success' && frame.mode === 'write'}
            <span>{sideEffectCount} side effects</span>
          {/if}
        </Card.Description>
      </div>

      <Card.Action class="flex items-center gap-1">
        <Button
          variant="ghost"
          size="sm"
          disabled={rerunDisabled}
          onclick={() => onRerun(frame)}
        >
          <RotateCcw aria-hidden="true" />
          Rerun query
        </Button>
        <Collapsible.Trigger>
          {#snippet child({ props })}
            <Button {...props} variant="ghost" size="icon-sm" aria-label="Collapse result">
              <ChevronDown class={open ? 'rotate-180 transition-transform' : 'transition-transform'} aria-hidden="true" />
            </Button>
          {/snippet}
        </Collapsible.Trigger>
        <DropdownMenu.Root>
          <DropdownMenu.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="ghost" size="icon-sm" aria-label="Result actions">
                <Ellipsis aria-hidden="true" />
              </Button>
            {/snippet}
          </DropdownMenu.Trigger>
          <DropdownMenu.Content align="end" class="w-48">
            <DropdownMenu.Item onclick={() => void copyGql()}>
              <Copy aria-hidden="true" />
              Copy GQL
            </DropdownMenu.Item>
            <DropdownMenu.Item onclick={addToFavorites}>
              <Heart aria-hidden="true" />
              Add to favorites
            </DropdownMenu.Item>
            <DropdownMenu.Item onclick={togglePinned}>
              {#if frame.pinned}<PinOff aria-hidden="true" />{:else}<Pin aria-hidden="true" />{/if}
              {frame.pinned ? 'Unpin result' : 'Pin result'}
            </DropdownMenu.Item>
            <DropdownMenu.Separator />
            <DropdownMenu.Item variant="destructive" onclick={() => workspace.removeFrame(frame.id)}>
              <CircleX aria-hidden="true" />
              Close result
            </DropdownMenu.Item>
          </DropdownMenu.Content>
        </DropdownMenu.Root>
      </Card.Action>
    </Card.Header>

    <Collapsible.Content>
      <Card.Content class="grid min-w-0 gap-4 pt-4">
        <div class="grid min-w-0 gap-1">
          <p class="text-muted-foreground text-xs font-semibold uppercase tracking-wide">Original GQL</p>
          <pre class="result-gql"><code>{frame.gql}</code></pre>
        </div>

        {#if frame.state === 'running'}
          <div class="grid gap-3" aria-label="Query running">
            <Skeleton class="result-skeleton h-5 w-2/5" />
            <Skeleton class="result-skeleton h-24 w-full" />
            <Skeleton class="result-skeleton h-5 w-3/5" />
          </div>
        {:else if frame.state === 'error' || frame.state === 'cancelled'}
          {@const copy = errorCopy(frame.response, frame.state === 'cancelled')}
          <Alert variant="destructive">
            <AlertTitle>{copy.title}</AlertTitle>
            <AlertDescription>{copy.description}</AlertDescription>
          </Alert>
        {:else if frame.mode === 'read' && queryResult !== null}
          {#if queryResult.rows.length === 0}
            <Alert>
              <AlertTitle>No rows</AlertTitle>
              <AlertDescription>The query completed successfully without returning rows.</AlertDescription>
            </Alert>
          {/if}
          <Tabs.Root value={workspace.settings.defaultResultTab} class="min-w-0">
            <Tabs.List aria-label="Read result views">
              <Tabs.Trigger value="graph">Graph</Tabs.Trigger>
              <Tabs.Trigger value="table">Table</Tabs.Trigger>
              <Tabs.Trigger value="raw">Raw</Tabs.Trigger>
            </Tabs.List>
            <Tabs.Content value="graph">
              <GraphResult
                extraction={graphExtraction}
                rows={queryResult.rows}
                sourceId={frame.id}
                motion={workspace.settings.graphMotion}
                {workspace}
              />
            </Tabs.Content>
            <Tabs.Content value="table">
              {#if queryResult.rows.length > 0}
                <ResultTable result={queryResult} />
              {:else}
                <p class="text-muted-foreground py-4 text-sm">There are no table rows to display.</p>
              {/if}
            </Tabs.Content>
            <Tabs.Content value="raw">
              <RawResult value={rawValue} />
            </Tabs.Content>
          </Tabs.Root>
        {:else if frame.mode === 'write' && receipt !== null}
          <Tabs.Root value="receipt" class="min-w-0">
            <Tabs.List aria-label="Write result views">
              <Tabs.Trigger value="receipt">Receipt</Tabs.Trigger>
              <Tabs.Trigger value="raw">Raw</Tabs.Trigger>
            </Tabs.List>
            <Tabs.Content value="receipt">
              <Card.Root class="shadow-none">
                <Card.Header>
                  <Card.Title>Transaction receipt</Card.Title>
                  <Card.Description>Committed transaction details and reported side effects.</Card.Description>
                </Card.Header>
                <Card.Content class="grid gap-4">
                  <div class="flex flex-wrap gap-2">
                    <Badge variant="outline">tx_id {receipt.tx_id}</Badge>
                    <Badge variant="outline">basis {receipt.basis}</Badge>
                    <Badge variant="outline">system_time {receipt.system_time}</Badge>
                  </div>
                  <div class="grid gap-2">
                    <h3 class="text-sm font-semibold">Side effects</h3>
                    {#if sideEffects.length > 0}
                      <div class="flex flex-wrap gap-2">
                        {#each sideEffects as [key, count] (key)}
                          <Badge variant="secondary">{effectLabel(key)} {count}</Badge>
                        {/each}
                      </div>
                    {:else}
                      <p class="text-muted-foreground text-sm">No side effects reported.</p>
                    {/if}
                  </div>
                </Card.Content>
              </Card.Root>
            </Tabs.Content>
            <Tabs.Content value="raw">
              <RawResult value={rawValue} />
            </Tabs.Content>
          </Tabs.Root>
        {:else}
          <Alert variant="destructive">
            <AlertTitle>Server error</AlertTitle>
            <AlertDescription>
              Result data is no longer available in this session. Rerun the original GQL to load it again.
            </AlertDescription>
          </Alert>
        {/if}

        <p class="sr-only" aria-live="polite">
          {copyStatus === 'copied'
            ? 'GQL copied to the clipboard.'
            : copyStatus === 'failed'
              ? 'GQL could not be copied.'
              : ''}
        </p>
      </Card.Content>
    </Collapsible.Content>
  </Collapsible.Root>
</Card.Root>
