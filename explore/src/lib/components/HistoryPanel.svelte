<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Card from '$lib/components/ui/card';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import { isSensitiveParameterKey, type HistoryEntry } from '$lib/logic/workspace';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';

  let {
    workspace,
    onRun,
  }: {
    workspace: WorkspaceStore;
    onRun: (entry: HistoryEntry) => void;
  } = $props();

  let history = $derived([...workspace.history]);

  function formatDate(timestamp: number): string {
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium' }).format(timestamp);
  }

  function visibleParameters(params: HistoryEntry['params']): string {
    const visible = Object.fromEntries(
      Object.entries(params).filter(([key]) => !isSensitiveParameterKey(key)),
    );
    return JSON.stringify(visible, null, 2);
  }
</script>

<section class="panel-page" aria-labelledby="history-heading">
  <div class="flex flex-wrap items-start justify-between gap-3">
    <div>
      <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Recent work</p>
      <h1 id="history-heading" class="text-2xl font-semibold tracking-tight">History</h1>
      <p class="text-muted-foreground mt-1 text-sm">Latest executions appear first.</p>
    </div>
    <Button variant="outline" disabled={history.length === 0} onclick={() => workspace.clearHistory(true)}>
      Clear history
    </Button>
  </div>

  {#if history.length === 0}
    <Card.Root>
      <Card.Header>
        <Card.Title>No history</Card.Title>
        <Card.Description>Completed queries appear here when history is enabled.</Card.Description>
      </Card.Header>
    </Card.Root>
  {:else}
    <ScrollArea class="panel-page-scroll">
      <div class="grid gap-4 pr-3">
        {#each history as entry, index (`${entry.finishedAt}-${index}`)}
          <Card.Root class="min-w-0">
            <Card.Header>
              <div class="flex flex-wrap items-center gap-2">
                <Badge variant={entry.outcome === 'success' ? 'secondary' : 'destructive'}>{entry.outcome}</Badge>
                <Badge variant="outline">{entry.mode}</Badge>
                {#if entry.runCount > 1}<Badge variant="outline">{entry.runCount} runs</Badge>{/if}
              </div>
              <Card.Title class="break-words font-mono text-sm">{entry.gql.split(/\r?\n/, 1)[0]}</Card.Title>
              <Card.Description class="flex flex-wrap gap-x-4 gap-y-1">
                <time datetime={new Date(entry.finishedAt).toISOString()}>{formatDate(entry.finishedAt)}</time>
                <span>{entry.durationMs} ms</span>
                <span>
                  {entry.mode === 'read'
                    ? `${entry.rowCount} ${entry.rowCount === 1 ? 'row' : 'rows'}`
                    : `${entry.effectCount} effects`}
                </span>
              </Card.Description>
            </Card.Header>
            <Card.Content class="grid min-w-0 gap-3">
              <pre class="saved-parameters"><code>{visibleParameters(entry.params)}</code></pre>
              <Button variant="outline" size="sm" class="justify-self-start" onclick={() => onRun(entry)}>
                Run history item
              </Button>
            </Card.Content>
          </Card.Root>
        {/each}
      </div>
    </ScrollArea>
  {/if}
</section>
