<script lang="ts">
  import { browser } from '$app/environment';
  import AppShell from '$lib/components/AppShell.svelte';
  import QueryComposer from '$lib/components/QueryComposer.svelte';
  import ResultFeed from '$lib/components/ResultFeed.svelte';
  import type { ExecutionFrame, Favorite, HistoryEntry } from '$lib/logic/workspace';
  import { createConnectionStore } from '$lib/stores/connection.svelte';
  import { createWorkspaceStore } from '$lib/stores/workspace.svelte';
  import type { StorageLike } from '$lib/logic/workspace';

  const memoryStorage: StorageLike = {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
  };

  const connection = createConnectionStore(fetch);
  const workspace = createWorkspaceStore(browser ? window.localStorage : memoryStorage);
  let composer = $state<{ rerunQuery: (frame: ExecutionFrame) => Promise<void> } | null>(null);
  let requestActive = $state(false);

  function runSaved(item: HistoryEntry | Favorite): void {
    const now = Date.now();
    void composer?.rerunQuery({
      id: crypto.randomUUID(),
      gql: item.gql,
      mode: item.mode,
      params: item.params,
      parameterSummary: '',
      state: 'success',
      startedAt: now,
      finishedAt: now,
      durationMs: 0,
      pinned: false,
    });
  }
</script>

<AppShell {connection} {workspace} onRunSaved={runSaved}>
  <div class="grid gap-8">
    <QueryComposer bind:this={composer} bind:active={requestActive} {connection} {workspace} />
    <ResultFeed
      {workspace}
      rerunDisabled={requestActive}
      onRerun={(frame) => void composer?.rerunQuery(frame)}
    />
  </div>
</AppShell>
