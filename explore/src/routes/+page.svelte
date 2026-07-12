<script lang="ts">
  import { browser } from '$app/environment';
  import AppShell from '$lib/components/AppShell.svelte';
  import QueryComposer from '$lib/components/QueryComposer.svelte';
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
</script>

<AppShell {connection} {workspace}>
  <QueryComposer {connection} {workspace} />
</AppShell>
