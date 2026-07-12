<script lang="ts">
  import ResultFrame from '$lib/components/ResultFrame.svelte';
  import type { ExecutionFrame } from '$lib/logic/workspace';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';

  let {
    workspace,
    onRerun,
  }: {
    workspace: WorkspaceStore;
    onRerun: (frame: ExecutionFrame) => void;
  } = $props();

  let frames = $derived([...workspace.frames].reverse());
</script>

{#if frames.length > 0}
  <section class="mx-auto grid w-full max-w-5xl gap-4" aria-labelledby="results-heading">
    <div class="flex items-center justify-between gap-3">
      <div>
        <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Execution feed</p>
        <h2 id="results-heading" class="text-xl font-semibold tracking-tight">Results</h2>
      </div>
      <p class="text-muted-foreground text-sm">{frames.length} {frames.length === 1 ? 'frame' : 'frames'}</p>
    </div>

    <div class="grid gap-4">
      {#each frames as frame (frame.id)}
        <ResultFrame {frame} {workspace} {onRerun} />
      {/each}
    </div>
  </section>
{/if}
