<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import * as ScrollArea from '$lib/components/ui/scroll-area';
  import { copyableJson } from '$lib/logic/results';
  import Check from '@lucide/svelte/icons/check';
  import Copy from '@lucide/svelte/icons/copy';

  let { value }: { value: unknown } = $props();
  let copyState = $state<'idle' | 'copied' | 'failed'>('idle');
  let json = $derived(copyableJson(value));

  async function copyJson(): Promise<void> {
    try {
      await navigator.clipboard.writeText(json);
      copyState = 'copied';
    } catch {
      copyState = 'failed';
    }
  }
</script>

<div class="grid min-w-0 gap-2">
  <div class="flex justify-end">
    <Button variant="outline" size="sm" aria-label="Copy raw JSON" onclick={() => void copyJson()}>
      {#if copyState === 'copied'}
        <Check aria-hidden="true" />
        Copied
      {:else}
        <Copy aria-hidden="true" />
        Copy JSON
      {/if}
    </Button>
  </div>
  <ScrollArea.Root orientation="both" class="raw-result max-h-[32rem] rounded-lg border bg-muted/35">
    <pre aria-label="Raw result JSON"><code>{json}</code></pre>
  </ScrollArea.Root>
  <p class="sr-only" aria-live="polite">
    {copyState === 'copied'
      ? 'Raw JSON copied to the clipboard.'
      : copyState === 'failed'
        ? 'Raw JSON could not be copied.'
        : ''}
  </p>
</div>
