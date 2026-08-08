<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import * as Tooltip from '$lib/components/ui/tooltip';
  import { degradedExplanation } from '$lib/logic/connection-status';
  import type { ConnectionSession } from '$lib/stores/connection.svelte';

  let {
    session,
    health,
    status,
  }: {
    session: ConnectionSession;
    health: unknown;
    status?: unknown;
  } = $props();

  let label = $derived(statusLabel(session, health));
  // Only "Degraded" has a cause worth explaining — the other labels say all
  // there is to say, and a tooltip on them would be noise.
  let explanation = $derived(label === 'Degraded' ? degradedExplanation(health, status) : null);
  let variant = $derived<'outline' | 'destructive' | 'secondary'>(
    session === 'authenticated'
      ? 'outline'
      : session === 'unauthenticated' || session === 'error'
        ? 'destructive'
        : 'secondary',
  );

  function statusLabel(current: ConnectionSession, currentHealth: unknown): string {
    if (current === 'unknown' || current === 'checking') return 'Checking';
    if (current === 'connecting') return 'Connecting';
    if (current === 'degraded') return 'Degraded';
    if (current === 'unauthenticated') return 'Authentication required';
    if (current === 'error') return 'Unavailable';
    if (isRecord(currentHealth) && currentHealth.status === 'degraded') return 'Degraded';
    return 'Connected';
  }

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }
</script>

<div
  class="flex items-center gap-2"
  role="status"
  aria-label="Connection status"
  aria-live="polite"
  aria-atomic="true"
>
  <span class="text-muted-foreground hidden text-xs sm:inline" aria-hidden="true">Connection status</span>
  {#if session === 'unknown' || session === 'checking'}
    <Skeleton class="h-5 w-20 motion-reduce:animate-none" aria-label={label} />
  {:else if explanation !== null}
    <Tooltip.Root>
      <Tooltip.Trigger>
        {#snippet child({ props })}
          <!-- A Badge renders a span, so it needs tabindex to be reachable:
               the tooltip opens on focus as well as hover. -->
          <Badge {...props} {variant} tabindex={0}>{label}</Badge>
        {/snippet}
      </Tooltip.Trigger>
      <Tooltip.Content class="flex-col items-start gap-1">
        <p class="font-medium">{explanation.summary}</p>
        {#if explanation.detail !== null}
          <p class="text-background/80">{explanation.detail}</p>
        {/if}
      </Tooltip.Content>
    </Tooltip.Root>
  {:else}
    <Badge {variant}>{label}</Badge>
  {/if}
</div>
