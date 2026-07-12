<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import type { ConnectionSession } from '$lib/stores/connection.svelte';

  let {
    session,
    health,
  }: {
    session: ConnectionSession;
    health: unknown;
  } = $props();

  let label = $derived(statusLabel(session, health));
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
  {:else}
    <Badge {variant}>{label}</Badge>
  {/if}
</div>
