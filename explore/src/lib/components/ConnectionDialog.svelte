<script lang="ts">
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Dialog from '$lib/components/ui/dialog';
  import { Input } from '$lib/components/ui/input';
  import { Label } from '$lib/components/ui/label';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import type { ConnectionStore } from '$lib/stores/connection.svelte';

  let {
    connection,
    open = $bindable(false),
    returnFocus,
  }: {
    connection: ConnectionStore;
    open?: boolean;
    returnFocus?: HTMLButtonElement | null;
  } = $props();

  let token = $state('');
  let wasOpen = $state(false);
  let mustRemainOpen = $derived(connection.session !== 'authenticated');
  let errorCopy = $derived(connectionErrorCopy(connection.error?.code));

  $effect(() => {
    if (mustRemainOpen && !open) open = true;
  });

  $effect(() => {
    if (wasOpen && !open && !mustRemainOpen) {
      queueMicrotask(() => returnFocus?.focus());
    }
    wasOpen = open;
  });

  async function connect(): Promise<void> {
    const submittedToken = token;
    token = '';
    await connection.connect(submittedToken);
    if (connection.session === 'authenticated') open = false;
  }

  function connectionErrorCopy(code: string | undefined): {
    title: string;
    description: string;
  } | null {
    if (code === undefined) return null;
    if (code === 'unauthorized') {
      return {
        title: 'Authentication required',
        description: 'The bearer token was not accepted. Check it and try again.',
      };
    }
    if (code === 'malformed_response' || code === 'not_acceptable') {
      return {
        title: 'Compatibility issue',
        description: 'The target returned a response this Explorer version cannot use.',
      };
    }
    if (code === 'network') {
      return {
        title: 'Network error',
        description: 'The configured target could not be reached. Your token was not retained.',
      };
    }
    return {
      title: 'Connection degraded',
      description: 'The target is not ready. Try connecting again when it recovers.',
    };
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Content showCloseButton={!mustRemainOpen}>
    <Dialog.Header>
      <Dialog.Title>Connect to Varve</Dialog.Title>
      <Dialog.Description>
        Authenticate this browser session to run GQL against the configured target.
      </Dialog.Description>
    </Dialog.Header>

    <div class="rounded-lg border bg-muted/35 p-3">
      <div class="flex items-center justify-between gap-3">
        <span class="text-muted-foreground text-xs font-medium uppercase tracking-wide">Target</span>
        {#if connection.config === null}
          <Skeleton class="h-5 w-24 motion-reduce:animate-none" />
        {:else}
          <Badge variant="outline">{connection.config.displayName}</Badge>
        {/if}
      </div>
      {#if connection.config !== null}
        <p class="mt-2 truncate font-mono text-xs" title={connection.config.target}>
          {connection.config.target}
        </p>
      {/if}
    </div>

    <form class="grid gap-4" autocomplete="off" onsubmit={(event) => { event.preventDefault(); void connect(); }}>
      <div class="grid gap-2">
        <Label for="bearer-token">Bearer token</Label>
        <Input
          id="bearer-token"
          name="Bearer token"
          type="password"
          bind:value={token}
          autocomplete="off"
          spellcheck="false"
          disabled={connection.session === 'connecting'}
          aria-invalid={connection.error?.code === 'unauthorized'}
        />
        <p class="text-muted-foreground text-xs">
          The token is exchanged for an HttpOnly session cookie and is never persisted in the UI.
        </p>
      </div>

      <div aria-live="polite" aria-atomic="true">
        {#if errorCopy !== null}
          <Alert variant="destructive">
            <AlertTitle>{errorCopy.title}</AlertTitle>
            <AlertDescription>{errorCopy.description}</AlertDescription>
          </Alert>
        {/if}
      </div>

      <Dialog.Footer>
        <Button type="submit" disabled={connection.session === 'connecting' || token.length === 0}>
          {connection.session === 'connecting' ? 'Connecting…' : 'Connect'}
        </Button>
      </Dialog.Footer>
    </form>
  </Dialog.Content>
</Dialog.Root>
