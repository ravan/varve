<script lang="ts">
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Button } from '$lib/components/ui/button';
  import * as Collapsible from '$lib/components/ui/collapsible';
  import { Input } from '$lib/components/ui/input';
  import { Label } from '$lib/components/ui/label';
  import { Textarea } from '$lib/components/ui/textarea';
  import type { ExecutionMode } from '$lib/types';
  import ChevronDown from '@lucide/svelte/icons/chevron-down';

  let {
    mode,
    parameters = $bindable('{}'),
    basis = $bindable(''),
    timeout = $bindable('30000'),
    parametersError,
    basisError,
    timeoutError,
  }: {
    mode: ExecutionMode;
    parameters?: string;
    basis?: string;
    timeout?: string;
    parametersError?: string;
    basisError?: string;
    timeoutError?: string;
  } = $props();

  let open = $state(false);
</script>

<Collapsible.Root bind:open class="rounded-lg border bg-card">
  <Collapsible.Trigger>
    {#snippet child({ props })}
      <Button {...props} variant="ghost" class="h-10 w-full justify-between rounded-lg px-3">
        <span>Parameters and basis</span>
        <ChevronDown class={open ? 'rotate-180 transition-transform' : 'transition-transform'} aria-hidden="true" />
      </Button>
    {/snippet}
  </Collapsible.Trigger>
  <Collapsible.Content class="grid gap-4 border-t p-3">
    <div class="grid gap-2">
      <Label for="query-parameters">Parameters</Label>
      <Textarea
        id="query-parameters"
        bind:value={parameters}
        class="min-h-24 font-mono text-xs"
        spellcheck="false"
        aria-invalid={parametersError !== undefined}
        aria-describedby={parametersError === undefined ? undefined : 'parameters-error'}
      />
      {#if parametersError !== undefined}
        <Alert id="parameters-error" variant="destructive">
          <AlertTitle>Invalid parameters</AlertTitle>
          <AlertDescription>{parametersError}</AlertDescription>
        </Alert>
      {/if}
    </div>

    {#if mode === 'read'}
      <div class="grid gap-4 sm:grid-cols-2">
        <div class="grid gap-2">
          <Label for="query-basis">Basis</Label>
          <Input
            id="query-basis"
            bind:value={basis}
            placeholder="Latest available"
            inputmode="numeric"
            aria-invalid={basisError !== undefined}
            aria-describedby={basisError === undefined ? undefined : 'basis-error'}
          />
          {#if basisError !== undefined}
            <p id="basis-error" class="text-destructive text-xs">{basisError}</p>
          {/if}
        </div>
        <div class="grid gap-2">
          <Label for="basis-timeout">Basis timeout (ms)</Label>
          <Input
            id="basis-timeout"
            bind:value={timeout}
            inputmode="numeric"
            aria-invalid={timeoutError !== undefined}
            aria-describedby={timeoutError === undefined ? undefined : 'timeout-error'}
          />
          {#if timeoutError !== undefined}
            <p id="timeout-error" class="text-destructive text-xs">{timeoutError}</p>
          {/if}
        </div>
      </div>
    {/if}
  </Collapsible.Content>
</Collapsible.Root>
