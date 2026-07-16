<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import { Input } from '$lib/components/ui/input';
  import { Label } from '$lib/components/ui/label';
  import {
    customRange,
    formatRangeSummary,
    RELATIVE_INTERVALS,
    relativeRange,
    type TimeRange,
  } from '$lib/logic/time-travel';
  import ChevronDown from '@lucide/svelte/icons/chevron-down';
  import { Popover } from 'bits-ui';

  let {
    range,
    recentRanges,
    onApply,
  }: {
    range: TimeRange;
    recentRanges: readonly TimeRange[];
    onApply: (range: TimeRange) => void;
  } = $props();

  let open = $state(false);
  let customStart = $state('');
  let customEnd = $state('');
  let customError = $state<string | null>(null);

  $effect(() => {
    if (!open) return;
    customStart = toLocalInput(range.startMs);
    customEnd = toLocalInput(range.endMs);
    customError = null;
  });

  function toLocalInput(timeMs: number): string {
    const date = new Date(timeMs);
    const pad = (value: number) => String(value).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function fromLocalInput(value: string): number {
    return value === '' ? Number.NaN : new Date(value).getTime();
  }

  function apply(next: TimeRange): void {
    open = false;
    onApply(next);
  }

  function applyRelative(index: number): void {
    apply(relativeRange(RELATIVE_INTERVALS[index], Date.now()));
  }

  function applyCustom(): void {
    const parsed = customRange(fromLocalInput(customStart), fromLocalInput(customEnd));
    if (!parsed.ok) {
      customError = parsed.error;
      return;
    }
    apply(parsed.range);
  }
</script>

<Popover.Root bind:open>
  <Popover.Trigger>
    {#snippet child({ props })}
      <Button {...props} variant="outline" size="sm" title="Set the timeline interval">
        {formatRangeSummary(range)}
        <ChevronDown aria-hidden="true" />
      </Button>
    {/snippet}
  </Popover.Trigger>
  <Popover.Portal>
    <Popover.Content
      sideOffset={8}
      align="start"
      class="bg-popover text-popover-foreground ring-foreground/10 z-50 rounded-lg p-4 shadow-md ring-1 outline-none"
    >
      <div class="flex flex-wrap gap-6">
        <div class="grid min-w-36 content-start gap-1">
          <p class="text-sm font-semibold">Relative intervals</p>
          {#each RELATIVE_INTERVALS as interval, index (interval.label)}
            <Button
              variant="ghost"
              size="sm"
              class="justify-start"
              onclick={() => applyRelative(index)}
            >
              {interval.label}
            </Button>
          {/each}
        </div>
        <div class="grid min-w-64 content-start gap-3">
          <div class="grid gap-2">
            <p class="text-sm font-semibold">Custom interval</p>
            <div class="grid gap-1.5">
              <Label for="interval-start" class="text-xs">From</Label>
              <Input id="interval-start" type="datetime-local" bind:value={customStart} />
            </div>
            <div class="grid gap-1.5">
              <Label for="interval-end" class="text-xs">To</Label>
              <Input id="interval-end" type="datetime-local" bind:value={customEnd} />
            </div>
            {#if customError !== null}
              <p class="text-destructive text-xs">{customError}</p>
            {/if}
            <Button size="sm" onclick={applyCustom}>Apply interval</Button>
          </div>
          {#if recentRanges.length > 0}
            <div class="grid gap-1">
              <p class="text-sm font-semibold">Recently used</p>
              {#each recentRanges as recent (String(recent.startMs) + '-' + String(recent.endMs))}
                <Button
                  variant="ghost"
                  size="sm"
                  class="justify-start font-normal"
                  onclick={() => apply(recent)}
                >
                  {formatRangeSummary(recent)}
                </Button>
              {/each}
            </div>
          {/if}
        </div>
      </div>
    </Popover.Content>
  </Popover.Portal>
</Popover.Root>
