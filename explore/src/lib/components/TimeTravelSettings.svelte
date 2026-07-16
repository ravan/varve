<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import { Input } from '$lib/components/ui/input';
  import { Label } from '$lib/components/ui/label';
  import * as Select from '$lib/components/ui/select';
  import {
    clampClusterSize,
    MAX_CLUSTER_SIZE,
    MIN_CLUSTER_SIZE,
    type GroupingMode,
  } from '$lib/logic/clustering';
  import type { TemporalAxis } from '$lib/logic/time-travel';
  import ChevronDown from '@lucide/svelte/icons/chevron-down';
  import Settings2 from '@lucide/svelte/icons/settings-2';
  import { Popover } from 'bits-ui';

  let {
    grouping,
    clusterSize,
    axis,
    onGroupingChange,
    onClusterSizeChange,
    onAxisChange,
  }: {
    grouping: GroupingMode;
    clusterSize: number;
    axis: TemporalAxis;
    onGroupingChange: (grouping: GroupingMode) => void;
    onClusterSizeChange: (clusterSize: number) => void;
    onAxisChange: (axis: TemporalAxis) => void;
  } = $props();

  const groupings: { value: GroupingMode; label: string; description: string }[] = [
    { value: 'auto', label: 'Auto grouping', description: 'Group size chosen from the result size.' },
    { value: 'none', label: 'No grouping', description: 'Every node stands alone.' },
    { value: 'type', label: 'Group by type', description: 'Cluster nodes that share a label.' },
  ];
  const axes: { value: TemporalAxis; label: string; description: string }[] = [
    { value: 'valid', label: 'Valid time', description: 'What was true in the world at the instant.' },
    { value: 'system', label: 'System time', description: 'What Varve knew at the instant.' },
  ];

  let groupingLabel = $derived(
    groupings.find(({ value }) => value === grouping)?.label ?? 'Auto grouping',
  );
  let axisLabel = $derived(axes.find(({ value }) => value === axis)?.label ?? 'Valid time');

  function commitClusterSize(event: Event): void {
    const input = event.currentTarget as HTMLInputElement;
    const clamped = clampClusterSize(Number(input.value));
    input.value = String(clamped);
    onClusterSizeChange(clamped);
  }
</script>

<Popover.Root>
  <Popover.Trigger>
    {#snippet child({ props })}
      <Button {...props} variant="outline" size="sm">
        <Settings2 aria-hidden="true" />
        Visualization settings
        <ChevronDown aria-hidden="true" />
      </Button>
    {/snippet}
  </Popover.Trigger>
  <Popover.Portal>
    <Popover.Content
      sideOffset={8}
      align="end"
      class="bg-popover text-popover-foreground ring-foreground/10 z-50 w-80 rounded-lg p-4 shadow-md ring-1 outline-none"
    >
      <div class="grid gap-4">
        <div class="grid gap-1.5">
          <Label for="grouping-select">Components grouping</Label>
          <Select.Root
            type="single"
            value={grouping}
            onValueChange={(value) => onGroupingChange(value as GroupingMode)}
          >
            <Select.Trigger id="grouping-select" class="w-full">{groupingLabel}</Select.Trigger>
            <Select.Content>
              {#each groupings as option (option.value)}
                <Select.Item value={option.value}>{option.label}</Select.Item>
              {/each}
            </Select.Content>
          </Select.Root>
          <p class="text-muted-foreground text-xs">
            {groupings.find(({ value }) => value === grouping)?.description}
          </p>
        </div>

        {#if grouping === 'type'}
          <div class="grid gap-1.5">
            <Label for="cluster-size">Nodes per group</Label>
            <Input
              id="cluster-size"
              type="number"
              min={MIN_CLUSTER_SIZE}
              max={MAX_CLUSTER_SIZE}
              value={String(clusterSize)}
              onchange={commitClusterSize}
            />
            <p class="text-muted-foreground text-xs">
              A group larger than this splits into numbered parts, for example Person (2/3).
            </p>
          </div>
        {/if}

        <div class="grid gap-1.5">
          <Label for="axis-select">Time axis</Label>
          <Select.Root
            type="single"
            value={axis}
            onValueChange={(value) => onAxisChange(value as TemporalAxis)}
          >
            <Select.Trigger id="axis-select" class="w-full">{axisLabel}</Select.Trigger>
            <Select.Content>
              {#each axes as option (option.value)}
                <Select.Item value={option.value}>{option.label}</Select.Item>
              {/each}
            </Select.Content>
          </Select.Root>
          <p class="text-muted-foreground text-xs">
            {axes.find(({ value }) => value === axis)?.description}
          </p>
        </div>
      </div>
    </Popover.Content>
  </Popover.Portal>
</Popover.Root>
