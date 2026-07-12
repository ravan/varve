<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import * as Card from '$lib/components/ui/card';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import { Separator } from '$lib/components/ui/separator';
  import type { GraphInspection } from '$lib/logic/graph';
  import { formatCell } from '$lib/logic/results';

  let { inspection }: { inspection: GraphInspection | null } = $props();
</script>

{#if inspection === null}
  <Card.Root class="border-0 bg-transparent shadow-none">
    <Card.Header class="px-0 pt-0">
      <Card.Title class="text-sm">Inspector</Card.Title>
      <Card.Description>Select a node or relationship to inspect it.</Card.Description>
    </Card.Header>
  </Card.Root>
{:else}
  <Card.Root class="min-w-0 border-0 bg-transparent shadow-none">
    <Card.Header class="px-0 pt-0">
      <div class="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{inspection.kind}</Badge>
        <Badge variant="outline">Inferred from GQL</Badge>
      </div>
      <Card.Title class="break-all font-mono text-sm">{inspection.id}</Card.Title>
      <Card.Description>
        {inspection.relatedRowCount} related {inspection.relatedRowCount === 1 ? 'row' : 'rows'}
      </Card.Description>
    </Card.Header>
    <Card.Content class="grid min-w-0 gap-4 px-0">
      <div class="grid gap-2">
        <h3 class="text-xs font-semibold uppercase tracking-wide">Identity</h3>
        <code class="inspector-value">{inspection.id}</code>
      </div>

      {#if inspection.kind === 'node'}
        <Separator />
        <div class="grid gap-2">
          <h3 class="text-xs font-semibold uppercase tracking-wide">Labels</h3>
          <div class="flex flex-wrap gap-2">
            {#if inspection.labels.length === 0}
              <span class="text-muted-foreground text-sm">No label inferred</span>
            {:else}
              {#each inspection.labels as label (label)}
                <Badge variant="outline">{label}</Badge>
              {/each}
            {/if}
          </div>
          <p class="text-muted-foreground text-xs">Inferred from GQL</p>
        </div>
      {:else}
        <Separator />
        <div class="grid gap-2">
          <h3 class="text-xs font-semibold uppercase tracking-wide">Relationship</h3>
          <Badge variant="outline">{inspection.relationshipType ?? 'Type not inferred'}</Badge>
          <code class="inspector-value">{inspection.source} → {inspection.target}</code>
          <p class="text-muted-foreground text-xs">Inferred from GQL</p>
        </div>
      {/if}

      <Separator />
      <div class="grid min-w-0 gap-2">
        <h3 class="text-xs font-semibold uppercase tracking-wide">Related row values</h3>
        {#if inspection.relatedValues.length === 0}
          <p class="text-muted-foreground text-sm">No related returned values.</p>
        {:else}
          <ScrollArea class="inspector-scroll rounded-md border">
            <dl class="grid gap-3 p-3">
              {#each inspection.relatedValues as item, index (`${item.column}-${index}`)}
                <div class="grid min-w-0 gap-1">
                  <dt class="text-muted-foreground truncate font-mono text-xs">{item.column}</dt>
                  <dd class="inspector-value">{formatCell(item.value)}</dd>
                </div>
              {/each}
            </dl>
          </ScrollArea>
        {/if}
      </div>
    </Card.Content>
  </Card.Root>
{/if}
