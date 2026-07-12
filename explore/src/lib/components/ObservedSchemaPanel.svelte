<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Card from '$lib/components/ui/card';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import type { ObservedSchema, ObservedSchemaEntry } from '$lib/logic/schema';

  let {
    schema,
    onQuery,
  }: {
    schema: ObservedSchema;
    onQuery: (gql: string) => void;
  } = $props();

  let labels = $derived(Object.entries(schema.labels));
  let relationships = $derived(Object.entries(schema.relationshipTypes));

  function formatDate(timestamp: number): string {
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(timestamp);
  }
</script>

<section class="panel-page" aria-labelledby="observed-schema-heading">
  <div>
    <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Workspace knowledge</p>
    <h1 id="observed-schema-heading" class="text-2xl font-semibold tracking-tight">Observed schema</h1>
    <p class="text-muted-foreground mt-1 text-sm">
      Derived from successful and favorite queries; not authoritative database metadata.
    </p>
  </div>

  <div class="panel-columns">
    {@render SchemaGroup({ title: 'Node labels', entries: labels, kind: 'label', onQuery })}
    {@render SchemaGroup({
      title: 'Relationship types',
      entries: relationships,
      kind: 'relationship',
      onQuery,
    })}
  </div>
</section>

{#snippet SchemaGroup({ title, entries, kind, onQuery: runQuery }: {
  title: string;
  entries: [string, ObservedSchemaEntry][];
  kind: 'label' | 'relationship';
  onQuery: (gql: string) => void;
})}
  <Card.Root class="min-w-0">
    <Card.Header>
      <Card.Title>{title}</Card.Title>
      <Card.Description>{entries.length} observed {entries.length === 1 ? 'name' : 'names'}</Card.Description>
    </Card.Header>
    <Card.Content>
      {#if entries.length === 0}
        <p class="text-muted-foreground py-6 text-sm">Nothing observed yet.</p>
      {:else}
        <ScrollArea class="panel-scroll">
          <div class="grid gap-3 pr-3">
            {#each entries as [name, entry] (name)}
              <div class="grid gap-3 rounded-lg border p-3">
                <div class="flex min-w-0 flex-wrap items-center gap-2">
                  <code class="min-w-0 break-all text-sm font-semibold">{name}</code>
                  <Badge variant="secondary">{entry.count} uses</Badge>
                </div>
                <p class="text-muted-foreground text-xs">Last seen {formatDate(entry.lastSeen)}</p>
                <Button variant="outline" size="sm" class="justify-self-start" onclick={() => runQuery(entry.starterGql)}>
                  Query {kind} {name}
                </Button>
              </div>
            {/each}
          </div>
        </ScrollArea>
      {/if}
    </Card.Content>
  </Card.Root>
{/snippet}
