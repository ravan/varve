<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Card from '$lib/components/ui/card';
  import * as Dialog from '$lib/components/ui/dialog';
  import { Input } from '$lib/components/ui/input';
  import { Label } from '$lib/components/ui/label';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import { Textarea } from '$lib/components/ui/textarea';
  import type { Favorite } from '$lib/logic/workspace';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import type { QueryParameters } from '$lib/types';
  import Plus from '@lucide/svelte/icons/plus';

  let {
    workspace,
    onRun,
    parametersDraft,
  }: {
    workspace: WorkspaceStore;
    onRun: (favorite: Favorite) => void;
    parametersDraft: QueryParameters | null;
  } = $props();

  let dialogOpen = $state(false);
  let editingId = $state<string | null>(null);
  let favoriteName = $state('');
  let notes = $state('');
  let favorites = $derived([...workspace.favorites].sort((left, right) => right.updatedAt - left.updatedAt));

  function openCreate(): void {
    editingId = null;
    favoriteName = workspace.queryDraft.trim().split(/\r?\n/, 1)[0]?.slice(0, 80) || 'Saved query';
    notes = '';
    dialogOpen = true;
  }

  function openEdit(favorite: Favorite): void {
    editingId = favorite.id;
    favoriteName = favorite.name;
    notes = favorite.notes ?? '';
    dialogOpen = true;
  }

  function saveFavorite(): void {
    const name = favoriteName.trim();
    if (name === '') return;
    const now = Date.now();
    if (editingId === null) {
      if (parametersDraft === null) return;
      workspace.addFavorite({
        id: crypto.randomUUID(),
        name,
        gql: workspace.queryDraft,
        mode: workspace.queryMode,
        params: parametersDraft,
        ...(notes.trim() === '' ? {} : { notes: notes.trim() }),
        createdAt: now,
        updatedAt: now,
      });
    } else {
      workspace.updateFavorite(editingId, {
        name,
        notes: notes.trim(),
        updatedAt: now,
      });
    }
    dialogOpen = false;
  }

  function visibleParameters(params: Favorite['params']): string {
    const visible = Object.fromEntries(
      Object.entries(params).filter(([key]) => !/(?:token|session|authorization|credential|secret)/i.test(key)),
    );
    return JSON.stringify(visible, null, 2);
  }
</script>

<section class="panel-page" aria-labelledby="favorites-heading">
  <div class="flex flex-wrap items-start justify-between gap-3">
    <div>
      <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Saved work</p>
      <h1 id="favorites-heading" class="text-2xl font-semibold tracking-tight">Favorites</h1>
      <p class="text-muted-foreground mt-1 text-sm">Named queries and their parameters.</p>
    </div>
    <Button disabled={workspace.queryDraft.trim() === '' || parametersDraft === null} onclick={openCreate}>
      <Plus aria-hidden="true" />
      Add current query
    </Button>
  </div>

  {#if parametersDraft === null}
    <p class="text-destructive text-sm">
      Add current query is unavailable while parameters are invalid or contain sensitive token/session fields.
    </p>
  {/if}

  {#if favorites.length === 0}
    <Card.Root>
      <Card.Header>
        <Card.Title>No favorites</Card.Title>
        <Card.Description>Save the current composer query to create one.</Card.Description>
      </Card.Header>
    </Card.Root>
  {:else}
    <ScrollArea class="panel-page-scroll">
      <div class="grid gap-4 pr-3">
        {#each favorites as favorite (favorite.id)}
          <Card.Root class="min-w-0">
            <Card.Header>
              <div class="flex flex-wrap items-center gap-2"><Badge variant="outline">{favorite.mode}</Badge></div>
              <Card.Title>{favorite.name}</Card.Title>
              {#if favorite.notes}<Card.Description>{favorite.notes}</Card.Description>{/if}
            </Card.Header>
            <Card.Content class="grid min-w-0 gap-3">
              <pre class="result-gql"><code>{favorite.gql}</code></pre>
              <div class="grid gap-1">
                <span class="text-muted-foreground text-xs font-semibold uppercase tracking-wide">Parameters</span>
                <pre class="saved-parameters"><code>{visibleParameters(favorite.params)}</code></pre>
              </div>
              <div class="flex flex-wrap gap-2">
                <Button size="sm" onclick={() => onRun(favorite)}>Run favorite</Button>
                <Button variant="outline" size="sm" onclick={() => openEdit(favorite)}>Edit favorite</Button>
                <Button
                  variant="outline"
                  size="sm"
                  onclick={() => workspace.duplicateFavorite(favorite.id, crypto.randomUUID(), Date.now())}
                >Duplicate favorite</Button>
                <Button variant="destructive" size="sm" onclick={() => workspace.deleteFavorite(favorite.id)}>
                  Delete favorite
                </Button>
              </div>
            </Card.Content>
          </Card.Root>
        {/each}
      </div>
    </ScrollArea>
  {/if}
</section>

<Dialog.Root bind:open={dialogOpen}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>{editingId === null ? 'Create favorite' : 'Edit favorite'}</Dialog.Title>
      <Dialog.Description>Name the saved query and optionally add notes.</Dialog.Description>
    </Dialog.Header>
    <div class="grid gap-4">
      <div class="grid gap-2">
        <Label for="favorite-name">Favorite name</Label>
        <Input id="favorite-name" bind:value={favoriteName} />
      </div>
      <div class="grid gap-2">
        <Label for="favorite-notes">Notes</Label>
        <Textarea id="favorite-notes" bind:value={notes} />
      </div>
    </div>
    <Dialog.Footer>
      <Button variant="outline" onclick={() => (dialogOpen = false)}>Cancel</Button>
      <Button
        disabled={favoriteName.trim() === '' || (editingId === null && parametersDraft === null)}
        onclick={saveFavorite}
      >Save favorite</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
