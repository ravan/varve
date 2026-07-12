<script lang="ts">
  import ConnectionDialog from '$lib/components/ConnectionDialog.svelte';
  import ConnectionStatus from '$lib/components/ConnectionStatus.svelte';
  import ElementInspector from '$lib/components/ElementInspector.svelte';
  import FavoritesPanel from '$lib/components/FavoritesPanel.svelte';
  import HistoryPanel from '$lib/components/HistoryPanel.svelte';
  import ObservedSchemaPanel from '$lib/components/ObservedSchemaPanel.svelte';
  import SettingsPanel from '$lib/components/SettingsPanel.svelte';
  import { Button } from '$lib/components/ui/button';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import { Separator } from '$lib/components/ui/separator';
  import * as Sheet from '$lib/components/ui/sheet';
  import * as Tooltip from '$lib/components/ui/tooltip';
  import type { ConnectionStore } from '$lib/stores/connection.svelte';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import type { Favorite, HistoryEntry } from '$lib/logic/workspace';
  import Database from '@lucide/svelte/icons/database';
  import History from '@lucide/svelte/icons/history';
  import Menu from '@lucide/svelte/icons/menu';
  import PanelRight from '@lucide/svelte/icons/panel-right';
  import Plus from '@lucide/svelte/icons/plus';
  import Settings from '@lucide/svelte/icons/settings';
  import Star from '@lucide/svelte/icons/star';
  import SunMoon from '@lucide/svelte/icons/sun-moon';
  import { setMode } from 'mode-watcher';
  import { onMount, tick, type Snippet } from 'svelte';

  let {
    connection,
    workspace,
    onRunSaved,
    children,
  }: {
    connection: ConnectionStore;
    workspace: WorkspaceStore;
    onRunSaved: (item: HistoryEntry | Favorite) => void;
    children: Snippet;
  } = $props();

  let navigationOpen = $state(false);
  let inspectorOpen = $state(false);
  let connectionOpen = $state(false);
  let reconnectButton = $state<HTMLButtonElement | null>(null);
  let activePanel = $state<'Observed schema' | 'History' | 'Favorites' | 'Settings' | null>(null);

  const navigation = [
    { name: 'New query', icon: Plus },
    { name: 'Observed schema', icon: Database },
    { name: 'History', icon: History },
    { name: 'Favorites', icon: Star },
    { name: 'Settings', icon: Settings },
  ] as const;

  onMount(() => {
    void connection.refresh();
    const interval = window.setInterval(() => {
      if (connection.session !== 'connecting') void connection.refresh();
    }, 15_000);
    return () => window.clearInterval(interval);
  });

  $effect(() => {
    setMode(workspace.settings.theme);
  });

  function selectNavigation(name: (typeof navigation)[number]['name']): void {
    if (name === 'New query') {
      workspace.startNewQuery();
      activePanel = null;
    } else {
      activePanel = name;
    }
    navigationOpen = false;
  }

  function startSchemaQuery(gql: string): void {
    workspace.startNewQuery();
    workspace.setQueryDraft(gql);
    activePanel = null;
  }

  async function runSaved(item: HistoryEntry | Favorite): Promise<void> {
    activePanel = null;
    await tick();
    onRunSaved(item);
  }

  function cycleTheme(): void {
    workspace.updateSettings({ theme: workspace.settings.theme === 'dark' ? 'light' : 'dark' });
  }
</script>

<Tooltip.Provider>
  <div class="app-shell">
    <aside class="desktop-rail border-r bg-sidebar text-sidebar-foreground">
      <div class="flex h-14 items-center gap-2 px-4">
        <span class="grid size-8 place-items-center rounded-lg bg-primary font-semibold text-primary-foreground"
          >V</span
        >
        <div class="min-w-0">
          <p class="truncate text-sm font-semibold">Varve Explorer</p>
          <p class="text-muted-foreground text-xs">Query workspace</p>
        </div>
      </div>
      <Separator />
      <ScrollArea class="h-[calc(100vh-3.6rem)]">
        <nav class="grid gap-1 p-3" aria-label="Workspace navigation">
          {#each navigation as item}
            <Tooltip.Root>
              <Tooltip.Trigger>
                {#snippet child({ props })}
                  <Button
                    {...props}
                    variant={(item.name === 'New query' && activePanel === null) || item.name === activePanel ? 'secondary' : 'ghost'}
                    class="w-full justify-start"
                    onclick={() => selectNavigation(item.name)}
                  >
                    <item.icon aria-hidden="true" />
                    {item.name}
                  </Button>
                {/snippet}
              </Tooltip.Trigger>
              <Tooltip.Content side="right">{item.name}</Tooltip.Content>
            </Tooltip.Root>
          {/each}
        </nav>
      </ScrollArea>
    </aside>

    <div class="app-workspace min-w-0">
      <header class="flex h-14 items-center gap-2 border-b bg-background/95 px-3 backdrop-blur">
        <Sheet.Root bind:open={navigationOpen}>
          <Sheet.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="ghost" size="icon" class="mobile-only" aria-label="Open navigation">
                <Menu aria-hidden="true" />
              </Button>
            {/snippet}
          </Sheet.Trigger>
          <Sheet.Content side="left" class="w-72" showCloseButton={true}>
            <Sheet.Header class="p-4 pb-0">
              <Sheet.Title>Varve Explorer</Sheet.Title>
              <Sheet.Description>Workspace navigation</Sheet.Description>
            </Sheet.Header>
            <Separator />
            <ScrollArea class="min-h-0 flex-1 px-3">
              <nav class="grid gap-1" aria-label="Mobile workspace navigation">
                {#each navigation as item}
                  <Button
                    variant={(item.name === 'New query' && activePanel === null) || item.name === activePanel ? 'secondary' : 'ghost'}
                    class="w-full justify-start"
                    onclick={() => selectNavigation(item.name)}
                  >
                    <item.icon aria-hidden="true" />
                    {item.name}
                  </Button>
                {/each}
              </nav>
            </ScrollArea>
          </Sheet.Content>
        </Sheet.Root>

        <ConnectionStatus session={connection.session} health={connection.health} />
        <span
          class="text-muted-foreground min-w-0 flex-1 truncate text-center font-mono text-xs"
          title={connection.config?.target}
        >
          {connection.config?.target ?? 'Loading target…'}
        </span>
        <Button
          bind:ref={reconnectButton}
          variant="outline"
          size="sm"
          onclick={() => (connectionOpen = true)}
        >Reconnect</Button>
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="ghost" size="icon-sm" aria-label="Theme" onclick={cycleTheme}>
                <SunMoon aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Theme</Tooltip.Content>
        </Tooltip.Root>
        <Sheet.Root bind:open={inspectorOpen}>
          <Sheet.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="ghost" size="icon" class="mobile-only" aria-label="Open inspector">
                <PanelRight aria-hidden="true" />
              </Button>
            {/snippet}
          </Sheet.Trigger>
          <Sheet.Content side="right" class="w-80">
            <Sheet.Header class="p-4 pb-0">
              <Sheet.Title>Inspector</Sheet.Title>
              <Sheet.Description>Selection details from the active graph result.</Sheet.Description>
            </Sheet.Header>
            <ScrollArea class="min-h-0 flex-1 px-4 pb-4">
              <ElementInspector inspection={workspace.inspection} />
            </ScrollArea>
          </Sheet.Content>
        </Sheet.Root>
      </header>

      <main class="min-w-0 overflow-x-hidden p-4 sm:p-6">
        {#if activePanel === 'Observed schema'}
          <ObservedSchemaPanel schema={workspace.observedSchema} onQuery={startSchemaQuery} />
        {:else if activePanel === 'History'}
          <HistoryPanel {workspace} onRun={runSaved} />
        {:else if activePanel === 'Favorites'}
          <FavoritesPanel {workspace} parametersDraft={workspace.queryParametersDraft} onRun={runSaved} />
        {:else if activePanel === 'Settings'}
          <SettingsPanel {workspace} />
        {:else}
          {@render children()}
        {/if}
      </main>
    </div>

    <aside class="desktop-inspector border-l bg-card p-4">
      <ElementInspector inspection={workspace.inspection} />
    </aside>
  </div>
</Tooltip.Provider>

<ConnectionDialog
  connection={connection}
  bind:open={connectionOpen}
  returnFocus={reconnectButton}
/>
