<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import * as Card from '$lib/components/ui/card';
  import { Checkbox } from '$lib/components/ui/checkbox';
  import * as Dialog from '$lib/components/ui/dialog';
  import { Label } from '$lib/components/ui/label';
  import * as Select from '$lib/components/ui/select';
  import { Separator } from '$lib/components/ui/separator';
  import { Switch } from '$lib/components/ui/switch';
  import type { ResultTab, ThemeSetting } from '$lib/logic/workspace';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';

  let { workspace }: { workspace: WorkspaceStore } = $props();

  let clearOpen = $state(false);

  const themes: { value: ThemeSetting; label: string }[] = [
    { value: 'system', label: 'System' },
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' },
  ];
  const resultTabs: { value: ResultTab; label: string }[] = [
    { value: 'graph', label: 'Graph' },
    { value: 'table', label: 'Table' },
    { value: 'raw', label: 'Raw' },
  ];

  let themeLabel = $derived(themes.find(({ value }) => value === workspace.settings.theme)?.label ?? 'System');
  let resultTabLabel = $derived(
    resultTabs.find(({ value }) => value === workspace.settings.defaultResultTab)?.label ?? 'Graph',
  );
</script>

<section class="panel-page" aria-labelledby="settings-heading">
  <div>
    <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Preferences</p>
    <h1 id="settings-heading" class="text-2xl font-semibold tracking-tight">Settings</h1>
    <p class="text-muted-foreground mt-1 text-sm">Persisted locally for this browser.</p>
  </div>

  <Card.Root>
    <Card.Header>
      <Card.Title>Appearance and results</Card.Title>
      <Card.Description>Choose how Explorer presents query results.</Card.Description>
    </Card.Header>
    <Card.Content class="grid gap-5">
      <div class="setting-row">
        <div>
          <Label for="theme-select">Theme</Label>
          <p class="text-muted-foreground text-sm">Follow the system or choose a fixed appearance.</p>
        </div>
        <Select.Root
          type="single"
          value={workspace.settings.theme}
          onValueChange={(value) => workspace.updateSettings({ theme: value as ThemeSetting })}
        >
          <Select.Trigger id="theme-select" class="w-36">{themeLabel}</Select.Trigger>
          <Select.Content>
            {#each themes as theme (theme.value)}
              <Select.Item value={theme.value}>{theme.label}</Select.Item>
            {/each}
          </Select.Content>
        </Select.Root>
      </div>
      <Separator />
      <div class="setting-row">
        <div>
          <Label for="result-tab-select">Default result tab</Label>
          <p class="text-muted-foreground text-sm">The first view for successful read queries.</p>
        </div>
        <Select.Root
          type="single"
          value={workspace.settings.defaultResultTab}
          onValueChange={(value) => workspace.updateSettings({ defaultResultTab: value as ResultTab })}
        >
          <Select.Trigger id="result-tab-select" class="w-36">{resultTabLabel}</Select.Trigger>
          <Select.Content>
            {#each resultTabs as tab (tab.value)}
              <Select.Item value={tab.value}>{tab.label}</Select.Item>
            {/each}
          </Select.Content>
        </Select.Root>
      </div>
      <Separator />
      <div class="setting-row">
        <div>
          <Label for="graph-motion">Graph motion</Label>
          <p class="text-muted-foreground text-sm">Animate graph layout unless reduced motion is requested.</p>
        </div>
        <Switch
          id="graph-motion"
          checked={workspace.settings.graphMotion}
          onCheckedChange={(checked) => workspace.updateSettings({ graphMotion: checked })}
          aria-label="Graph motion"
        />
      </div>
      <Separator />
      <div class="setting-row">
        <div>
          <Label for="history-enabled">History</Label>
          <p class="text-muted-foreground text-sm">Keep completed query summaries in this browser.</p>
        </div>
        <Switch
          id="history-enabled"
          checked={workspace.settings.historyEnabled}
          onCheckedChange={(checked) => workspace.updateSettings({ historyEnabled: checked })}
          aria-label="History"
        />
      </div>
      <Separator />
      <div class="flex items-start gap-3">
        <Checkbox
          id="confirm-clear"
          checked={workspace.settings.confirmBeforeClear}
          onCheckedChange={(checked) => workspace.updateSettings({ confirmBeforeClear: checked === true })}
        />
        <div>
          <Label for="confirm-clear">Always confirm clear data</Label>
          <p class="text-muted-foreground text-sm">Require a confirmation before deleting local workspace data.</p>
        </div>
      </div>
    </Card.Content>
  </Card.Root>

  <Card.Root class="border-destructive/40">
    <Card.Header>
      <Card.Title>Clear local data</Card.Title>
      <Card.Description>Remove results, history, favorites, observed schema, and settings. Your authenticated session cookie is preserved.</Card.Description>
    </Card.Header>
    <Card.Content>
      <Button variant="destructive" onclick={() => (clearOpen = true)}>Clear data</Button>
    </Card.Content>
  </Card.Root>
</section>

<Dialog.Root bind:open={clearOpen}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Clear Explorer data?</Dialog.Title>
      <Dialog.Description>
        This removes local workspace data but preserves the authenticated session cookie.
      </Dialog.Description>
    </Dialog.Header>
    <Dialog.Footer>
      <Button variant="outline" onclick={() => (clearOpen = false)}>Cancel</Button>
      <Button
        variant="destructive"
        onclick={() => {
          workspace.clearWorkspace(true);
          workspace.clearInspection();
          clearOpen = false;
        }}
      >Clear data</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
