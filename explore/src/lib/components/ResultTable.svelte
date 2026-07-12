<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import * as ScrollArea from '$lib/components/ui/scroll-area';
  import * as Table from '$lib/components/ui/table';
  import {
    formatCell,
    isCanonicalBytesObject,
    pageRows,
    sortRows,
    type NormalizedCell,
    type NormalizedQueryResponse,
    type SortDirection,
  } from '$lib/logic/results';
  import ArrowDown from '@lucide/svelte/icons/arrow-down';
  import ArrowUp from '@lucide/svelte/icons/arrow-up';
  import ArrowUpDown from '@lucide/svelte/icons/arrow-up-down';
  import ChevronLeft from '@lucide/svelte/icons/chevron-left';
  import ChevronRight from '@lucide/svelte/icons/chevron-right';

  const PAGE_SIZE = 50;

  let { result }: { result: NormalizedQueryResponse } = $props();

  let sortColumn = $state<string | null>(null);
  let sortDirection = $state<SortDirection>('asc');
  let page = $state(1);

  let orderedRows = $derived(
    sortColumn === null ? [...result.rows] : sortRows(result.rows, sortColumn, sortDirection),
  );
  let pageCount = $derived(Math.max(1, Math.ceil(orderedRows.length / PAGE_SIZE)));
  let visibleRows = $derived(pageRows(orderedRows, Math.min(page, pageCount), PAGE_SIZE));

  function sortBy(column: string): void {
    if (sortColumn === column) {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
      sortColumn = column;
      sortDirection = 'asc';
    }
    page = 1;
  }

  function valueType(cell: NormalizedCell): string {
    if (cell.kind === 'missing') return 'missing';
    if (cell.value === null) return 'null';
    if (isCanonicalBytesObject(cell.value)) return 'bytes';
    if (Array.isArray(cell.value)) return 'array';
    if (typeof cell.value === 'object') return 'object';
    return typeof cell.value;
  }
</script>

<div class="grid min-w-0 gap-3">
  <ScrollArea.Root
    orientation="both"
    class="result-table-scroll max-h-[32rem] w-full rounded-lg border"
    aria-label="Query result table"
  >
    <Table.Root>
      <Table.Header>
        <Table.Row>
          {#each result.columns as column (column)}
            <Table.Head
              aria-sort={sortColumn === column
                ? sortDirection === 'asc'
                  ? 'ascending'
                  : 'descending'
                : 'none'}
            >
              <Button
                variant="ghost"
                size="sm"
                class="-ml-2 font-semibold"
                aria-label={`Sort by ${column}`}
                onclick={() => sortBy(column)}
              >
                {column}
                {#if sortColumn !== column}
                  <ArrowUpDown aria-hidden="true" />
                {:else if sortDirection === 'asc'}
                  <ArrowUp aria-hidden="true" />
                {:else}
                  <ArrowDown aria-hidden="true" />
                {/if}
              </Button>
            </Table.Head>
          {/each}
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each visibleRows as row, rowIndex (`${page}-${rowIndex}`)}
          <Table.Row>
            {#each result.columns as column (column)}
              {@const cell = row[column]}
              {@const type = valueType(cell)}
              <Table.Cell class="result-cell" data-value-type={type}>
                <span class={`result-value result-value--${type}`}>
                  <span class="result-value__type">{type}</span>
                  <span class="result-value__content">{formatCell(cell)}</span>
                </span>
              </Table.Cell>
            {/each}
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  </ScrollArea.Root>

  <nav class="flex flex-wrap items-center justify-between gap-2" aria-label="Result pages">
    <Button
      variant="outline"
      size="sm"
      disabled={page <= 1}
      onclick={() => (page -= 1)}
    >
      <ChevronLeft aria-hidden="true" />
      Previous
    </Button>
    <p class="text-muted-foreground text-sm" aria-live="polite">Page {page} of {pageCount}</p>
    <Button
      variant="outline"
      size="sm"
      disabled={page >= pageCount}
      onclick={() => (page += 1)}
    >
      Next
      <ChevronRight aria-hidden="true" />
    </Button>
  </nav>
</div>
