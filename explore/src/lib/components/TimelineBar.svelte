<script lang="ts">
  import {
    formatInstant,
    fractionOfTime,
    timeAtFraction,
    timelineTicks,
    zoomRange,
    type TimeRange,
  } from '$lib/logic/time-travel';

  let {
    range,
    selectedMs,
    onSelect,
    onZoom,
  }: {
    range: TimeRange;
    selectedMs: number;
    onSelect: (timeMs: number) => void;
    onZoom: (range: TimeRange) => void;
  } = $props();

  const CLICK_THRESHOLD_PX = 4;

  let track = $state<HTMLDivElement | null>(null);
  let dragStartFraction = $state<number | null>(null);
  let dragCurrentFraction = $state<number | null>(null);

  let ticks = $derived(timelineTicks(range, 6));
  let selectedFraction = $derived(fractionOfTime(range, selectedMs));
  let dragSelection = $derived(
    dragStartFraction === null || dragCurrentFraction === null
      ? null
      : {
          left: Math.min(dragStartFraction, dragCurrentFraction),
          width: Math.abs(dragCurrentFraction - dragStartFraction),
        },
  );

  function fractionFromEvent(event: PointerEvent): number {
    if (track === null) return 0;
    const rect = track.getBoundingClientRect();
    if (rect.width === 0) return 0;
    return Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width));
  }

  function handlePointerDown(event: PointerEvent): void {
    if (event.button !== 0 || track === null) return;
    try {
      track.setPointerCapture(event.pointerId);
    } catch {
      // Capture is best-effort; the drag still works while the pointer stays on the track.
    }
    dragStartFraction = fractionFromEvent(event);
    dragCurrentFraction = dragStartFraction;
  }

  function handlePointerMove(event: PointerEvent): void {
    if (dragStartFraction !== null) dragCurrentFraction = fractionFromEvent(event);
  }

  function handlePointerUp(event: PointerEvent): void {
    if (dragStartFraction === null || track === null) return;
    const start = dragStartFraction;
    const end = fractionFromEvent(event);
    dragStartFraction = null;
    dragCurrentFraction = null;

    const movedPx = Math.abs(end - start) * track.getBoundingClientRect().width;
    if (movedPx <= CLICK_THRESHOLD_PX) {
      onSelect(timeAtFraction(range, end));
    } else {
      onZoom(zoomRange(range, start, end));
    }
  }

  function handlePointerCancel(): void {
    dragStartFraction = null;
    dragCurrentFraction = null;
  }

  function handleKeydown(event: KeyboardEvent): void {
    const stepMs = Math.max(1_000, Math.round((range.endMs - range.startMs) / 100));
    if (event.key === 'ArrowLeft') onSelect(Math.max(range.startMs, selectedMs - stepMs));
    else if (event.key === 'ArrowRight') onSelect(Math.min(range.endMs, selectedMs + stepMs));
    else if (event.key === 'Home') onSelect(range.startMs);
    else if (event.key === 'End') onSelect(range.endMs);
    else return;
    event.preventDefault();
  }
</script>

<div class="timeline" aria-label="Time travel timeline">
  <div
    bind:this={track}
    class="timeline-track"
    role="slider"
    tabindex="0"
    aria-label="Selected time"
    aria-valuemin={range.startMs}
    aria-valuemax={range.endMs}
    aria-valuenow={selectedMs}
    aria-valuetext={formatInstant(selectedMs)}
    onpointerdown={handlePointerDown}
    onpointermove={handlePointerMove}
    onpointerup={handlePointerUp}
    onpointercancel={handlePointerCancel}
    onkeydown={handleKeydown}
  >
    {#each ticks as tick (tick.timeMs)}
      <div class="timeline-tick" style={`left: ${tick.fraction * 100}%`}>
        <span class="timeline-tick-label">{tick.label}</span>
        <span class="timeline-tick-mark"></span>
      </div>
    {/each}

    <div class="timeline-baseline"></div>

    {#if dragSelection !== null && dragSelection.width > 0}
      <div
        class="timeline-drag"
        style={`left: ${dragSelection.left * 100}%; width: ${dragSelection.width * 100}%`}
      ></div>
    {/if}

    <div class="timeline-marker" style={`left: ${selectedFraction * 100}%`}>
      <span class="timeline-marker-bubble">{formatInstant(selectedMs)}</span>
      <span class="timeline-marker-line"></span>
      <span class="timeline-marker-dot"></span>
    </div>
  </div>
  <p class="text-muted-foreground mt-1 text-xs">
    Click to travel to an instant. Drag to zoom into a range.
  </p>
</div>

<style>
  .timeline-track {
    position: relative;
    height: 4.25rem;
    cursor: crosshair;
    touch-action: none;
    user-select: none;
    outline-offset: 2px;
  }

  .timeline-baseline {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 1.1rem;
    height: 2px;
    border-radius: 1px;
    background: var(--color-primary, #16a34a);
    opacity: 0.85;
    pointer-events: none;
  }

  .timeline-tick {
    position: absolute;
    top: 0.25rem;
    bottom: 0;
    transform: translateX(-50%);
    display: flex;
    flex-direction: column;
    align-items: center;
    pointer-events: none;
  }

  .timeline-tick-label {
    font-size: 0.7rem;
    color: var(--color-muted-foreground, #64748b);
    white-space: nowrap;
  }

  .timeline-tick-mark {
    flex: 1;
    width: 1px;
    margin-top: 0.2rem;
    background: color-mix(in srgb, var(--color-muted-foreground, #64748b) 35%, transparent);
  }

  .timeline-drag {
    position: absolute;
    top: 1.4rem;
    bottom: 0.35rem;
    border-radius: 0.25rem;
    background: color-mix(in srgb, var(--color-primary, #16a34a) 18%, transparent);
    border: 1px solid color-mix(in srgb, var(--color-primary, #16a34a) 55%, transparent);
    pointer-events: none;
  }

  .timeline-marker {
    position: absolute;
    top: 0;
    bottom: 0;
    transform: translateX(-50%);
    display: flex;
    flex-direction: column;
    align-items: center;
    pointer-events: none;
  }

  .timeline-marker-bubble {
    background: var(--color-foreground, #0f172a);
    color: var(--color-background, #ffffff);
    border-radius: 0.3rem;
    padding: 0.1rem 0.4rem;
    font-size: 0.7rem;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .timeline-marker-line {
    flex: 1;
    width: 2px;
    background: var(--color-foreground, #0f172a);
  }

  .timeline-marker-dot {
    width: 0.55rem;
    height: 0.55rem;
    margin-bottom: 0.85rem;
    border-radius: 9999px;
    background: var(--color-foreground, #0f172a);
    border: 2px solid var(--color-background, #ffffff);
  }
</style>
