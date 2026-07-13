import cytoscape, { type ElementDefinition } from 'cytoscape';
import fcose from 'cytoscape-fcose';
import { describe, expect, it } from 'vitest';

import type { GraphExtraction } from './graph';
import {
  createGraphElements,
  createGraphStyles,
  createLayoutPolicy,
} from './graph-viewport-policy';

cytoscape.use(fcose);

const extraction: GraphExtraction = {
  available: true,
  nodes: [
    { id: 'a', labels: ['Person'], caption: 'Ada' },
    { id: 'b', labels: ['Person'], caption: 'Grace' },
  ],
  edges: [{ id: 'ab', source: 'a', target: 'b', type: 'KNOWS', inferred: true }],
  totalNodes: 2,
  totalEdges: 1,
  truncated: false,
};

describe('graph viewport policy', () => {
  it('maps captioned circular nodes and directed relationships', () => {
    expect(createGraphElements(extraction)).toEqual([
      {
        group: 'nodes',
        data: {
          id: 'a',
          caption: 'Ada',
          colorKey: 'Person',
          labels: ['Person'],
        },
      },
      {
        group: 'nodes',
        data: {
          id: 'b',
          caption: 'Grace',
          colorKey: 'Person',
          labels: ['Person'],
        },
      },
      {
        group: 'edges',
        data: {
          id: 'ab',
          source: 'a',
          target: 'b',
          caption: 'KNOWS',
          relationshipType: 'KNOWS',
        },
      },
    ]);
  });

  it('defines 38px circles plus visible relationship shafts and arrows', () => {
    const styles = createGraphStyles('light');
    const node = styles.find(({ selector }) => selector === 'node');
    const edge = styles.find(({ selector }) => selector === 'edge');

    expect(node !== undefined && 'style' in node ? node.style : undefined).toMatchObject({
      shape: 'ellipse',
      width: 38,
      height: 38,
      'border-width': 2,
      'text-valign': 'bottom',
    });
    expect(edge !== undefined && 'style' in edge ? edge.style : undefined).toMatchObject({
      width: 2,
      'curve-style': 'bezier',
      'target-arrow-shape': 'triangle',
      'arrow-scale': 1,
    });
  });

  it('selects layout quality and animation from density and reduced motion', () => {
    expect(
      createLayoutPolicy({ nodeCount: 500, edgeCount: 1_000, motion: true, reducedMotion: false }),
    ).toMatchObject({
      name: 'fcose',
      quality: 'default',
      animate: true,
    });
    expect(
      createLayoutPolicy({ nodeCount: 501, edgeCount: 1_000, motion: true, reducedMotion: false }),
    ).toMatchObject({
      quality: 'draft',
      animate: false,
    });
    expect(
      createLayoutPolicy({ nodeCount: 500, edgeCount: 1_001, motion: true, reducedMotion: false }),
    ).toMatchObject({
      quality: 'draft',
      animate: false,
    });
    expect(
      createLayoutPolicy({ nodeCount: 2, edgeCount: 1, motion: true, reducedMotion: true }),
    ).toMatchObject({
      quality: 'default',
      animate: false,
    });
  });

  it('separates two connected nodes', async () => {
    const cy = cytoscape({
      headless: true,
      styleEnabled: true,
      elements: createGraphElements(extraction),
    });
    await runLayout(
      cy,
      createLayoutPolicy({ nodeCount: 2, edgeCount: 1, motion: false, reducedMotion: false }),
    );

    expect(cy.getElementById('a').position()).not.toEqual(cy.getElementById('b').position());
    cy.destroy();
  });

  it('lays out the 2k-node/4k-edge draft fixture within the CI timeout', async () => {
    const elements: ElementDefinition[] = [
      ...Array.from({ length: 2_000 }, (_, index) => ({ data: { id: `n-${index}` } })),
      ...Array.from({ length: 4_000 }, (_, index) => ({
        data: {
          id: `e-${index}`,
          source: `n-${index % 2_000}`,
          target: `n-${(index * 37 + 1) % 2_000}`,
        },
      })),
    ];
    const cy = cytoscape({ headless: true, elements });
    const started = performance.now();

    await runLayout(
      cy,
      createLayoutPolicy({
        nodeCount: 2_000,
        edgeCount: 4_000,
        motion: false,
        reducedMotion: false,
      }),
    );

    expect(performance.now() - started).toBeLessThan(10_000);
    cy.destroy();
  }, 10_000);
});

function runLayout(
  cy: cytoscape.Core,
  options: ReturnType<typeof createLayoutPolicy>,
): Promise<void> {
  return new Promise((resolve) => {
    cy.one('layoutstop', () => resolve());
    cy.layout(options).run();
  });
}
