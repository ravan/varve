import { describe, expect, it } from 'vitest';

import type { GraphNode } from './graph';
import {
  clampClusterSize,
  computeClusters,
  DEFAULT_CLUSTER_SIZE,
  MAX_CLUSTER_SIZE,
  MIN_CLUSTER_SIZE,
  optimalClusterSize,
} from './clustering';

function node(id: string, ...labels: string[]): GraphNode {
  return { id, labels };
}

describe('optimalClusterSize', () => {
  it('grows with the square root of the node count', () => {
    expect(optimalClusterSize(16)).toBe(4);
    expect(optimalClusterSize(100)).toBe(10);
    expect(optimalClusterSize(101)).toBe(11);
  });

  it('clamps degenerate and extreme inputs', () => {
    expect(optimalClusterSize(0)).toBe(DEFAULT_CLUSTER_SIZE);
    expect(optimalClusterSize(Number.NaN)).toBe(DEFAULT_CLUSTER_SIZE);
    expect(optimalClusterSize(2)).toBe(4);
    expect(optimalClusterSize(1_000_000)).toBe(40);
  });
});

describe('clampClusterSize', () => {
  it('bounds and rounds the manual setting', () => {
    expect(clampClusterSize(0)).toBe(MIN_CLUSTER_SIZE);
    expect(clampClusterSize(7.6)).toBe(8);
    expect(clampClusterSize(9_999)).toBe(MAX_CLUSTER_SIZE);
    expect(clampClusterSize(Number.NaN)).toBe(DEFAULT_CLUSTER_SIZE);
  });
});

describe('computeClusters', () => {
  const people = [node('p1', 'Person'), node('p2', 'Person'), node('p3', 'Person')];

  it('returns no clusters when grouping is off', () => {
    expect(computeClusters(people, 'none', 10)).toEqual({ clusters: [], parentByNodeId: {} });
  });

  it('groups nodes by their first label', () => {
    const clustering = computeClusters(
      [...people, node('s1', 'Service'), node('s2', 'Service')],
      'type',
      10,
    );

    expect(clustering.clusters.map(({ label }) => label)).toEqual(['Person', 'Service']);
    expect(clustering.parentByNodeId.p1).toBe('cluster:Person:0');
    expect(clustering.parentByNodeId.s2).toBe('cluster:Service:0');
  });

  it('splits oversized groups into numbered parts', () => {
    const clustering = computeClusters(
      [...people, node('p4', 'Person'), node('p5', 'Person')],
      'type',
      2,
    );

    expect(clustering.clusters.map(({ label }) => label)).toEqual([
      'Person (1/3)',
      'Person (2/3)',
      'Person (3/3)',
    ]);
    expect(clustering.clusters.map(({ memberIds }) => memberIds)).toEqual([
      ['p1', 'p2'],
      ['p3', 'p4'],
      ['p5'],
    ]);
  });

  it('leaves single-node groups unclustered and groups unlabeled nodes together', () => {
    const clustering = computeClusters(
      [node('lonely', 'Singleton'), node('u1'), node('u2')],
      'type',
      10,
    );

    expect(clustering.clusters.map(({ label }) => label)).toEqual(['Unlabeled']);
    expect(clustering.parentByNodeId.lonely).toBeUndefined();
  });

  it('derives the capacity from the node count in auto mode', () => {
    const many = Array.from({ length: 25 }, (_, index) => node(`n${index}`, 'Thing'));
    const clustering = computeClusters(many, 'auto', 999);

    expect(clustering.clusters).toHaveLength(5);
    expect(clustering.clusters.every(({ memberIds }) => memberIds.length <= 5)).toBe(true);
  });
});
