import type { GraphNode } from './graph';

export type GroupingMode = 'auto' | 'none' | 'type';

export interface GraphCluster {
  readonly id: string;
  readonly label: string;
  readonly memberIds: readonly string[];
}

export interface GraphClustering {
  readonly clusters: readonly GraphCluster[];
  readonly parentByNodeId: Readonly<Record<string, string>>;
}

export const MIN_CLUSTER_SIZE = 2;
export const MAX_CLUSTER_SIZE = 500;
export const DEFAULT_CLUSTER_SIZE = 10;

const UNLABELED_GROUP = 'Unlabeled';

export const EMPTY_CLUSTERING: GraphClustering = { clusters: [], parentByNodeId: {} };

/**
 * Picks the cluster capacity for auto grouping. With clusters of capacity m a
 * group of n nodes renders as ceil(n/m) cluster boxes of up to m members; the
 * on-screen element count per group is roughly n/m + m, which is minimal at
 * m = sqrt(n). The result is clamped so tiny results still group meaningfully
 * and huge results never produce unreadable mega-clusters.
 */
export function optimalClusterSize(totalNodes: number): number {
  if (!Number.isFinite(totalNodes) || totalNodes <= 0) return DEFAULT_CLUSTER_SIZE;
  return Math.min(40, Math.max(4, Math.ceil(Math.sqrt(totalNodes))));
}

export function clampClusterSize(size: number): number {
  if (!Number.isFinite(size)) return DEFAULT_CLUSTER_SIZE;
  return Math.min(MAX_CLUSTER_SIZE, Math.max(MIN_CLUSTER_SIZE, Math.round(size)));
}

/**
 * Groups nodes by their first label. Groups larger than the capacity are split
 * into numbered parts ("Person (2/3)") so no cluster exceeds the capacity;
 * capacity comes from the setting, or from optimalClusterSize in auto mode.
 * Single-node groups stay unclustered - a box around one node is noise.
 */
export function computeClusters(
  nodes: readonly GraphNode[],
  mode: GroupingMode,
  clusterSize: number,
): GraphClustering {
  if (mode === 'none' || nodes.length === 0) return EMPTY_CLUSTERING;

  const capacity =
    mode === 'auto' ? optimalClusterSize(nodes.length) : clampClusterSize(clusterSize);

  const groups = new Map<string, string[]>();
  for (const node of nodes) {
    const group = node.labels[0] ?? UNLABELED_GROUP;
    const members = groups.get(group) ?? [];
    members.push(node.id);
    groups.set(group, members);
  }

  const clusters: GraphCluster[] = [];
  const parentByNodeId: Record<string, string> = {};

  for (const [group, members] of [...groups.entries()].sort(([a], [b]) => compareText(a, b))) {
    if (members.length < MIN_CLUSTER_SIZE) continue;

    const partCount = Math.ceil(members.length / capacity);
    for (let part = 0; part < partCount; part += 1) {
      const memberIds = members.slice(part * capacity, (part + 1) * capacity);
      const id = `cluster:${group}:${part}`;
      clusters.push({
        id,
        label: partCount === 1 ? group : `${group} (${part + 1}/${partCount})`,
        memberIds,
      });
      for (const memberId of memberIds) parentByNodeId[memberId] = id;
    }
  }

  return { clusters, parentByNodeId };
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}
