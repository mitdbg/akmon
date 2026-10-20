declare module "*.css" {
  const stylesheet: string;
  export default stylesheet;
}

declare module "d3-flame-graph" {
  export interface FlamegraphHierarchyNode {
    data: {
      name: string;
      value: number;
      raw_value?: number;
      trace_id?: string;
      span_id?: string;
      gap_id?: string;
      tooltip?: string;
      children?: FlamegraphHierarchyNode["data"][];
    };
    parent?: FlamegraphHierarchyNode;
    x0: number;
    x1: number;
  }

  export interface FlamegraphChart {
    (selection: unknown): void;
    width(value: number): FlamegraphChart;
    height(value: number): FlamegraphChart;
    cellHeight(value: number): FlamegraphChart;
    minHeight(value: number): FlamegraphChart;
    minFrameSize(value: number): FlamegraphChart;
    transitionDuration(value: number): FlamegraphChart;
    sort(value: boolean): FlamegraphChart;
    inverted(value: boolean): FlamegraphChart;
    getName(
      handler: (node: FlamegraphHierarchyNode) => string,
    ): FlamegraphChart;
    search(term: string): void;
    clear(): void;
    resetZoom(): void;
    destroy(): void;
    setDetailsHandler(
      handler: (detail: string | null) => void,
    ): FlamegraphChart;
    onClick(
      handler: (node: FlamegraphHierarchyNode) => void,
    ): FlamegraphChart;
    setSearchHandler(
      handler: (
        searchResults: FlamegraphHierarchyNode[],
        searchSum: number,
        totalValue: number,
      ) => void,
    ): FlamegraphChart;
    setLabelHandler(
      handler: (node: FlamegraphHierarchyNode) => string,
    ): FlamegraphChart;
    setColorMapper(
      handler: (node: FlamegraphHierarchyNode, originalColor: string) => string,
    ): FlamegraphChart;
  }

  export default function flamegraph(): FlamegraphChart;
}
