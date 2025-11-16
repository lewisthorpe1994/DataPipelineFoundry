import { useEffect, useMemo } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  BaseEdge,
  Controls,
  DefaultEdgeOptions,
  Edge,
  EdgeProps,
  EdgeTypes,
  MarkerType,
  Node,
  NodeProps,
  NodeTypes,
  Position,
  Handle,
  getSmoothStepPath,
  useEdgesState,
  useNodesState
} from "reactflow";
import dagre from "dagre";
import "reactflow/dist/style.css";

import kafkaIcon from "@/assets/Apache_Kafka_logo.svg?url";
import dpfIcon from "@/assets/dpf.svg?url";
import dbIcon from "@/assets/db.svg?url";
import externalIcon from "@/assets/external.svg?url";
import { cn } from "@/lib/utils";
import type { Manifest } from "@/types/manifest";

interface DagGraphProps {
  manifest: Manifest;
}

interface TurboNodeData {
  label: string;
  resourceType: string;
  detail?: string;
  icon?: string;
  accentColor: string;
  background: string;
}

interface TurboEdgeData {
  color: string;
}

const ICONS = {
  kafka: kafkaIcon,
  db: dbIcon,
  dpf: dpfIcon,
  external: externalIcon,
  none: undefined
} as const;

const NODE_WIDTH = 380;
const NODE_HEIGHT = 190;

const nodeTypes: NodeTypes = { turbo: TurboNode };
const edgeTypes: EdgeTypes = { turbo: TurboEdge };

const defaultEdgeOptions: DefaultEdgeOptions = {
  type: "turbo",
  markerEnd: { type: MarkerType.ArrowClosed, color: "#67e8f9", width: 18, height: 18 }
};

type LayoutResult = {
  nodes: Node<TurboNodeData>[];
  edges: Edge<TurboEdgeData>[];
};

export function DagGraph({ manifest }: DagGraphProps) {
  const layouted = useMemo(() => buildGraph(manifest), [manifest]);
  const [nodes, setNodes, onNodesChange] = useNodesState<TurboNodeData>(layouted.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<TurboEdgeData>(layouted.edges);

  useEffect(() => {
    setNodes(layouted.nodes);
    setEdges(layouted.edges);
  }, [layouted.nodes, layouted.edges, setNodes, setEdges]);

  return (
    <div className="absolute inset-0 h-full w-full overflow-hidden rounded-xl border border-border bg-slate-950">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        fitView
        fitViewOptions={{ padding: 0.25 }}
        minZoom={0.4}
        maxZoom={1.4}
        nodesConnectable={false}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        panOnScroll
        zoomOnPinch
        zoomOnScroll
        panOnDrag
        proOptions={{ hideAttribution: true }}
        className="h-full w-full"
      >
        <Background id="turbo-grid" variant={BackgroundVariant.Dots} size={1} gap={24} color="#1f2937" />
        <Controls showInteractive={false} className="border-none bg-slate-900/80 text-white" />
      </ReactFlow>
    </div>
  );
}

function buildGraph(manifest: Manifest): LayoutResult {
  const nodeMap = new Map<string, Node<TurboNodeData>>();
  const externalNodes = new Map<string, Node<TurboNodeData>>();
  const edges: Edge<TurboEdgeData>[] = [];
  const manifestNodes = manifest.nodes ?? [];

  manifestNodes.forEach((node) => {
    const type = getNodeType(node);
    const palette = getPaletteForType(type);
    const icon = getIconForType(type);

    const normalizedResource = (node.resource_type ?? type ?? "model").trim() || "model";
    const detail = (type || node.resource_type || "").trim();

    nodeMap.set(node.name, {
      id: node.name,
      type: "turbo",
      data: {
        label: formatLabel(node.name),
        resourceType: normalizedResource.toUpperCase(),
        detail: detail ? detail.toUpperCase() : undefined,
        icon,
        accentColor: palette.accent,
        background: palette.background
      },
      position: { x: 0, y: 0 }
    });
  });

  manifestNodes.forEach((node) => {
    const dependencies = node.depends_on ?? [];
    const palette = getPaletteForType(getNodeType(node));

    dependencies.forEach((dep) => {
      const matched = manifestNodes.find(
        (candidate) => candidate.name === dep || candidate.name.endsWith(`.${dep}`)
      );
      const source = matched ? matched.name : dep;

      if (!nodeMap.has(source) && !externalNodes.has(source)) {
        const externalPalette = getPaletteForType("external");
        externalNodes.set(source, {
          id: source,
          type: "turbo",
          data: {
            label: formatLabel(source),
            resourceType: "EXTERNAL",
            detail: "External dependency",
            icon: ICONS.external,
            accentColor: externalPalette.accent,
            background: externalPalette.background
          },
          position: { x: 0, y: 0 }
        });
      }

      edges.push({
        id: `${source}|${node.name}`,
        source,
        target: node.name,
        type: "turbo",
        data: { color: palette.edge },
        markerEnd: { type: MarkerType.ArrowClosed, color: palette.edge, width: 18, height: 18 }
      });
    });
  });

  const layoutNodes = layout([...nodeMap.values(), ...externalNodes.values()], edges);
  return { nodes: layoutNodes, edges };
}

function layout(nodes: Node<TurboNodeData>[], edges: Edge<TurboEdgeData>[]) {
  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({ rankdir: "LR", nodesep: 120, ranksep: 160, edgesep: 40 });

  nodes.forEach((node) => {
    graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });
  edges.forEach((edge) => {
    graph.setEdge(edge.source, edge.target);
  });

  dagre.layout(graph);

  return nodes.map((node) => {
    const position = graph.node(node.id);
    return {
      ...node,
      position: {
        x: position?.x ? position.x - NODE_WIDTH / 2 : 0,
        y: position?.y ? position.y - NODE_HEIGHT / 2 : 0
      },
      targetPosition: Position.Left,
      sourcePosition: Position.Right
    };
  });
}

function TurboNode({ data, selected }: NodeProps<TurboNodeData>) {
  const handleGlow = `0 6px 20px ${data.accentColor}66`;

  return (
    <div className="relative h-full w-full">
      <Handle
        type="target"
        position={Position.Left}
        isConnectable={false}
        style={{
          background: data.accentColor,
          width: 24,
          height: 24,
          borderRadius: 999,
          border: "3px solid #020617",
          boxShadow: handleGlow
        }}
        className="-translate-x-1/2"
      />
      <div
        className={cn(
          "flex h-full w-full flex-col rounded-3xl border-2 text-white shadow-[0_30px_80px_rgba(15,23,42,0.6)]",
          selected && "ring-2 ring-cyan-300/70"
        )}
        style={{
          borderColor: data.accentColor,
          backgroundImage: data.background,
          boxShadow: `0 20px 60px rgba(15,23,42,0.45), inset 0 0 0 1px ${data.accentColor}33`
        }}
      >
        <div className="flex items-center justify-between gap-3 border-b border-white/15 px-6 py-4">
          <div className="flex flex-col">
            <span className="text-[10px] font-semibold uppercase tracking-[0.35em] text-white/60">
              {data.resourceType}
            </span>
            <span className="text-lg font-semibold leading-tight text-white break-words">
              {data.label}
            </span>
          </div>
          {data.icon ? (
            <span className="inline-flex h-12 w-12 items-center justify-center rounded-2xl bg-white/10 ring-1 ring-white/20">
              <img src={data.icon} alt="" className="h-8 w-8 object-contain" />
            </span>
          ) : null}
        </div>
        <div className="flex flex-1 items-center px-6 text-sm text-white/80">
          <p className="leading-relaxed">{data.detail ?? "External dependency"}</p>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        isConnectable={false}
        style={{
          background: data.accentColor,
          width: 24,
          height: 24,
          borderRadius: 999,
          border: "3px solid #020617",
          boxShadow: handleGlow
        }}
        className="translate-x-1/2"
      />
    </div>
  );
}

function TurboEdge({ id, sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition, data, markerEnd }: EdgeProps<TurboEdgeData>) {
  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 24
  });

  const stroke = data?.color ?? "#67e8f9";

  return (
    <BaseEdge
      id={id}
      path={edgePath}
      markerEnd={markerEnd}
      style={{
        stroke,
        strokeWidth: 3,
        filter: "drop-shadow(0px 8px 24px rgba(103,232,249,0.35))"
      }}
    />
  );
}

function formatLabel(name: string): string {
  return name.replace(/([._\-])/g, "$1\u200b");
}

function getNodeType(node: { node_type?: string; nodeType?: string; type?: string; resource_type?: string } | undefined) {
  const raw = node?.resource_type ?? node?.node_type ?? node?.nodeType ?? node?.type ?? "";
  return raw.trim();
}

function getIconForType(nodeType?: string) {
  const value = (nodeType ?? "").toLowerCase();
  if (value.includes("kafka")) {
    return ICONS.kafka;
  }
  if (value.includes("dpf")) {
    return ICONS.dpf;
  }
  if (value.includes("db") || value.includes("database")) {
    return ICONS.db;
  }
  if (value.includes("external")) {
    return ICONS.external;
  }
  return undefined;
}

interface Palette {
  accent: string;
  background: string;
  edge: string;
}

function getPaletteForType(nodeType?: string): Palette {
  const value = (nodeType ?? "").toLowerCase();

  if (value.includes("kafka")) {
    return {
      accent: "#f97316",
      background: "linear-gradient(135deg, #451a03, #7c2d12)",
      edge: "#fdba74"
    };
  }

  if (value.includes("dpf")) {
    return {
      accent: "#a855f7",
      background: "linear-gradient(135deg, #312e81, #4c1d95)",
      edge: "#c4b5fd"
    };
  }

  if (value.includes("db")) {
    return {
      accent: "#22c55e",
      background: "linear-gradient(135deg, #052e16, #166534)",
      edge: "#4ade80"
    };
  }

  if (value.includes("external")) {
    return {
      accent: "#94a3b8",
      background: "linear-gradient(135deg, #0f172a, #1f2937)",
      edge: "#cbd5f5"
    };
  }

  return {
    accent: "#06b6d4",
    background: "linear-gradient(135deg, #0f172a, #0e7490)",
    edge: "#67e8f9"
  };
}
