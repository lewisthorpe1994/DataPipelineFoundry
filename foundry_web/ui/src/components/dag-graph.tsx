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
  Handle,
  MarkerType,
  Node,
  NodeProps,
  NodeTypes,
  Position,
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

import "./dag-graph.css";

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

const NODE_WIDTH = 360;
const NODE_HEIGHT = 180;

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
    <div className="dag-graph absolute inset-0 h-full w-full overflow-hidden rounded-xl border border-border">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        fitView
        fitViewOptions={{ padding: 0.25 }}
        minZoom={0.08}
        maxZoom={1.5}
        nodesConnectable={false}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        zoomOnScroll
        zoomOnPinch
        panOnDrag
        proOptions={{ hideAttribution: true }}
        className="h-full w-full"
      >
        <Background id="turbo-grid" variant={BackgroundVariant.Dots} size={1} gap={24} color="#1f2937" />
        <Controls showInteractive={false} />
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
  graph.setGraph({ rankdir: "LR", nodesep: 150, ranksep: 180, edgesep: 60 });

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
    <div className="turbo-node relative h-full w-full">
      <Handle
        type="target"
        position={Position.Left}
        isConnectable={false}
        style={{
          background: data.accentColor,
          width: 20,
          height: 20,
          borderRadius: 999,
          border: "3px solid #020617",
          boxShadow: handleGlow
        }}
        className="-translate-x-1/2"
      />
      <div className={cn("turbo-frame", selected && "selected")}
        style={{
          boxShadow: `0 30px 80px rgba(15,23,42,0.5)`
        }}
      >
        <div
          className="turbo-inner"
          style={{
            backgroundImage: data.background,
            borderColor: `${data.accentColor}55`
          }}
        >
          <div className="turbo-header">
            <div className="turbo-meta">
              <span className="turbo-resource">{data.resourceType}</span>
              <span className="turbo-title">{data.label}</span>
            </div>
            {data.icon ? (
              <span className="turbo-icon">
                <img src={data.icon} alt="" className="h-7 w-7 object-contain" />
              </span>
            ) : null}
          </div>
          <div className="turbo-detail">{data.detail ?? "External dependency"}</div>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        isConnectable={false}
        style={{
          background: data.accentColor,
          width: 20,
          height: 20,
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
