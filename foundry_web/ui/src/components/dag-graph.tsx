import { useEffect, useMemo } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  DefaultEdgeOptions,
  Edge,
  EdgeProps,
  EdgeTypes,
  Handle,
  Node,
  NodeProps,
  NodeTypes,
  Position,
  getBezierPath,
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
import { Cloud } from "lucide-react";

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
  markerEnd: "edge-circle"
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
        <svg className="absolute h-0 w-0">
          <defs>
            <linearGradient id="edge-gradient">
              <stop offset="0%" stopColor="#ae53ba" />
              <stop offset="100%" stopColor="#2a8af6" />
            </linearGradient>
            <marker
              id="edge-circle"
              viewBox="-5 -5 10 10"
              refX="0"
              refY="0"
              markerUnits="strokeWidth"
              markerWidth="10"
              markerHeight="10"
              orient="auto"
            >
              <circle stroke="#2a8af6" strokeOpacity="0.75" r="2" cx="0" cy="0" />
            </marker>
          </defs>
        </svg>
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
        type: "turbo"
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
      <div className="cloud gradient">
        <div>
          {data.icon ? (
            <img src={data.icon} alt="" className="cloud-icon" />
          ) : (
            <Cloud className="h-4 w-4 text-white" />
          )}
        </div>
      </div>
      <div
        className={cn("wrapper gradient", selected && "selected")}
        style={{ boxShadow: `0 30px 80px ${data.accentColor}33` }}
      >
        <div
          className="inner"
          style={{
            backgroundImage: data.background,
            borderColor: `${data.accentColor}55`
          }}
        >
          <div className="body">
            <div>
              <div className="resource" style={{ color: `${data.accentColor}cc` }}>
                {data.resourceType}
              </div>
              <div className="title">{data.label}</div>
              <div className="subtitle">{data.detail ?? "External dependency"}</div>
            </div>
          </div>
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
      </div>
    </div>
  );
}

function TurboEdge({ id, sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition, markerEnd }: EdgeProps<TurboEdgeData>) {
  const xEqual = sourceX === targetX;
  const yEqual = sourceY === targetY;

  const [edgePath] = getBezierPath({
    sourceX: xEqual ? sourceX + 0.0001 : sourceX,
    sourceY: yEqual ? sourceY + 0.0001 : sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition
  });

  return (
    <path
      id={id}
      className="react-flow__edge-path"
      d={edgePath}
      markerEnd={markerEnd}
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
