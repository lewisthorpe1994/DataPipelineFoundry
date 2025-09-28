import { useEffect, useRef } from "react";
import cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";

import kafkaIcon from "@/assets/kafka.svg?url";
import dpfIcon from "@/assets/dpf.svg?url";
import dbIcon from "@/assets/db.svg?url";
import externalIcon from "@/assets/external.svg?url";
import type { Manifest } from "@/types/manifest";

cytoscape.use(dagre);

const ICONS = {
  kafka: `url("${kafkaIcon}")`,
  db: `url("${dbIcon}")`,
  dpf: `url("${dpfIcon}")`,
  external: `url("${externalIcon}")`,
  none: "none"
} as const;

type CyStylesheet = Record<string, unknown>;
type CyElement = {
  data: Record<string, unknown>;
  classes?: string;
};

const graphStyle: CyStylesheet[] = [
  {
    selector: "edge",
    style: {
      "curve-style": "unbundled-bezier",
      "target-arrow-shape": "triangle-backcurve",
      "target-arrow-color": "#006f8a",
      "arrow-scale": 1.5,
      "target-distance-from-node": "10px",
      "source-distance-from-node": "5px",
      "line-color": "#006f8a",
      width: 3,
      "source-endpoint": "50% 0%",
      "target-endpoint": "270deg"
    } as any
  },
  {
    selector: "node",
    style: {
      "background-color": "#0094b3",
      "border-color": "#0094b3",
      "font-size": "40px",
      shape: "roundrectangle",
      color: "#fff",
      padding: "28px",
      content: "data(label)",
      "font-weight": 500,
      "font-family": "Inter",
      "text-valign": "center",
      "text-halign": "center",
      "text-wrap": "wrap",
      "text-max-width": "240px",
      "line-height": 1.35,
      width: 360,
      height: 190,
      ghost: "yes",
      "ghost-offset-x": "2px",
      "ghost-offset-y": "4px",
      "ghost-opacity": 0.35,
      "text-outline-color": "#000",
      "text-outline-width": "1px",
      "text-outline-opacity": 0.2
    } as any
  },
  {
    selector: "node[icon != \"none\"]",
    style: {
      "background-image": "data(icon)",
      "background-width": "50px",
      "background-height": "60px",
      "background-position-x": "100%",
      "background-position-y": "100%",
      "background-offset-x": "-16px",
      "background-offset-y": "-16px",
      "background-fit": "none",
      "background-repeat": "no-repeat"
    } as any
  },
  {
    selector: "node[resource_type=\"source\"]",
    style: { "background-color": "#5fb825", "border-color": "#5fb825" } as any
  },
  {
    selector: "node[resource_type=\"exposure\"]",
    style: { "background-color": "#ff694b", "border-color": "#ff694b" } as any
  },
  {
    selector: "node[resource_type=\"metric\"]",
    style: { "background-color": "#ff5688", "border-color": "#ff5688" } as any
  },
  {
    selector: "node[resource_type=\"semantic_model\"]",
    style: { "background-color": "#ffa8c2", "border-color": "#ffa8c2" } as any
  },
  {
    selector: "node[resource_type=\"saved_query\"]",
    style: { "background-color": "#ff7f50", "border-color": "#ff7f50" } as any
  },
  {
    selector: "node[resource_type=\"external\"]",
    style: {
      "background-color": "#64748b",
      "border-color": "#475569"
    } as any
  }
];

interface DagGraphProps {
  manifest: Manifest;
}

export function DagGraph({ manifest }: DagGraphProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const elements = buildElements(manifest);
    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: graphStyle,
      layout: {
        name: "dagre",
        rankDir: "LR",
        rankSep: 200,
        edgeSep: 30,
        nodeSep: 50
      },
      wheelSensitivity: 0.1
    });

    cy.resize();
    cy.center();
    cy.fit(undefined, 40);

    const handleResize = () => {
      cy.resize();
      cy.fit(undefined, 40);
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      cy.destroy();
    };
  }, [manifest]);

  return <div ref={containerRef} className="h-full w-full" />;
}

function buildElements(manifest: Manifest): CyElement[] {
  const nodeMap = new Map<string, CyElement>();
  const manifestNodes = manifest.nodes;

  manifestNodes.forEach((node) => {
    const nodeType = getNodeType(node as any);
    const icon = getIconForType(nodeType);

    const element: CyElement = {
      data: {
        id: node.name,
        label: formatLabel(node.name),
        resource_type: node.resource_type ?? "model",
        icon
      },
      classes: "horizontal"
    };

    nodeMap.set(node.name, element);
  });

  const externalNodes = new Map<string, CyElement>();
  const edges: CyElement[] = [];

  manifestNodes.forEach((node) => {
    (node.depends_on ?? []).forEach((dep) => {
      const matched = manifestNodes.find(
        (candidate) => candidate.name === dep || candidate.name.endsWith(`.${dep}`)
      );
      const source = matched ? matched.name : dep;

      if (!nodeMap.has(source) && !externalNodes.has(source)) {
        externalNodes.set(source, {
          data: {
            id: source,
            label: formatLabel(source),
            resource_type: "external",
            icon: ICONS.external
          },
          classes: "horizontal"
        });
      }

      edges.push({
        data: {
          id: `${source}|${node.name}`,
          source,
          target: node.name
        },
        classes: "horizontal"
      });
    });
  });

  return [...nodeMap.values(), ...externalNodes.values(), ...edges];
}

function formatLabel(name: string): string {
  // Insert zero-width spaces after separators so Cytoscape can wrap long tokens.
  return name.replace(/([._\-])/g, "$1\u200b");
}

function getNodeType(node: { node_type?: string; nodeType?: string; type?: string } | undefined) {
  const raw = node?.node_type ?? node?.nodeType ?? node?.type ?? "";
  return raw.trim();
}

function getIconForType(nodeType?: string): string {
  console.log("node type", nodeType);
  const value = (nodeType ?? "").toLowerCase();
  if (value.includes("kafka")) {
    return ICONS.kafka;
  }
  if (value === "dpf" || value.includes("dpf")) {
    return ICONS.dpf;
  }
  if (value.includes("db") || value.includes("database")) {
    return ICONS.db;
  }
  if (value === "external" || value.includes("external")) {
    return ICONS.external;
  }
  return ICONS.none;
}
