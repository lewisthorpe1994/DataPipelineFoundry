export interface ManifestNode {
  name: string;
  compiled_executable?: string | null;
  depends_on?: string[];
  executable?: boolean;
  resource_type?: string;
  node_type?: string;
  nodeType?: string;
  type?: string;
}

export interface Manifest {
  nodes: ManifestNode[];
}
