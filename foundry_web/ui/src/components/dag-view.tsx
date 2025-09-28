import { useCallback, useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { DagGraph } from "@/components/dag-graph";
import type { Manifest } from "@/types/manifest";

interface FetchState {
  loading: boolean;
  error?: string;
  manifest?: Manifest;
  fetchedAt?: Date;
}

const initialState: FetchState = {
  loading: true
};

async function fetchManifest(signal?: AbortSignal): Promise<Manifest> {
  const baseUrl = import.meta.env.VITE_BACKEND_URL ?? "";
  const response = await fetch(`${baseUrl}/api/manifest`, { signal });
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return (await response.json()) as Manifest;
}

export function DagView() {
  const [state, setState] = useState<FetchState>(initialState);

  const refresh = useCallback(async () => {
    setState((prev) => ({ ...prev, loading: true, error: undefined }));
    try {
      const manifest = await fetchManifest();
      setState({ manifest, loading: false, fetchedAt: new Date() });
    } catch (error) {
      setState((prev) => ({
        ...prev,
        loading: false,
        error: (error as Error).message
      }));
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    async function load() {
      setState(initialState);
      try {
        const manifest = await fetchManifest(controller.signal);
        setState({ manifest, loading: false, fetchedAt: new Date() });
      } catch (error) {
        if ((error as Error).name === "AbortError") {
          return;
        }
        setState({ loading: false, error: (error as Error).message });
      }
    }

    load();
    return () => controller.abort();
  }, []);

  const summary = useMemo(() => {
    if (!state.manifest) {
      return { total: 0, executable: 0 };
    }
    const total = state.manifest.nodes.length;
    const executable = state.manifest.nodes.filter((node) => node.executable).length;
    return { total, executable };
  }, [state.manifest]);

  return (
    <div className="flex h-full flex-col gap-4">
      <div className="flex flex-col justify-between gap-4 border-b bg-card/60 p-4 sm:flex-row sm:items-center">
        <div>
          <h1 className="text-xl font-semibold">Project DAG</h1>
          <p className="text-sm text-muted-foreground">
            Visualise compiled model dependencies from your manifest.json.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="text-sm text-muted-foreground">
            <div>{summary.total} nodes</div>
            <div>{summary.executable} executable</div>
          </div>
          <Button variant="outline" onClick={refresh} disabled={state.loading}>
            {state.loading ? "Refreshing" : "Refresh"}
          </Button>
        </div>
      </div>
      <div className="flex-1 overflow-hidden">
        {state.loading && !state.manifest ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            Loading manifest…
          </div>
        ) : null}

        {state.error ? (
          <div className="flex h-full items-center justify-center">
            <div className="rounded-md border border-destructive/40 bg-destructive/10 px-6 py-4 text-sm text-destructive">
              Failed to load manifest: {state.error}
            </div>
          </div>
        ) : null}

        {state.manifest ? (
          <div className="h-full min-h-[600px] rounded-lg border bg-card p-2">
            {state.manifest.nodes.length ? (
              <DagGraph manifest={state.manifest} />
            ) : (
              <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
                No nodes found in manifest.
              </div>
            )}
          </div>
        ) : null}
      </div>
      {state.fetchedAt ? (
        <div className="border-t bg-card/40 px-4 py-2 text-right text-xs text-muted-foreground">
          Last updated {state.fetchedAt.toLocaleTimeString()}
        </div>
      ) : null}
    </div>
  );
}
