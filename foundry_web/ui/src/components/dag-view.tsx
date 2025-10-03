import { useCallback, useEffect, useMemo, useState } from "react";

import { DagGraph } from "@/components/dag-graph";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
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
    <div className="flex flex-col gap-6">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Total nodes</CardTitle>
            <CardDescription>Tracked resources in this manifest.</CardDescription>
          </CardHeader>
          <CardContent>
            {state.loading && !state.manifest ? (
              <Skeleton className="h-9 w-24" />
            ) : (
              <div className="text-3xl font-semibold">{summary.total}</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Executable nodes</CardTitle>
            <CardDescription>Models and connectors ready to run.</CardDescription>
          </CardHeader>
          <CardContent>
            {state.loading && !state.manifest ? (
              <Skeleton className="h-9 w-24" />
            ) : (
              <div className="text-3xl font-semibold">{summary.executable}</div>
            )}
            <p className="mt-2 text-xs text-muted-foreground">
              {summary.total > 0
                ? `${Math.round((summary.executable / summary.total) * 100)}% of manifest`
                : "Awaiting manifest data"}
            </p>
          </CardContent>
        </Card>
        <Card className="sm:col-span-2 xl:col-span-1">
          <CardHeader>
            <CardTitle>Manifest status</CardTitle>
            <CardDescription>Refresh to pull the latest compiled graph.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <div className="text-sm text-muted-foreground">
              {state.fetchedAt
                ? `Last updated ${state.fetchedAt.toLocaleString()}`
                : "Manifest has not been loaded yet"}
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <Button variant="outline" onClick={refresh} disabled={state.loading}>
                {state.loading ? "Refreshing" : "Refresh"}
              </Button>
              {state.error ? (
                <span className="text-xs text-destructive">
                  {state.error}
                </span>
              ) : null}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="flex min-h-[620px] flex-1 flex-col overflow-hidden">
        <CardHeader className="flex flex-col gap-2 border-b bg-muted/40 py-4 sm:flex-row sm:items-center sm:justify-between sm:space-y-0">
          <div>
            <CardTitle className="text-lg">Manifest graph</CardTitle>
            <CardDescription>Visual dependency map generated from your compiled manifest.</CardDescription>
          </div>
          {state.manifest ? (
            <span className="text-xs text-muted-foreground">
              {state.manifest.nodes.length} total nodes
            </span>
          ) : null}
        </CardHeader>
        <CardContent className="flex-1 p-0">
          {state.loading && !state.manifest ? (
            <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
              Loading manifest…
            </div>
          ) : null}

          {state.error && !state.manifest ? (
            <div className="flex h-full items-center justify-center">
              <div className="rounded-md border border-destructive/40 bg-destructive/10 px-6 py-4 text-sm text-destructive">
                Failed to load manifest: {state.error}
              </div>
            </div>
          ) : null}

          {state.manifest ? (
            <div className="relative min-h-[560px] p-4">
              {state.manifest.nodes.length ? (
                <DagGraph manifest={state.manifest} />
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
                  No nodes found in manifest.
                </div>
              )}
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
