import { useMemo, useState } from "react";
import { CalendarClock, Home, Workflow } from "lucide-react";

import { Sidebar, type TabDefinition, type TabKey } from "@/components/sidebar";
import { DagView } from "@/components/dag-view";
import { Placeholder } from "@/components/placeholders";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const TABS: TabDefinition[] = [
  {
    key: "home",
    label: "Home",
    description: "Project overview",
    icon: Home
  },
  {
    key: "scheduler",
    label: "Scheduler",
    description: "Coming soon",
    icon: CalendarClock
  },
  {
    key: "dag",
    label: "DAG",
    description: "Model lineage",
    icon: Workflow
  }
];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("dag");

  const content = useMemo(() => {
    switch (activeTab) {
      case "dag":
        return <DagView />;
      case "scheduler":
        return (
          <Placeholder
            title="Scheduler coming soon"
            description="Plan and monitor execution windows for models and connectors."
          />
        );
      case "home":
      default:
        return (
          <Placeholder
            title="Welcome to Foundry Web"
            description="Use the sidebar to switch between project features. The DAG view is ready today; the rest is on the roadmap."
          />
        );
    }
  }, [activeTab]);

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <Sidebar tabs={TABS} activeTab={activeTab} onSelect={setActiveTab} />
      <main className="flex-1 overflow-hidden">
        <header className="flex items-center justify-between border-b px-4 py-3 shadow-sm">
          <div>
            <div className="text-sm font-medium text-muted-foreground">Foundry</div>
            <h1 className="text-lg font-semibold capitalize">{activeTab}</h1>
          </div>
          <div className="hidden gap-2 md:flex">
            <Button
              variant="outline"
              className={cn(activeTab === "dag" && "border-primary text-primary")}
              onClick={() => setActiveTab("dag")}
            >
              View DAG
            </Button>
          </div>
        </header>
        <div className="md:hidden border-b bg-card/50 px-3 py-2">
          <div className="flex items-center gap-2">
            {TABS.map((tab) => (
              <Button
                key={tab.key}
                variant={tab.key === activeTab ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
              </Button>
            ))}
          </div>
        </div>
        <section className="flex h-[calc(100vh-4.5rem)] flex-col overflow-hidden p-4">
          <div className="flex-1 overflow-hidden rounded-lg bg-muted/30 p-2">
            {content}
          </div>
        </section>
      </main>
    </div>
  );
}
