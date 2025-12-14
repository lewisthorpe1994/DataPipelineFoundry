import { useMemo, useState } from "react";
import { CalendarClock, Home, Workflow } from "lucide-react";

import { AppSidebar, type TabDefinition, type TabKey } from "@/components/sidebar";
import { DagView } from "@/components/dag-view";
import { Placeholder } from "@/components/placeholders";
import { Button } from "@/components/ui/button";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Separator } from "@/components/ui/separator";
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
  const activeTabMeta = useMemo(
    () => TABS.find((tab) => tab.key === activeTab),
    [activeTab]
  );

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
    <SidebarProvider defaultOpen className="bg-background text-foreground">
      <AppSidebar tabs={TABS} activeTab={activeTab} onSelect={setActiveTab} />
      <SidebarInset>
        <header className="flex h-16 shrink-0 items-center gap-2 border-b bg-background/95 px-4 shadow-sm backdrop-blur transition-[width,height] ease-linear supports-[backdrop-filter]:bg-background/75 group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
          <div className="flex flex-1 items-center gap-2">
            <SidebarTrigger className="-ml-1" />
            <Separator orientation="vertical" className="mr-2 h-4" />
            <Breadcrumb>
              <BreadcrumbList>
                <BreadcrumbItem className="hidden md:block">
                  <BreadcrumbLink href="#">Data Pipeline Foundry</BreadcrumbLink>
                </BreadcrumbItem>
                <BreadcrumbSeparator className="hidden md:block" />
                <BreadcrumbItem>
                  <BreadcrumbPage>{activeTabMeta?.label ?? "Overview"}</BreadcrumbPage>
                </BreadcrumbItem>
              </BreadcrumbList>
            </Breadcrumb>
          </div>
          <div className="hidden shrink-0 items-center gap-2 md:flex">
            <Button
              variant="outline"
              className={cn(activeTab === "dag" && "border-primary text-primary")}
              onClick={() => setActiveTab("dag")}
            >
              View DAG
            </Button>
          </div>
        </header>
        <div className="border-b bg-card/50 px-3 py-2 md:hidden">
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
        <section className="flex min-h-[calc(100vh-4rem)] flex-col gap-4 p-4 pt-4">
          <div className="flex-1 overflow-hidden rounded-lg bg-muted/30 p-2">
            {content}
          </div>
        </section>
      </SidebarInset>
    </SidebarProvider>
  );
}
