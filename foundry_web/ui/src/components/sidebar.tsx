import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";

export type TabKey = "home" | "scheduler" | "dag";

export interface TabDefinition {
  key: TabKey;
  label: string;
  description?: string;
  icon: LucideIcon;
}

interface SidebarProps {
  tabs: TabDefinition[];
  activeTab: TabKey;
  onSelect: (key: TabKey) => void;
}

export function Sidebar({ tabs, activeTab, onSelect }: SidebarProps) {
  return (
    <aside className="hidden h-screen max-h-screen w-64 border-r bg-card/40 shadow-sm md:flex md:flex-col">
      <div className="px-5 pb-4 pt-6">
        <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Foundry
        </div>
        <div className="mt-2 text-lg font-semibold">Control Center</div>
      </div>
      <nav className="flex-1 space-y-1 px-3 pb-6">
        {tabs.map((tab) => {
          const Icon = tab.icon;
          const isActive = tab.key === activeTab;
          return (
            <Button
              key={tab.key}
              variant={isActive ? "secondary" : "ghost"}
              className={cn(
                "w-full justify-start gap-3 text-sm",
                isActive && "shadow-inner"
              )}
              onClick={() => onSelect(tab.key)}
            >
              <Icon className="h-4 w-4" />
              <div className="flex flex-col items-start">
                <span>{tab.label}</span>
                {tab.description ? (
                  <span className="text-xs text-muted-foreground">
                    {tab.description}
                  </span>
                ) : null}
              </div>
            </Button>
          );
        })}
      </nav>
    </aside>
  );
}
