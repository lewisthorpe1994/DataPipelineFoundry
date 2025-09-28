import type { LucideIcon } from "lucide-react";

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/ui/sidebar";
import Logo from "@/assets/dpf-logo.svg";

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

export function AppSidebar({ tabs, activeTab, onSelect }: SidebarProps) {
  return (
    <Sidebar
      collapsible="icon"
      className="border-r bg-sidebar text-sidebar-foreground"
      variant="sidebar"
    >
      <SidebarHeader className="px-2 py-2">
        <SidebarMenuButton>
          {/*<div className={"bg-blue-500 rounded-lg p-1"}>*/}
          <img src={Logo} alt="FlowFoundry Logo" className="h-12 w-12" />
          {/*</div>*/}
          <div className="text-xs font-semibold uppercase tracking-wider text-sidebar-foreground/70">
            Data Pipeline Foundry
          </div>
        </SidebarMenuButton>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Control Center</SidebarGroupLabel>
          <SidebarMenu>
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = tab.key === activeTab;

              return (
                <SidebarMenuItem key={tab.key}>
                  <SidebarMenuButton
                    type="button"
                    isActive={isActive}
                    onClick={() => onSelect(tab.key)}
                    className="items-center"
                    tooltip={tab.label}
                  >
                    <Icon className="h-4 w-4 shrink-0" />
                    <div className="flex flex-col text-left group-data-[collapsible=icon]:hidden">
                      <span className="text-sm font-mono">{tab.label}</span>
                      {tab.description ? (
                        <span className="text-xs text-muted-foreground">
                          {tab.description}
                        </span>
                      ) : null}
                    </div>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              );
            })}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="border-t border-sidebar-border px-4 py-3 text-xs text-muted-foreground">
        FlowFoundry
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
