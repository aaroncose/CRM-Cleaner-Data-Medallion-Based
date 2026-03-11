import { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Upload,
  GitCompare,
  Users,
  MessageSquare,
  BarChart3,
  Settings,
  Database,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'
import { useAppStore } from '../store/appStore'

interface LayoutProps {
  children: ReactNode
}

const navItems = [
  { path: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { path: '/process', icon: Upload, label: 'Procesar CSV' },
  { path: '/compare', icon: GitCompare, label: 'Comparar' },
  { path: '/review', icon: Users, label: 'Revisión' },
  { path: '/chat', icon: MessageSquare, label: 'Chat' },
  { path: '/metrics', icon: BarChart3, label: 'Métricas' },
  { path: '/settings', icon: Settings, label: 'Configuración' },
]

export default function Layout({ children }: LayoutProps) {
  const { sidebarCollapsed, toggleSidebar } = useAppStore()

  return (
    <div className="flex h-screen bg-vscode-bg">
      {/* Single Collapsible Sidebar */}
      <div
        className={`${
          sidebarCollapsed ? 'w-14' : 'w-52'
        } bg-vscode-sidebar border-r border-vscode-border flex flex-col transition-all duration-200`}
      >
        {/* Header */}
        <div className="px-3 py-3 border-b border-vscode-border flex items-center justify-between">
          <div className={`flex items-center gap-2 ${sidebarCollapsed ? 'justify-center w-full' : ''}`}>
            <Database className="w-5 h-5 text-vscode-accent flex-shrink-0" />
            {!sidebarCollapsed && (
              <span className="text-sm font-semibold text-vscode-text truncate">
                Data Cleaner
              </span>
            )}
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 py-2 overflow-y-auto">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              title={sidebarCollapsed ? item.label : undefined}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 mx-1 rounded-md text-sm transition-colors ${
                  isActive
                    ? 'bg-vscode-accent/20 text-vscode-accent'
                    : 'text-vscode-text-muted hover:text-vscode-text hover:bg-vscode-bg-lighter'
                } ${sidebarCollapsed ? 'justify-center' : ''}`
              }
            >
              <item.icon className="w-4 h-4 flex-shrink-0" />
              {!sidebarCollapsed && <span className="truncate">{item.label}</span>}
            </NavLink>
          ))}
        </nav>

        {/* Footer with collapse button */}
        <div className="border-t border-vscode-border p-2">
          <button
            onClick={toggleSidebar}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-md text-vscode-text-muted hover:text-vscode-text hover:bg-vscode-bg-lighter transition-colors"
            title={sidebarCollapsed ? 'Expandir' : 'Colapsar'}
          >
            {sidebarCollapsed ? (
              <ChevronRight className="w-4 h-4" />
            ) : (
              <>
                <ChevronLeft className="w-4 h-4" />
                <span className="text-xs">Colapsar</span>
              </>
            )}
          </button>
          {!sidebarCollapsed && (
            <div className="text-center text-xs text-vscode-text-muted mt-2">v0.2.0</div>
          )}
        </div>
      </div>

      {/* Main content area */}
      <main className="flex-1 overflow-auto">
        <div className="p-6">{children}</div>
      </main>
    </div>
  )
}
