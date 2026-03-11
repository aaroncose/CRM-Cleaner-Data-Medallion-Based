import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { Upload, GitCompare, MessageSquare, BarChart3, Clock, CheckCircle, AlertCircle } from 'lucide-react'

interface RunSummary {
  run_id: string
  file_name: string
  status: string
  created_at: string
  record_count: number
}

export default function Dashboard() {
  const [recentRuns, setRecentRuns] = useState<RunSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchRecentRuns()
  }, [])

  const fetchRecentRuns = async () => {
    try {
      const response = await fetch('/api/runs')
      if (response.ok) {
        const data = await response.json()
        setRecentRuns(data.runs || [])
      }
    } catch (error) {
      console.error('Error fetching runs:', error)
    } finally {
      setLoading(false)
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="w-4 h-4 text-vscode-success" />
      case 'failed':
        return <AlertCircle className="w-4 h-4 text-vscode-error" />
      default:
        return <Clock className="w-4 h-4 text-vscode-warning" />
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-vscode-text">Dashboard</h1>
        <p className="text-vscode-text-muted mt-1">
          Bienvenido al sistema de procesamiento de datos Data Cleaner
        </p>
      </div>

      {/* Quick actions */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Link to="/process" className="card hover:border-vscode-accent transition-colors group">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-vscode-accent/10 rounded-lg group-hover:bg-vscode-accent/20 transition-colors">
              <Upload className="w-5 h-5 text-vscode-accent" />
            </div>
            <div>
              <h3 className="font-medium text-vscode-text">Procesar CSV</h3>
              <p className="text-sm text-vscode-text-muted">Subir y procesar datos</p>
            </div>
          </div>
        </Link>

        <Link to="/compare" className="card hover:border-vscode-accent transition-colors group">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-vscode-info/10 rounded-lg group-hover:bg-vscode-info/20 transition-colors">
              <GitCompare className="w-5 h-5 text-vscode-info" />
            </div>
            <div>
              <h3 className="font-medium text-vscode-text">Comparar Datos</h3>
              <p className="text-sm text-vscode-text-muted">Ver transformaciones</p>
            </div>
          </div>
        </Link>

        <Link to="/chat" className="card hover:border-vscode-accent transition-colors group">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-vscode-success/10 rounded-lg group-hover:bg-vscode-success/20 transition-colors">
              <MessageSquare className="w-5 h-5 text-vscode-success" />
            </div>
            <div>
              <h3 className="font-medium text-vscode-text">Chat RAG</h3>
              <p className="text-sm text-vscode-text-muted">Consultar datos</p>
            </div>
          </div>
        </Link>

        <Link to="/metrics" className="card hover:border-vscode-accent transition-colors group">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-vscode-warning/10 rounded-lg group-hover:bg-vscode-warning/20 transition-colors">
              <BarChart3 className="w-5 h-5 text-vscode-warning" />
            </div>
            <div>
              <h3 className="font-medium text-vscode-text">Métricas</h3>
              <p className="text-sm text-vscode-text-muted">Visualizar estadísticas</p>
            </div>
          </div>
        </Link>
      </div>

      {/* Recent runs */}
      <div className="card">
        <h2 className="text-lg font-semibold text-vscode-text mb-4">Ejecuciones Recientes</h2>
        {loading ? (
          <div className="text-center py-8 text-vscode-text-muted">
            Cargando...
          </div>
        ) : recentRuns.length === 0 ? (
          <div className="text-center py-8">
            <p className="text-vscode-text-muted">No hay ejecuciones recientes</p>
            <Link to="/process" className="btn-primary mt-4 inline-block">
              Procesar primer archivo
            </Link>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="table-header">
                  <th className="text-left px-4 py-2">Estado</th>
                  <th className="text-left px-4 py-2">Archivo</th>
                  <th className="text-left px-4 py-2">ID</th>
                  <th className="text-left px-4 py-2">Registros</th>
                  <th className="text-left px-4 py-2">Fecha</th>
                  <th className="text-left px-4 py-2">Acciones</th>
                </tr>
              </thead>
              <tbody>
                {recentRuns.map((run) => (
                  <tr key={run.run_id} className="table-row">
                    <td className="px-4 py-3">{getStatusIcon(run.status)}</td>
                    <td className="px-4 py-3 text-vscode-text">{run.file_name}</td>
                    <td className="px-4 py-3 font-mono text-sm text-vscode-text-muted">
                      {run.run_id.slice(0, 8)}...
                    </td>
                    <td className="px-4 py-3 text-vscode-text">{run.record_count}</td>
                    <td className="px-4 py-3 text-vscode-text-muted text-sm">
                      {new Date(run.created_at).toLocaleString('es-ES')}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-2">
                        <Link
                          to={`/compare?run_id=${run.run_id}`}
                          className="text-vscode-accent hover:text-vscode-accent-hover text-sm"
                        >
                          Ver
                        </Link>
                        <Link
                          to={`/metrics?run_id=${run.run_id}`}
                          className="text-vscode-accent hover:text-vscode-accent-hover text-sm"
                        >
                          Métricas
                        </Link>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

    </div>
  )
}
