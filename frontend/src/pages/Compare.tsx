import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { ArrowRight, ChevronDown, ChevronUp, Search, AlertTriangle, CheckCircle, GitCompare, X } from 'lucide-react'
import { useAppStore } from '../store/appStore'

interface Record {
  [key: string]: unknown
}

interface CompareRow {
  row_number: number
  raw: Record
  clean: Record
  modified: boolean
  modified_fields: string[]
}

interface CompareData {
  run_id: string
  total_rows: number
  modified_rows: number
  rows: CompareRow[]
}

type ViewMode = 'raw' | 'clean' | 'diff'

export default function Compare() {
  const [searchParams] = useSearchParams()
  const runId = searchParams.get('run_id')
  const { compareRunId, setCompareRunId } = useAppStore()
  const [data, setData] = useState<CompareData | null>(null)
  const [loading, setLoading] = useState(false)
  const [selectedRecord, setSelectedRecord] = useState<number | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [viewMode, setViewMode] = useState<ViewMode>('diff')
  const [showOnlyModified, setShowOnlyModified] = useState(false)
  const [availableRuns, setAvailableRuns] = useState<{run_id: string, file_name: string}[]>([])
  const selectedRunId = runId || compareRunId

  useEffect(() => {
    fetchAvailableRuns()
  }, [])

  useEffect(() => {
    if (selectedRunId) {
      fetchCompareData(selectedRunId)
    }
  }, [selectedRunId])

  useEffect(() => {
    if (runId) {
      setCompareRunId(runId)
    }
  }, [runId, setCompareRunId])

  const fetchAvailableRuns = async () => {
    try {
      const response = await fetch('/api/runs')
      if (response.ok) {
        const data = await response.json()
        setAvailableRuns(data.runs || [])
      }
    } catch (error) {
      console.error('Error fetching runs:', error)
    }
  }

  const fetchCompareData = async (id: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/data/compare/${id}`)
      if (response.ok) {
        const result = await response.json()
        setData(result)
      }
    } catch (error) {
      console.error('Error fetching compare data:', error)
    } finally {
      setLoading(false)
    }
  }

  const getFilteredRows = (): CompareRow[] => {
    if (!data) return []
    let rows = data.rows

    if (showOnlyModified) {
      rows = rows.filter(row => row.modified)
    }

    if (searchTerm) {
      rows = rows.filter(row => {
        const rawMatch = Object.values(row.raw).some(
          val => String(val).toLowerCase().includes(searchTerm.toLowerCase())
        )
        const cleanMatch = Object.values(row.clean).some(
          val => String(val).toLowerCase().includes(searchTerm.toLowerCase())
        )
        return rawMatch || cleanMatch
      })
    }

    return rows
  }

  const filteredRows = getFilteredRows()

  const getColumns = (): string[] => {
    if (!data || data.rows.length === 0) return []
    const allKeys = new Set<string>()
    data.rows.forEach(row => {
      Object.keys(row.raw).forEach(key => allKeys.add(key))
      Object.keys(row.clean).forEach(key => allKeys.add(key))
    })
    return Array.from(allKeys)
  }

  const renderValue = (value: unknown): string => {
    if (value === null || value === undefined) return '-'
    if (typeof value === 'object') return JSON.stringify(value)
    return String(value)
  }

  const getDisplayRecord = (row: CompareRow): Record => {
    if (viewMode === 'raw') return row.raw
    if (viewMode === 'clean') return row.clean
    return row.clean // For diff mode, show clean with highlights
  }

  if (!selectedRunId) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Comparar Datos</h1>
          <p className="text-vscode-text-muted mt-1">
            Visualiza las transformaciones entre datos originales y limpiados
          </p>
        </div>

        {availableRuns.length > 0 ? (
          <div className="card">
            <h2 className="text-lg font-semibold text-vscode-text mb-4">Selecciona una ejecución</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {availableRuns.map(run => (
                <button
                  key={run.run_id}
                  onClick={() => setCompareRunId(run.run_id)}
                  className="card hover:border-vscode-accent transition-colors text-left"
                >
                  <p className="font-medium text-vscode-text">{run.file_name}</p>
                  <p className="text-sm text-vscode-text-muted font-mono">{run.run_id.slice(0, 8)}...</p>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="card text-center py-12">
            <GitCompare className="w-12 h-12 text-vscode-text-muted mx-auto mb-4" />
            <p className="text-vscode-text-muted">
              Procesa un archivo CSV para comparar datos
            </p>
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Comparar Datos</h1>
          <p className="text-vscode-text-muted mt-1">
            Ejecución: <span className="font-mono">{selectedRunId}</span>
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={selectedRunId || ''}
            onChange={(e) => setCompareRunId(e.target.value || null)}
            className="select"
          >
            {availableRuns.map(run => (
              <option key={run.run_id} value={run.run_id}>
                {run.file_name}
              </option>
            ))}
          </select>
          <button
            onClick={() => setCompareRunId(null)}
            className="btn-secondary p-2"
            title="Limpiar selección"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Stats */}
      {data && (
        <div className="grid grid-cols-3 gap-4">
          <div className="card">
            <p className="text-sm text-vscode-text-muted">Total Registros</p>
            <p className="text-2xl font-bold text-vscode-text">{data.total_rows}</p>
          </div>
          <div className="card">
            <p className="text-sm text-vscode-text-muted">Modificados</p>
            <p className="text-2xl font-bold text-vscode-warning">{data.modified_rows}</p>
          </div>
          <div className="card">
            <p className="text-sm text-vscode-text-muted">Sin cambios</p>
            <p className="text-2xl font-bold text-vscode-success">{data.total_rows - data.modified_rows}</p>
          </div>
        </div>
      )}

      {/* View mode selector */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 bg-vscode-bg-light rounded-lg p-1 w-fit">
          {(['raw', 'clean', 'diff'] as const).map((mode, i) => (
            <div key={mode} className="flex items-center">
              <button
                onClick={() => setViewMode(mode)}
                className={`px-4 py-2 rounded text-sm font-medium transition-colors ${
                  viewMode === mode
                    ? 'bg-vscode-accent text-white'
                    : 'text-vscode-text-muted hover:text-vscode-text'
                }`}
              >
                {mode === 'raw' ? 'Original' : mode === 'clean' ? 'Limpio' : 'Diferencias'}
              </button>
              {i < 2 && (
                <ArrowRight className="w-4 h-4 text-vscode-text-muted mx-1" />
              )}
            </div>
          ))}
        </div>

        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={showOnlyModified}
            onChange={(e) => setShowOnlyModified(e.target.checked)}
            className="w-4 h-4 accent-vscode-accent"
          />
          <span className="text-sm text-vscode-text-muted">Solo modificados</span>
        </label>
      </div>

      {/* Search */}
      <div className="relative w-64">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-vscode-text-muted" />
        <input
          type="text"
          placeholder="Buscar en registros..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="input pl-10 w-full"
        />
      </div>

      {loading ? (
        <div className="card text-center py-12">
          <p className="text-vscode-text-muted">Cargando datos...</p>
        </div>
      ) : (
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="table-header">
                  <th className="text-left px-4 py-2 w-12">#</th>
                  <th className="text-left px-4 py-2 w-16">Estado</th>
                  {getColumns().slice(0, 7).map(col => (
                    <th key={col} className="text-left px-4 py-2 font-mono text-xs">
                      {col}
                    </th>
                  ))}
                  <th className="text-left px-4 py-2 w-12"></th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.slice(0, 100).map((row, i) => {
                  const record = getDisplayRecord(row)
                  return (
                    <>
                      <tr
                        key={row.row_number}
                        className={`table-row cursor-pointer ${selectedRecord === i ? 'bg-vscode-bg-lighter' : ''}`}
                        onClick={() => setSelectedRecord(selectedRecord === i ? null : i)}
                      >
                        <td className="px-4 py-3 text-vscode-text-muted text-sm">{row.row_number}</td>
                        <td className="px-4 py-3">
                          {row.modified ? (
                            <AlertTriangle className="w-4 h-4 text-vscode-warning" title="Modificado" />
                          ) : (
                            <CheckCircle className="w-4 h-4 text-vscode-success" title="Sin cambios" />
                          )}
                        </td>
                        {getColumns().slice(0, 7).map(col => {
                          const isModified = row.modified_fields.includes(col)
                          return (
                            <td
                              key={col}
                              className={`px-4 py-3 text-sm truncate max-w-[200px] ${
                                isModified && viewMode === 'diff'
                                  ? 'bg-vscode-warning/10 text-vscode-warning'
                                  : 'text-vscode-text'
                              }`}
                            >
                              {renderValue(record[col])}
                            </td>
                          )
                        })}
                        <td className="px-4 py-3">
                          {selectedRecord === i ? (
                            <ChevronUp className="w-4 h-4 text-vscode-text-muted" />
                          ) : (
                            <ChevronDown className="w-4 h-4 text-vscode-text-muted" />
                          )}
                        </td>
                      </tr>
                      {selectedRecord === i && (
                        <tr key={`${row.row_number}-detail`}>
                          <td colSpan={10} className="bg-vscode-bg p-4">
                            <div className="grid grid-cols-2 gap-4">
                              <div>
                                <h4 className="text-sm font-medium text-vscode-text-muted mb-2">Original (Raw)</h4>
                                <pre className="text-sm text-vscode-text font-mono overflow-x-auto p-3 bg-vscode-bg-lighter rounded">
                                  {JSON.stringify(row.raw, null, 2)}
                                </pre>
                              </div>
                              <div>
                                <h4 className="text-sm font-medium text-vscode-text-muted mb-2">Limpio (Clean)</h4>
                                <pre className="text-sm text-vscode-text font-mono overflow-x-auto p-3 bg-vscode-bg-lighter rounded">
                                  {JSON.stringify(row.clean, null, 2)}
                                </pre>
                              </div>
                            </div>
                            {row.modified_fields.length > 0 && (
                              <div className="mt-3">
                                <span className="text-sm text-vscode-text-muted">Campos modificados: </span>
                                {row.modified_fields.map(field => (
                                  <span key={field} className="badge badge-warning ml-1">{field}</span>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </>
                  )
                })}
              </tbody>
            </table>
          </div>
          {filteredRows.length > 100 && (
            <div className="px-4 py-3 bg-vscode-bg-lighter text-center text-sm text-vscode-text-muted">
              Mostrando 100 de {filteredRows.length} registros
            </div>
          )}
        </div>
      )}
    </div>
  )
}
