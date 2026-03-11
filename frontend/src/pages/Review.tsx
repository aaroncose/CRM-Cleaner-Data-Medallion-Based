import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Users, Check, X, CheckCheck, AlertCircle } from 'lucide-react'
import { useAppStore } from '../store/appStore'

interface EntityGroup {
  group_id: string
  canonical: string
  variants: string[]
  similarity: number
  status: 'pending' | 'approved' | 'rejected'
}

export default function Review() {
  const [searchParams] = useSearchParams()
  const runId = searchParams.get('run_id')
  const { reviewRunId, setReviewRunId } = useAppStore()
  const [groups, setGroups] = useState<EntityGroup[]>([])
  const [loading, setLoading] = useState(false)
  const [selectedGroups, setSelectedGroups] = useState<Set<string>>(new Set())
  const [filter, setFilter] = useState<'all' | 'pending' | 'approved' | 'rejected'>('all')
  const [availableRuns, setAvailableRuns] = useState<{run_id: string, file_name: string}[]>([])
  const selectedRunId = runId || reviewRunId

  useEffect(() => {
    fetchAvailableRuns()
  }, [])

  useEffect(() => {
    if (selectedRunId) {
      fetchReviewGroups(selectedRunId)
    }
  }, [selectedRunId])

  useEffect(() => {
    if (runId) {
      setReviewRunId(runId)
    }
  }, [runId, setReviewRunId])

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

  const fetchReviewGroups = async (id: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/review/${id}`)
      if (response.ok) {
        const data = await response.json()
        setGroups(data.groups || [])
      }
    } catch (error) {
      console.error('Error fetching review groups:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleApprove = async (groupIds: string[]) => {
    if (!selectedRunId) return
    try {
      await fetch(`/api/review/${selectedRunId}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ group_ids: groupIds }),
      })
      setGroups(groups.map(g =>
        groupIds.includes(g.group_id) ? { ...g, status: 'approved' } : g
      ))
      setSelectedGroups(new Set())
    } catch (error) {
      console.error('Error approving groups:', error)
    }
  }

  const handleReject = async (groupIds: string[]) => {
    if (!selectedRunId) return
    try {
      await fetch(`/api/review/${selectedRunId}/reject`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ group_ids: groupIds }),
      })
      setGroups(groups.map(g =>
        groupIds.includes(g.group_id) ? { ...g, status: 'rejected' } : g
      ))
      setSelectedGroups(new Set())
    } catch (error) {
      console.error('Error rejecting groups:', error)
    }
  }

  const handleApproveAll = async () => {
    if (!selectedRunId) return
    try {
      const response = await fetch(`/api/review/${selectedRunId}/approve-all?threshold=0.9`, {
        method: 'POST',
      })
      if (response.ok) {
        fetchReviewGroups(selectedRunId)
      }
    } catch (error) {
      console.error('Error approving all:', error)
    }
  }

  const toggleGroup = (groupId: string) => {
    const newSelected = new Set(selectedGroups)
    if (newSelected.has(groupId)) {
      newSelected.delete(groupId)
    } else {
      newSelected.add(groupId)
    }
    setSelectedGroups(newSelected)
  }

  const filteredGroups = groups.filter(g => filter === 'all' || g.status === filter)
  const pendingCount = groups.filter(g => g.status === 'pending').length

  if (!selectedRunId) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Revisión de Entidades</h1>
          <p className="text-vscode-text-muted mt-1">
            Revisa y aprueba las unificaciones de entidades duplicadas
          </p>
        </div>

        {availableRuns.length > 0 ? (
          <div className="card">
            <h2 className="text-lg font-semibold text-vscode-text mb-4">Selecciona una ejecución</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {availableRuns.map(run => (
                <button
                  key={run.run_id}
                  onClick={() => setReviewRunId(run.run_id)}
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
            <Users className="w-12 h-12 text-vscode-text-muted mx-auto mb-4" />
            <p className="text-vscode-text-muted">
              Procesa un archivo CSV para revisar entidades
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
          <h1 className="text-2xl font-bold text-vscode-text">Revisión de Entidades</h1>
          <p className="text-vscode-text-muted mt-1">
            Ejecución: <span className="font-mono">{selectedRunId}</span>
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={selectedRunId || ''}
            onChange={(e) => setReviewRunId(e.target.value || null)}
            className="select"
          >
            {availableRuns.map(run => (
              <option key={run.run_id} value={run.run_id}>
                {run.file_name}
              </option>
            ))}
          </select>
          <button
            onClick={() => setReviewRunId(null)}
            className="btn-secondary p-2"
            title="Limpiar selección"
          >
            <X className="w-4 h-4" />
          </button>
          {pendingCount > 0 && (
            <button onClick={handleApproveAll} className="btn-primary flex items-center gap-2">
              <CheckCheck className="w-4 h-4" />
              Aprobar todos ({'>'}90% similitud)
            </button>
          )}
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="card">
          <p className="text-sm text-vscode-text-muted">Total grupos</p>
          <p className="text-2xl font-bold text-vscode-text">{groups.length}</p>
        </div>
        <div className="card">
          <p className="text-sm text-vscode-text-muted">Pendientes</p>
          <p className="text-2xl font-bold text-vscode-warning">{pendingCount}</p>
        </div>
        <div className="card">
          <p className="text-sm text-vscode-text-muted">Aprobados</p>
          <p className="text-2xl font-bold text-vscode-success">
            {groups.filter(g => g.status === 'approved').length}
          </p>
        </div>
        <div className="card">
          <p className="text-sm text-vscode-text-muted">Rechazados</p>
          <p className="text-2xl font-bold text-vscode-error">
            {groups.filter(g => g.status === 'rejected').length}
          </p>
        </div>
      </div>

      {/* Filter and bulk actions */}
      <div className="flex items-center justify-between">
        <div className="flex gap-2">
          {(['all', 'pending', 'approved', 'rejected'] as const).map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1.5 rounded text-sm ${
                filter === f
                  ? 'bg-vscode-accent text-white'
                  : 'bg-vscode-bg-lighter text-vscode-text-muted hover:text-vscode-text'
              }`}
            >
              {f === 'all' ? 'Todos' : f === 'pending' ? 'Pendientes' : f === 'approved' ? 'Aprobados' : 'Rechazados'}
            </button>
          ))}
        </div>
        {selectedGroups.size > 0 && (
          <div className="flex gap-2">
            <button
              onClick={() => handleApprove(Array.from(selectedGroups))}
              className="btn-primary flex items-center gap-2 text-sm"
            >
              <Check className="w-4 h-4" />
              Aprobar ({selectedGroups.size})
            </button>
            <button
              onClick={() => handleReject(Array.from(selectedGroups))}
              className="btn-secondary flex items-center gap-2 text-sm text-vscode-error"
            >
              <X className="w-4 h-4" />
              Rechazar ({selectedGroups.size})
            </button>
          </div>
        )}
      </div>

      {/* Groups list */}
      {loading ? (
        <div className="card text-center py-12">
          <p className="text-vscode-text-muted">Cargando grupos...</p>
        </div>
      ) : filteredGroups.length === 0 ? (
        <div className="card text-center py-12">
          <AlertCircle className="w-12 h-12 text-vscode-text-muted mx-auto mb-4" />
          <p className="text-vscode-text-muted">
            {filter === 'all' ? 'No hay grupos de entidades para revisar' : `No hay grupos ${filter}`}
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {filteredGroups.map(group => (
            <div
              key={group.group_id}
              className={`card flex items-center gap-4 ${
                group.status === 'approved' ? 'border-vscode-success/30' :
                group.status === 'rejected' ? 'border-vscode-error/30' : ''
              }`}
            >
              {group.status === 'pending' && (
                <input
                  type="checkbox"
                  checked={selectedGroups.has(group.group_id)}
                  onChange={() => toggleGroup(group.group_id)}
                  className="w-4 h-4 accent-vscode-accent"
                />
              )}
              <div className="flex-1">
                <div className="flex items-center gap-3 mb-2">
                  <span className="font-medium text-vscode-text">{group.canonical}</span>
                  <span className={`badge ${
                    group.similarity >= 0.9 ? 'badge-success' :
                    group.similarity >= 0.7 ? 'badge-warning' : 'badge-error'
                  }`}>
                    {(group.similarity * 100).toFixed(0)}% similitud
                  </span>
                  <span className={`badge ${
                    group.status === 'approved' ? 'badge-success' :
                    group.status === 'rejected' ? 'badge-error' : 'badge-warning'
                  }`}>
                    {group.status === 'pending' ? 'Pendiente' :
                     group.status === 'approved' ? 'Aprobado' : 'Rechazado'}
                  </span>
                </div>
                <div className="flex flex-wrap gap-2">
                  {group.variants.map((v, i) => (
                    <span key={i} className="px-2 py-1 bg-vscode-bg rounded text-sm text-vscode-text-muted">
                      {v}
                    </span>
                  ))}
                </div>
              </div>
              {group.status === 'pending' && (
                <div className="flex gap-2">
                  <button
                    onClick={() => handleApprove([group.group_id])}
                    className="p-2 rounded bg-vscode-success/10 text-vscode-success hover:bg-vscode-success/20"
                    title="Aprobar"
                  >
                    <Check className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => handleReject([group.group_id])}
                    className="p-2 rounded bg-vscode-error/10 text-vscode-error hover:bg-vscode-error/20"
                    title="Rechazar"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
