import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { BarChart3, PieChart, TrendingUp, X } from 'lucide-react'
import { useAppStore } from '../store/appStore'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts'

interface ChartData {
  labels: string[]
  values: number[]
}

interface MetricData {
  name: string
  value: string | number
  chart_type?: 'bar' | 'pie' | 'donut' | 'bar_horizontal' | null
  chart_data?: ChartData | Record<string, number>
}

const COLORS = ['#007acc', '#4ec9b0', '#dcdcaa', '#f14c4c', '#569cd6', '#c586c0', '#ce9178', '#6a9955']

export default function Metrics() {
  const [searchParams] = useSearchParams()
  const runId = searchParams.get('run_id')
  const { metricsRunId, setMetricsRunId } = useAppStore()
  const [metrics, setMetrics] = useState<MetricData[]>([])
  const [loading, setLoading] = useState(false)
  const [availableRuns, setAvailableRuns] = useState<{run_id: string, file_name: string}[]>([])
  const selectedRunId = runId || metricsRunId

  useEffect(() => {
    fetchAvailableRuns()
  }, [])

  useEffect(() => {
    if (selectedRunId) {
      fetchMetrics(selectedRunId)
    }
  }, [selectedRunId])

  useEffect(() => {
    if (runId) {
      setMetricsRunId(runId)
    }
  }, [runId, setMetricsRunId])

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

  const fetchMetrics = async (id: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/metrics/${id}`)
      if (response.ok) {
        const data = await response.json()
        setMetrics(data.fixed_metrics || [])
      }
    } catch (error) {
      console.error('Error fetching metrics:', error)
    } finally {
      setLoading(false)
    }
  }

  const transformChartData = (chartData: ChartData | Record<string, number>): { name: string; value: number }[] => {
    if ('labels' in chartData && 'values' in chartData) {
      return chartData.labels.map((label, i) => ({
        name: label,
        value: chartData.values[i],
      }))
    }
    return Object.entries(chartData).map(([name, value]) => ({ name, value }))
  }

  const renderChart = (metric: MetricData) => {
    if (!metric.chart_data || !metric.chart_type) return null

    const data = transformChartData(metric.chart_data)

    switch (metric.chart_type) {
      case 'bar':
        return (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#3c3c3c" />
              <XAxis dataKey="name" tick={{ fill: '#808080', fontSize: 12 }} />
              <YAxis tick={{ fill: '#808080', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#252526', border: '1px solid #3c3c3c' }}
                labelStyle={{ color: '#d4d4d4' }}
              />
              <Bar dataKey="value" fill="#007acc" />
            </BarChart>
          </ResponsiveContainer>
        )

      case 'bar_horizontal':
        return (
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={data} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#3c3c3c" />
              <XAxis type="number" tick={{ fill: '#808080', fontSize: 12 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: '#808080', fontSize: 10 }} width={100} />
              <Tooltip
                contentStyle={{ backgroundColor: '#252526', border: '1px solid #3c3c3c' }}
                labelStyle={{ color: '#d4d4d4' }}
              />
              <Bar dataKey="value" fill="#4ec9b0" />
            </BarChart>
          </ResponsiveContainer>
        )

      case 'pie':
      case 'donut':
        return (
          <ResponsiveContainer width="100%" height={200}>
            <RechartsPieChart>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={metric.chart_type === 'donut' ? 40 : 0}
                outerRadius={70}
                paddingAngle={2}
                dataKey="value"
              >
                {data.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ backgroundColor: '#252526', border: '1px solid #3c3c3c' }}
                labelStyle={{ color: '#d4d4d4' }}
              />
              <Legend
                wrapperStyle={{ fontSize: 12, color: '#808080' }}
                formatter={(value) => <span style={{ color: '#d4d4d4' }}>{value}</span>}
              />
            </RechartsPieChart>
          </ResponsiveContainer>
        )

      default:
        return null
    }
  }

  const renderStatCard = (metric: MetricData) => {
    if (metric.chart_data && typeof metric.chart_data === 'object' && 'sum' in metric.chart_data) {
      const stats = metric.chart_data as Record<string, number>
      return (
        <div className="grid grid-cols-2 gap-4 mt-4">
          <div className="bg-vscode-bg rounded p-3">
            <p className="text-xs text-vscode-text-muted">Suma</p>
            <p className="text-lg font-semibold text-vscode-text">{stats.sum?.toLocaleString('es-ES', { maximumFractionDigits: 2 })}</p>
          </div>
          <div className="bg-vscode-bg rounded p-3">
            <p className="text-xs text-vscode-text-muted">Media</p>
            <p className="text-lg font-semibold text-vscode-text">{stats.mean?.toLocaleString('es-ES', { maximumFractionDigits: 2 })}</p>
          </div>
          <div className="bg-vscode-bg rounded p-3">
            <p className="text-xs text-vscode-text-muted">Mínimo</p>
            <p className="text-lg font-semibold text-vscode-text">{stats.min?.toLocaleString('es-ES', { maximumFractionDigits: 2 })}</p>
          </div>
          <div className="bg-vscode-bg rounded p-3">
            <p className="text-xs text-vscode-text-muted">Máximo</p>
            <p className="text-lg font-semibold text-vscode-text">{stats.max?.toLocaleString('es-ES', { maximumFractionDigits: 2 })}</p>
          </div>
        </div>
      )
    }
    return null
  }

  if (!selectedRunId) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Métricas</h1>
          <p className="text-vscode-text-muted mt-1">
            Visualiza estadísticas y gráficos de tus datos
          </p>
        </div>

        {availableRuns.length > 0 ? (
          <div className="card">
            <h2 className="text-lg font-semibold text-vscode-text mb-4">Selecciona una ejecución</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {availableRuns.map(run => (
                <button
                  key={run.run_id}
                  onClick={() => setMetricsRunId(run.run_id)}
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
            <BarChart3 className="w-12 h-12 text-vscode-text-muted mx-auto mb-4" />
            <p className="text-vscode-text-muted">
              Procesa un archivo CSV para ver métricas
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
          <h1 className="text-2xl font-bold text-vscode-text">Métricas</h1>
          <p className="text-vscode-text-muted mt-1">
            Ejecución: <span className="font-mono">{selectedRunId}</span>
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={selectedRunId || ''}
            onChange={(e) => setMetricsRunId(e.target.value || null)}
            className="select"
          >
            {availableRuns.map(run => (
              <option key={run.run_id} value={run.run_id}>
                {run.file_name}
              </option>
            ))}
          </select>
          <button
            onClick={() => setMetricsRunId(null)}
            className="btn-secondary p-2"
            title="Limpiar selección"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {loading ? (
        <div className="card text-center py-12">
          <p className="text-vscode-text-muted">Cargando métricas...</p>
        </div>
      ) : metrics.length === 0 ? (
        <div className="card text-center py-12">
          <TrendingUp className="w-12 h-12 text-vscode-text-muted mx-auto mb-4" />
          <p className="text-vscode-text-muted">
            No hay métricas disponibles para esta ejecución
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {metrics.map((metric, i) => (
            <div key={i} className="card">
              <div className="flex items-center gap-2 mb-4">
                {metric.chart_type === 'pie' || metric.chart_type === 'donut' ? (
                  <PieChart className="w-5 h-5 text-vscode-accent" />
                ) : (
                  <BarChart3 className="w-5 h-5 text-vscode-accent" />
                )}
                <h3 className="font-semibold text-vscode-text">{metric.name}</h3>
              </div>
              <p className="text-2xl font-bold text-vscode-text mb-4">{metric.value}</p>
              {renderChart(metric)}
              {renderStatCard(metric)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
