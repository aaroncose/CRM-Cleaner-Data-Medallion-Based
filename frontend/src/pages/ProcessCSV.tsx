import { useState, useCallback, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Upload, FileSpreadsheet, Play, CheckCircle, AlertCircle, Loader2, RotateCcw } from 'lucide-react'
import { useAppStore } from '../store/appStore'

interface SchemaTemplate {
  name: string
  description: string
  columns: { name: string; type: string; required: boolean; allowed_values?: string[] }[]
}

export default function ProcessCSV() {
  const navigate = useNavigate()
  const {
    processing,
    setProcessingStep,
    setProcessingFile,
    setProcessingRunId,
    setProcessingSchema,
    setProcessingLlmEnabled,
    addProcessingProgress,
    resetProcessing,
  } = useAppStore()

  const [file, setFile] = useState<File | null>(null)
  const [templates, setTemplates] = useState<SchemaTemplate[]>([])
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Restore file name display
  const displayFileName = processing.fileName || file?.name

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const droppedFile = e.dataTransfer.files[0]
    if (droppedFile && droppedFile.name.endsWith('.csv')) {
      setFile(droppedFile)
      setError(null)
    } else {
      setError('Solo se permiten archivos CSV')
    }
  }, [])

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      setFile(selectedFile)
      setError(null)
    }
  }

  const uploadFile = async () => {
    if (!file) return

    setError(null)
    setUploading(true)
    const formData = new FormData()
    formData.append('file', file)

    try {
      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      })

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}))
        throw new Error(errData.detail || 'Error al subir archivo')
      }

      const data = await response.json()
      setProcessingFile(data.file_id, file.name)

      // Detect schema from uploaded file
      const schemaResponse = await fetch('/api/schema/detect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_id: data.file_id, sample_rows: 100 }),
      })

      if (schemaResponse.ok) {
        const schemaData = await schemaResponse.json()
        setProcessingSchema(schemaData.columns || [])
      } else {
        // If schema detection fails, use columns from upload response
        const cols = data.columns?.map((name: string) => ({
          name,
          type: data.detected_types?.[name] || 'string',
          required: true,
        })) || []
        setProcessingSchema(cols)
      }

      // Get templates (optional)
      try {
        const templatesResponse = await fetch('/api/schema/templates')
        if (templatesResponse.ok) {
          const templatesData = await templatesResponse.json()
          setTemplates(templatesData.templates || [])
        }
      } catch {
        // Templates are optional
      }

      setProcessingStep('schema')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error desconocido')
    } finally {
      setUploading(false)
    }
  }

  const applyTemplate = (template: SchemaTemplate) => {
    setProcessingSchema(template.columns)
  }

  const startProcessing = async () => {
    if (!processing.fileId) return

    setProcessingStep('processing')
    setError(null)

    try {
      const response = await fetch('/api/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          file_id: processing.fileId,
          llm_enabled: processing.llmEnabled,
        }),
      })

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}))
        throw new Error(errData.detail || 'Error al iniciar procesamiento')
      }

      const data = await response.json()
      const newRunId = data.run_id
      setProcessingRunId(newRunId)

      // Stream status updates
      const eventSource = new EventSource(`/api/process/status/${newRunId}/stream`)

      eventSource.onmessage = (event) => {
        try {
          const status = JSON.parse(event.data.replace(/'/g, '"'))
          if (status.message) {
            addProcessingProgress(status.message)
          }

          if (status.status === 'completed') {
            eventSource.close()
            setProcessingStep('complete')
          } else if (status.status === 'error') {
            eventSource.close()
            setError(status.message || 'Error en el procesamiento')
            setProcessingStep('schema')
          }
        } catch (e) {
          console.log('SSE parse error:', e)
        }
      }

      eventSource.onerror = () => {
        eventSource.close()
        // Check final status
        setTimeout(async () => {
          try {
            const statusRes = await fetch(`/api/process/status/${newRunId}`)
            const statusData = await statusRes.json()
            if (statusData.status === 'completed') {
              setProcessingStep('complete')
            } else if (statusData.status === 'error') {
              setError(statusData.message || 'Error en el procesamiento')
              setProcessingStep('schema')
            }
          } catch {
            setProcessingStep('complete')
          }
        }, 1000)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error desconocido')
      setProcessingStep('schema')
    }
  }

  const handleReset = () => {
    resetProcessing()
    setFile(null)
    setError(null)
    setTemplates([])
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Procesar CSV</h1>
          <p className="text-vscode-text-muted mt-1">
            Sube un archivo CSV para procesarlo a través de la arquitectura Medallion
          </p>
        </div>
        {processing.step !== 'upload' && (
          <button onClick={handleReset} className="btn-secondary flex items-center gap-2">
            <RotateCcw className="w-4 h-4" />
            Reiniciar
          </button>
        )}
      </div>

      {/* Progress steps */}
      <div className="flex items-center gap-4">
        {(['upload', 'schema', 'processing', 'complete'] as const).map((s, i) => (
          <div key={s} className="flex items-center gap-2">
            <div
              className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium ${
                processing.step === s
                  ? 'bg-vscode-accent text-white'
                  : ['upload', 'schema', 'processing', 'complete'].indexOf(processing.step) > i
                  ? 'bg-vscode-success text-white'
                  : 'bg-vscode-bg-lighter text-vscode-text-muted'
              }`}
            >
              {['upload', 'schema', 'processing', 'complete'].indexOf(processing.step) > i ? (
                <CheckCircle className="w-4 h-4" />
              ) : (
                i + 1
              )}
            </div>
            <span
              className={`text-sm ${
                processing.step === s ? 'text-vscode-text' : 'text-vscode-text-muted'
              }`}
            >
              {s === 'upload' ? 'Subir' : s === 'schema' ? 'Esquema' : s === 'processing' ? 'Procesando' : 'Completo'}
            </span>
            {i < 3 && <div className="w-12 h-px bg-vscode-border" />}
          </div>
        ))}
      </div>

      {error && (
        <div className="bg-vscode-error/10 border border-vscode-error/30 rounded-lg p-4 flex items-center gap-3">
          <AlertCircle className="w-5 h-5 text-vscode-error flex-shrink-0" />
          <span className="text-vscode-error">{error}</span>
        </div>
      )}

      {/* Step: Upload */}
      {processing.step === 'upload' && (
        <div className="card">
          <div
            onDrop={handleDrop}
            onDragOver={(e) => e.preventDefault()}
            className="border-2 border-dashed border-vscode-border rounded-lg p-12 text-center hover:border-vscode-accent transition-colors"
          >
            {file ? (
              <div className="flex flex-col items-center gap-4">
                <FileSpreadsheet className="w-12 h-12 text-vscode-accent" />
                <div>
                  <p className="text-vscode-text font-medium">{file.name}</p>
                  <p className="text-vscode-text-muted text-sm">{(file.size / 1024).toFixed(1)} KB</p>
                </div>
                <button onClick={uploadFile} className="btn-primary" disabled={uploading}>
                  {uploading ? (
                    <>
                      <Loader2 className="w-4 h-4 animate-spin mr-2" />
                      Subiendo...
                    </>
                  ) : (
                    'Continuar'
                  )}
                </button>
              </div>
            ) : (
              <div className="flex flex-col items-center gap-4">
                <Upload className="w-12 h-12 text-vscode-text-muted" />
                <div>
                  <p className="text-vscode-text">Arrastra un archivo CSV aquí</p>
                  <p className="text-vscode-text-muted text-sm">o haz clic para seleccionar</p>
                </div>
                <label className="btn-secondary cursor-pointer">
                  Seleccionar archivo
                  <input type="file" accept=".csv" onChange={handleFileSelect} className="hidden" />
                </label>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Step: Schema */}
      {processing.step === 'schema' && (
        <div className="space-y-4">
          <div className="card">
            <h2 className="text-lg font-semibold text-vscode-text mb-4">
              Esquema Detectado
              {displayFileName && (
                <span className="text-sm font-normal text-vscode-text-muted ml-2">
                  ({displayFileName})
                </span>
              )}
            </h2>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="table-header">
                    <th className="text-left px-4 py-2">Columna</th>
                    <th className="text-left px-4 py-2">Tipo</th>
                    <th className="text-left px-4 py-2">Requerido</th>
                    <th className="text-left px-4 py-2">Valores permitidos</th>
                  </tr>
                </thead>
                <tbody>
                  {processing.schema.map((col, i) => (
                    <tr key={i} className="table-row">
                      <td className="px-4 py-3 font-mono text-sm text-vscode-text">{col.name}</td>
                      <td className="px-4 py-3">
                        <span className="badge badge-info">{col.type}</span>
                      </td>
                      <td className="px-4 py-3">
                        {col.required ? (
                          <span className="badge badge-success">Sí</span>
                        ) : (
                          <span className="badge text-vscode-text-muted">No</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-vscode-text-muted text-sm">
                        {col.allowed_values?.join(', ') || '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {templates.length > 0 && (
            <div className="card">
              <h3 className="text-md font-semibold text-vscode-text mb-3">Plantillas disponibles</h3>
              <div className="flex gap-3">
                {templates.map((t) => (
                  <button key={t.name} onClick={() => applyTemplate(t)} className="btn-secondary text-sm">
                    {t.name}
                  </button>
                ))}
              </div>
            </div>
          )}

          <div className="card">
            <h3 className="text-md font-semibold text-vscode-text mb-3">Opciones de procesamiento</h3>
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={processing.llmEnabled}
                onChange={(e) => setProcessingLlmEnabled(e.target.checked)}
                className="w-4 h-4 accent-vscode-accent"
              />
              <span className="text-vscode-text">Habilitar análisis con LLM (deduplicación inteligente)</span>
            </label>
          </div>

          <div className="flex gap-3">
            <button onClick={() => setProcessingStep('upload')} className="btn-secondary">
              Atrás
            </button>
            <button onClick={startProcessing} className="btn-primary flex items-center gap-2">
              <Play className="w-4 h-4" />
              Iniciar procesamiento
            </button>
          </div>
        </div>
      )}

      {/* Step: Processing */}
      {processing.step === 'processing' && (
        <div className="card">
          <div className="flex items-center gap-3 mb-6">
            <Loader2 className="w-6 h-6 text-vscode-accent animate-spin" />
            <h2 className="text-lg font-semibold text-vscode-text">Procesando...</h2>
          </div>
          <div className="bg-vscode-bg rounded-lg p-4 font-mono text-sm max-h-96 overflow-y-auto">
            {processing.progress.map((msg, i) => (
              <div key={i} className="text-vscode-text-muted py-1">
                <span className="text-vscode-success">→</span> {msg}
              </div>
            ))}
            <div className="text-vscode-accent py-1 animate-pulse">▌</div>
          </div>
        </div>
      )}

      {/* Step: Complete */}
      {processing.step === 'complete' && processing.runId && (
        <div className="card text-center py-8">
          <CheckCircle className="w-16 h-16 text-vscode-success mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-vscode-text mb-2">Procesamiento completado</h2>
          <p className="text-vscode-text-muted mb-6">
            ID de ejecución: <span className="font-mono">{processing.runId}</span>
          </p>
          <div className="flex justify-center gap-3">
            <button
              onClick={() => navigate(`/compare?run_id=${processing.runId}`)}
              className="btn-primary"
            >
              Ver resultados
            </button>
            <button
              onClick={() => navigate(`/metrics?run_id=${processing.runId}`)}
              className="btn-secondary"
            >
              Ver métricas
            </button>
            <button onClick={handleReset} className="btn-secondary">
              Procesar otro archivo
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
