import { useState, useEffect } from 'react'
import { Settings as SettingsIcon, Save, Check, AlertCircle, Eye, EyeOff } from 'lucide-react'

interface Config {
  provider: 'openai' | 'anthropic' | 'google' | 'ollama'
  model_name: string
  confidence_threshold: number
  dedup_auto_threshold: number
  dedup_review_threshold: number
}

export default function Settings() {
  const [config, setConfig] = useState<Config>({
    provider: 'openai',
    model_name: 'gpt-4o-mini',
    confidence_threshold: 0.8,
    dedup_auto_threshold: 0.95,
    dedup_review_threshold: 0.7,
  })
  const [apiKey, setApiKey] = useState('')
  const [showApiKey, setShowApiKey] = useState(false)
  const [loading, setLoading] = useState(false)
  const [saved, setSaved] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchConfig()
  }, [])

  const fetchConfig = async () => {
    try {
      const response = await fetch('/api/config')
      if (response.ok) {
        const data = await response.json()
        setConfig(data)
      }
    } catch (error) {
      console.error('Error fetching config:', error)
    }
  }

  const saveConfig = async () => {
    setLoading(true)
    setError(null)
    setSaved(false)

    try {
      const updateData: Partial<Config> & { api_key?: string } = { ...config }
      if (apiKey) {
        updateData.api_key = apiKey
      }

      const response = await fetch('/api/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updateData),
      })

      if (response.ok) {
        setSaved(true)
        setApiKey('')
        setTimeout(() => setSaved(false), 3000)
      } else {
        const data = await response.json()
        setError(data.detail || 'Error al guardar configuración')
      }
    } catch (error) {
      setError('Error de conexión')
    } finally {
      setLoading(false)
    }
  }

  const modelOptions: Record<string, string[]> = {
    openai: ['gpt-4o-mini', 'gpt-4o', 'gpt-4-turbo', 'gpt-3.5-turbo'],
    anthropic: ['claude-sonnet-4-20250514', 'claude-3-5-sonnet-20241022', 'claude-3-haiku-20240307'],
    google: ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-2.0-flash'],
    ollama: ['llama3.2', 'llama3.1', 'mistral', 'mixtral', 'qwen2.5'],
  }

  return (
    <div className="max-w-2xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-vscode-text">Configuración</h1>
        <p className="text-vscode-text-muted mt-1">
          Configura los parámetros del sistema
        </p>
      </div>

      {error && (
        <div className="bg-vscode-error/10 border border-vscode-error/30 rounded-lg p-4 flex items-center gap-3">
          <AlertCircle className="w-5 h-5 text-vscode-error" />
          <span className="text-vscode-error">{error}</span>
        </div>
      )}

      {saved && (
        <div className="bg-vscode-success/10 border border-vscode-success/30 rounded-lg p-4 flex items-center gap-3">
          <Check className="w-5 h-5 text-vscode-success" />
          <span className="text-vscode-success">Configuración guardada correctamente</span>
        </div>
      )}

      {/* LLM Provider */}
      <div className="card">
        <div className="flex items-center gap-2 mb-4">
          <SettingsIcon className="w-5 h-5 text-vscode-accent" />
          <h2 className="text-lg font-semibold text-vscode-text">Proveedor LLM</h2>
        </div>

        <div className="space-y-4">
          <div>
            <label className="block text-sm text-vscode-text-muted mb-2">Proveedor</label>
            <div className="grid grid-cols-2 gap-3">
              <label className="flex items-center gap-2 cursor-pointer p-2 rounded hover:bg-vscode-bg-lighter">
                <input
                  type="radio"
                  name="provider"
                  value="openai"
                  checked={config.provider === 'openai'}
                  onChange={() => setConfig({ ...config, provider: 'openai', model_name: 'gpt-4o-mini' })}
                  className="accent-vscode-accent"
                />
                <span className="text-vscode-text">OpenAI</span>
              </label>
              <label className="flex items-center gap-2 cursor-pointer p-2 rounded hover:bg-vscode-bg-lighter">
                <input
                  type="radio"
                  name="provider"
                  value="anthropic"
                  checked={config.provider === 'anthropic'}
                  onChange={() => setConfig({ ...config, provider: 'anthropic', model_name: 'claude-sonnet-4-20250514' })}
                  className="accent-vscode-accent"
                />
                <span className="text-vscode-text">Anthropic (Claude)</span>
              </label>
              <label className="flex items-center gap-2 cursor-pointer p-2 rounded hover:bg-vscode-bg-lighter">
                <input
                  type="radio"
                  name="provider"
                  value="google"
                  checked={config.provider === 'google'}
                  onChange={() => setConfig({ ...config, provider: 'google', model_name: 'gemini-1.5-flash' })}
                  className="accent-vscode-accent"
                />
                <span className="text-vscode-text">Google (Gemini)</span>
              </label>
              <label className="flex items-center gap-2 cursor-pointer p-2 rounded hover:bg-vscode-bg-lighter">
                <input
                  type="radio"
                  name="provider"
                  value="ollama"
                  checked={config.provider === 'ollama'}
                  onChange={() => setConfig({ ...config, provider: 'ollama', model_name: 'llama3.2' })}
                  className="accent-vscode-accent"
                />
                <span className="text-vscode-text">Ollama (local)</span>
              </label>
            </div>
          </div>

          <div>
            <label className="block text-sm text-vscode-text-muted mb-2">Modelo</label>
            <select
              value={config.model_name}
              onChange={(e) => setConfig({ ...config, model_name: e.target.value })}
              className="select w-full"
            >
              {modelOptions[config.provider].map(model => (
                <option key={model} value={model}>{model}</option>
              ))}
            </select>
          </div>

          {config.provider !== 'ollama' && (
            <div>
              <label className="block text-sm text-vscode-text-muted mb-2">
                API Key {config.provider === 'openai' && '(OpenAI)'}
                {config.provider === 'anthropic' && '(Anthropic)'}
                {config.provider === 'google' && '(Google AI)'}
              </label>
              <div className="relative">
                <input
                  type={showApiKey ? 'text' : 'password'}
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder={
                    config.provider === 'openai' ? 'sk-...' :
                    config.provider === 'anthropic' ? 'sk-ant-...' :
                    'AIza...'
                  }
                  className="input w-full pr-10"
                />
                <button
                  onClick={() => setShowApiKey(!showApiKey)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-vscode-text-muted hover:text-vscode-text"
                >
                  {showApiKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
              <p className="text-xs text-vscode-text-muted mt-1">
                Deja en blanco para mantener la clave actual
              </p>
            </div>
          )}

          {config.provider === 'ollama' && (
            <div className="bg-vscode-bg rounded p-3 text-sm text-vscode-text-muted">
              <p>Asegúrate de que Ollama esté ejecutándose en <code className="font-mono">http://localhost:11434</code></p>
            </div>
          )}
        </div>
      </div>

      {/* Thresholds */}
      <div className="card">
        <h2 className="text-lg font-semibold text-vscode-text mb-4">Umbrales de confianza</h2>

        <div className="space-y-6">
          <div>
            <div className="flex justify-between mb-2">
              <label className="text-sm text-vscode-text-muted">Umbral de confianza general</label>
              <span className="text-sm text-vscode-text font-mono">{config.confidence_threshold.toFixed(2)}</span>
            </div>
            <input
              type="range"
              min="0.5"
              max="1"
              step="0.05"
              value={config.confidence_threshold}
              onChange={(e) => setConfig({ ...config, confidence_threshold: parseFloat(e.target.value) })}
              className="w-full accent-vscode-accent"
            />
          </div>

          <div>
            <div className="flex justify-between mb-2">
              <label className="text-sm text-vscode-text-muted">Umbral auto-aprobación deduplicación</label>
              <span className="text-sm text-vscode-text font-mono">{config.dedup_auto_threshold.toFixed(2)}</span>
            </div>
            <input
              type="range"
              min="0.7"
              max="1"
              step="0.05"
              value={config.dedup_auto_threshold}
              onChange={(e) => setConfig({ ...config, dedup_auto_threshold: parseFloat(e.target.value) })}
              className="w-full accent-vscode-accent"
            />
            <p className="text-xs text-vscode-text-muted mt-1">
              Entidades con similitud mayor a este umbral se unifican automáticamente
            </p>
          </div>

          <div>
            <div className="flex justify-between mb-2">
              <label className="text-sm text-vscode-text-muted">Umbral revisión manual</label>
              <span className="text-sm text-vscode-text font-mono">{config.dedup_review_threshold.toFixed(2)}</span>
            </div>
            <input
              type="range"
              min="0.5"
              max="0.95"
              step="0.05"
              value={config.dedup_review_threshold}
              onChange={(e) => setConfig({ ...config, dedup_review_threshold: parseFloat(e.target.value) })}
              className="w-full accent-vscode-accent"
            />
            <p className="text-xs text-vscode-text-muted mt-1">
              Entidades entre este umbral y el de auto-aprobación requieren revisión manual
            </p>
          </div>
        </div>
      </div>

      {/* Save button */}
      <div className="flex justify-end">
        <button
          onClick={saveConfig}
          disabled={loading}
          className="btn-primary flex items-center gap-2"
        >
          {loading ? (
            <>Guardando...</>
          ) : (
            <>
              <Save className="w-4 h-4" />
              Guardar configuración
            </>
          )}
        </button>
      </div>
    </div>
  )
}
