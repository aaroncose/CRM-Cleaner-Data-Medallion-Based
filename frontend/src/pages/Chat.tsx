import { useState, useRef, useEffect } from 'react'
import { Send, Bot, User, Loader2, Database, ChevronDown, ChevronUp, Trash2 } from 'lucide-react'
import { useAppStore } from '../store/appStore'

export default function Chat() {
  const {
    chatMessages,
    chatRunId,
    addChatMessage,
    setChatRunId,
    clearChat,
  } = useAppStore()

  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [showSources, setShowSources] = useState<number | null>(null)
  const [availableRuns, setAvailableRuns] = useState<{ run_id: string; file_name: string }[]>([])
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    fetchAvailableRuns()
  }, [])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [chatMessages])

  const fetchAvailableRuns = async () => {
    try {
      const response = await fetch('/api/runs')
      if (response.ok) {
        const data = await response.json()
        const runs = data.runs || []
        setAvailableRuns(runs)
        if (runs.length > 0 && !chatRunId) {
          setChatRunId(runs[0].run_id)
        }
      }
    } catch (error) {
      console.error('Error fetching runs:', error)
    }
  }

  const sendMessage = async () => {
    if (!input.trim() || loading) return

    const userMessage = {
      role: 'user' as const,
      content: input.trim(),
      timestamp: new Date().toISOString(),
    }

    addChatMessage(userMessage)
    setInput('')
    setLoading(true)

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage.content,
          run_id: chatRunId,
        }),
      })

      if (response.ok) {
        const data = await response.json()
        const assistantMessage = {
          role: 'assistant' as const,
          content: data.answer || data.response || 'Sin respuesta',
          sources: data.supporting_data?.map((s: any) => s.content || JSON.stringify(s)),
          timestamp: new Date().toISOString(),
        }
        addChatMessage(assistantMessage)
      } else {
        const errorData = await response.json().catch(() => ({}))
        const assistantMessage = {
          role: 'assistant' as const,
          content: errorData.detail || 'Error al procesar la consulta. Por favor, inténtalo de nuevo.',
          timestamp: new Date().toISOString(),
        }
        addChatMessage(assistantMessage)
      }
    } catch (error) {
      console.error('Error sending message:', error)
      const errorMessage = {
        role: 'assistant' as const,
        content: 'Error de conexión. Por favor, verifica que el servidor esté funcionando.',
        timestamp: new Date().toISOString(),
      }
      addChatMessage(errorMessage)
    } finally {
      setLoading(false)
    }
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const formatTime = (timestamp: string) => {
    return new Date(timestamp).toLocaleTimeString('es-ES', {
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const suggestedQuestions = [
    '¿Cuál es el total de ingresos?',
    '¿Cuántas facturas están pendientes?',
    '¿Quiénes son los principales proveedores?',
    '¿Cuál es el promedio de importe por categoría?',
  ]

  return (
    <div className="h-[calc(100vh-7rem)] flex flex-col">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-vscode-text">Chat RAG</h1>
          <p className="text-vscode-text-muted">Consulta tus datos en lenguaje natural</p>
        </div>
        <div className="flex items-center gap-3">
          {chatMessages.length > 0 && (
            <button
              onClick={clearChat}
              className="btn-secondary flex items-center gap-2 text-sm"
              title="Limpiar chat"
            >
              <Trash2 className="w-4 h-4" />
              Limpiar
            </button>
          )}
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-vscode-text-muted" />
            <select
              value={chatRunId || ''}
              onChange={(e) => setChatRunId(e.target.value || null)}
              className="select text-sm"
            >
              <option value="">Sin contexto</option>
              {availableRuns.map((run) => (
                <option key={run.run_id} value={run.run_id}>
                  {run.file_name} ({run.run_id.slice(0, 8)})
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto bg-vscode-bg-light rounded-lg p-4 mb-4">
        {chatMessages.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center">
            <Bot className="w-16 h-16 text-vscode-text-muted mb-4" />
            <h3 className="text-lg font-medium text-vscode-text mb-2">Comienza una conversación</h3>
            <p className="text-vscode-text-muted text-center mb-6 max-w-md">
              Puedo responder preguntas sobre tus datos procesados usando recuperación aumentada por
              generación (RAG)
            </p>
            <div className="flex flex-wrap gap-2 justify-center max-w-lg">
              {suggestedQuestions.map((q, i) => (
                <button
                  key={i}
                  onClick={() => setInput(q)}
                  className="px-3 py-2 bg-vscode-bg-lighter rounded-lg text-sm text-vscode-text-muted hover:text-vscode-text hover:bg-vscode-border transition-colors"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {chatMessages.map((msg, i) => (
              <div key={i} className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}>
                {msg.role === 'assistant' && (
                  <div className="w-8 h-8 rounded-full bg-vscode-accent/20 flex items-center justify-center flex-shrink-0">
                    <Bot className="w-4 h-4 text-vscode-accent" />
                  </div>
                )}
                <div className={`max-w-[70%] ${msg.role === 'user' ? 'order-first' : ''}`}>
                  <div
                    className={`rounded-lg p-3 ${
                      msg.role === 'user'
                        ? 'bg-vscode-accent text-white'
                        : 'bg-vscode-bg border border-vscode-border'
                    }`}
                  >
                    <p className="whitespace-pre-wrap">{msg.content}</p>
                  </div>
                  {msg.sources && msg.sources.length > 0 && (
                    <div className="mt-2">
                      <button
                        onClick={() => setShowSources(showSources === i ? null : i)}
                        className="flex items-center gap-1 text-xs text-vscode-text-muted hover:text-vscode-text"
                      >
                        {showSources === i ? (
                          <ChevronUp className="w-3 h-3" />
                        ) : (
                          <ChevronDown className="w-3 h-3" />
                        )}
                        {msg.sources.length} fuentes
                      </button>
                      {showSources === i && (
                        <div className="mt-2 p-2 bg-vscode-bg rounded text-xs text-vscode-text-muted max-h-32 overflow-y-auto">
                          {msg.sources.map((s, j) => (
                            <div key={j} className="py-1 border-b border-vscode-border last:border-0">
                              {s}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                  <p className="text-xs text-vscode-text-muted mt-1">{formatTime(msg.timestamp)}</p>
                </div>
                {msg.role === 'user' && (
                  <div className="w-8 h-8 rounded-full bg-vscode-bg-lighter flex items-center justify-center flex-shrink-0">
                    <User className="w-4 h-4 text-vscode-text-muted" />
                  </div>
                )}
              </div>
            ))}
            {loading && (
              <div className="flex gap-3">
                <div className="w-8 h-8 rounded-full bg-vscode-accent/20 flex items-center justify-center">
                  <Bot className="w-4 h-4 text-vscode-accent" />
                </div>
                <div className="bg-vscode-bg border border-vscode-border rounded-lg p-3">
                  <Loader2 className="w-5 h-5 text-vscode-accent animate-spin" />
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>

      {/* Input area */}
      <div className="flex gap-3">
        <div className="flex-1 relative">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Escribe tu pregunta..."
            rows={1}
            className="input w-full resize-none pr-12"
            disabled={loading}
          />
        </div>
        <button
          onClick={sendMessage}
          disabled={!input.trim() || loading}
          className="btn-primary px-4 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Send className="w-5 h-5" />
        </button>
      </div>
    </div>
  )
}
