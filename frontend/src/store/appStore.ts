import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface Message {
  role: 'user' | 'assistant'
  content: string
  sources?: string[]
  timestamp: string
}

interface ProcessingState {
  step: 'upload' | 'schema' | 'processing' | 'complete'
  fileId: string | null
  fileName: string | null
  runId: string | null
  progress: string[]
  schema: ColumnConfig[]
  llmEnabled: boolean
}

interface ColumnConfig {
  name: string
  type: string
  required: boolean
  allowed_values?: string[]
}

interface AppState {
  // Sidebar
  sidebarCollapsed: boolean
  toggleSidebar: () => void

  // Chat
  chatMessages: Message[]
  chatRunId: string | null
  addChatMessage: (message: Message) => void
  setChatRunId: (runId: string | null) => void
  clearChat: () => void

  // Processing
  processing: ProcessingState
  setProcessingStep: (step: ProcessingState['step']) => void
  setProcessingFile: (fileId: string, fileName: string) => void
  setProcessingRunId: (runId: string) => void
  setProcessingSchema: (schema: ColumnConfig[]) => void
  setProcessingLlmEnabled: (enabled: boolean) => void
  addProcessingProgress: (msg: string) => void
  resetProcessing: () => void

  // Metrics
  metricsRunId: string | null
  setMetricsRunId: (runId: string | null) => void

  // Compare
  compareRunId: string | null
  setCompareRunId: (runId: string | null) => void

  // Review
  reviewRunId: string | null
  setReviewRunId: (runId: string | null) => void
}

const initialProcessingState: ProcessingState = {
  step: 'upload',
  fileId: null,
  fileName: null,
  runId: null,
  progress: [],
  schema: [],
  llmEnabled: true,
}

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      // Sidebar
      sidebarCollapsed: false,
      toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),

      // Chat
      chatMessages: [],
      chatRunId: null,
      addChatMessage: (message) =>
        set((state) => ({ chatMessages: [...state.chatMessages, message] })),
      setChatRunId: (runId) => set({ chatRunId: runId }),
      clearChat: () => set({ chatMessages: [] }),

      // Processing
      processing: initialProcessingState,
      setProcessingStep: (step) =>
        set((state) => ({ processing: { ...state.processing, step } })),
      setProcessingFile: (fileId, fileName) =>
        set((state) => ({ processing: { ...state.processing, fileId, fileName } })),
      setProcessingRunId: (runId) =>
        set((state) => ({ processing: { ...state.processing, runId } })),
      setProcessingSchema: (schema) =>
        set((state) => ({ processing: { ...state.processing, schema } })),
      setProcessingLlmEnabled: (enabled) =>
        set((state) => ({ processing: { ...state.processing, llmEnabled: enabled } })),
      addProcessingProgress: (msg) =>
        set((state) => ({
          processing: { ...state.processing, progress: [...state.processing.progress, msg] },
        })),
      resetProcessing: () => set({ processing: initialProcessingState }),

      // Metrics
      metricsRunId: null,
      setMetricsRunId: (runId) => set({ metricsRunId: runId }),

      // Compare
      compareRunId: null,
      setCompareRunId: (runId) => set({ compareRunId: runId }),

      // Review
      reviewRunId: null,
      setReviewRunId: (runId) => set({ reviewRunId: runId }),
    }),
    {
      name: 'data-cleaner-storage',
      partialize: (state) => ({
        sidebarCollapsed: state.sidebarCollapsed,
        chatMessages: state.chatMessages,
        chatRunId: state.chatRunId,
        processing: state.processing,
        metricsRunId: state.metricsRunId,
        compareRunId: state.compareRunId,
        reviewRunId: state.reviewRunId,
      }),
    }
  )
)
