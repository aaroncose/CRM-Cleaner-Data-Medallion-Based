import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Database, Layers, Sparkles, ArrowRight } from 'lucide-react'

export default function Intro() {
  const navigate = useNavigate()
  const [loaded, setLoaded] = useState(false)
  const [showContent, setShowContent] = useState(false)

  useEffect(() => {
    // Trigger animations
    setTimeout(() => setLoaded(true), 100)
    setTimeout(() => setShowContent(true), 600)
  }, [])

  const handleEnter = () => {
    navigate('/dashboard')
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#0d1117] via-[#161b22] to-[#0d1117] flex flex-col items-center justify-center relative overflow-hidden">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-vscode-accent/5 rounded-full blur-3xl animate-pulse" />
        <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-purple-500/5 rounded-full blur-3xl animate-pulse delay-1000" />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-teal-500/5 rounded-full blur-3xl animate-pulse delay-500" />
      </div>

      {/* Grid pattern overlay */}
      <div
        className="absolute inset-0 opacity-[0.02]"
        style={{
          backgroundImage: `linear-gradient(rgba(255,255,255,0.1) 1px, transparent 1px),
                           linear-gradient(90deg, rgba(255,255,255,0.1) 1px, transparent 1px)`,
          backgroundSize: '50px 50px'
        }}
      />

      {/* Main content */}
      <div className="relative z-10 text-center px-6">
        {/* Logo */}
        <div
          className={`mb-8 transition-all duration-1000 ease-out ${
            loaded ? 'opacity-100 scale-100' : 'opacity-0 scale-50'
          }`}
        >
          <div className="relative inline-flex items-center justify-center">
            {/* Outer ring */}
            <div className="absolute w-32 h-32 rounded-full border border-vscode-accent/20 animate-[spin_20s_linear_infinite]" />
            <div className="absolute w-40 h-40 rounded-full border border-purple-500/10 animate-[spin_30s_linear_infinite_reverse]" />

            {/* Logo container */}
            <div className="w-24 h-24 rounded-2xl bg-gradient-to-br from-vscode-accent to-purple-600 flex items-center justify-center shadow-2xl shadow-vscode-accent/20">
              <Database className="w-12 h-12 text-white" />
            </div>

            {/* Floating icons */}
            <div className="absolute -top-2 -right-2 w-8 h-8 rounded-lg bg-teal-500/20 flex items-center justify-center animate-bounce">
              <Layers className="w-4 h-4 text-teal-400" />
            </div>
            <div className="absolute -bottom-2 -left-2 w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center animate-bounce delay-300">
              <Sparkles className="w-4 h-4 text-purple-400" />
            </div>
          </div>
        </div>

        {/* Title */}
        <h1
          className={`text-5xl md:text-6xl font-bold mb-4 transition-all duration-1000 delay-200 ${
            loaded ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'
          }`}
        >
          <span className="bg-gradient-to-r from-white via-gray-200 to-gray-400 bg-clip-text text-transparent">
            Data Cleaner
          </span>
          <span className="bg-gradient-to-r from-vscode-accent to-purple-500 bg-clip-text text-transparent ml-3">
            Medallion-Based
          </span>
        </h1>

        {/* Subtitle */}
        <p
          className={`text-lg md:text-xl text-gray-400 mb-4 transition-all duration-1000 delay-300 ${
            loaded ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'
          }`}
        >
          Arquitectura de datos inteligente
        </p>

        {/* Description */}
        <p
          className={`text-sm text-gray-500 max-w-md mx-auto mb-10 transition-all duration-1000 delay-400 ${
            showContent ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'
          }`}
        >
          Limpieza, validación y análisis de datos con IA
          usando la arquitectura Bronze → Silver → Gold
        </p>

        {/* Features */}
        <div
          className={`flex flex-wrap justify-center gap-4 mb-12 transition-all duration-1000 delay-500 ${
            showContent ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'
          }`}
        >
          {[
            { label: 'Bronze', desc: 'Ingesta' },
            { label: 'Silver', desc: 'Limpieza' },
            { label: 'Gold', desc: 'Analytics' },
          ].map((item, i) => (
            <div
              key={item.label}
              className="flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10"
            >
              <div className={`w-2 h-2 rounded-full ${
                i === 0 ? 'bg-amber-500' : i === 1 ? 'bg-gray-400' : 'bg-yellow-400'
              }`} />
              <span className="text-sm text-gray-300">{item.label}</span>
              <span className="text-xs text-gray-500">• {item.desc}</span>
            </div>
          ))}
        </div>

        {/* Enter button */}
        <button
          onClick={handleEnter}
          className={`group relative inline-flex items-center gap-3 px-8 py-4 rounded-xl
            bg-gradient-to-r from-vscode-accent to-purple-600
            text-white font-medium text-lg
            shadow-xl shadow-vscode-accent/25
            hover:shadow-2xl hover:shadow-vscode-accent/40
            hover:scale-105 active:scale-100
            transition-all duration-300 delay-600 ${
              showContent ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-10'
            }`}
        >
          <span>Comenzar</span>
          <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />

          {/* Button glow effect */}
          <div className="absolute inset-0 rounded-xl bg-gradient-to-r from-vscode-accent to-purple-600 blur-xl opacity-50 group-hover:opacity-75 transition-opacity -z-10" />
        </button>

        {/* Version */}
        <p
          className={`mt-12 text-xs text-gray-600 transition-all duration-1000 delay-700 ${
            showContent ? 'opacity-100' : 'opacity-0'
          }`}
        >
          v0.2.0 • Powered by RAG & LLM
        </p>
      </div>

      {/* Bottom gradient fade */}
      <div className="absolute bottom-0 left-0 right-0 h-32 bg-gradient-to-t from-[#0d1117] to-transparent" />
    </div>
  )
}
