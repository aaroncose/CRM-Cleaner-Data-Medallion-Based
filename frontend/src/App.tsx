import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Intro from './pages/Intro'
import Dashboard from './pages/Dashboard'
import ProcessCSV from './pages/ProcessCSV'
import Compare from './pages/Compare'
import Review from './pages/Review'
import Chat from './pages/Chat'
import Metrics from './pages/Metrics'
import Settings from './pages/Settings'

function AppLayout({ children }: { children: React.ReactNode }) {
  return <Layout>{children}</Layout>
}

function App() {
  return (
    <Routes>
      {/* Intro page without layout */}
      <Route path="/" element={<Intro />} />

      {/* Main app with layout */}
      <Route path="/dashboard" element={<AppLayout><Dashboard /></AppLayout>} />
      <Route path="/process" element={<AppLayout><ProcessCSV /></AppLayout>} />
      <Route path="/compare" element={<AppLayout><Compare /></AppLayout>} />
      <Route path="/review" element={<AppLayout><Review /></AppLayout>} />
      <Route path="/chat" element={<AppLayout><Chat /></AppLayout>} />
      <Route path="/metrics" element={<AppLayout><Metrics /></AppLayout>} />
      <Route path="/settings" element={<AppLayout><Settings /></AppLayout>} />
    </Routes>
  )
}

export default App
