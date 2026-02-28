import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext.jsx';
import ProtectedRoute from './components/ProtectedRoute.jsx';
import Layout from './components/Layout.jsx';
import Login from './pages/Login.jsx';
import Signup from './pages/Signup.jsx';
import Dashboard from './pages/Dashboard.jsx';
import CreateJob from './pages/CreateJob.jsx';
import JobDetail from './pages/JobDetail.jsx';
import TableDetail from './pages/TableDetail.jsx';
import PerformanceDashboard from './pages/PerformanceDashboard.jsx';
import PerformanceMetricsPage from './pages/PerformanceMetricsPage.jsx';
import TeamManagement from './pages/TeamManagement.jsx';
import BillingPage from './pages/BillingPage.jsx';
import DatabaseConnectionsPage from './pages/DatabaseConnectionsPage.jsx';
import SchemaMigrationPage from './pages/SchemaMigrationPage.jsx';
import DataViewerPage from './pages/DataViewerPage.jsx';

function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          {/* Public Routes */}
          <Route path="/login" element={<Login />} />
          <Route path="/signup" element={<Signup />} />

          {/* Protected Routes */}
          <Route path="/" element={<ProtectedRoute><Layout /></ProtectedRoute>}>
            <Route index element={<Dashboard />} />
            <Route path="jobs/new" element={<CreateJob />} />
            <Route path="jobs/:jobId" element={<JobDetail />} />
            <Route path="jobs/:jobId/performance" element={<PerformanceMetricsPage />} />
            <Route path="jobs/:jobId/tables/:tableId" element={<TableDetail />} />
            <Route path="performance" element={<PerformanceDashboard />} />
            <Route path="team" element={<TeamManagement />} />
            <Route path="billing" element={<BillingPage />} />
            <Route path="schema/connections" element={<DatabaseConnectionsPage />} />
            <Route path="schema/migration" element={<SchemaMigrationPage />} />
            <Route path="schema/data-viewer" element={<DataViewerPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;
