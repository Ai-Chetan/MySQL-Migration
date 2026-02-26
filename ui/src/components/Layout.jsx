import { useState } from 'react';
import { Outlet, Link, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext.jsx';
import { Database, LayoutDashboard, Plus, Activity, BarChart3, Users, CreditCard, LogOut, ChevronDown, User, Workflow } from 'lucide-react';

export default function Layout() {
  const location = useLocation();
  const { user, tenant, logout } = useAuth();
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [schemaMenuOpen, setSchemaMenuOpen] = useState(false);

  const isActive = (path) => {
    if (path === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(path);
  };

  const handleLogout = () => {
    logout();
    setUserMenuOpen(false);
  };

  return (
    <div className="min-h-screen bg-neutral-50">
      {/* Header */}
      <header className="bg-white border-b border-neutral-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            {/* Logo */}
            <Link to="/" className="flex items-center space-x-3">
              <div className="w-10 h-10 bg-gradient-to-br from-primary-600 to-primary-800 rounded-lg flex items-center justify-center">
                <Database className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-xl font-bold text-neutral-900">Migration Platform</h1>
                <p className="text-xs text-neutral-500">Enterprise Data Migration</p>
              </div>
            </Link>

            {/* Navigation */}
            <nav className="flex items-center space-x-1">
              <Link
                to="/"
                className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                  isActive('/') && location.pathname === '/'
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-neutral-600 hover:bg-neutral-100'
                }`}
              >
                <LayoutDashboard className="w-4 h-4" />
                <span>Jobs</span>
              </Link>
              <Link
                to="/jobs/new"
                className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                  isActive('/jobs/new')
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-neutral-600 hover:bg-neutral-100'
                }`}
              >
                <Plus className="w-4 h-4" />
                <span>New Migration</span>
              </Link>
              <Link
                to="/performance"
                className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                  isActive('/performance')
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-neutral-600 hover:bg-neutral-100'
                }`}
              >
                <BarChart3 className="w-4 h-4" />
                <span>Performance</span>
              </Link>
              <Link
                to="/team"
                className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                  isActive('/team')
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-neutral-600 hover:bg-neutral-100'
                }`}
              >
                <Users className="w-4 h-4" />
                <span>Team</span>
              </Link>
              <Link
                to="/billing"
                className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                  isActive('/billing')
                    ? 'bg-primary-50 text-primary-700'
                    : 'text-neutral-600 hover:bg-neutral-100'
                }`}
              >
                <CreditCard className="w-4 h-4" />
                <span>Billing</span>
              </Link>
              
              {/* Schema Migration Dropdown */}
              <div className="relative">
                <button
                  onClick={() => setSchemaMenuOpen(!schemaMenuOpen)}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                    isActive('/schema')
                      ? 'bg-primary-50 text-primary-700'
                      : 'text-neutral-600 hover:bg-neutral-100'
                  }`}
                >
                  <Workflow className="w-4 h-4" />
                  <span>Schema</span>
                  <ChevronDown className={`w-3 h-3 transition-transform ${schemaMenuOpen ? 'rotate-180' : ''}`} />
                </button>

                {/* Schema Dropdown Menu */}
                {schemaMenuOpen && (
                  <>
                    <div 
                      className="fixed inset-0 z-10" 
                      onClick={() => setSchemaMenuOpen(false)}
                    ></div>
                    <div className="absolute left-0 mt-2 w-56 bg-white rounded-lg shadow-strong border border-neutral-200 py-2 z-20">
                      <Link
                        to="/schema/connections"
                        onClick={() => setSchemaMenuOpen(false)}
                        className={`flex items-center space-x-2 px-4 py-2 text-sm transition-colors ${
                          location.pathname === '/schema/connections'
                            ? 'bg-primary-50 text-primary-700'
                            : 'text-neutral-700 hover:bg-neutral-100'
                        }`}
                      >
                        <Database className="w-4 h-4" />
                        <span>Database Connections</span>
                      </Link>
                      <Link
                        to="/schema/migration"
                        onClick={() => setSchemaMenuOpen(false)}
                        className={`flex items-center space-x-2 px-4 py-2 text-sm transition-colors ${
                          location.pathname === '/schema/migration'
                            ? 'bg-primary-50 text-primary-700'
                            : 'text-neutral-700 hover:bg-neutral-100'
                        }`}
                      >
                        <Workflow className="w-4 h-4" />
                        <span>Schema Migration</span>
                      </Link>
                    </div>
                  </>
                )}
              </div>
            </nav>

            {/* User Menu */}
            <div className="flex items-center space-x-3">
              <div className="flex items-center space-x-2 px-3 py-1.5 bg-accent-50 rounded-lg">
                <Activity className="w-4 h-4 text-accent-600 animate-pulse" />
                <span className="text-sm font-medium text-accent-700">Live</span>
              </div>
              
              <div className="relative">
                <button
                  onClick={() => setUserMenuOpen(!userMenuOpen)}
                  className="flex items-center space-x-3 px-3 py-2 rounded-lg hover:bg-neutral-100 transition-colors"
                >
                  <div className="w-8 h-8 bg-primary-100 rounded-full flex items-center justify-center">
                    <User className="w-4 h-4 text-primary-700" />
                  </div>
                  <div className="text-left hidden md:block">
                    <p className="text-sm font-medium text-neutral-900">{user?.email}</p>
                    <p className="text-xs text-neutral-500">{tenant?.name}</p>
                  </div>
                  <ChevronDown className={`w-4 h-4 text-neutral-500 transition-transform ${userMenuOpen ? 'rotate-180' : ''}`} />
                </button>

                {/* Dropdown Menu */}
                {userMenuOpen && (
                  <>
                    <div 
                      className="fixed inset-0 z-10" 
                      onClick={() => setUserMenuOpen(false)}
                    ></div>
                    <div className="absolute right-0 mt-2 w-56 bg-white rounded-lg shadow-strong border border-neutral-200 py-2 z-20">
                      <div className="px-4 py-3 border-b border-neutral-200">
                        <p className="text-sm font-medium text-neutral-900">{user?.email}</p>
                        <p className="text-xs text-neutral-500 mt-1">{tenant?.name}</p>
                        <span className={`inline-block mt-2 text-xs px-2 py-1 rounded ${
                          user?.role === 'admin' ? 'bg-primary-100 text-primary-700' : 'bg-neutral-100 text-neutral-700'
                        }`}>
                          {user?.role}
                        </span>
                      </div>
                      <button
                        onClick={handleLogout}
                        className="w-full flex items-center space-x-2 px-4 py-2 text-sm text-error-600 hover:bg-error-50 transition-colors"
                      >
                        <LogOut className="w-4 h-4" />
                        <span>Sign Out</span>
                      </button>
                    </div>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        <Outlet />
      </main>

      {/* Footer */}
      <footer className="border-t border-neutral-200 mt-16">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex items-center justify-between">
            <p className="text-sm text-neutral-500">
              Migration Platform v1.0 | Phase 1 Production
            </p>
            <div className="flex items-center space-x-6 text-sm text-neutral-500">
              <a href="/api/docs" target="_blank" rel="noreferrer" className="hover:text-primary-600 transition-colors">
                API Documentation
              </a>
              <a href="https://github.com" target="_blank" rel="noreferrer" className="hover:text-primary-600 transition-colors">
                GitHub
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}
