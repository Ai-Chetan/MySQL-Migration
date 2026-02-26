import { createContext, useContext, useState, useEffect } from 'react';
import apiClient from '../services/api.js';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [tenant, setTenant] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    if (token) {
      loadUser();
    } else {
      setLoading(false);
    }
  }, []);

  const loadUser = async () => {
    try {
      const [userData, tenantData] = await Promise.all([
        apiClient.getMe(),
        apiClient.getTenantInfo()
      ]);
      setUser(userData);
      setTenant(tenantData);
    } catch (error) {
      console.error('Failed to load user:', error);
      logout();
    } finally {
      setLoading(false);
    }
  };

  const login = async (email, password) => {
    const response = await apiClient.login(email, password);
    localStorage.setItem('auth_token', response.access_token);
    localStorage.setItem('user', JSON.stringify({
      id: response.user_id,
      role: response.role,
      tenant_id: response.tenant_id
    }));
    await loadUser();
    return response;
  };

  const signup = async (email, password, tenantName) => {
    const response = await apiClient.signup(email, password, tenantName);
    localStorage.setItem('auth_token', response.access_token);
    localStorage.setItem('user', JSON.stringify({
      id: response.user_id,
      role: response.role,
      tenant_id: response.tenant_id
    }));
    await loadUser();
    return response;
  };

  const logout = () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('user');
    setUser(null);
    setTenant(null);
  };

  return (
    <AuthContext.Provider value={{ user, tenant, login, signup, logout, loading }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
