import axios from 'axios';

class ApiClient {
  constructor() {
    this.client = axios.create({
      baseURL: '/api',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add auth token to requests
    this.client.interceptors.request.use((config) => {
      const token = localStorage.getItem('auth_token');
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    });

    // Handle 401 errors
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response?.status === 401) {
          localStorage.removeItem('auth_token');
          localStorage.removeItem('user');
          window.location.href = '/login';
        }
        return Promise.reject(error);
      }
    );
  }

  // Auth
  async signup(email, password, tenantName) {
    const response = await this.client.post('/auth/signup', {
      email,
      password,
      tenant_name: tenantName
    });
    return response.data;
  }

  async login(email, password) {
    const response = await this.client.post('/auth/login', { email, password });
    return response.data;
  }

  async getMe() {
    const response = await this.client.get('/auth/me');
    return response.data;
  }

  async getTenantInfo() {
    const response = await this.client.get('/tenant');
    return response.data;
  }

  async listTenantUsers() {
    const response = await this.client.get('/tenant/users');
    return response.data;
  }

  async inviteUser(email, role) {
    const response = await this.client.post('/tenant/invite', { email, role });
    return response.data;
  }

  // Health Check
  async healthCheck() {
    const response = await this.client.get('/health');
    return response.data;
  }

  // Metrics
  async getMetrics() {
    const response = await this.client.get('/stats');
    return response.data;
  }

  // Analytics
  async getRealtimePerformance() {
    const response = await this.client.get('/analytics/performance/realtime');
    return response.data;
  }

  async getJobThroughput(jobId) {
    const response = await this.client.get(`/analytics/throughput/${jobId}`);
    return response.data;
  }

  async getActiveWorkers() {
    const response = await this.client.get('/analytics/workers/active');
    return response.data;
  }

  async getUsageStats(period = 'month') {
    const response = await this.client.get(`/analytics/usage?period=${period}`);
    return response.data;
  }

  // Performance Metrics
  async getPerformanceRealtime(jobId) {
    const response = await this.client.get(`/performance/realtime/${jobId}`);
    return response.data;
  }

  async getPerformanceHistory(jobId, hours = 1) {
    const response = await this.client.get(`/performance/history/${jobId}?hours=${hours}`);
    return response.data;
  }

  async getWorkerStats(jobId) {
    const response = await this.client.get(`/performance/workers/${jobId}`);
    return response.data;
  }

  async getBatchSizeHistory(jobId, tableName = null) {
    const url = tableName 
      ? `/performance/batch-size-history/${jobId}?table_name=${tableName}`
      : `/performance/batch-size-history/${jobId}`;
    const response = await this.client.get(url);
    return response.data;
  }

  async getConstraintStatus(jobId) {
    const response = await this.client.get(`/performance/constraints/${jobId}`);
    return response.data;
  }

  // Billing & Usage
  async getPlans() {
    const response = await this.client.get('/billing/plans');
    return response.data;
  }

  async getCurrentUsage() {
    const response = await this.client.get('/billing/usage/current');
    return response.data;
  }

  async getUsageHistory(days = 30) {
    const response = await this.client.get(`/billing/usage/history?days=${days}`);
    return response.data;
  }

  async getInvoices(limit = 10) {
    const response = await this.client.get(`/billing/invoices?limit=${limit}`);
    return response.data;
  }

  async getCurrentPlan() {
    const response = await this.client.get('/billing/plan/current');
    return response.data;
  }

  async upgradePlan(planId) {
    const response = await this.client.post('/billing/plan/upgrade', { plan_id: planId });
    return response.data;
  }

  async checkQuota(action = 'job_creation') {
    const response = await this.client.get(`/billing/quota/check?action=${action}`);
    return response.data;
  }

  // Audit Logs
  async getAuditLogs(filters = {}) {
    const params = new URLSearchParams();
    if (filters.action) params.append('action', filters.action);
    if (filters.user_id) params.append('user_id', filters.user_id);
    if (filters.resource_type) params.append('resource_type', filters.resource_type);
    if (filters.status) params.append('status', filters.status);
    if (filters.days) params.append('days', filters.days);
    if (filters.limit) params.append('limit', filters.limit);
    
    const response = await this.client.get(`/audit/logs?${params.toString()}`);
    return response.data;
  }

  async getAuditSummary(days = 30) {
    const response = await this.client.get(`/audit/summary?days=${days}`);
    return response.data;
  }

  async getAuditActionTypes() {
    const response = await this.client.get('/audit/actions');
    return response.data;
  }

  async getUserActivity(userId, days = 30) {
    const response = await this.client.get(`/audit/user-activity/${userId}?days=${days}`);
    return response.data;
  }

  // Jobs
  async createJob(request) {
    const response = await this.client.post('/migrations', request);
    return response.data;
  }

  async getJob(jobId) {
    const response = await this.client.get(`/migrations/${jobId}`);
    return response.data;
  }

  async listJobs() {
    const response = await this.client.get('/migrations');
    return response.data;
  }

  async resumeJob(jobId) {
    const response = await this.client.post(`/migrations/${jobId}/resume`);
    return response.data;
  }

  // Tables
  async getJobTables(jobId) {
    const response = await this.client.get(`/migrations/${jobId}/tables`);
    return response.data;
  }

  // Chunks
  async getTableChunks(jobId, tableId) {
    const response = await this.client.get(`/migrations/${jobId}/tables/${tableId}/chunks`);
    return response.data;
  }

  async retryChunk(jobId, chunkId) {
    const response = await this.client.post(`/migrations/${jobId}/chunks/${chunkId}/retry`);
    return response.data;
  }

  // Generic HTTP methods for flexibility
  async get(url, config) {
    const response = await this.client.get(url, config);
    return response;
  }

  async post(url, data, config) {
    const response = await this.client.post(url, data, config);
    return response;
  }

  async put(url, data, config) {
    const response = await this.client.put(url, data, config);
    return response;
  }

  async patch(url, data, config) {
    const response = await this.client.patch(url, data, config);
    return response;
  }

  async delete(url, config) {
    const response = await this.client.delete(url, config);
    return response;
  }
}

const apiClient = new ApiClient();
export default apiClient;
