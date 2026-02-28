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
    const response = await this.client.get('/metrics');
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
}

const apiClient = new ApiClient();
export default apiClient;
