import { useState, useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext.jsx';
import { Users, Mail, UserPlus, Shield, AlertCircle, CheckCircle2, Loader2 } from 'lucide-react';
import apiClient from '../services/api.js';

export default function TeamManagement() {
  const { user } = useAuth();
  const [teamMembers, setTeamMembers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [inviting, setInviting] = useState(false);
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteRole, setInviteRole] = useState('user');
  const [inviteSuccess, setInviteSuccess] = useState(false);

  useEffect(() => {
    loadTeamMembers();
  }, []);

  const loadTeamMembers = async () => {
    try {
      const members = await apiClient.listTenantUsers();
      setTeamMembers(members);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const handleInvite = async (e) => {
    e.preventDefault();
    setInviting(true);
    setError(null);
    setInviteSuccess(false);

    try {
      await apiClient.inviteUser(inviteEmail, inviteRole);
      setInviteSuccess(true);
      setInviteEmail('');
      setInviteRole('user');
      loadTeamMembers();
      setTimeout(() => setInviteSuccess(false), 3000);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to invite user');
    } finally {
      setInviting(false);
    }
  };

  const isAdmin = user?.role === 'admin';

  const getRoleBadge = (role) => {
    if (role === 'admin') {
      return <span className="badge-primary">Admin</span>;
    } else if (role === 'user') {
      return <span className="badge-secondary">User</span>;
    } else {
      return <span className="badge-neutral">Viewer</span>;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-primary-600 animate-spin mx-auto mb-3" />
          <p className="text-neutral-600">Loading team members...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-neutral-900">Team Management</h1>
        <p className="text-neutral-600 mt-2">Manage your organization's users and permissions</p>
      </div>

      {/* Invite Section - Admin Only */}
      {isAdmin && (
        <div className="card p-6">
          <div className="flex items-center space-x-3 mb-6">
            <div className="w-10 h-10 bg-primary-100 rounded-lg flex items-center justify-center">
              <UserPlus className="w-5 h-5 text-primary-600" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-neutral-900">Invite Team Member</h2>
              <p className="text-sm text-neutral-600">Send an invitation to join your organization</p>
            </div>
          </div>

          {error && (
            <div className="bg-error-50 border border-error-200 rounded-lg p-4 mb-4">
              <div className="flex items-start space-x-3">
                <AlertCircle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
                <p className="text-sm text-error-700">{error}</p>
              </div>
            </div>
          )}

          {inviteSuccess && (
            <div className="bg-accent-50 border border-accent-200 rounded-lg p-4 mb-4">
              <div className="flex items-start space-x-3">
                <CheckCircle2 className="w-5 h-5 text-accent-600 flex-shrink-0 mt-0.5" />
                <p className="text-sm text-accent-700">Invitation sent successfully!</p>
              </div>
            </div>
          )}

          <form onSubmit={handleInvite} className="flex flex-col md:flex-row gap-4">
            <div className="flex-1">
              <input
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                placeholder="colleague@company.com"
                required
                className="input w-full"
              />
            </div>
            <div className="w-full md:w-48">
              <select
                value={inviteRole}
                onChange={(e) => setInviteRole(e.target.value)}
                className="input w-full"
              >
                <option value="viewer">Viewer</option>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </div>
            <button
              type="submit"
              disabled={inviting}
              className="btn-primary whitespace-nowrap"
            >
              {inviting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Sending...
                </>
              ) : (
                <>
                  <Mail className="w-4 h-4 mr-2" />
                  Send Invite
                </>
              )}
            </button>
          </form>
        </div>
      )}

      {/* Team Members List */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-neutral-900">Team Members</h2>
          <span className="badge-primary">{teamMembers.length} Members</span>
        </div>

        <div className="space-y-3">
          {teamMembers.map((member) => (
            <div 
              key={member.id}
              className="border border-neutral-200 rounded-lg p-4 hover:border-primary-300 transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-4">
                  <div className="w-12 h-12 bg-primary-100 rounded-full flex items-center justify-center">
                    <span className="text-primary-700 font-semibold text-lg">
                      {member.email[0].toUpperCase()}
                    </span>
                  </div>
                  <div>
                    <p className="font-semibold text-neutral-900">{member.email}</p>
                    <p className="text-sm text-neutral-600">
                      {member.id === user?.id && '(You) '}
                      Member since {new Date(member.created_at).toLocaleDateString()}
                    </p>
                  </div>
                </div>
                <div className="flex items-center space-x-3">
                  {getRoleBadge(member.role)}
                  {member.role === 'admin' && (
                    <div className="w-8 h-8 bg-primary-100 rounded-lg flex items-center justify-center">
                      <Shield className="w-4 h-4 text-primary-600" />
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        {teamMembers.length === 0 && (
          <div className="text-center py-12">
            <Users className="w-12 h-12 text-neutral-400 mx-auto mb-3" />
            <p className="text-neutral-600">No team members yet</p>
            <p className="text-sm text-neutral-500 mt-1">Invite your first team member to get started</p>
          </div>
        )}
      </div>

      {/* Permissions Info */}
      <div className="card p-6 bg-neutral-50">
        <h3 className="font-semibold text-neutral-900 mb-4">Role Permissions</h3>
        <div className="space-y-3 text-sm">
          <div className="flex items-start space-x-3">
            <Shield className="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-neutral-900">Admin</p>
              <p className="text-neutral-600">Full access - can manage users, create migrations, and modify settings</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <Users className="w-5 h-5 text-neutral-600 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-neutral-900">User</p>
              <p className="text-neutral-600">Can create and manage migrations, view analytics</p>
            </div>
          </div>
          <div className="flex items-start space-x-3">
            <Mail className="w-5 h-5 text-neutral-600 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-neutral-900">Viewer</p>
              <p className="text-neutral-600">Read-only access to migrations and analytics</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
