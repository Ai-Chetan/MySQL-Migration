# Database Migration Platform

A comprehensive database migration platform with two powerful modes:
1. **Enterprise Migration Engine** - Chunk-based parallel processing for large-scale migrations
2. **Schema Migration Tool** - Interactive schema transformation with single/split/merge operations

Built with FastAPI, React, and PostgreSQL for production-ready database migrations.

---

## 🚀 Quick Start

### Prerequisites
- **PostgreSQL** (local installation - for metadata storage)
- **Python 3.9+**
- **Node.js 18+**
- psql command-line tool (for schema initialization)

### Installation Steps

1. **Clone & Configure**
   ```bash
   cd Data_Migration
   ```

2. **Setup Environment**
   ```bash
   # Copy example environment file
   cp .env.example .env
   
   # Edit .env and set your local PostgreSQL password
   # METADATA_DB_PASSWORD=your_postgres_password
   ```

3. **Create PostgreSQL Database**
   ```sql
   -- Connect to PostgreSQL and create database
   CREATE DATABASE migration_metadata;
   ```

4. **Start All Services**
   ```powershell
   # Windows PowerShell
   .\start.ps1
   ```

   This will:
   - ✅ Verify PostgreSQL connection
   - ✅ Initialize database schema
   - ✅ Start FastAPI backend (http://localhost:8000)
   - ✅ Start React frontend (http://localhost:3000)

5. **Access the Platform**
   - **Web UI**: http://localhost:3000
   - **API Documentation**: http://localhost:8000/docs
   - **API Health**: http://localhost:8000/health

---

## 📁 Architecture

```
Data_Migration/
├── services/
│   ├── api/                           # FastAPI Backend
│   │   ├── main.py                   # FastAPI app entry point
│   │   ├── config.py                 # Configuration (Redis disabled)
│   │   ├── auth.py                   # JWT authentication
│   │   ├── metadata/                 # PostgreSQL repository
│   │   ├── planner/                  # Migration planning engine
│   │   ├── schema_migration/         # Schema transformation services
│   │   │   ├── connection_manager.py
│   │   │   ├── schema_parser.py
│   │   │   ├── schema_comparator.py
│   │   │   ├── mapping_engine.py
│   │   │   ├── data_type_analyzer.py
│   │   │   ├── migration_executor.py
│   │   │   └── script_generator.py
│   │   └── routers/                  # API endpoints
│   │       ├── migrations.py         # Migration jobs
│   │       ├── auth.py              # Authentication
│   │       ├── analytics.py         # Performance metrics
│   │       └── schema_migration.py  # Schema tool endpoints
│   └── worker/                       # Worker service (Redis disabled)
│
├── shared/                           # Shared utilities
│   ├── models.py                    # Data models
│   ├── utils.py                     # Common utilities
│   └── chunking.py                  # Chunking logic
│
├── ui/                              # React Frontend
│   ├── src/
│   │   ├── pages/                   # Page components
│   │   │   ├── Login.jsx
│   │   │   ├── Signup.jsx
│   │   │   ├── Dashboard.jsx
│   │   │   ├── CreateJob.jsx
│   │   │   ├── SchemaMigrationPage.jsx
│   │   │   ├── DatabaseConnectionsPage.jsx
│   │   │   ├── DataViewerPage.jsx
│   │   │   ├── PerformanceDashboard.jsx
│   │   │   └── TeamManagement.jsx
│   │   ├── components/              # Reusable components
│   │   │   ├── ColumnMappingDialog.jsx
│   │   │   ├── SplitTableDialog.jsx
│   │   │   └── MergeTablesDialog.jsx
│   │   ├── contexts/                # React contexts
│   │   │   └── AuthContext.jsx
│   │   └── services/                # API client
│   │       └── api.js
│   └── package.json
│
├── .env                             # Environment configuration
├── schema.sql                       # PostgreSQL schema
├── docker-compose.yml               # Docker setup (optional)
├── start.ps1                        # Startup script
└── README.md                        # This file
```

---

## 🎯 Features

### Mode 1: Enterprise Migration Engine

Perfect for **large-scale data migrations** with millions of rows.

#### Core Capabilities
- **Chunk-based Processing**: Automatically divides tables into 100K-row chunks
- **Parallel Execution**: Multiple chunks processed simultaneously
- **Batch Inserts**: 5000-row batches for optimal performance (configurable)
- **Crash Recovery**: Resume from where it stopped with chunk-level retry
- **Progress Tracking**: Real-time monitoring of job/table/chunk status

#### Enterprise Features
- ✅ **Multi-Tenancy**: Complete tenant isolation with per-tenant data
- ✅ **Authentication**: JWT-based auth with role-based access control
- ✅ **Team Management**: Invite users, assign roles (admin/user/viewer)
- ✅ **Performance Analytics**: Real-time throughput, worker status, queue depth
- ✅ **Usage Tracking**: Migration counts, rows migrated, compute hours

#### Creating a Migration Job
```json
{
  "source_config": {
    "host": "source-db.example.com",
    "port": 3306,
    "database": "old_db",
    "username": "user",
    "password": "pass"
  },
  "target_config": {
    "host": "target-db.example.com",
    "port": 3306,
    "database": "new_db",
    "username": "user",
    "password": "pass"
  },
  "table_mappings": {
    "users": {
      "target_table": "customers",
      "column_mapping": {
        "id": "customer_id",
        "name": "full_name"
      }
    }
  },
  "chunk_size": 100000
}
```

---

### Mode 2: Schema Migration Tool

Perfect for **schema transformations** with complex table operations.

#### Key Features
- **Multi-Database Support**: MySQL, PostgreSQL, MariaDB
- **Schema Parsing**: Load schemas from text files
- **Visual Comparison**: Side-by-side schema comparison with change highlighting
- **Three Migration Types**:
  - **Single** (1→1): Direct table-to-table mapping
  - **Split** (1→N): Split one table into multiple targets
  - **Merge** (N→1): Combine multiple tables with JOINs

#### Unique Capabilities

##### 1. Auto-Mapping
- Automatically maps tables with matching names (case-insensitive)
- Triggers on schema file upload
- Saves time for large schemas with consistent naming

##### 2. Data Type Safety Analysis
- **✅ Safe**: No data loss (INT → BIGINT, VARCHAR(50) → VARCHAR(100))
- **⚠️ Lossy**: Potential data loss (BIGINT → INT, DECIMAL(10,2) → INT)
- **❌ Unsafe**: Incompatible types (VARCHAR → DATE, TEXT → INT)
- Automatic CAST generation when safe
- Warnings before lossy conversions

##### 3. Manual Confirmation Gate
Six safety checkboxes before execution:
1. ✅ Compared Schemas?
2. ✅ Checked Data Types?
3. ✅ Verified Mappings?
4. ✅ Checked Default Values?
5. ✅ Database Backed Up?
6. ✅ Proceed with Create?

##### 4. Color-Coded Status System
- 🔴 **Red**: Not mapped, not in schema
- 🟠 **Orange**: In schema but not mapped
- 🟣 **Purple**: Mapped but target not in schema
- 🔵 **Blue**: Single mapping (valid)
- 🟦 **Teal**: Split operation
- 🟢 **Green**: Merge operation

##### 5. Advanced Merge Operations
```
MERGE: orders, shipments, payments -> full_orders

JOIN Conditions:
INNER JOIN shipments ON orders.id = shipments.order_id
INNER JOIN payments ON orders.id = payments.order_id

Column Mappings:
orders.id -> order_id
orders.customer_id -> customer_id
shipments.tracking_number -> tracking
payments.amount -> total_paid
```

##### 6. Manual Script Generation
Generate Python migration scripts with:
- Complete database connection setup
- Data transformation logic with TODOs
- Batch processing (5000 rows/batch)
- Error handling and rollback
- Progress tracking
- User confirmation prompts

#### Schema File Format
```txt
# Comments start with # or --

Table: users
id INT PRIMARY KEY AUTO_INCREMENT
username VARCHAR(50) NOT NULL UNIQUE
email VARCHAR(100) NOT NULL
created_at DATETIME DEFAULT CURRENT_TIMESTAMP
status ENUM('active','inactive') DEFAULT 'active'

Table: orders
order_id INT PRIMARY KEY AUTO_INCREMENT
user_id INT NOT NULL
total DECIMAL(10,2) NOT NULL
order_date DATETIME DEFAULT CURRENT_TIMESTAMP
```

#### Workflow Example
1. **Connect**: Select source database connection
2. **Upload**: Upload new schema file (.txt format)
3. **Auto-Map**: Tables with matching names are automatically mapped
4. **Configure**: 
   - Single mappings for direct copies
   - Split for normalizing wide tables
   - Merge for denormalizing related tables
5. **Review**: View schema comparison, check data types
6. **Confirm**: Check all 6 safety boxes
7. **Execute**: Migration creates `tablename_new` tables
8. **Verify**: View old and new data, export as CSV/JSON

---

## 🛠️ Technology Stack

### Backend
- **FastAPI**: High-performance async API framework
- **PostgreSQL**: Metadata storage (local installation)
- **PyMySQL / psycopg2**: Database connectors
- **PyJWT**: JWT authentication
- **Passlib**: Bcrypt password hashing

### Frontend
- **React 18**: Modern UI framework
- **Vite**: Fast build tool & dev server
- **Tailwind CSS**: Utility-first styling
- **Recharts**: Performance charts & analytics
- **Axios**: HTTP client with JWT interceptors
- **React Router**: Client-side routing
- **Lucide Icons**: Modern icon library

### Infrastructure
- **Local PostgreSQL**: Metadata database
- **Docker** (Optional): For containerized deployment
- ~~**Redis**~~ (Disabled): Will be added later for distributed queue

---

## 🔐 Security & Authentication

### JWT Authentication
- Secure token-based authentication
- Configurable expiration (24 hours default)
- Auto-refresh with Axios interceptors
- HTTP-only storage in localStorage

### Password Security
- Bcrypt hashing (industry standard)
- Minimum 8 characters
- Never stored in plain text

### Multi-Tenancy
- Complete tenant isolation
- Row-level security in all queries
- Tenant ID automatically scoped from JWT

### Roles & Permissions
- **Admin**: Full access (users, migrations, settings)
- **User**: Create/manage migrations, view analytics
- **Viewer**: Read-only access

---

## 📊 API Endpoints

### Authentication
```http
POST   /api/auth/signup          # Create account
POST   /api/auth/login           # Login and get JWT
GET    /api/auth/me              # Get current user info
GET    /api/tenant               # Get tenant details
POST   /api/tenant/invite        # Invite team member (admin)
GET    /api/tenant/users         # List team members
```

### Enterprise Migrations
```http
POST   /api/migrations           # Create migration job
GET    /api/migrations           # List all jobs (tenant-scoped)
GET    /api/migrations/{id}      # Get job details
POST   /api/migrations/{id}/retry # Retry failed chunks
GET    /api/migrations/{id}/tables/{table} # Table details
```

### Schema Migration Tool
```http
GET    /api/schema-migration/connections            # List connections
POST   /api/schema-migration/connections            # Create connection
POST   /api/schema-migration/connections/{id}/test  # Test connection
GET    /api/schema-migration/connections/{id}/tables # List tables
POST   /api/schema-migration/schema/parse           # Parse schema file
POST   /api/schema-migration/schema/compare         # Compare schemas
POST   /api/schema-migration/mappings/single        # Create single mapping
POST   /api/schema-migration/mappings/split         # Create split mapping
POST   /api/schema-migration/mappings/merge         # Create merge mapping
POST   /api/schema-migration/migrate/execute        # Execute migration
POST   /api/schema-migration/script/generate        # Generate Python script
GET    /api/schema-migration/connections/{id}/tables/{table}/data # View data
POST   /api/schema-migration/connections/{id}/tables/{table}/export # Export CSV/JSON
```

### Analytics
```http
GET    /api/analytics/performance/realtime  # Live metrics
GET    /api/analytics/throughput/{job_id}   # Per-table stats
GET    /api/analytics/workers/active        # Worker status
GET    /api/analytics/usage                 # Usage statistics
```

---

## 🎨 UI Pages

### Core Pages
- **Login / Signup**: User authentication
- **Dashboard**: Job list with real-time status
- **Create Job**: Enterprise migration job creation
- **Job Detail**: Chunk-level progress tracking

### Schema Migration Pages
- **Database Connections**: Manage database connections
- **Schema Migration**: Interactive schema transformation tool
  - 4-step wizard (Connect → Upload → Map → Execute)
  - Auto-mapping on schmea load
  - Column mapping dialogs
  - Split table configuration
  - Merge table builder with JOIN conditions
  - 6-checkbox safety confirmation gate
  - Manual script generation
- **Data Viewer**: Browse table data, export CSV/JSON

### Analytics Pages
- **Performance Dashboard**: Real-time throughput charts
- **Team Management**: User invitations and roles
- **Billing & Usage**: Usage tracking and metrics

---

## 🐳 Docker Deployment (Optional)

While the default setup uses local PostgreSQL, you can optionally use Docker:

```bash
# Start with Docker (PostgreSQL + API + UI)
docker-compose up -d

# Scale workers (when Redis is enabled)
# docker-compose up -d --scale worker=5
```

**Note**: Redis service is currently commented out in `docker-compose.yml`. Will be enabled in future updates for distributed job queue functionality.

---

## 🔧 Development

### Run Backend Only
```bash
# Activate virtual environment (if using one)
# .\venv\Scripts\activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run API
python -m uvicorn services.api.main:app --reload --port 8000
```

### Run Frontend Only
```bash
cd ui
npm install
npm run dev
```

### Database Schema Updates
```bash
# Apply schema changes
psql -h localhost -U postgres -d migration_metadata -f schema.sql
```

---

## 📝 Configuration

### Environment Variables (.env)
```env
# PostgreSQL (Local)
METADATA_DB_HOST=localhost
METADATA_DB_PORT=5432
METADATA_DB_NAME=migration_metadata
METADATA_DB_USER=postgres
METADATA_DB_PASSWORD=your_password_here

# Redis (DISABLED - TO BE ADDED LATER)
# REDIS_HOST=localhost
# REDIS_PORT=6379
# REDIS_DB=0

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO

# JWT Settings
JWT_SECRET=your-secret-key-here
JWT_ALGORITHM=HS256
JWT_EXPIRATION_HOURS=24

# Migration Defaults
DEFAULT_CHUNK_SIZE=100000
DEFAULT_BATCH_SIZE=5000
BATCH_SIZE=5000
MAX_RETRIES=3
```

---

## 🚀 Production Deployment

### 1. Database Setup
```sql
-- Create production database
CREATE DATABASE migration_metadata;

-- Apply schema
\i schema.sql

-- Create read-only user for analytics (optional)
CREATE USER migration_readonly WITH PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_readonly;
```

### 2. Environment Configuration
```bash
# Update .env with production values
METADATA_DB_HOST=prod-postgres.example.com
METADATA_DB_PASSWORD=secure_random_password
JWT_SECRET=$(openssl rand -hex 32)
LOG_LEVEL=WARNING
```

### 3. Docker Deployment
```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# Check logs
docker-compose logs -f api
```

### 4. Monitoring
- **Health Check**: http://your-domain.com/health
- **Metrics**: /api/analytics/performance/realtime
- **Logs**: `docker-compose logs -f`

---

## 🐛 Troubleshooting

### Issue: "Cannot connect to PostgreSQL"
**Solution**:
```bash
# Check if PostgreSQL is running
Get-Service postgresql*

# Verify connection
psql -h localhost -U postgres -d migration_metadata

# Check .env file has correct password
```

### Issue: "Module not found" errors
**Solution**:
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# For UI
cd ui
npm install
```

### Issue: "Port already in use"
**Solution**:
```powershell
# Find process using port 8000
netstat -ano | findstr :8000

# Kill process
Stop-Process -Id <PID> -Force
```

### Issue: Schema migration fails with "lossy conversion"
**Solution**:
- Review data type comparison in schema comparison modal
- Check "Checked Data Types?" checkbox to acknowledge
- Or use manual script generation for custom conversion logic

---

## 📖 Advanced Topics

### Custom Transformations
Use **Manual Script Generation** to create Python scripts with custom transformation logic:
```python
# Generated script includes TODOs for custom logic
new_data_dict['email'] = old_row.get('email')
# TODO: Add email validation
# TODO: Convert to lowercase
```

### Merge with Complex JOINs
```sql
-- Multi-table merge with LEFT JOIN
INNER JOIN orders_detail ON orders.id = orders_detail.order_id
LEFT JOIN promotions ON orders.promo_code = promotions.code

-- Column mapping includes all source tables
orders.id -> order_id
orders_detail.quantity -> qty
promotions.discount -> discount_amount
```

### Batch Size Tuning
```env
# Small batches for low memory
DEFAULT_BATCH_SIZE=1000

# Large batches for high throughput
DEFAULT_BATCH_SIZE=10000

# Schema Migration default (matches reference implementation)
BATCH_SIZE=5000
```

---

## 🗺️ Roadmap

### Current Status
- ✅ Enterprise migration engine (PostgreSQL-based)
- ✅ Schema migration tool (100% feature parity with desktop version)
- ✅ JWT authentication & multi-tenancy
- ✅ Performance analytics
- ✅ Team management
- ⏳ Redis queue support (to be added)
- ⏳ Worker service (to be enabled with Redis)

### Future Enhancements
- 🔜 Redis-based distributed job queue
- 🔜 Worker pool with horizontal scaling
- 🔜 WebSocket real-time updates (eliminate polling)
- 🔜 Schema diff visualization
- 🔜 Automated rollback mechanisms
- 🔜 Slack/Teams notifications
- 🔜 Multi-region support
- 🔜 Data validation rules
- 🔜 Visual JOIN builder for merges
- 🔜 Migration templates & presets

---

## 📄 License

MIT License - See LICENSE file for details

---

## 💬 Support

For issues, questions, or feature requests:
- Open an issue on GitHub
- Check [API Documentation](http://localhost:8000/docs) for endpoint details
- Review logs: `docker-compose logs -f api`

---

## 🙏 Acknowledgments

- Built with FastAPI, React, and PostgreSQL
- Schema migration logic inspired by industry best practices
- UI design inspired by modern SaaS platforms
- Icons from Lucide
- Charts powered by Recharts

---

**Database Migration Platform** | Enterprise-Grade Migrations Made Simple

## 🚀 Features

### Core Migration Engine
- **Chunk-based Processing**: Divides large tables into 100K-row chunks for parallel processing
- **Worker Pool**: Scalable worker service with horizontal scaling support
- **Redis Queue**: Distributed job queue for reliable task distribution
- **Resumability**: Automatic crash recovery with chunk-level retry logic
- **Batch Inserts**: 1000-row batch operations for optimal performance
- **Heartbeat Monitoring**: Real-time worker health tracking

### Enterprise Features (Phases 3-5)
- ✅ **Authentication & Authorization**: JWT-based authentication with role-based access control
- ✅ **Multi-Tenancy**: Full tenant isolation with per-tenant data segregation
- ✅ **Real-time Performance Monitoring**: Live metrics, throughput charts, worker status
- ✅ **Team Management**: User invitations, role management (admin/user/viewer)
- ✅ **Usage Analytics**: Billing metrics, daily activity tracking, usage reports
- ✅ **Production-Grade UI**: Modern React interface with Tailwind CSS & Recharts

## 📁 Architecture

```
Data_Migration/
├── services/
│   ├── api/                    # FastAPI Control Plane
│   │   ├── main.py            # FastAPI app with routers
│   │   ├── auth.py            # JWT authentication & user management
│   │   ├── metadata/          # Metadata repository (PostgreSQL)
│   │   ├── planner/           # Migration planning & chunking
│   │   └── routers/           # API endpoints
│   │       ├── migrations.py  # Migration job management (protected)
│   │       ├── auth.py        # Signup, login, tenant management
│   │       └── analytics.py   # Performance & usage metrics
│   └── worker/                # Worker Service
│       ├── worker.py          # Worker main process
│       ├── executor.py        # Chunk execution engine
│       └── db.py              # MySQL connector
├── shared/                    # Shared models & utilities
│   ├── models.py             # Pydantic models
│   ├── utils.py              # Common utilities
│   └── chunking.py           # Chunking logic
├── ui/                       # React Frontend
│   ├── src/
│   │   ├── pages/            # React pages
│   │   │   ├── Login.jsx
│   │   │   ├── Signup.jsx
│   │   │   ├── Dashboard.jsx
│   │   │   ├── CreateJob.jsx
│   │   │   ├── JobDetail.jsx
│   │   │   ├── TableDetail.jsx
│   │   │   ├── PerformanceDashboard.jsx
│   │   │   ├── TeamManagement.jsx
│   │   │   └── BillingPage.jsx
│   │   ├── components/       # React components
│   │   │   ├── Layout.jsx
│   │   │   └── ProtectedRoute.jsx
│   │   ├── contexts/         # React contexts
│   │   │   └── AuthContext.jsx
│   │   └── services/         # API client
│   │       └── api.js
│   ├── package.json
│   └── vite.config.js
├── docker-compose.yml        # Full stack orchestration
├── schema.sql                # PostgreSQL metadata schema
└── README.md
```

## 🛠️ Technology Stack

### Backend
- **FastAPI**: High-performance async API framework
- **PostgreSQL**: Metadata storage (jobs, chunks, users, tenants)
- **Redis**: Job queue and worker coordination
- **MySQL Connector**: Source & target database access
- **PyJWT**: JWT authentication
- **Passlib**: Password hashing with bcrypt
- **Docker**: Containerization & orchestration

### Frontend
- **React 18**: Modern UI framework
- **Vite**: Fast build tool
- **Tailwind CSS**: Utility-first styling
- **Recharts**: Data visualization & charts
- **Axios**: HTTP client with JWT interceptors
- **React Router**: Client-side routing
- **Lucide Icons**: Modern icon library
- **date-fns**: Date formatting


### Infrastructure
- **Docker Compose**: Multi-container orchestration
- **Nginx**: (Optional) Reverse proxy & load balancing

## 🚦 Quick Start

### Prerequisites
- Docker & Docker Compose
- Node.js 18+ (for local UI development)
- MySQL 8.0+ (source & target databases)

### 1. Clone & Configure

```bash
cd Data_Migration
```

### 2. Configure Environment

Create `.env` file (or use environment variables):

```env
# Metadata Database (PostgreSQL)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=migration_metadata
POSTGRES_USER=migration_user
POSTGRES_PASSWORD=secure_password

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# JWT Secret (generate with: openssl rand -hex 32)
JWT_SECRET=your_secure_random_secret_key_here
JWT_ALGORITHM=HS256
JWT_EXPIRATION_HOURS=24

# Source MySQL Database
SOURCE_HOST=source_mysql_host
SOURCE_PORT=3306
SOURCE_DB=source_database
SOURCE_USER=source_user
SOURCE_PASSWORD=source_password

# Target MySQL Database
TARGET_HOST=target_mysql_host
TARGET_PORT=3306
TARGET_DB=target_database
TARGET_USER=target_user
TARGET_PASSWORD=target_password
```

### 3. Start Services

```bash
# Build and start all services
docker-compose up --build

# Scale workers (optional)
docker-compose up --scale worker=5
```

Services will be available at:
- **UI**: http://localhost:3000
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

### 4. Initialize Database

On first run, PostgreSQL schema is automatically created. Access the UI and create your account:

1. Navigate to http://localhost:3000
2. Click "Sign Up"
3. Enter company name, email, and password
4. You'll be automatically logged in with admin privileges

## 📖 Usage Guide

### Create Migration Job

1. **Navigate to "New Migration"**
2. **Enter Job Details**:
   - Job name
   - Source/target connection details
   - Table mappings (JSON format)

**Example Table Mapping**:
```json
{
  "users": {
    "target_table": "users_new",
    "column_mapping": {
      "id": "user_id",
      "name": "full_name",
      "email": "email_address"
    }
  },
  "orders": {
    "target_table": "orders_new",
    "column_mapping": {
      "id": "order_id",
      "user_id": "customer_id"
    }
  }
}
```

3. **Submit** - Job is automatically planned and chunked
4. **Monitor Progress** - Real-time updates on job detail page

### Monitor Performance

Navigate to **Performance Dashboard** to view:
- **Real-time Throughput**: Rows/second, chunks/minute
- **Active Workers**: Live worker status with heartbeats
- **Queue Depth**: Pending chunks in queue
- **Historical Charts**: Throughput over time with Recharts

### Manage Team (Admin Only)

Navigate to **Team Management**:
- Invite users via email
- Assign roles: Admin, User, or Viewer
- View all team members
- Role permissions:
  - **Admin**: Full access (manage users, create migrations, settings)
  - **User**: Create and manage migrations, view analytics
  - **Viewer**: Read-only access

### Track Usage & Billing

Navigate to **Billing & Usage**:
- View current plan (Free/Pro/Enterprise)
- Track migrations count, rows migrated, compute hours
- Daily activity charts
- Usage statistics by period (24h/7d/30d)

## 🔐 Authentication & Security

### JWT Authentication
- **Token-based**: Secure JWT tokens with configurable expiration
- **HTTP-only**: Tokens stored in localStorage, auto-attached to requests
- **Role-based Access**: Admin/User/Viewer roles with endpoint protection
- **Auto-refresh**: Axios interceptors handle 401 errors and redirect to login

### Password Security
- **Bcrypt Hashing**: Industry-standard password hashing
- **Min 8 Characters**: Password complexity requirements
- **No Plain Text**: Passwords never stored in plain text

### Multi-Tenancy
- **Tenant Isolation**: All data scoped to tenant ID
- **Row-level Security**: Database queries automatically filter by tenant
- **Audit Logging**: All actions tracked with user/tenant context

## 🎨 UI Design

### Color Palette
- **Primary (Indigo)**: `#6366f1` - Main actions, navigation, branding
- **Neutral Gray**: `#737373` - Text, borders, backgrounds
- **Accent (Emerald)**: `#10b981` - Success states, highlights, live indicators

### Design Principles
- **Clean & Modern**: Inspired by enterprise SaaS platforms
- **Consistent Spacing**: Tailwind's spacing scale (multiples of 4px)
- **Responsive**: Mobile-first design with responsive breakpoints
- **Accessible**: WCAG 2.1 AA compliant colors and contrast
- **Production-Grade**: No common templates, unique, professional design

## 📊 API Endpoints

### Authentication
```
POST   /api/auth/signup       - Create account
POST   /api/auth/login        - Login and get JWT
GET    /api/auth/me           - Get current user
GET    /api/tenant            - Get tenant info
GET    /api/tenant/users      - List tenant users (admin)
POST   /api/tenant/invite     - Invite user (admin)
```

### Migrations (Protected)
```
POST   /api/migrations        - Create migration job
GET    /api/migrations        - List all jobs
GET    /api/migrations/{id}   - Get job details
POST   /api/migrations/{id}/retry - Retry failed chunks
```

### Analytics (Protected)
```
GET    /api/analytics/performance/realtime - Live metrics
GET    /api/analytics/throughput/{job_id}  - Per-table stats
GET    /api/analytics/workers/active       - Worker status
GET    /api/analytics/usage                - Usage statistics
```

## 🐳 Docker Services

### API Service
- **Port**: 8000
- **Container**: `migration-api`
- **Dependencies**: PostgreSQL, Redis
- **Healthcheck**: `/health` endpoint

### Worker Service
- **Scalable**: Use `docker-compose up --scale worker=N`
- **Container**: `migration-worker-1`, `migration-worker-2`, etc.
- **Dependencies**: PostgreSQL, Redis, Source/Target MySQL

### UI Service
- **Port**: 3000
- **Container**: `migration-ui`
- **Build**: Vite production build served by Nginx

### Infrastructure
- **PostgreSQL**: Port 5432, metadata storage
- **Redis**: Port 6379, job queue

## 🔧 Development

### Run API Locally
```bash
cd services/api
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Run Worker Locally
```bash
cd services/worker
pip install -r requirements.txt
python worker.py
```

### Run UI Locally
```bash
cd ui
npm install
npm run dev
```

### Run Tests (Future)
```bash
pytest services/api/tests/
pytest services/worker/tests/
```

## 🚀 Production Deployment

### Environment Variables
Set all required environment variables (see `.env` section above).

### Scaling Workers
```bash
docker-compose up -d --scale worker=10
```

### Database Backups
```bash
# Backup PostgreSQL metadata
docker exec migration-postgres pg_dump -U migration_user migration_metadata > backup.sql

# Restore
cat backup.sql | docker exec -i migration-postgres psql -U migration_user migration_metadata
```

### Monitoring
- **Logs**: `docker-compose logs -f api worker`
- **Worker Status**: Performance Dashboard > Worker Pool Status
- **Metrics**: Analytics API endpoints for custom monitoring integrations

## 📝 Phases & Roadmap

### ✅ Phase 1: Core Migration Engine
- Chunk-based processing
- Worker pool with Redis queue
- Metadata tracking
- Resumability & retry logic
- Docker Compose orchestration

### ✅ Phase 2: Operational UI
- Job creation & management
- Real-time job/table/chunk status
- Retry buttons for failed chunks
- Table-level detail views

### ✅ Phase 3: Performance Monitoring
- Real-time throughput charts
- Worker status dashboard
- Queue depth visualization
- Historical performance metrics

### ✅ Phase 4: SaaS Features
- JWT authentication & authorization
- Multi-tenancy with tenant isolation
- Team management & user invitations
- Role-based access control

### ✅ Phase 5: Enterprise Analytics
- Usage tracking for billing
- Daily/weekly/monthly activity reports
- Advanced monitoring dashboards
- Tenant-level analytics

### 🔮 Phase 6: Advanced Features (Future)
- WebSocket for real-time UI updates (eliminate polling)
- Schema diff visualization
- Multi-region support
- Custom alerting & notifications
- Slack/Teams integrations
- Advanced retry strategies
- Data validation rules
- Automated rollback mechanisms

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License.

## 💬 Support

For issues, questions, or feature requests:
- Open an issue on GitHub
- Contact: support@migration-platform.com

## 🙏 Acknowledgments

- Built with FastAPI, React, and PostgreSQL
- UI inspired by modern SaaS platforms
- Icons from Lucide
- Charts powered by Recharts

---

**Migration Platform v1.0** | Enterprise Database Migration Made Simple

