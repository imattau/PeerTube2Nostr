# PeerTube2Nostr Web Dashboard 🚀

A modular, cutting-edge, mobile-first full-stack web application designed to manage the publishing of PeerTube videos to the Nostr network. This application replaces the legacy CLI with a professional management suite.

![Vue 3](https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![Tailwind CSS](https://img.shields.io/badge/Tailwind_CSS-38B2AC?style=for-the-badge&logo=tailwind-css&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

## ✨ Features

- **📱 Mobile-First Design**: Fully responsive dark-themed UI built with Vue 3 and Tailwind CSS.
- **📊 Rich Dashboard**: Real-time stats, visual pending queue with thumbnails, and system logs.
- **🛠 Modular Backend**: Clean separation of concerns (Database, PeerTube API, Nostr, Background Runner).
- **📡 Relay Health**: Real-time latency monitoring for all configured Nostr relays.
- **🔒 Security**: API Key protection and secure local storage for credentials.
- **⚙️ Background Runner**: Automated polling and rate-limited publishing.

---

## 🏗 Architecture

- **Frontend**: Vue 3 (Composition API), Vite, Pinia (State), Axios, Lucide Icons.
- **Backend**: FastAPI (Python 3.11), Uvicorn, SQLite (WAL Mode).
- **Deployment**: Docker Compose (Nginx for frontend, Python for backend).

---

## 🚀 Installation & Setup

### Option 1: Docker (Recommended)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-repo/PeerTube2Nostr.git
   cd PeerTube2Nostr/webapp
   ```

2. **Configure Environment**:
   Create a `.env` file or export variables:
   ```bash
   export API_KEY=your_secure_password
   export NOSTR_NSEC=nsec1...
   ```

3. **Launch**:
   ```bash
   docker-compose up --build -d
   ```
   The dashboard will be available at `http://localhost:3000`.

### Option 2: Manual Development Setup

#### Backend
```bash
cd webapp/backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export API_KEY=your_key
uvicorn main:app --reload
```

#### Frontend
```bash
cd webapp/frontend
npm install
npm run dev
```

---

## 📖 Usage Guide

### 1. Initial Setup
- **API Key**: If an `API_KEY` is set on the backend, click the **Key icon** in the Logs section of the UI to enter your key.
- **NSEC**: Go to the **Credentials** section and click "Update NSEC" to provide your Nostr private key.

### 2. Managing Sources
- Click **"Add Source"** and provide a PeerTube Channel URL (e.g., `https://example.tube/c/mychannel`) or an RSS feed URL.
- The system will automatically attempt to identify the ingest type.

### 3. Monitoring Relays
- Add relays (e.g., `wss://relay.damus.io`).
- The UI will display the **Latency (ms)**. Relays showing high latency or errors will be automatically deprioritized.

### 4. Background Runner
- Use the **Play/Square** icons in the header to start or stop the background polling process.
- View upcoming posts in the **Pending Queue** with full visual thumbnails.

---

## 🛡 Security Note

- **API Security**: The `X-API-Key` header is required for all state-changing operations if an `API_KEY` is configured in the environment.
- **Local Storage**: Your API Key is stored in your browser's local storage for convenience.
- **NSEC Storage**: NSECs are stored on the server filesystem with `0600` permissions (Owner Read/Write only).

---

## 🛠 Troubleshooting

- **Logs**: Check the "System Logs" section at the bottom of the dashboard for real-time error reporting.
- **CORS Issues**: Ensure the frontend port (default 3000) is allowed in the backend's middleware configuration.
- **Database Lock**: The app uses SQLite WAL mode to handle concurrent web and runner access. If you see "Database is locked", restart the backend service.
