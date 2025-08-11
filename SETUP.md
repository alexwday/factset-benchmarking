# Setup Guide - FactSet Fundamentals Benchmarking

## Quick Setup (Recommended)

### macOS/Linux:
```bash
# Clone and enter the repository
git clone https://github.com/alexwday/factset-benchmarking.git
cd factset-benchmarking

# Run the setup script
chmod +x setup.sh
./setup.sh

# Edit your credentials
nano .env  # Add your FactSet API credentials

# Run the analysis
source venv/bin/activate
python analyze_fundamentals_final.py
```

### Windows:
```cmd
# Clone and enter the repository
git clone https://github.com/alexwday/factset-benchmarking.git
cd factset-benchmarking

# Run the setup script
setup.bat

# Edit your credentials in .env file

# Run the analysis
venv\Scripts\activate
python analyze_fundamentals_final.py
```

## Manual Setup

If you encounter the setuptools error or prefer manual setup:

### 1. Create and activate virtual environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate it
# macOS/Linux:
source venv/bin/activate
# Windows:
# venv\Scripts\activate
```

### 2. Fix setuptools issue first
```bash
# IMPORTANT: Upgrade pip and setuptools BEFORE installing other packages
pip install --upgrade pip setuptools wheel
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
# Copy example file
cp .env.example .env

# Edit with your credentials
nano .env  # or any text editor
```

Required in `.env`:
- `API_USERNAME` - Your FactSet username
- `API_PASSWORD` - Your FactSet API password

Optional:
- Proxy settings (if behind corporate firewall)
- SSL certificate path (if custom cert required)

### 5. (Optional) Add SSL Certificate
```bash
mkdir -p certs
# Copy your .cer file to certs/ directory
cp /path/to/certificate.cer certs/
```

### 6. Run the analysis
```bash
python analyze_fundamentals_final.py
```

## Troubleshooting

### "Cannot import 'setuptools.build_meta'" Error
This happens when setuptools is missing or outdated. Fix:
```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### SSL Certificate Errors
If you see SSL errors, you likely need your organization's certificate:
1. Get your organization's .cer file
2. Place it in `certs/` directory
3. Update `.env` with: `SSL_CERT_PATH=certs/your-cert.cer`

### Proxy Authentication Errors
For corporate proxies, ensure all these are set in `.env`:
```
PROXY_USER=your_username
PROXY_PASSWORD=your_password
PROXY_URL=proxy.company.com:8080
PROXY_DOMAIN=YOUR_DOMAIN
```

### FactSet API Authentication Failed
- Verify your username/password are correct
- Check if your account has API access enabled
- Ensure you're using API credentials, not web login credentials

### Rate Limiting Messages
Normal behavior - the script respects FactSet's 10 req/sec limit and will pause as needed.

## Expected Output

After successful run, check `output/` directory for:

1. **fundamentals_matrix_YYYYMMDD_HHMMSS.xlsx**
   - Complete matrix of all metrics vs all 91 banks
   - Shows quarter counts for each metric-bank combination
   
2. **factset_metrics_catalog.json**
   - All available FactSet Fundamentals metrics with descriptions
   
3. **ANALYSIS_SUMMARY.txt**
   - Human-readable summary of coverage statistics