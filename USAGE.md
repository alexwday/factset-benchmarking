# FactSet Fundamentals API Client

## Overview
This project contains a consolidated Python script (`factset_fundamentals.py`) for fetching and analyzing financial fundamentals data from the FactSet API for 91 monitored financial institutions.

## Script Structure

### Main Script: `factset_fundamentals.py`
- **Purpose**: Fetch and analyze fundamentals data for banks and financial institutions
- **Features**:
  - Automatic ticker format detection
  - Support for multiple exchange suffixes (US, CA, GB, etc.)
  - Bank-specific metrics (Tier 1 ratios, Net Interest Margin, etc.)
  - CSV and JSON output formats
  - Comprehensive error handling and logging

### Configuration: `config/banks_config.py`
- Contains 91 monitored institutions across categories:
  - Canadian Banks (7)
  - U.S. Banks (7)
  - European Banks (14)
  - U.S. Boutiques (9)
  - Canadian Asset Managers (4)
  - U.S. Regionals (15)
  - U.S. Wealth & Asset Managers (10)
  - UK Wealth & Asset Managers (3)
  - Nordic Banks (4)
  - Canadian Insurers (6)
  - Canadian Monoline Lenders (4)
  - Australian Banks (5)
  - Trusts (3)

## Setup

### 1. Environment Variables
Create a `.env` file in the project root with your FactSet credentials:
```bash
API_USERNAME=your_username
API_PASSWORD=your_password

# SSL Certificate (optional - for custom corporate certificates)
SSL_CERT_PATH=/path/to/certificate.cer

# Proxy settings (required if behind corporate firewall)
PROXY_URL=proxy.company.com:8080
PROXY_USER=your_proxy_user
PROXY_PASSWORD=your_proxy_password
PROXY_DOMAIN=DOMAIN
USE_PROXY=true  # Set to false to disable proxy
```

### 2. Virtual Environment
The project includes a pre-configured virtual environment. Activate it:
```bash
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

### 3. Dependencies
All required packages are already installed in the virtual environment:
- `fds.sdk.FactSetFundamentals` - FactSet SDK
- `pandas` - Data analysis
- `python-dotenv` - Environment variable management

## Usage

### Basic Usage
```bash
# Activate virtual environment
source venv/bin/activate

# Run the analysis
python factset_fundamentals.py
```

### What the Script Does
1. **Tests API Connection** - Validates credentials and connectivity
2. **Fetches Available Metrics** - Gets list of supported financial metrics
3. **Analyzes All Banks** - Processes each institution in the config
4. **Generates Output Files**:
   - `bank_fundamentals.json` - Detailed JSON results
   - `bank_fundamentals.csv` - Tabular data for analysis
   - `factset_fundamentals.log` - Execution log

### Output Format

#### JSON Output (`bank_fundamentals.json`)
```json
{
  "JPM-US": {
    "name": "JPMorgan Chase & Co.",
    "type": "US_Banks",
    "working_ticker": "JPM-US",
    "data": [
      {
        "ticker": "JPM-US",
        "metric": "FF_SALES",
        "value": 123456.78,
        "date": "2024-12-31",
        "fiscal_period": "FY",
        "fiscal_year": 2024
      }
    ],
    "timestamp": "2025-08-11T15:30:00"
  }
}
```

#### CSV Output (`bank_fundamentals.csv`)
Contains flattened data with columns:
- ticker
- name
- type
- metric
- value
- date
- fiscal_period
- fiscal_year

## Key Metrics

### Standard Financial Metrics
- `FF_SALES` - Revenue
- `FF_NET_INC` - Net Income
- `FF_EPS_BASIC` - Basic Earnings per Share
- `FF_ASSETS` - Total Assets
- `FF_LIAB` - Total Liabilities
- `FF_EQUITY` - Total Equity
- `FF_OPER_INC` - Operating Income
- `FF_EBIT` - EBIT
- `FF_EBITDA` - EBITDA
- `FF_CASH` - Cash and Cash Equivalents
- `FF_DIV_PER_SHR` - Dividends per Share
- `FF_BK_VAL_PER_SHR` - Book Value per Share
- `FF_OPER_CASH_FLOW` - Operating Cash Flow
- `FF_FREE_CASH_FLOW` - Free Cash Flow

### Bank-Specific Metrics
- `FF_INT_INCOME` - Interest Income
- `FF_INT_EXP` - Interest Expense
- `FF_NET_INT_INC` - Net Interest Income
- `FF_LOAN_LOSS_PROV` - Loan Loss Provision
- `FF_LOANS` - Total Loans
- `FF_DEPOSITS` - Total Deposits
- `FF_TIER1_CAP_RATIO` - Tier 1 Capital Ratio
- `FF_TOT_CAP_RATIO` - Total Capital Ratio

## Troubleshooting

### Authentication Failed (401 Error)
- Verify your API credentials in the `.env` file
- Ensure your FactSet account has access to the Fundamentals API
- Check if your credentials require a specific authentication method

### Proxy Issues
- If behind a corporate firewall, set `USE_PROXY=true` in `.env`
- Verify proxy settings are correct
- Try without proxy if on a direct connection

### No Valid Tickers
The script automatically tries multiple ticker formats:
- Plain ticker (e.g., `JPM`)
- With US suffix (e.g., `JPM-US`)
- With country suffix (e.g., `RY-CA`)
- With exchange suffix (e.g., `JPM-NYSE`)

If still failing, check the FactSet documentation for the correct ticker format for your target companies.

### Rate Limiting
The script includes a 0.5-second delay between requests to avoid rate limiting. If you encounter rate limit errors, increase the delay in the `analyze_banks()` method.

## Files Cleaned Up
The following redundant scripts have been removed:
- `analyze_fundamentals.py` (and all variants)
- `diagnostics/` folder with test scripts

All functionality has been consolidated into the single `factset_fundamentals.py` script for easier maintenance.

## Next Steps
1. Set up your FactSet API credentials in `.env`
2. Run the script to test connectivity
3. Review the output files for data quality
4. Adjust the ticker formats in `config/banks_config.py` if needed
5. Add or remove institutions as required for your analysis