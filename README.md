# FactSet Fundamentals API Benchmarking

Python-based analysis tool for FactSet Fundamentals API data covering 91 monitored financial institutions.

## Purpose

This project provides comprehensive analysis of:
- All available FactSet Fundamentals line items and their descriptions
- Data coverage across 91 monitored banks
- Quarterly fundamentals availability from 2018 to present
- Latest available data for each institution

## Setup

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure environment variables in `.env`:**
```env
# FactSet API Configuration
API_USERNAME=your_factset_username
API_PASSWORD=your_factset_api_password

# Proxy Configuration (if required)
PROXY_USER=your_proxy_username
PROXY_PASSWORD=your_proxy_password
PROXY_URL=oproxy.fg.rbc.com:8080
PROXY_DOMAIN=MAPLE

# SSL Certificate Path
SSL_CERT_PATH=certs/rbc-ca-bundle.cer
```

3. **Add SSL certificate (if required):**
   - Create `certs/` directory
   - Place `rbc-ca-bundle.cer` file in the directory

## Usage

Run the analysis:
```bash
python analyze_fundamentals.py
```

## Output Files

The script generates multiple output files in the `output/` directory:

### 1. Metrics Catalog (`factset_metrics_catalog.json`)
- Complete list of all FactSet Fundamentals metrics
- Organized by category (Income Statement, Balance Sheet, etc.)
- Includes metric IDs, names, and descriptions

### 2. Analysis Report (`fundamentals_analysis_YYYYMMDD_HHMMSS.json`)
- Detailed JSON with all analysis results
- Bank-by-bank data availability
- Date ranges and metric coverage

### 3. Excel Report (`fundamentals_analysis_YYYYMMDD_HHMMSS.xlsx`)
- **Coverage Summary**: Statistics by institution type
- **Bank Details**: Individual bank data availability
- **Metrics Catalog**: First 1000 metrics with descriptions

### 4. Summary Report (`ANALYSIS_SUMMARY.txt`)
- Human-readable summary of findings
- Coverage percentages
- List of banks without data

## Monitored Institutions (91 Total)

| Category | Count |
|----------|-------|
| Canadian Banks | 7 |
| U.S. Banks | 7 |
| European Banks | 14 |
| U.S. Boutiques | 9 |
| Canadian Asset Managers | 4 |
| U.S. Regionals | 15 |
| U.S. Wealth & Asset Managers | 10 |
| UK Wealth & Asset Managers | 3 |
| Nordic Banks | 4 |
| Canadian Insurers | 6 |
| Canadian Monoline Lenders | 4 |
| Australian Banks | 5 |
| Trusts | 3 |

## Key Metrics Analyzed

- **FF_SALES**: Revenue
- **FF_NET_INC**: Net Income
- **FF_EPS**: Earnings Per Share
- **FF_ASSETS**: Total Assets
- **FF_LIAB**: Total Liabilities
- **FF_EQUITY**: Total Equity
- **FF_ROE**: Return on Equity
- **FF_ROA**: Return on Assets
- **FF_BPS**: Book Value Per Share
- **FF_DIV_YLD**: Dividend Yield
- **FF_NIM**: Net Interest Margin
- **FF_TIER1_RATIO**: Tier 1 Capital Ratio

## Project Structure

```
factset-benchmarking/
├── analyze_fundamentals.py     # Main analysis script
├── config/
│   └── banks_config.py         # 91 monitored institutions
├── output/                     # Generated reports
├── certs/                      # SSL certificates
├── requirements.txt            # Python dependencies
├── .env                        # Environment configuration
└── README.md                   # This file
```

## Features

- **Proxy Support**: Configured for corporate proxy with NTLM authentication
- **SSL Certificate**: Supports custom CA bundles
- **Rate Limiting**: Respects FactSet API limits (10 req/sec)
- **Progress Tracking**: Visual progress bars during analysis
- **Error Handling**: Comprehensive logging and error recovery
- **Batch Processing**: Efficient processing of multiple banks

## Logging

Logs are written to:
- Console output (INFO level)
- `fundamentals_analysis.log` file (detailed logs)

## Notes

- The script analyzes quarterly (QTR) data from 2018 to present
- API rate limits are automatically handled
- Missing data for specific banks is tracked and reported
- All 91 institutions are processed regardless of data availability