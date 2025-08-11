# FactSet Fundamentals v2 API Examples

This script demonstrates how to use the FactSet Fundamentals v2 API with SSL certificate and proxy authentication methods suitable for internal corporate environments.

## Features

- SSL certificate configuration for secure API connections
- Proxy authentication with NTLM support
- Environment variable integration for credentials
- Configuration file support (config.yaml) for monitored institutions
- Multiple example use cases for fundamental data retrieval

## Prerequisites

1. Python 3.8 or higher
2. FactSet API credentials (username and password)
3. Corporate proxy credentials (if behind a proxy)
4. SSL certificate file (optional, for enhanced security)

## Installation

1. Install required packages:
```bash
pip install -r requirements_fundamentals.txt
```

2. Create a `.env` file in the project root with your credentials:
```env
# FactSet API Credentials
API_USERNAME=your_factset_username
API_PASSWORD=your_factset_password

# Proxy Configuration (if applicable)
PROXY_USER=your_proxy_username
PROXY_PASSWORD=your_proxy_password
PROXY_URL=proxy.company.com:8080
PROXY_DOMAIN=YOURDOMAIN
```

3. Ensure `config.yaml` exists in the `example/` folder (or root directory) with your monitored institutions list.

## Usage

Run the script from your local machine:
```bash
python factset_fundamentals_v2.py
```

The script will:
1. Validate environment variables
2. Load configuration from config.yaml
3. Set up SSL certificate (if available)
4. Configure proxy authentication
5. Initialize FactSet API client
6. Run example queries

## Examples Included

### Example 1: Fundamental Metrics
Retrieves key financial metrics like P/E ratio, Market Cap, Sales, EPS, etc.

### Example 2: Company Snapshot
Gets company profile information including name, industry, sector, employees, etc.

### Example 3: Financial Statements
Retrieves income statement, balance sheet, and cash flow data.

### Example 4: Bank-Specific Fundamentals
Uses the monitored institutions from config.yaml to get bank-specific metrics like:
- Net Interest Income
- Net Interest Margin
- Tier 1 Capital Ratio
- Loan Loss Provisions
- Non-Performing Loans Ratio
- Return on Assets/Equity

### Example 5: Financial Ratios and Multiples
Gets valuation multiples and financial ratios including:
- Valuation: P/E, P/B, P/S, EV/EBITDA
- Profitability: Gross/Operating/Net Margins
- Liquidity: Current/Quick Ratios
- Leverage: Debt-to-Equity, Interest Coverage

## Output

Results are saved to CSV files in the `factset_fundamentals_output/` directory:
- `fundamentals_metrics.csv` - Basic fundamental metrics
- `financial_statements.csv` - Financial statement data
- `banks_fundamentals.csv` - Bank-specific fundamentals
- `ratios_multiples.csv` - Financial ratios and valuation multiples

## Configuration

The script uses configuration from `config.yaml` which includes:
- SSL certificate path
- API settings (rate limiting, retries)
- Monitored institutions list with tickers and metadata

## Troubleshooting

1. **SSL Certificate Issues**: If you don't have an SSL certificate file, the script will proceed without it (less secure but functional).

2. **Proxy Authentication**: Ensure your proxy credentials are correct and the PROXY_DOMAIN matches your corporate domain.

3. **API Rate Limiting**: The script respects FactSet's rate limits. Adjust the delay settings in config.yaml if needed.

4. **Missing Data**: Some metrics may not be available for all companies. The script handles missing data gracefully.

## Security Notes

- Never commit the `.env` file to version control
- Store SSL certificates securely
- Use environment variables for all sensitive credentials
- The script sanitizes URLs in logs to prevent credential exposure

## Testing from Remote Environment

Since this is designed to be tested from your internal work computer:
1. Commit and push the script to your remote repository
2. Pull the changes on your work computer
3. Ensure the `.env` file is configured on the work machine
4. Run the script as described above

## Support

For FactSet API documentation, visit: https://developer.factset.com/api-catalog/factset-fundamentals-api

For issues with the script, check:
- Environment variables are set correctly
- Network connectivity to FactSet API
- Valid API credentials
- Proxy configuration (if applicable)