# Input Folder

Place required files here:

## SSL Certificate
- **File**: `rbc-ca-bundle.cer`
- **Description**: RBC SSL certificate bundle for FactSet API connections
- **Required**: Yes, for API authentication

## How to obtain the certificate:
1. Download from your NAS share at: `Finance Data and Analytics/DSA/Earnings Call Transcripts/Inputs/Certificate/rbc-ca-bundle.cer`
2. Place it in this folder as `rbc-ca-bundle.cer`

## Alternative locations:
If you prefer, you can also place the certificate at:
- Project root directory: `./rbc-ca-bundle.cer`
- Custom path via environment variable: `SSL_CERT_PATH=/path/to/certificate.cer`