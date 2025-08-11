"""
Monitored Financial Institutions Configuration
Total: 91 institutions across multiple categories
"""

monitored_institutions = {
    # Canadian Banks (7)
    'RY-CA': {'name': 'Royal Bank of Canada', 'type': 'Canadian_Banks'},
    'BMO-CA': {'name': 'Bank of Montreal', 'type': 'Canadian_Banks'},
    'CM-CA': {'name': 'Canadian Imperial Bank of Commerce', 'type': 'Canadian_Banks'},
    'NA-CA': {'name': 'National Bank of Canada', 'type': 'Canadian_Banks'},
    'BNS-CA': {'name': 'Bank of Nova Scotia', 'type': 'Canadian_Banks'},
    'TD-CA': {'name': 'Toronto-Dominion Bank', 'type': 'Canadian_Banks'},
    'LB-CA': {'name': 'Laurentian Bank', 'type': 'Canadian_Banks'},
    
    # U.S. Banks (7)
    'JPM-US': {'name': 'JPMorgan Chase & Co.', 'type': 'US_Banks'},
    'BAC-US': {'name': 'Bank of America Corporation', 'type': 'US_Banks'},
    'WFC-US': {'name': 'Wells Fargo & Company', 'type': 'US_Banks'},
    'C-US': {'name': 'Citigroup Inc.', 'type': 'US_Banks'},
    'MS-US': {'name': 'Morgan Stanley', 'type': 'US_Banks'},
    'GS-US': {'name': 'Goldman Sachs Group Inc.', 'type': 'US_Banks'},
    'JEF': {'name': 'Jefferies Financial Group Inc.', 'type': 'US_Banks'},
    
    # European Banks (14)
    'UBS': {'name': 'UBS Group AG', 'type': 'European_Banks'},
    'BCS': {'name': 'Barclays PLC', 'type': 'European_Banks'},
    'DBK-DE': {'name': 'Deutsche Bank AG', 'type': 'European_Banks'},
    'GLE-FR': {'name': 'Societe Generale', 'type': 'European_Banks'},
    'BNP-FR': {'name': 'BNP Paribas', 'type': 'European_Banks'},
    'BBVA-ES': {'name': 'Banco Bilbao Vizcaya Argentaria S.A.', 'type': 'European_Banks'},
    'SAN-ES': {'name': 'Banco Santander S.A.', 'type': 'European_Banks'},
    'HSBA-GB': {'name': 'HSBC Holdings plc', 'type': 'European_Banks'},
    'LLOY-GB': {'name': 'Lloyds Banking Group plc', 'type': 'European_Banks'},
    'ING': {'name': 'ING Groep N.V.', 'type': 'European_Banks'},
    'STAN-GB': {'name': 'Standard Chartered PLC', 'type': 'European_Banks'},
    'RBS-GB': {'name': 'NatWest Group plc', 'type': 'European_Banks'},
    'UCG-IT': {'name': 'UniCredit S.p.A.', 'type': 'European_Banks'},
    'ISP-IT': {'name': 'Intesa Sanpaolo', 'type': 'European_Banks'},
    
    # U.S. Boutiques (9)
    'LAZ-US': {'name': 'Lazard Ltd', 'type': 'US_Boutiques'},
    'EVR-US': {'name': 'Evercore Inc', 'type': 'US_Boutiques'},
    'HLI-US': {'name': 'Houlihan Lokey Inc', 'type': 'US_Boutiques'},
    'MC-US': {'name': 'Moelis & Company', 'type': 'US_Boutiques'},
    'PIPR-US': {'name': 'Piper Sandler Companies', 'type': 'US_Boutiques'},
    'PJT-US': {'name': 'PJT Partners Inc', 'type': 'US_Boutiques'},
    'SF-US': {'name': 'Stifel Financial Corp', 'type': 'US_Boutiques'},
    'RJF-US': {'name': 'Raymond James Financial Inc.', 'type': 'US_Boutiques'},
    'CF-CA': {'name': 'Canaccord Genuity Group Inc', 'type': 'US_Boutiques'},
    
    # Canadian Asset Managers (4)
    'CIX-CA': {'name': 'CI Financial Corp', 'type': 'Canadian_Asset_Managers'},
    'AGF.B-CA': {'name': 'AGF Management Limited', 'type': 'Canadian_Asset_Managers'},
    'IGM-CA': {'name': 'IGM Financial Inc.', 'type': 'Canadian_Asset_Managers'},
    'FSZ-CA': {'name': 'Fiera Capital Corporation', 'type': 'Canadian_Asset_Managers'},
    
    # U.S. Regionals (15)
    'TFC-US': {'name': 'Truist Financial Corporation', 'type': 'US_Regionals'},
    'USB-US': {'name': 'U.S. Bancorp', 'type': 'US_Regionals'},
    'PNC-US': {'name': 'PNC Financial Services Group', 'type': 'US_Regionals'},
    'RF-US': {'name': 'Regions Financial Corporation', 'type': 'US_Regionals'},
    'FITB-US': {'name': 'Fifth Third Bancorp', 'type': 'US_Regionals'},
    'MTB-US': {'name': 'M&T Bank Corporation', 'type': 'US_Regionals'},
    'HBAN-US': {'name': 'Huntington Bancshares Incorporated', 'type': 'US_Regionals'},
    'KEY-US': {'name': 'KeyCorp', 'type': 'US_Regionals'},
    'WAL-US': {'name': 'Western Alliance Bancorporation', 'type': 'US_Regionals'},
    'FLG-US': {'name': 'Flagstar Financial Inc.', 'type': 'US_Regionals'},
    'ALLY-US': {'name': 'Ally Financial Inc.', 'type': 'US_Regionals'},
    'FHN-US': {'name': 'First Horizon Corporation', 'type': 'US_Regionals'},
    'CMA-US': {'name': 'Comerica Incorporated', 'type': 'US_Regionals'},
    'CFG-US': {'name': 'Citizens Financial Group Inc.', 'type': 'US_Regionals'},
    'SNV-US': {'name': 'Synovus Financial Corp.', 'type': 'US_Regionals'},
    
    # U.S. Wealth & Asset Managers (10)
    'SCHW': {'name': 'Charles Schwab Corporation', 'type': 'US_Wealth_Asset_Managers'},
    'BLK-US': {'name': 'BlackRock Inc.', 'type': 'US_Wealth_Asset_Managers'},
    'BEN-US': {'name': 'Franklin Resources Inc.', 'type': 'US_Wealth_Asset_Managers'},
    'IVZ-US': {'name': 'Invesco Ltd.', 'type': 'US_Wealth_Asset_Managers'},
    'AMG-US': {'name': 'Affiliated Managers Group Inc.', 'type': 'US_Wealth_Asset_Managers'},
    'AMP-US': {'name': 'Ameriprise Financial Inc.', 'type': 'US_Wealth_Asset_Managers'},
    'AB-US': {'name': 'AllianceBernstein Holding LP', 'type': 'US_Wealth_Asset_Managers'},
    'FHI-US': {'name': 'Federated Hermes Inc.', 'type': 'US_Wealth_Asset_Managers'},
    'JHG-US': {'name': 'Janus Henderson Group plc', 'type': 'US_Wealth_Asset_Managers'},
    'TROW-US': {'name': 'T. Rowe Price Group Inc.', 'type': 'US_Wealth_Asset_Managers'},
    
    # UK Wealth & Asset Managers (3)
    'RAT-GB': {'name': 'Rathbones Group Plc', 'type': 'UK_Wealth_Asset_Managers'},
    'SJP-GB': {'name': "St. James's Place plc", 'type': 'UK_Wealth_Asset_Managers'},
    'QLT-GB': {'name': 'Quilter plc', 'type': 'UK_Wealth_Asset_Managers'},
    
    # Nordic Banks (4)
    'DANSKE-DK': {'name': 'Danske Bank A/S', 'type': 'Nordic_Banks'},
    'NDA-FI': {'name': 'Nordea Bank Abp', 'type': 'Nordic_Banks'},
    'SWEDA-SE': {'name': 'Swedbank AB', 'type': 'Nordic_Banks'},
    'SEBA-SE': {'name': 'Skandinaviska Enskilda Banken AB', 'type': 'Nordic_Banks'},
    
    # Canadian Insurers (6)
    'MFC-CA': {'name': 'Manulife Financial Corporation', 'type': 'Canadian_Insurers'},
    'SLF-CA': {'name': 'Sun Life Financial Inc.', 'type': 'Canadian_Insurers'},
    'IAG-CA': {'name': 'iA Financial Corporation Inc.', 'type': 'Canadian_Insurers'},
    'GWO-CA': {'name': 'Great-West Lifeco Inc.', 'type': 'Canadian_Insurers'},
    'POW-CA': {'name': 'Power Corporation of Canada', 'type': 'Canadian_Insurers'},
    'IFC-CA': {'name': 'Intact Financial Corporation', 'type': 'Canadian_Insurers'},
    
    # Canadian Monoline Lenders (4)
    'EQB-CA': {'name': 'Equitable Group Inc.', 'type': 'Canadian_Monoline_Lenders'},
    'FN-CA': {'name': 'First National Financial Corp.', 'type': 'Canadian_Monoline_Lenders'},
    'MKP-CA': {'name': 'MCAN Mortgage Corporation', 'type': 'Canadian_Monoline_Lenders'},
    'GSY-CA': {'name': 'goeasy Ltd.', 'type': 'Canadian_Monoline_Lenders'},
    
    # Australian Banks (5)
    'WBC-AU': {'name': 'Westpac Banking Corporation', 'type': 'Australian_Banks'},
    'CBA-AU': {'name': 'Commonwealth Bank of Australia', 'type': 'Australian_Banks'},
    'ANZ-AU': {'name': 'ANZ Group Holdings Limited', 'type': 'Australian_Banks'},
    'NAB-AU': {'name': 'National Australia Bank Limited', 'type': 'Australian_Banks'},
    'BOQ-AU': {'name': 'Bank of Queensland Limited', 'type': 'Australian_Banks'},
    
    # Trusts (3)
    'BK-US': {'name': 'Bank of New York Mellon Corporation', 'type': 'Trusts'},
    'STT-US': {'name': 'State Street Corporation', 'type': 'Trusts'},
    'NTRS-US': {'name': 'Northern Trust Corporation', 'type': 'Trusts'}
}