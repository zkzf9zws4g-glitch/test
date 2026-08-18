# Verification Report

5 rows randomly selected (seed=42) from `edgar_sample.csv` (50 total rows), each re-fetched live from its `source_filing_url` and independently re-parsed, to confirm the CSV matches the live filing.

**Overall: ALL 5 ROWS PASSED**

## Row 1: Kraft Robert O. / Hillman Solutions Corp. (HLMN)
- URL: https://www.sec.gov/Archives/edgar/data/1558592/000182249222000022/wf-form4_164667748616049.xml
- HTTP status: 200
- transaction_code: CSV=P, live=P -- OK
- shares: CSV=47500.0, live=47500 -- OK
- price_per_share: CSV=10.353, live=10.353 -- OK
- **PASS**

## Row 2: TOTAL S.A. / SUNPOWER CORP (SPWR)
- URL: https://www.sec.gov/Archives/edgar/data/879764/000089924320006386/doc4.xml
- HTTP status: 200
- transaction_code: CSV=P, live=P -- OK
- shares: CSV=258662.0, live=258662 -- OK
- price_per_share: CSV=8.4658, live=8.4658 -- OK
- **PASS**

## Row 3: TOTAL S.A. / SUNPOWER CORP (SPWR)
- URL: https://www.sec.gov/Archives/edgar/data/879764/000089924320006386/doc4.xml
- HTTP status: 200
- transaction_code: CSV=P, live=P -- OK
- shares: CSV=81235.0, live=81235 -- OK
- price_per_share: CSV=8.8566, live=8.8566 -- OK
- **PASS**

## Row 4: LIVEK WILLIAM PAUL / COMSCORE, INC. (SCOR)
- URL: https://www.sec.gov/Archives/edgar/data/1466605/000115817222000015/wf-form4_164668987831148.xml
- HTTP status: 200
- transaction_code: CSV=P, live=P -- OK
- shares: CSV=200000.0, live=200000 -- OK
- price_per_share: CSV=2.67, live=2.67 -- OK
- **PASS**

## Row 5: Hanrahan Daniel J / CEDAR FAIR L P (FUN)
- URL: https://www.sec.gov/Archives/edgar/data/1319154/000081153220000052/wf-form4_158292724283533.xml
- HTTP status: 200
- transaction_code: CSV=P, live=P -- OK
- shares: CSV=11250.0, live=11250 -- OK
- price_per_share: CSV=44.36, live=44.36 -- OK
- **PASS**
