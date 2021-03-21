# Stock-Fundamentals-Database
Pulls current stock fundamental ratios from Finviz and stores in a specified database.
    
    Ratios pulled from Finviz:
       Market_cap
       P/B
       P/E
       Forward_P/E
       PEG
       Debt_Eq
       EPS_ttm
       Dividend_pct
       ROE
       ROI
       EPS_QoQ
       Insider_own


*Requirements* 
pandas, requests, time, datetime, contextlib, sqlite3, os, BeautifulSoup, sqlite3


*Required prep BEFORE running program*
&nbsp;&nbsp;&nbsp;1. Specify desired stocks and fundamental ratios
&nbsp;&nbsp;&nbsp;2. Insert file path for database and csv
&nbsp;&nbsp;&nbsp;3. Rename database and/or csv file if desired
