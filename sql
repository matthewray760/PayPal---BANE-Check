import pyodbc as py
import pandas as pd
import datetime
import tkinter as tk







def execute_query_bane(security_ids):
        # Set up the database connection parameters
        server = 'PROD-SQL-RO'
        database = 'LM'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207*'
        driver = '{ODBC Driver 17 for SQL Server}'
        MultiSubnetFailover=True
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''

        select securityid, calcTypeCode from baneData
	        where securityid IN ({security_ids})
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df



def execute_query_sc(Date):
        # Set up the database connection parameters
        server = 'PROD-SQL-RO'
        database = 'LM'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207*'
        driver = '{ODBC Driver 17 for SQL Server}'
        MultiSubnetFailover=True
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
                DECLARE @UltimateParentClientID INT = 43296
                DECLARE @PriorBusinessDay DATE = '{Date}';

                ; WITH 
                AccountList AS 
                (
                SELECT ag.ultimateParentClientID
                , a.ID [AccountID] 
                FROM LM.dbo.clientParentRelationships ag
                    JOIN LM.dbo.Accounts a ON a.ClientID = ag.clientID
                WHERE ag.ultimateParentClientID = @UltimateParentClientID
                    AND a.Active = 'y'
                    AND a.Reconcile = 'y'
                    AND a.Demo = 'n'
                    AND a.StableDate IS NOT NULL
                    --AND a.SystemTrial = 0
                )
                , SecurityHoldings AS 
                (
                SELECT DISTINCT al.ultimateParentClientID, al.AccountID, l.SecurityID
                FROM AccountList al
                    JOIN LM.dbo.Lots l ON al.AccountID = l.AccountID
                WHERE l.EntryDate < GETDATE() AND (l.ExitDate >= GETDATE() OR l.ExitDate IS NULL)
                    AND EXISTS (SELECT 1 
                                FROM SecurityData.dbo.SecurityMaster sm
                                WHERE sm.ID = l.SecurityID
                                AND sm.tsCreated >= @PriorBusinessDay)
                )

                SELECT DISTINCT CONVERT(DATE,sm.tsCreated) as tsCreated
                , sm.ID [SecurityID] 
                , sa.AccountID
                , sm.Active
                , sm.TypeID
                , st.ShortName [SmType]
                , sm.CurrencyID
                , c.Code [Currency]
                , sm.Cusip
                , sm.ISIN
                , sm.Name
                , sm.detailedDescription
                , sm.Issuer
                , CONVERT(DATE,sm.IssueDate) as issue_date
                , CONVERT(DATE,sm.MaturityDate) as maturity_date
                , sm.CountryDomicile
                , sm.CountryOfIncorporation
                , sm.Coupon
                , sm.CpnFrequency
                , std.isFloatingRate
                , sm.FloaterSpread
                , sm.ReferenceIndex

                FROM SecurityHoldings sa
                    LEFT JOIN SecurityData.dbo.SecurityMaster sm ON sm.ID = sa.SecurityID
                    LEFT JOIN Securitydata.dbo.smTypes st ON sm.TypeID = st.ID
                    LEFT JOIN Securitydata.dbo.Currencies c ON sm.couponCurrencyId = c.ID
                    LEFT JOIN Securitydata.dbo.SecurityTypeData std ON std.securityId =sa.SecurityID
                --ORDER BY sm.tsCreated
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df
