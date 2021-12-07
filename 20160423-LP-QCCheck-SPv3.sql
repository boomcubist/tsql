USE Cashless

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/************************ QCCheck ********************************/
-- 
/*****************************************************************/

IF EXISTS(SELECT * FROM sys.databases WHERE compatibility_level < 90)
	RAISERROR ('QCCheck cannot be installed when databases are still in 2000 compatibility mode.', 10,1) WITH LOG, NOWAIT;
GO

-- create table for logging 
    IF OBJECT_ID(N'Cashless.dbo.QCCheckErrorLog') IS NULL
	   EXEC ('
		  CREATE TABLE Cashless.dbo.QCCheckErrorLog
			 (
				CheckID smallint,
				CheckQuery varchar(max),
				CheckStartTime datetime,
				CheckEndTime datetime,
				ErrorLine varchar(10),
				ErrorMessage varchar(1000),
				CONSTRAINT PK_QCCheckLog PRIMARY KEY CLUSTERED (CheckID ASC, CheckStartTime ASC)
			 );
			 ')

-- index rebuild log table
    IF object_id(N'Cashless.dbo.QCIndexRebuildLog') IS NULL
	   EXEC ('
		  CREATE TABLE Cashless.dbo.QCIndexRebuildLog
			 (
				ID tinyint identity (1,1),
				RebuildStatement varchar(2000),
				RebuildStartTime datetime,
				RebuildEndTIme datetime,
				CONSTRAINT PK_QCIndexRebuildLog PRIMARY KEY CLUSTERED (ID ASC)
			 );
			 ')

 -- index use monitoring table
     IF object_id(N'Cashless.dbo.QCIndexRecord') IS NULL
	   EXEC ('
		  CREATE TABLE Cashless.dbo.QCIndexRecord
			 (
				ID int identity (1,1),
				IndexName varchar(2000),
				CONSTRAINT PK_QCIndexRecord PRIMARY KEY CLUSTERED (ID ASC)
			 );
			 ')

 -- application version table
     IF object_id(N'Cashless.dbo.QCApplicationVersion') IS NULL
	   EXEC ('
		  CREATE TABLE Cashless.dbo.QCApplicationVersion
			 (
				ID int identity (1,1),
				ApplicationName varchar(32),
				ApplicationVersion varchar(10)
				CONSTRAINT PK_QCApplicationVersion PRIMARY KEY CLUSTERED (ID ASC)
			 );
			 ')

-- drop the temporary table if it exists
    IF object_id(N'TempDb..##check') IS NOT NULL
    DROP TABLE ##check

    IF object_id('dbo.QCCheck') IS NULL
	   EXEC ('CREATE PROCEDURE dbo.QCCheck AS RETURN 0;')
    GO

ALTER PROCEDURE QCCheck 
	   @diagnose tinyint = 0,
	   @analyse tinyint = 0,
	   @saveresults tinyint = 0,
	   @resultslocation varchar(1000) = null,
	   @emailresults tinyint = 0,
	   @help tinyint = 0,
	   @rebuildindexes tinyint = 0,
	   @checkdb tinyint = 0,
	   @pathoverride tinyint = 0,
	   @trustepath varchar(1000) = null,
	   @terminalinfo tinyint = 0,
	   @activitymonitor tinyint = 0,
	   @capturevents tinyint = 0,
	   @timetomonitor varchar(10) = 0,
	   @captureindexuse tinyint = 0,
	   @advanced tinyint = 0,
	   @scriptversion varchar(2) = null OUTPUT,
	   @scriptdate varchar(15) = null OUTPUT
	   
AS
BEGIN

    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    
    SET @scriptversion = '1'
    SET @scriptdate = '20160505'

----------------------------------------------------------------------
-- 0. @help
----------------------------------------------------------------------

IF (@help) > 0 or ((@diagnose) = 0 and (@analyse) = 0 and (@rebuildindexes) = 0 and (@checkdb) = 0 and (@saveresults) = 0 and (@emailresults) = 0)
    BEGIN
       PRINT N' 
	   Version 1 - released on 05-05-2015
	    - Initial release
	
	   Parameters that can be passed:
    	   
		  @diagnose = 1 
			 runs through a number of different queries that have been designed to identify issues. the potential issue will be returned, along with a query statement that can be used for further investigatio.
		  
		  @analyse = 1  
			 use this option to find view configuration options for the SQL Server and Trust-e.
		  
		  @rebuildindexes = 1
			 analyse fragmentation of indexes with 500 pages or greater, re-organising those that have fragmentation between 10% and 30% and rebuilding those that are greater than 30%.
		  
		  @checkdb = 1
			 performs a DBCC CHECKDB on all databases.

		  @diagnose = 1, @saveresults = 1
			 save the returned result of the diagnostics query to a text file to the Trust-e folder.

		  @diagnose = 1, @saveresults = 1, @resultslocation = ''C:\Example\Location\''
			 change the default location for saving the results.

		  @diagnose = 1, @emailresults = 1
			 email the returned result of the diagnostics query to support@nrsltd.com.'

    END;

----------------------------------------------------------------------
-- end of help
----------------------------------------------------------------------

    IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

    IF object_id(N'TempDb..##truste') IS NOT NULL
	   DROP TABLE ##truste

    IF object_id(N'TempDb..##issues') IS NOT NULL 
	   DROP TABLE ##issues
		  CREATE TABLE ##issues
			 (
				CheckID int,
				CheckType varchar(100),
				Issue varchar(500),
				Query varchar(max),
				Severity tinyint
			 );
    
    DECLARE @runtime datetime = getdate();

    DECLARE @statement nvarchar(max),
		  @statement1 nvarchar(max),
		  @statement2 nvarchar(max),
		  @statement3 nvarchar(max);

-- logging variables
    DECLARE @checkid smallint,
		  @checkname varchar(100),
		  @checkstarttime datetime,
		  @checkendtime datetime;

-- SQL Server application version 
    DECLARE @sqlversion varchar(15) = (SELECT CONVERT(varchar(128),  SERVERPROPERTY('ProductVersion')));

-- declare the application version variables
    DECLARE @trusteversion varchar(15),
		  @tillcontroller varchar(15),
		  @tillxp varchar(15),
		  @revalcontroller varchar(15),
		  @vendingcontroller varchar(15),
		  @bioreg varchar(15),
		  @sync varchar(15),
		  @getversion varchar(200),
		  @version varchar(15);

-- get the system type
    DECLARE @sitetype varchar(30),
		  @independant varchar(30),
		  @branch varchar(30),
		  @headoffice varchar(30),
		  @hybrid varchar(30),
		  @syncreplica int;

    SET @independant = 'Single'
    SET @branch = 'Branch'
    SET @headoffice = 'Head Office';
 
    IF (SELECT OptionValue FROM Options WHERE OptionName = 'HybridMode') = 'Y'
	   SET @hybrid = 'Hybrid';
		 
    SELECT @sitetype = OfficeType FROM sync_LocalReplica;
    IF @sitetype <> @headoffice
	   SELECT @syncreplica = LocalReplicaID FROM sync_LocalReplica

    DECLARE @siteguid uniqueidentifier = (SELECT SiteGUID FROM LocalSite)

    DECLARE @autoeod varchar(1)
    
    SELECT @autoeod = OptionValue FROM Options WHERE OptionName = 'AutoEOD'
    SELECT @autoeod = isnull(@autoeod,'n')		  
    
    DECLARE @dataversion varchar(7) = (SELECT DatabaseVersion FROM sync_LocalReplica);

    DECLARE @xpcmd varchar(8000)
    
    IF object_id(N'tempdb..##nrs2000cashless') IS NOT NULL 
	   DROP TABLE ##nrs2000cashless
		  CREATE TABLE ##nrs2000cashless
			 (
				Data varchar(2000)
			 );

BEGIN    
    -- v7 release versions
    IF left(@dataversion,1) = '7'
	   BEGIN
		  SET @trusteversion = '7.1.19'
		  SET @tillcontroller = '7.1.7'
		  SET @tillxp = '7.1.9'
		  SET @revalcontroller = '7.1.0'
		  SET @vendingcontroller = '7.1.0'
		  SET @bioreg = '8.0.0.0'
		  SET @sync = '3.7.27'
	   END;
    ELSE
-- v6 release versions
    IF left(@dataversion,1) = '6'
	   BEGIN
		  SET @trusteversion = '6.1.31'
		  SET @tillcontroller = '6.1.9'
		  SET @tillxp = '6.1.25'
		  SET @revalcontroller = '6.0.1'
		  SET @vendingcontroller = '6.0.5'
		  SET @bioreg = '7.0.0'
		  SET @sync = '3.7.26'
	   END;
    ELSE
-- v5 release versions
    IF left(@dataversion,1) = '5'
	   BEGIN
		  SET @trusteversion = '5.0.6'
		  SET @tillcontroller = '5.0.3'
		  SET @tillxp = '5.0.5'
		  SET @revalcontroller = '5.0.7'
		  SET @vendingcontroller = '5.0.4'
		  SET @bioreg = '5.0.0'
		  SET @sync = '3.7.26'
	   END;
    ELSE
-- v4 release versions
    IF left(@dataversion,1) = '4'
	   BEGIN
		  SET @trusteversion = '4.2.0.42'
		  SET @tillcontroller = '4.2.3.0'
		  SET @tillxp = '4.2.6.0'
		  SET @revalcontroller = '4.2.0.0'
		  SET @vendingcontroller = 'N/A'
		  SET @bioreg = 'N/A'
		  SET @sync = '3.7.0.26'
	   END;
END;

    
-- Make sure the version is 2008 R2 SP3 or higher 
    /*
    IF (SELECT CAST(left(replace(@sqlversion, '.',''),2) as int)) = 10  
    and (SELECT CAST(substring(replace(@sqlversion, '.',''),5,1) as int)) < 2
    PRINT 'This is best ran on SQL Server 2008 R2 SP2 or higher. Some of the queries will not be executed'
    */
	
-- check that xp_cmdshell is enabled and if not enable it
    IF NOT EXISTS (SELECT value FROM sys.configurations WHERE name = 'xp_cmdshell' and value = 1)
	   BEGIN
		    EXEC sp_configure @configname='show advanced options', @configvalue=1
		    RECONFIGURE
		    EXEC sp_configure @configname=xp_cmdshell, @configvalue=1
		    RECONFIGURE
	   END;

----------------------------------------------------------------------
-- 1.1 @diagnose
----------------------------------------------------------------------

IF (@diagnose) > 0 or (@analyse) > 0 or (@rebuildindexes) > 0 or (@checkdb) > 0
BEGIN

    SET @checkid = 101;
    SET @checkstarttime = getdate();

    BEGIN TRY 
	   SET @statement = '
		  SELECT '+@scriptversion+' as [Script Version], '+@scriptdate+' as [Release  Date]
		  INTO ##check'
	   
	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'QCCheck  ', 'Check that this is the current release version of the script.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();	
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

END;

----------------------------------------------------------------------
-- end of script check
----------------------------------------------------------------------

----------------------------------------------------------------------
-- 1. @diagnose
----------------------------------------------------------------------

IF (@diagnose) > 0
BEGIN 

------------------------------------------------------------------
-- 102 are there any rounding problems in the transactionhistory
------------------------------------------------------------------

    SET @checkid = 102;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 count(t.LineID) as ''RoundingCount''
		  INTO ##check
		  FROM TransactionHistory t 
		  WHERE floor(t.RunningCashBalance*100) != t.RunningCashBalance*100
		  HAVING count(t.LineID) > 0'
    
	   EXEC sp_executesql @statement
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are records in TransactionHistory that have 4 decimal places.', @statement, 8)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();	
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 103 find duplicated FSM period records
------------------------------------------------------------------

    SET @checkid = 103;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   IF @sitetype <> @headoffice   
	   SET @statement = '    
		  WITH numberguid AS
			  (
			  SELECT 
				  AccountFSMPeriodGUID, 
				  AccountGUID, 
				  ROW_NUMBER() over (PARTITION BY AccountGUID ORDER BY AccountFSMPeriodGUID) rn
			  FROM 
				  dbo.AccountFSMPeriod
			  )

			  SELECT 
				AccountCode, 
				Surname, 
				Forename, 
				FormName, 
				StartDate, 
				ExpiryDate, 
				AccountFSMPeriodGUID
			  INTO ##check
			  FROM 
				  AccountFSMPeriod f
				  join Account a on f.AccountGUID = a.AccountGUID
				  join AccountTypes t on a.AccountTypesGUID = t.AccountTypesGUID
				  join Class c ON a.ClassGUID = c.ClassGUID
			  WHERE 
				  t.Description = ''Pupils''
			  and AccountFSMPeriodGUID not in 
				(
				    SELECT 
					   AccountFSMPeriodGUID 
				    FROM 
					   numberguid 
				    WHERE 
					   rn =1
				);'

	   EXEC sp_executesql @statement
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are duplicate records in the AccountFSM table.', @statement, 5)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();	
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 104 find duplicate siteoptions records
------------------------------------------------------------------

    SET @checkid = 104;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement =	' 
		  WITH Duplicates AS 
			 (
				 SELECT 
				    OptionName as [Duplicate OptionName],
				    SiteOptionsGUID,
				    ROW_NUMBER() OVER (PARTITION BY OptionName ORDER BY SiteOptionsGUID) rn
				 FROM 
				    SiteOptions
				 WHERE 
				    nrsSyncStamp IS NULL
				 -- nrsSyncStamp null to stop deleting the head office record
			 )
    
		   SELECT 
			 [Duplicate OptionName], 
			 SiteOptionsGUID, 
			 rn as [Duplicate Number] 
		   INTO ##check
		   FROM Duplicates WHERE rn > 1;'

	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are duplicate records in the SiteOptions table.', @statement, 9)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();	
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 105 has the Accountlimts primary key been added 
------------------------------------------------------------------

    SET @checkid = 105;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT * 
		  INTO ##check
		  FROM sys.indexes 
		  WHERE object_id = object_id(N''[dbo].[AccountLimits]'') and name = N''AccountLimits6_OLD'''
	   
	   EXEC sp_executesql @statement

	   IF NOT EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'The deadlock fix script has not been applied.', @statement, 6)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();	
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 106 have there been any negative unused FSM records?
------------------------------------------------------------------

    SET @checkid = 106;
    SET @checkstarttime = getdate();
    	   
    BEGIN TRY
	   IF @sitetype <> @headoffice
	   SET @statement = '
		  SELECT t.LineID 
		  INTO ##check
		  FROM TransactionHistory t
		  WHERE t.FunctionCode = 168 and t.FreeValue > 0';
		  
	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are records in dbo.TransactionHistory where the FSM allocation is negative.', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check 

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 107 are there any UIFSM accounts in a negative?
------------------------------------------------------------------

    SET @checkid = 107; 
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  DECLARE @uifsmaccounttypesguid uniqueidentifier = 
		  (SELECT AccountTypesGUID FROM AccountTypes WHERE [Description] = ''UIFSM'')

		  SELECT a.AccountCode, p.CashBalance 
		  INTO ##check
		  FROM Purses p 
			 join Account a on p.AccountGUID = a.AccountGUID
		  WHERE p.CashBalance < 0 and a.AccountTypesGUID = @uifsmaccounttypesguid'

	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are UIFSM accounts in a negative balance', @statement, 5)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 108 if the overdraft limit is zero are there any accounts in a negative balance?
------------------------------------------------------------------

    SET @checkid = 108; 
    SET @checkstarttime = getdate();

    BEGIN TRY
	   DECLARE @overdraft decimal
	   IF EXISTS (SELECT * FROM SiteAccountTypes)
		  BEGIN
			 SELECT @overdraft = Overdraft FROM SiteAccountTypes sat join AccountTypes at on at.AccountTypesGUID = sat.AccountTypesGUID WHERE sat.SiteGUID = @siteguid and at.Description = 'Pupils'
		  END
		  ELSE
		  BEGIN
			 SELECT @overdraft = DefaultOverdraft FROM AccountTypes WHERE Description = 'Pupils'
		  END
		  IF (@overdraft) > 0.00
			 BEGIN
				SET @statement = '
				    SELECT a.AccountCode, p.CashBalance
				    INTO ##check 
				    FROM Purses p 
					   join Account a on p.AccountGUID = a.AccountGUID
				    WHERE p.CashBalance < 0'
			 END

    	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are accounts with a negative balance but the overdraft is £0.00.', @statement, 5)
	   END;


	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check	   

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 109 are there any calendars with more than 364 days
------------------------------------------------------------------

    SET @checkid = 109;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 count(WorkingDate) AS ''Daycount'', 
			 YearName
		  INTO ##check
		  FROM 
			 DateRange
		  GROUP BY 
			 YearName
		  HAVING 
			 count(WorkingDate) > 364'

    	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are calendar records with more than 364 days in dbo.DateRange.', @statement, 5)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 110 are the VAT levels correct?
------------------------------------------------------------------

    SET @checkid = 110;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement =
		  'SELECT 
			 pl.PriceLevelDescription, 
			 v.VatDescription
		  INTO ##check
		  FROM 
			 PriceLevel pl
			 join Vat v on v.VatGUID = pl.PriceLevelDefaultVATGUID
		  WHERE (PriceLevelDescription = ''Staff Paid'' and VatDescription <> ''Standard'')
			or (PriceLevelDescription = ''Staff Free'' and VatDescription <> ''Zero'')
			or (PriceLevelDescription = ''Pupils'' and VatDescription <> ''Zero'')
			or (PriceLevelDescription = ''UIFSM'' and VatDescription <> ''Zero'')'

    	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is a configuration issue with the VAT and PriceLevels.', @statement, 3)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 111 are there any duplicate accounts?
------------------------------------------------------------------

    SET @checkid = 111;
    SET @checkstarttime = getdate();

    BEGIN TRY
    IF @sitetype <> @headoffice
	   SET @statement = '
		  SELECT 
			 a1.Surname as s1, 
			 a2.Surname as s2, 
			 a1.Forename as f1, 
			 a2.forename as f2, 
			 COUNT(a1.Surname) as [NumberOfAccounts]
		  INTO ##check
		  FROM Account as a1
			 join Account as a2 on a1.AccountGUID = a2.AccountGUID
		  WHERE a1.Surname = a2.Surname and a1.Forename = a2.Forename
			 and a1.AccountDisabled = 0
		  GROUP BY a1.Surname, a2.Surname, a1.Forename, a2.Forename
		  HAVING COUNT(a1.Surname) > 1'
    	   
	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are duplicate Surname and Forename records in dbo.Account', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 112 have the parentpay fix scripts been ran?
------------------------------------------------------------------

    SET @checkid = 112; 
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  WITH ppFixes AS
			 (
			 SELECT 
				name as [ParentPay Fix], 
				modify_date as [Last Modified]
			 FROM 
				sys.objects 
			 WHERE 
				type = ''p''
				and (name = ''ppCancellationRecord4'' and modify_date < ''20151009'')
				or (name = ''ppProductSaleRecord4'' and modify_date < ''20150915'')
				or (name = ''ppCancelationCount3'' and modify_date < ''20131008'')
				or (name = ''ppProductSaleRecord3'' and modify_date < ''20130321'')
				or (name = ''ppAddAccountRecord4'' and modify_date < ''20121010'')
				or (name = ''ppAddBalanceRecord'' and modify_date < ''20130313'')
				or (name = ''ppAddBalanceRecord2'' and modify_date < ''20130313'')
				or (name = ''ppMenuProduct'' and modify_date < ''20121016'')
			 UNION
			 SELECT 
				name as [ParentPay Fix], 
				modify_date as [Last Modified]
			 FROM 
				sys.objects 
			 WHERE 
				type = ''v''
				and (name = ''ppUniqueCancellationsView3'' and modify_date < ''20150909'')
			 )
			 SELECT * INTO ##check FROM ppFixes'
	   
	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  IF (SELECT OptionValue FROM SiteOptions WHERE OptionName = 'ParentPayLinkActive' and SiteGUID IS NOT NULL) = 'y'
		  BEGIN
			 INSERT ##issues
			 VALUES (@checkid, 'Trust-e  ', 'ParentPay Fix stored procedures aren''t upto date.', @statement, 3)
		  END;
		  ELSE
		  BEGIN
			 INSERT ##issues
			 -- lower priority if ParentPay isn't enabled, would still require attention. 
			 VALUES (@checkid, 'Trust-e  ', 'ParentPay Fix stored procedures aren''t upto date.', @statement, 10)
		  END;
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 113 find tables with data with another syncReplicaID
------------------------------------------------------------------

    SET @checkid = 113;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   IF (@sitetype) <> (@headoffice)
	   BEGIN
	   SET @statement = '
		  DECLARE @sql varchar(100), 
				@tablename varchar(50), 
				@tableid int, 
				@syncreplica int,
				@rn int;

		  SELECT @syncreplica = SyncReplicaID FROM Site

		  DECLARE @tables table 
			 (
				rn int, 
				tablename varchar(50)
			 );
		  INSERT @tables

			 SELECT 
				row_number() over (order by table_name desc), 
				table_name
			 FROM
				INFORMATION_SCHEMA.TABLES t
			 WHERE
				t.TABLE_CATALOG = ''Cashless'' and
				EXISTS (
				    SELECT *
				    FROM INFORMATION_SCHEMA.COLUMNS c
				    WHERE
					   c.TABLE_CATALOG = t.TABLE_CATALOG and
					   c.TABLE_SCHEMA = t.TABLE_SCHEMA and
					   c.TABLE_NAME = t.TABLE_NAME and
					   c.COLUMN_NAME = ''nrsSyncStamp'' and
					   c.TABLE_NAME <> ''sync_Tombstone'')  
			 and TABLE_TYPE = ''Base Table'' 
			 ORDER BY TABLE_NAME 

			 SELECT @tableid = min(rn) FROM @tables
			 SELECT @rn = count(*) FROM @tables
			 SELECT @tablename = tablename FROM @tables WHERE rn = @tableid

			 WHILE @tableid < @rn
			 BEGIN

				SELECT @tablename = tablename FROM @tables WHERE rn = @tableid
				
				SET @sql = ''select * from ''+@tablename+'' where syncreplicaid <> ''+convert(varchar,@syncreplica)
				
				DECLARE @tableselect table (sqlstatement varchar(2000));
				    INSERT @tableselect
					   SELECT @sql as sqlstatement

				SET @tableid = @tableid + 1

			 END;

		  SELECT * 
		  INTO ##check
		  FROM @tableselect WHERE sqlstatement is not null'
	   
    	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are tables with a different SyncReplicaID', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check
    END;

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 114 make sure the over isn't on if TakeCash is enabled
------------------------------------------------------------------

IF (@dataversion) > '6.1.0' and EXISTS (SELECT * FROM sys.columns WHERE Name = N'TakeCash' AND object_id = object_id(N'Terminal'))
BEGIN
    SET @statement = '
	   SELECT * 
	   INTO ##takecash
	   FROM Terminal WHERE TakeCash = 1'

    EXEC sp_executesql @statement

    IF EXISTS (SELECT * FROM ##takecash)
    BEGIN

    SET @checkid = 114;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ''Terminal'' and column_name = ''TakeCash'')				    
			 begin
			 SELECT 
				count(*) as ''AccountTypes with overdraft''
			 INTO ##check
			 FROM SiteAccountTypes
			 WHERE Overdraft > 0	
			 end		 
			 '

    	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'Both TakeCash is enabled and an Overdraft is allowed', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH
    
    IF object_id(N'TempDb..##takecash') IS NOT NULL
    DROP TABLE ##takecash
    
    END;
END;
ELSE
    GOTO check115

------------------------------------------------------------------
-- 115 have any reval transactions been performed after the EOD
------------------------------------------------------------------

check115:
    SET @checkid = 115;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   IF (@autoeod) = 'n'
	   BEGIN
		  SET @statement = '
			 SELECT t.LineID, t.AccountValue, t.PursesGUID
			 INTO ##check
			 FROM TransactionHistory t
			 WHERE t.FunctionCode = 180
			 and t.DateOfSale > (SELECT MAX(DateofSale) FROM TransactionHistory WHERE FunctionCode = 243);'
	   END;
		  ELSE
	   BEGIN
		  SET @statement = '
			 SELECT t.LineID, t.AccountValue, t.PursesGUID
			 INTO ##check
			 FROM TransactionHistory t
			 WHERE t.FunctionCode = 180
			 and t.DateOfSale < (SELECT MAX(DateofSale) FROM TransactionHistory WHERE FunctionCode = 243);'
    	   END;

	   EXEC sp_executesql @statement

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Reval   ', 'Reval transactions were recorded after the EOD was performed.', @statement, 5)
	   END

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check
    
	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 116 sql server advanced options
------------------------------------------------------------------

    SET @checkid = 116;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			   c.name, 
			   c.value, 
			   c.value_in_use, 
			   c.minimum, 
			   c.maximum, 
			   c.[description]
		  INTO ##check
		  FROM 
			 sys.configurations c WITH (NOLOCK)
		  WHERE c.name in 
			(''awe enabled'', ''max server memory (MB)'', ''min server memory (MB)'', ''optimize for ad hoc workloads'',
			 ''remote access'', ''remote admin connections'')
		  ORDER BY 
			c.name 
		  OPTION (RECOMPILE);'

    	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT count([name]) FROM ##check WHERE value <> 1 OR value <> 2147483647 GROUP BY [name], [value])
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Database', 'Database option''s aren''t configured correctly.', @statement, 5)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check
		  
	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 117 check for auto shrink and auto close
------------------------------------------------------------------

    SET @checkid = 117; 
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			  db_name(d.database_id) AS [Database Name], 
			  f.name AS [Filename], 
			  f.physical_name AS [Location],
			  convert(bigint, f.growth/128.0) AS [Growth in MB], 
			  f.growth AS [Growth],
			  convert(bigint, f.size/128.0) AS [Total Size in MB],
			  CASE WHEN d.is_auto_close_on = 0 THEN ''False'' ELSE ''True'' END as [Auto_Close], 
			  CASE WHEN d.is_auto_shrink_on = 0 THEN ''False'' ELSE ''True'' END as [Auto_Shrink]
		  INTO ##check
		  FROM 
			  sys.master_files f WITH (NOLOCK)
			  join sys.databases d WITH (NOLOCK) on f.database_id = d.database_id
		  WHERE f.database_id > 4 
			  and f.database_id <> 32767
			  or f.database_id = 2
		  ORDER BY 
			 db_name(f.database_id) 
		  OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT count([Database Name]) FROM ##check WHERE [Auto_Close] = 'True' OR [Auto_Shrink] = 'True' GROUP BY [Database Name], [Auto_Close], [Auto_Shrink])
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Database', 'Auto close, or auto shrink, are set incorrectly on the application databases.', @statement, 3)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 118 check backups are regulary being taken
------------------------------------------------------------------

    SET @checkid = 118; 
    SET @checkstarttime = getdate();

    BEGIN TRY
    SET @statement = '
	   WITH backups AS
	   (
		  SELECT 
			  b.database_name, 
			  b.name,	
			  cast(b.backup_finish_date AS smalldatetime) AS [Backup Finish Date], 
			  convert(decimal(18,2),b.backup_size/1073741824.0) AS [Backup Size (GB)], 
			  convert(decimal(18,2),b.compressed_backup_size/1073741824.0) AS [Compressed Backup Size (GB)], 
			  CASE
				  WHEN b.type = ''D'' THEN ''Database''
				  WHEN b.type = ''I'' THEN ''Differential''
				  WHEN b.type = ''L'' THEN ''Transaction Log''
			  END AS [Backup Type],	
			  b.recovery_model, 
			  m.physical_device_name 
		  FROM 
			 msdb.dbo.backupset b 
			 join msdb.dbo.backupmediafamily m on b.media_set_id = m.media_set_id
	   )
	   SELECT * 
	   INTO ##check
	   FROM backups 
	   WHERE [Backup Finish Date] in 
		  (
			 SELECT [Finish Date] 
			 FROM 
				(
				    SELECT max([Backup Finish Date]) as [Finish Date], database_name 
				    FROM backups
				    GROUP BY database_name
				) as a
	   )
	   ORDER BY 
		  backups.[Backup Finish Date], 
		  backups.database_name'
    
	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT count(database_name) FROM ##check WHERE [Backup Finish Date] < dateadd(dd,-5,getdate()) GROUP BY [database_name], [Backup Finish Date])
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Database', 'The application databases haven''t been backed in at least 3 days.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check
	   
	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 119 are DBCC CheckDBs being performed
------------------------------------------------------------------

    SET @checkid = 119;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  DECLARE @biometric varchar(70) = ''DBCC DBINFO(''''Biometric'''') WITH TABLERESULTS, NO_INFOMSGS'';
		  DECLARE @cashless varchar(70) = ''DBCC DBINFO(''''Cashless'''') WITH TABLERESULTS, NO_INFOMSGS'';
		  DECLARE @misreporting varchar(70) = ''DBCC DBINFO(''''MISReporting'''') WITH TABLERESULTS, NO_INFOMSGS'';
		  DECLARE @nrssync varchar(70) = ''DBCC DBINFO(''''NRSSync'''') WITH TABLERESULTS, NO_INFOMSGS'';

		  DECLARE @dbcc TABLE (ID int identity(1, 1), parentobject varchar(70), object varchar(70), field varchar(70), value varchar(70))
    
		  IF db_id(''Biometric'') IS NOT NULL
			  INSERT @dbcc 
				  EXEC (@biometric);

		  IF db_id(''Cashless'') IS NOT NULL
			  INSERT @dbcc
				  EXEC (@cashless);

		  IF db_id(''MISReporting'') IS NOT NULL
			   INSERT @dbcc
				  EXEC (@misreporting);

		  IF db_id(''NRSSync'') IS NOT NULL
			  INSERT @dbcc
				  EXEC (@nrssync);

			  SELECT 
				  d1.value as [Database], 
				  d2.value as [LastGoodDBCC]
			  INTO ##check	     
			  FROM @dbcc d1
				  join @dbcc d2 on d2.ID = (SELECT min(ID) FROM @dbcc WHERE Field = ''dbi_dbccLastKnownGood'' and ID > d1.ID)
			  WHERE 
				d1.Field = ''dbi_dbname''
			  ORDER BY 
				d2.Value 
			 OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT count(*) FROM ##check WHERE [LastGoodDBCC] < dateadd(dd,-5,getdate()) GROUP BY [Database], [LastGoodDBCC])
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Database', 'A good DBCC CheckDB hasn''t been recently recorded for one or more application databases.', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 120 find tables that have no primary key
------------------------------------------------------------------

    SET @checkid = 120;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 t.name AS [Table without a Primary Key]
		  INTO ##check
		  FROM 
			 sys.tables t
		  WHERE 
			 OBJECTPROPERTY(object_id,''TableHasPrimaryKey'') = 0
			 and type = ''U''
		  ORDER by 
			 t.name
		  OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT count(*) FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Database', 'One or more table''s don''t have a primary key column.', @statement, 3)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 121 find records where the price doesn't match the price category price
------------------------------------------------------------------

    SET @checkid = 121;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   IF (SELECT count(*) FROM PriceCategory) > 1
	   BEGIN
	   SET @statement = '
		  DECLARE @course uniqueidentifier,
				@menuschedulingarea uniqueidentifier,
				@pricelevel uniqueidentifier;
		  
		  SELECT @course = CourseGUID FROM Course WHERE CourseDescription = ''Main Meals''
		  SELECT @menuschedulingarea = MenuSchedulingAreaGUID FROM MenuSchedulingArea
		  SELECT @pricelevel = PriceLevelGUID FROM PriceLevel WHERE PriceLevelDescription = ''Pupils''
		   
		  SELECT 
			 pc.Name, 
			 p.PCPrice, 
			 c.CourseDescription, 
			 PLU.PLUDescription, 
			 cast(substring(pc.Name, 2, 4) as decimal) as [Difference]
		  INTO ##check
		  FROM Prices p 
			 join PLU ON p.PLUGUID = PLU.PLUGUID
			 join Course c ON PLU.CourseGUID = c.CourseGUID
			 join Site s ON p.syncReplicaID = s.SyncReplicaID
			 join PriceCategory pc ON s.PriceCategoryGUID = pc.PriceCategoryGUID
			 join MenuSchedulingArea ms ON PLU.MenuSchedulingAreaGUID = ms.MenuSchedulingAreaGUID
		  WHERE substring(pc.Name, 2, 4) <> p.PCPrice
			 and PLU.CourseGUID = @course -- main meal courses
			 and PLU.MenuSchedulingAreaGUID = @menuschedulingarea
			 and p.PriceLevelGUID = @pricelevel'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are prices that don''t match the price category price', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();

	   END;
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 122 are there any orphaned BioIDs
------------------------------------------------------------------

    SET @checkid = 122;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT b.BIOID, p.PursesGUID, p.CardNumber
		  INTO ##check
		  FROM Biometric.dbo.Template b
			 left join Purses p on b.BIOID = p.CardNumber
		  WHERE p.CardNumber is null'

	   EXEC sp_executesql @statement 
	   
	   IF (SELECT count(*) FROM ##check) > 0
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is one or more orphaned BIOID in the biometric database.', @statement, 5)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 123 are there any account that don't have biometrics or allowpinentry
------------------------------------------------------------------
IF (SELECT PurseEntryMethod FROM PurseTypes WHERE PurseDescription = 'Catering') = 'Finger'
BEGIN
    SET @checkid = 123;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 a.AccountCode, a.Surname, a.Forename, p.AllowPinEntry, b.BIOID
		  INTO ##check
		  FROM Account a 
			 join Purses p on a.AccountGUID = p.AccountGUID
			 left join Biometric.dbo.Template b on p.CardNumber = b.BIOID
		  WHERE b.BIOID is null
			 and p.AllowPinEntry = 0
			 and a.AccountDisabled = 0
			 and a.AccountTypesGUID not in 
				(SELECT AccountTypesGUID FROM AccountTypes WHERE [Description] in (''Hospitality'', ''Visitor''))'

	   EXEC sp_executesql @statement 
	   
	   IF (SELECT count(*) FROM ##check) > 0
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is at least one account without a registered template and PIN entry disabled.', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH
END
ELSE
GOTO checkid133

------------------------------------------------------------------
-- 124 is the period free spend set to the same as the FSM allocation amount
------------------------------------------------------------------

------------------------------------------------------------------
-- 125 are there any accounts in Trust-e that aren't in the ParentPay payload
------------------------------------------------------------------

------------------------------------------------------------------
-- 126 get the application path of Trust-e
------------------------------------------------------------------

    SET @checkid = 126;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  IF OBJECT_ID(N''tempdb..#nrs2000cashless'') IS NOT NULL  
		  BEGIN  
			 DROP TABLE #nrs2000cashless
		  END; 

		  CREATE TABLE #nrs2000cashless(data VARCHAR(2000));

			 BULK INSERT #nrs2000cashless 
				FROM ''c:\windows\nrs2000Cashless.ini'' 
				WITH
				    (
					   ROWTERMINATOR = ''\n''
				    );

		  SELECT '+@trustepath+' = replace(data, ''WorkingPath = '', '''') 
		  FROM #nrs2000cashless
		  WHERE left(data,4) = ''work''
		  DROP TABLE #nrs2000cashless'

	   EXEC sp_executesql @statement 

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 127 is Trust-e the latest release 
------------------------------------------------------------------

    SET @checkid = 127;
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\Trust-e.exe"';
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo';

    BEGIN TRY
		  CREATE TABLE ##truste (id int identity (1,1),data VARCHAR(2000));
			 
			 INSERT ##truste
			 EXEC xp_cmdshell @getversion, no_output;

			 SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
			 FROM ##truste
			 WHERE id = 4
 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'Trust-e' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'Trust-e', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'Trust-e'
		  END;

		  IF OBJECT_ID(N'tempdb..##truste') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##truste
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'Trust-e') <> @trusteversion

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@trusteversion+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''Trust-e'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'Trust-e isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 128 is the tillxp the latest release 
------------------------------------------------------------------

    SET @checkid = 128;
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\TillXP.exe"';
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo';

    BEGIN TRY
		  
		  CREATE TABLE ##tillxp (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##tillxp
				EXEC xp_cmdshell @getversion

		  SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
		  FROM ##tillxp
		  WHERE id = 4
 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'TillXP' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'TillXP', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'TillXP'
		  END

		  IF OBJECT_ID(N'tempdb..##tillxp') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##tillxp
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'Trust-e') <> @tillxp

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@tillxp+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''TillXP'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'The TillXP isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 129 is the tillcontroller the latest release 
------------------------------------------------------------------ 

    SET @checkid = 129
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\TillController.exe"'
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo'

    BEGIN TRY
		  
		  CREATE TABLE ##tillcontroller (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##tillcontroller
				EXEC xp_cmdshell @getversion

		  SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
		  FROM ##tillcontroller
		  WHERE id = 4

 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'TillController' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'TillController', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'TillController'
		  END

		  IF OBJECT_ID(N'tempdb..##tillcontroller') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##tillcontroller
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'TillController') <> @tillcontroller

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@tillcontroller+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''TillController'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'The TillController isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH   

------------------------------------------------------------------
-- 130 is bioreg the latest release 
------------------------------------------------------------------ 

    SET @checkid = 130
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\Bioreg.exe"'
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo'

    BEGIN TRY
		  
		  CREATE TABLE ##bioreg (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##bioreg
				EXEC xp_cmdshell @getversion

		  SELECT @version = left(data,10) --convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
					   --convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
					   --convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
		  FROM ##bioreg
		  WHERE id = 4

 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'Bioreg' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'Bioreg', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'Bioreg'
		  END

		  IF OBJECT_ID(N'tempdb..##bioreg') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##bioreg
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'Bioreg') <> @bioreg

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@bioreg+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''Bioreg'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'Bioreg isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 131 is the revalcontroller the latest release 
------------------------------------------------------------------ 

    SET @checkid = 131
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\RevalController\TrusteRevalController.exe"'
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo'

    BEGIN TRY
		  
		  CREATE TABLE ##reval (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##reval
				EXEC xp_cmdshell @getversion

		  SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
					   convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
		  FROM ##reval
		  WHERE id = 4

 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'RevalController' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'RevalController', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'RevalController'
		  END

		  IF OBJECT_ID(N'tempdb..##reval') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##reval
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'RevalController') <> @revalcontroller

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@revalcontroller+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''RevalController'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'The RevalController isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 132 is the revalcontroller the latest release 
------------------------------------------------------------------ 

    SET @checkid = 132
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Truste\MDB Vending Machine Controller\Vending Controller.exe"'
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo'

    BEGIN TRY
		  
		  CREATE TABLE ##vend (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##vend
				EXEC xp_cmdshell @getversion

			 SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
			 FROM ##vend
			 WHERE id = 4
 
		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'VendingController' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'VendingController', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'VendingController'
		  END

		  IF OBJECT_ID(N'tempdb..##vend') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##vend
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'VendingController') <> @vendingcontroller

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@vendingcontroller+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''VendingController'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'The VendingController isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 133 is Sync the latest release 
------------------------------------------------------------------ 

    SET @checkid = 133
    SET @checkstarttime = getdate(); 
    SET @trustepath = '"C:\Program Files (x86)\NRS\Sync\SyncUNC.exe"'
    SET @getversion = 'powershell.exe -noprofile (Get-Item '''+@trustepath+''').VersionInfo'

    BEGIN TRY
		  
		  CREATE TABLE ##sync (id int identity (1,1),data VARCHAR(2000));
			 INSERT ##sync
				EXEC xp_cmdshell @getversion

			 SELECT @version = convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 3))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 2))) + '.' + 
						  convert(VARCHAR, convert(INT, PARSENAME(left(data,9), 1)))  
			 FROM ##sync
			 WHERE id = 4

		  IF NOT EXISTS (SELECT * FROM QCApplicationVersion WHERE ApplicationName = 'Sync' and ApplicationVersion IS NOT NULL)
		  BEGIN
			 INSERT QCApplicationVersion
			 SELECT 'Sync', @version
		  END;
		  ELSE
		  BEGIN
			 UPDATE QCApplicationVersion
			 SET ApplicationVersion = @version
			 WHERE ApplicationName = 'Sync'
		  END

		  IF OBJECT_ID(N'tempdb..##sync') IS NOT NULL  
		  BEGIN
			 DROP TABLE ##sync
		  END;

	   IF (SELECT ApplicationVersion FROM QCApplicationVersion WHERE ApplicationName = 'Sync') <> @sync

	   SET @statement = '
		  SELECT ApplicationName, ApplicationVersion, '''+@sync+''' as CurrentRelease FROM QCApplicationVersion WHERE ApplicationName = ''Sync'''

	   INSERT ##issues
	   VALUES (@checkid, 'Trust-e  ', 'Sync isn''t the most up to date release version.', @statement, 3)

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 134 are there any triggers missing
------------------------------------------------------------------
checkid133:
    SET @checkid = 134;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT name as TableName
		  INTO ##check
		  FROM sys.tables t
		  WHERE not exists 
			 (SELECT * FROM sys.triggers tr WHERE is_disabled = 0 AND tr.parent_id = t.object_id) 
		  and exists 
			 (SELECT * FROM sys.all_columns ac WHERE name = ''nrsSyncStamp'' and ac.object_id = t.object_id)
		  and name not in (''dtproperties'', ''WorkingDate'', ''sync_Tombstone'')
		  ORDER BY name;'

	   EXEC sp_executesql @statement 
	   
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is one or more replicable tables that doesn''t have an active trigger.', @statement, 3)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 135 are there any orphaned purse records
------------------------------------------------------------------
 
    SET @checkid = 135;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  WITH OrphanedAccountGUIDs AS 
			 (
				SELECT ''Purses'' AS [Table], p.AccountGUID
				FROM Purses p
				    left join Account a on p.AccountGUID = a.AccountGUID
				WHERE a.AccountGUID is null
				UNION 
				SELECT ''Account'' AS [Table], a.AccountGUID
				FROM Account a
				    left join Purses p on p.AccountGUID = a.AccountGUID
				WHERE p.AccountGUID is null
			 )
		  SELECT * 
		  INTO ##check
		  FROM OrphanedAccountGUIDs' 

	   EXEC sp_executesql @statement 
	   
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is one or more orphaned AccountGUIDs in Account or Purses.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 136 are there any duplicate transactionhistory finishes
------------------------------------------------------------------

    SET @checkid = 136;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 t.FunctionCode, 
			 t.ReceiptNumber, 
			 count(t.ReceiptNumber) as [Receipts]
		  INTO ##check
		  FROM dbo.TransactionHistory t
		  GROUP BY t.FunctionCode, t.ReceiptNumber
		  HAVING t.FunctionCode = 165 and count(t.ReceiptNumber) > 1'

	   EXEC sp_executesql @statement 
	   
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are duplicate finishes in transactionhistory.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 137 are there any zero PINs
------------------------------------------------------------------

    SET @checkid = 137;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 a.AccountCode, a.Surname, a.Forename, p.CardNumber
		  INTO ##check
		  FROM Account a 
			 join Purses p on a.AccountGUID = p.AccountGUID
		  WHERE p.CardNumber = ''0'' or p.CardNumber = ''-1''
			 and a.AccountDisabled = 0'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There is one or more account without a valid PIN.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 138 are there any free meal de-allocations without an allocation
------------------------------------------------------------------

    SET @checkid = 138;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 WorkingDate, 
			 PursesGUID, 
			 FunctionCode
		  INTO ##check
		  FROM   TransactionHistory
		  WHERE  FunctionCode = 168
		  and    PursesGUID not in (SELECT PursesGUID
						  FROM   TransactionHistory
						  WHERE  FunctionCode = 167)'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'There are accounts that an Unused FSM entry without having the FSM allocation first .', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 139 are there any suspect pages
------------------------------------------------------------------

    SET @checkid = 139;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT *
		  INTO ##check
		  FROM msdb..suspect_pages'

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Datebase  ', 'There are, or have been, suspect pages in the database.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 140 running out of space disk space
------------------------------------------------------------------

    SET @checkid = 140;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT DISTINCT
			 vs.volume_mount_point AS [Drive Letter], 
			 vs.file_system_type AS [File System], 
			 vs.logical_volume_name AS [Drive Name], 
			 convert(decimal(18,2),vs.total_bytes/1073741824.0) AS [Total Size (GB)],
			 convert(decimal(18,2),vs.available_bytes/1073741824.0) AS [Available Size (GB)],  
			 cast(cast(vs.available_bytes AS float)/ cast(vs.total_bytes AS float) AS decimal(18,2)) * 100 AS [Space Free %] 
		  INTO ##check
		  FROM 
			 sys.master_files AS f WITH (NOLOCK)
			 CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.[file_id]) AS vs 
		  OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement 

	   IF (SELECT [Space Free %] FROM ##check) < 15.00
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'OS  ', 'The SQL database is stored on a disk that has less than 15%.', @statement, 2)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 141 EOD button and Auto EOD 
------------------------------------------------------------------
nextcheck:
IF (@autoeod) = 'y'
BEGIN
    SET @checkid = 141;
    SET @checkstarttime = getdate();

    BEGIN TRY

	   SET @statement = '
		  SELECT u.UserName, mi.MenuSection, mi.mMenuItemID 
		  INTO ##check
		  FROM mUserMenuItem mi
			 join UserNames u on u.UserNamesGUID = mi.UserNamesGUID
		  WHERE mMenuItemID = 64 and u.UserName not in (''nrs'',''itsupport'')
		  and u.SiteGUID = '''+cast(@siteguid as char(36))+''''

	   EXEC sp_executesql @statement 

	   IF EXISTS (SELECT * FROM ##check) 
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'Trust-e  ', 'At least one user has an EOD button, but it''s set to an automatic EOD.', @statement, 4)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   SET @checkendtime = getdate();

    
    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH
END;

------------------------------------------------------------------
-- 1. are there any overlapping sync schedules
------------------------------------------------------------------
------------------------------------------------------------------
-- 1. does SQL Server have the latest service packs installed
------------------------------------------------------------------
------------------------------------------------------------------
-- 1. is sync the latest release 
------------------------------------------------------------------
------------------------------------------------------------------
-- 1. are there any records in tables from sites that shouldn't be there
------------------------------------------------------------------
------------------------------------------------------------------
END; -- end of @diagnose
------------------------------------------------------------------

IF (@analyse) > 0
BEGIN

------------------------------------------------------------------
-- 201 get backup history
------------------------------------------------------------------
 
    SET @checkid = 201;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  DECLARE @enddate datetime, @months smallint;
		  SET @enddate = GetDate(); 
		  SET @months = 6; 
		  
		  WITH BackupHistory AS
			 (
				SELECT 
				    b.database_name AS DatabaseName,
				    YEAR(b.backup_start_date) * 100 + MONTH(b.backup_start_date) AS YearMonth,
				    convert(numeric(10, 1), min(bf.file_size / 1048576.0)) AS MinSizeMB,
				    convert(numeric(10, 1), MAX(bf.file_size / 1048576.0)) AS MaxSizeMB,
				    convert(numeric(10, 1), AVG(bf.file_size / 1048576.0)) AS AvgSizeMB
			 FROM msdb.dbo.backupset b
				    join msdb.dbo.backupfile bf ON b.backup_set_id = bf.backup_set_id
			 WHERE NOT b.database_name IN
					   (''master'', ''msdb'', ''model'', ''tempdb'')
				    and bf.file_type = ''D''
				    and b.backup_start_date BETWEEN DATEADD(mm, - @months, @endDate) AND @endDate
			 GROUP BY b.database_name,
				    YEAR(b.backup_start_date),
				    MONTH(b.backup_start_date)
			 )
				SELECT 
				    m.DatabaseName,
				    m.YearMonth,
				    m.MinSizeMB,
				    m.MaxSizeMB,
				    m.AvgSizeMB,
				    m.AvgSizeMB - (
					   SELECT TOP 1 s.AvgSizeMB
					   FROM BackupHistory s
					   WHERE s.DatabaseName = m.DatabaseName
					   and s.YearMonth < m.YearMonth
					   ORDER BY s.YearMonth DESC
						  ) AS GrowthMB
				    FROM BackupHistory m
				    ORDER BY m.DatabaseName,
							 m.YearMonth'
    
	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 202 index fragmentation
------------------------------------------------------------------
	   
    SET @checkid = 202;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 db_name(database_id) AS [Database Name], 
			 object_name(ps.object_id) AS [Object Name], 
			 i.name AS [Index Name], 
			 ps.index_id, 
			 index_type_desc,
			 avg_fragmentation_in_percent, 
			 fragment_count, page_count
		  FROM 
			 sys.dm_db_index_physical_stats(db_id(),NULL, NULL, NULL ,''LIMITED'') AS ps 
			 join sys.indexes AS i WITH (NOLOCK) on ps.[object_id] = i.[object_id]  
			 and ps.index_id = i.index_id
		  WHERE 
			 database_id = db_id()
			 and page_count > 500
		  ORDER BY 
			 avg_fragmentation_in_percent DESC
		  OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 203 index read/write stats
------------------------------------------------------------------
	   
    SET @checkid = 203;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 convert(varchar(120),
			 object_name(ios.object_id)) AS [Object Name], 
			 i.[name] AS [Index Name], 
			 sum(ios.range_scan_count + ios.singleton_lookup_count) AS ''Reads'',
			 sum(ios.leaf_insert_count + ios.leaf_update_count + ios.leaf_delete_count) AS ''Writes''
		  FROM   
			 sys.dm_db_index_operational_stats (db_id(),NULL,NULL,NULL ) ios
			 join sys.indexes AS i on i.object_id = ios.object_id 
			 and i.index_id = ios.index_id
		  WHERE  
			 OBJECTPROPERTY(ios.object_id,''IsUserTable'') = 1
		  GROUP BY 
			 object_name(ios.object_id),
			 i.name
		  ORDER BY 
			 Reads ASC, Writes DESC
		  OPTION (RECOMPILE);'

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 204 Get table and index sizes
------------------------------------------------------------------
	   
    SET @checkid = 204;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  IF object_id(N''tempdb..#FragmentedIndexes'') IS NOT NULL 
			 BEGIN  
				DROP TABLE #SpaceUsed
			 END;

			 CREATE TABLE #SpaceUsed
				(
				    name nvarchar(128),
				    rows varchar(50),
				    reserved varchar(50),
				    data varchar(50),
				    index_size varchar(50),
				    unused varchar(50)
				)

			 DECLARE @id nvarchar(128)
			 DECLARE c CURSOR LOCAL FAST_FORWARD FOR
				SELECT name FROM sysobjects WHERE xtype=''U''

				OPEN c
				FETCH c INTO @id

				WHILE @@fetch_status = 0 
				BEGIN

				    INSERT #SpaceUsed
					   EXEC sp_spaceused @id

				    FETCH c INTO @id
				END

				CLOSE c
				DEALLOCATE c

			 SELECT * FROM #SpaceUsed
			 ORDER BY convert(int, substring(data, 1, len(data)-3)) DESC

			 DROP TABLE #SpaceUsed
			    BEGIN

			 IF object_id(N''tempdb..##sql_error_log'') IS NOT NULL 
				BEGIN  
				    DROP TABLE ##sql_error_log 
				END;

			 IF object_id(N''tempdb..#ErrLogsDL'') IS NOT NULL 
				BEGIN  
				    DROP TABLE #ErrLogsDL 
				END;

			 DECLARE @site_value int;
			 DECLARE @NumberOfLogfiles int;

			 CREATE TABLE #ErrLogsDL
				(
				    [Archive #] int,
				    [Date] varchar(255),
				    [Log File Size (Byte)] bigint
				)

			 INSERT #ErrLogsDL([Archive #],[Date], [Log File Size (Byte)])
			 EXEC xp_enumerrorlogs
     
				SET @NumberOfLogfiles = (SELECT count(*) FROM #ErrLogsDL);

			 CREATE TABLE ##sql_error_log 
				(
				    LogDate datetime,
				    Processinfo nvarchar(max),
				    [text] nvarchar (max)
				)

				SET @site_value = 0;
				WHILE @site_value < @NumberOfLogfiles
				    BEGIN
					   INSERT ##sql_error_log
						  EXEC sp_readerrorlog @site_value
							 SET @site_value = @site_value + 1;
				    END;
    
				    SELECT 
					   LogDate,
					   Processinfo,
					   [text]
				    FROM 
					   ##sql_error_log
				    ORDER BY 
					   logdate

			 DROP TABLE ##sql_error_log;
			 DROP TABLE #ErrLogsDL;

		  END;'

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 205 trust-e limits and FSM values
------------------------------------------------------------------

    SET @checkid = 205;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 at.Description, p.Name, a.DefaultLimit 
		  FROM AccountTypeLimits a
			 join SiteAccountTypes s on a.AccountTypesGUID = s.AccountTypesGUID
			 join PODTypes p on a.PODTypesGUID = p.PODTypesGUID
			 join AccountTypes at on at.AccountTypesGUID = a.AccountTypesGUID
		  ORDER BY at.SortOrder, p.DefaultStartTime'

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 206 type of end of day
------------------------------------------------------------------

    SET @checkid = 206;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT CASE WHEN'+(@autoeod)+' = ''y'' THEN ''Automatic'' ELSE ''Manual'' END AS ''Type of End Of Day'''

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 207 calendar
------------------------------------------------------------------

    SET @checkid = 207;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT	
			 YearName, 
			 count(WorkingDate) AS [Number of days]
		  FROM 
			 DateRange
		  GROUP BY 
			 YearName
		  ORDER BY 
			 YearName'
 
 	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 208 site periods of the day
------------------------------------------------------------------

    SET @checkid = 208;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  SELECT 
			 Name,
			 convert(varchar(8),StartTime,108) as StartTime,
			 convert(varchar(8),EndTime,108) as EndTIme
		  FROM
			 PODTypesForSites
		  ORDER BY 
			 convert(varchar(8),StartTime,108),
			 convert(varchar(8),EndTime,108)'	  
 
 	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
-- 209 terminals and if they're online or not
------------------------------------------------------------------

------------------------------------------------------------------
END -- end of @analyse
------------------------------------------------------------------

IF (@rebuildindexes) > 0 
BEGIN

------------------------------------------------------------------
-- 301 rebuild indexes
------------------------------------------------------------------
	   
    SET @checkid = 301;
    SET @checkstarttime = getdate();

    -- check that there is enough space on the hard drive to build the indexes again
    -- check that rebuilding indexes won't exceed the 10GB express threshold
    -- different step if the sitetype is headoffice

    BEGIN TRY
	   SET @statement = '
		  IF object_id(N''tempdb..#FragmentedIndexes'') IS NOT NULL 
			 BEGIN  
				DROP TABLE #FragmentedIndexes
			 END;

		  CREATE TABLE #FragmentedIndexes -- create a temporary table to hold the index fragmentation
			 (
				DatabaseName sysname, 
				SchemaName sysname, 
				TableName sysname, 
				IndexName sysname, 
				FragmentationPercent float
			 )

		  INSERT INTO #FragmentedIndexes -- inserts index fragmentation information from the system tables into the temporarty table
			 SELECT
				DB_NAME(DB_ID()) AS DatabaseName, 
				ss.name AS SchemaName, 
				OBJECT_NAME (s.object_id) AS TableName, 
				i.name AS IndexName, 
				s.avg_fragmentation_in_percent AS FragmentationPercent
			 FROM 
				sys.dm_db_index_physical_stats(db_id(),NULL, NULL, NULL, ''SAMPLED'') s
				join sys.indexes i ON s.[object_id] = i.[object_id] and s.index_id = i.index_id
				join sys.objects o ON s.object_id = o.object_id
				join sys.schemas ss ON ss.[schema_id] = o.[schema_id]
			 WHERE
				s.database_id = DB_ID()  -- use within the current database context 
				and i.index_id != 0      -- index_id = 0 is a heap, ignoring these
				and s.record_count > 0   -- only get indexes with something in it
				and o.is_ms_shipped = 0  -- not shipped with sql server, therefore an index we need to worry about

		  DECLARE @RebuildIndexesSQL nvarchar(max) = '';
								 
			 SELECT
				@RebuildIndexesSQL = @RebuildIndexesSQL +
				CASE WHEN FragmentationPercent > 30 THEN CHAR(10) + ''ALTER INDEX '' + QUOTENAME(IndexName) + '' ON '' + QUOTENAME(SchemaName) + ''.'' + QUOTENAME(TableName) + '' REBUILD;''
				    WHEN FragmentationPercent > 10 THEN CHAR(10) + ''ALTER INDEX '' + QUOTENAME(IndexName) + '' ON '' + QUOTENAME(SchemaName) + ''.'' + QUOTENAME(TableName) + '' REORGANIZE;''
				END
			 FROM 
				#FragmentedIndexes
			 WHERE 
				FragmentationPercent > 10

		  DECLARE @startoffset int, 
				@length int,
				@startdate datetime;

		  SET @startoffset = 0
		  SET @length = 4000

			 WHILE (@StartOffset < len(@RebuildIndexesSQL)) -- start the rebuild routine 
			 BEGIN
				--SET @startdate = getdate()
				--INSERT NRSSync.dbo.QCIndexRebuildLog
				--SELECT substring(@RebuildIndexesSQL, @StartOffset, @Length), @startdate, getdate()
				PRINT substring(@RebuildIndexesSQL, @StartOffset, @Length)
				SET @StartOffset = @StartOffset + @Length
			 END
				--SELECT substring(@RebuildIndexesSQL, @StartOffset, @Length), @startdate, getdate()
				PRINT substring(@RebuildIndexesSQL, @StartOffset, @Length)
  
			 EXEC sp_executesql @RebuildIndexesSQL       -- reorganise or rebuild
  
		  DROP TABLE #FragmentedIndexes; -- drop the temporary table'

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
END;-- end of @rebuildindexes
------------------------------------------------------------------

------------------------------------------------------------------
-- 302 CheckDB all databases
------------------------------------------------------------------

IF @checkdb > 0
    BEGIN

    SET @checkid = 302;
    SET @checkstarttime = getdate();

    BEGIN TRY
	   SET @statement = '
		  EXEC sp_MSforeachDB ''DBCC CHECKDB (?) WITH NO_INFOMSGS, EXTENDED_LOGICAL_CHECKS, DATA_PURITY'''

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

------------------------------------------------------------------
END; -- checkdb
------------------------------------------------------------------	
    	   
    SET @checkid = 901;
    SET @checkstarttime = getdate();

    BEGIN TRY

	   SET @statement = '
		  SELECT * 
		  INTO ##check
		  FROM QCCheckErrorLog
		  WHERE CheckStartTime > '''+convert(varchar(12),@runtime)+''''
	   
	   EXEC sp_executesql @statement
	   
	   IF EXISTS (SELECT * FROM ##check)
	   BEGIN
		  INSERT ##issues
		  VALUES (@checkid, 'QCCheck  ', 'One or more queries may have failed to run. Check the error table for more details.', @statement, 1)
	   END;

	   IF object_id(N'TempDb..##check') IS NOT NULL
	   DROP TABLE ##check

	   EXEC sp_executesql @statement

	   SET @checkendtime = getdate();

    END TRY

    BEGIN CATCH
	   INSERT QCCheckErrorLog
	   VALUES (@checkid,@statement,@checkstarttime,@checkendtime,error_line(),error_message())
    END CATCH

    IF EXISTS (SELECT * FROM ##issues)
        
    DECLARE @sitename varchar(50),
		  @body nvarchar(max),
		  @subject varchar(200),
		  @mailist nvarchar(200);

    SET @sitename = (SELECT Name FROM Site)
    
    SET @mailist = N'lpopman@nrsltd.com'

    BEGIN
	   IF (@saveresults) > 0
	   BEGIN
		  BEGIN TRY
			 DECLARE @outputfile varchar(100), @filepath varchar(100), @bcpcmd varchar(1000)
			 
			 IF @resultslocation IS NOT NULL 
				--BEGIN
				 SET @filepath = CASE WHEN right(@resultslocation,1) = '\' THEN @resultslocation ELSE @resultslocation+'\' END
				--END;
			 --ELSE 
				--BEGIN
				 SET @filepath = 'C:\temp\'
 
						 
			 SET @bcpcmd = '
				bcp "SELECT CheckID,CheckType,Severity,Issue FROM ##issues" queryout '
			 
			 SET @outputfile = 'QCCheckDiagnostics-'+@sitename+'-'+convert(varchar,getdate(),105) + '.txt"'
			 
			 SET @bcpcmd = @bcpcmd + '"' + @filepath + @outputfile + ' -c -Unrs -Pnrs -S'+@@servername+''
			 
				EXEC xp_cmdshell @bcpcmd, no_output

				PRINT 'Sucessfully exported results to "'+@filepath+@outputfile
		  END TRY

		  BEGIN CATCH
			 RAISERROR('THERE WAS A PROBLEM WRITING THE RESULTS TO TEXT',10,1)WITH LOG, NOWAIT;
		  END CATCH
	   END;
    ELSE IF (@emailresults) > 0
	   BEGIN
		  BEGIN TRY 
				
			 SET @subject = 'QC Check Diagnostics Results for '+@sitename+' | '+convert(varchar,getdate(),105)

			 CREATE TABLE #mailcheck (result varchar(15))
			 INSERT #mailcheck
				EXEC msdb.dbo.sysmail_help_status_sp;

			 IF (SELECT result FROM #mailcheck) = 'STARTED'
				BEGIN
				
				    SET @body = N'<html><body>'+
					   N'<div>Diagnostic Query results</div>'+
					   N'<table>'+
					   N'<tr>'+
					   N'<th>CheckID</th>
						<th>CheckType</th>
						<th>Severity</th>
						<th>Issue</th>
						<th>Query</th>'+
					   N'</tr>'+
					   cast(
						  (
						  SELECT 
							 CheckID AS 'td', '',
							 CheckType AS 'td', '',
							 Severity AS 'td', '',
							 Issue AS 'td', '',
							 replace(replace(Query, ('"'),('''')),'INTO ##check', '') AS 'td', ''
						  FROM 
							 ##issues
						  FOR XML PATH('tr'), ELEMENTS 
						  ) AS nvarchar(max)
						  ) + N'</table></body></html>'
				    
				    EXEC msdb.dbo.sp_send_dbmail
					   @profile_name = 'Communication Status',
					   @body = @body,
					   @body_format = 'HTML',
					   @recipients = @mailist,
					   @subject = @subject
				
					   PRINT N'Mail sent successfully to '+@mailist

				END;
			 ELSE
				BEGIN
				    PRINT 'email results not configured yet'
				END;

			IF object_id(N'TempDb..#mailcheck') IS NOT NULL
			DROP TABLE #mailcheck

		  END TRY

		  BEGIN CATCH
			 SELECT error_message()
			 --RAISERROR('THERE WAS A PROBLEM WRITING THE RESULTS TO EMAIL',10,1)WITH LOG, NOWAIT;		  
		  END CATCH
	   END;
    ELSE
	   BEGIN
		  SELECT 
			 CheckID,
			 CheckType,
			 Severity,
			 Issue, 
			 replace(replace(Query, ('"'),('''')),'INTO ##check', '') as [Query to run for further investigation]
		  FROM 
			 ##issues
		  WHERE CheckID = 901
		  UNION ALL
		  SELECT 
			 CheckID,
			 CheckType,
			 Severity,
			 Issue, 
			 replace(replace(Query, ('"'),('''')),'INTO ##check', '') as [Query to run for further investigation]
		  FROM 
			 ##issues
	   END;
    END;

    IF object_id(N'TempDb..##issues') IS NOT NULL
    DROP TABLE ##issues

------------------------------------------------------------------
END;--end of stored procedure
------------------------------------------------------------------
GO