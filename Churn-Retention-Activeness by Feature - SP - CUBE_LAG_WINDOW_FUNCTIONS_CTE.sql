DECLARE @StartDate  AS DATE = '2022-11-01'--'2022-01-01'--'2022-06-01'--'2017-01-01'--'2016-08-23'-- '2022-11-01'--'2016-08-23'
	   ,@EndDate	AS DATE = '2023-07-01'--'2018-01-01'--'2018-01-01'--'2023-05-01'
	   ,@SPDate		AS DATE = DATEADD(DAY,1,EOMONTH(GETDATE(),-2))--@StartDate = DATEADD(DAY,1,EOMONTH(GETDATE(),-7)) ; @EndDate = DATEADD(DAY,1,EOMONTH(GETDATE(),-1)) 
;WITH RetentionParameters AS
(	SELECT UserKey,DATEADD(DAY,1,EOMONTH(CreateDate,-1)) MonthIndicator,FeatureType, COUNT(UserKey) TxCount
	from
	(
		SELECT UserKey,CreateDate,FeatureType,IsCancellation FROM			[DWH_Papara].[DBO].[FACT_Transactions] with (Nolock) WHERE IsCancellation = 0
		--UNION
		--SELECT UserKey,CreateDate,FeatureType,IsCancellation FROM    [Ledger2020Before].[DBO].[FACT_Transactions] with (Nolock) WHERE IsCancellation = 0
	) k
	WHERE CreateDate >= @StartDate AND CreateDate < @EndDate group by UserKey,DATEADD(DAY,1,EOMONTH(CreateDate,-1)),FeatureType
),RankingParameters AS
(
	select distinct UserKey		  , 
					MonthIndicator,
					FeatureType	  ,
				  --TxCount		  ,
					/*LOGICAL FLAGGING*/CASE WHEN TxCount <= 10 THEN 0 else 1 END  IsTxCountBiggerThan10
	from RetentionParameters 
),ActivenessFlags AS
(
	select
		   MonthIndicator
		  ,UserKey
		  ,			   /*CUBIC FLAGGING*/COALESCE(FeatureType,100) FeatureType
		  ,			   /*CUBIC FLAGGING*/COALESCE(IsTxCountBiggerThan10,100) IsTxCountBiggerThan10
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-2)) = LAG(MonthIndicator)   OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHadPreviousMonth
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-3)) = LAG(MonthIndicator,2) OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHad2MonthsAgo
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-4)) = LAG(MonthIndicator,3) OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHad3MonthsAgo
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-5)) = LAG(MonthIndicator,4) OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHad4MonthsAgo
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-6)) = LAG(MonthIndicator,5) OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHad5MonthsAgo
		  ,/*COUNTING ELEMENT ASSIGNING*/IIF(DATEADD(DAY,1,EOMONTH(MonthIndicator,-7)) = LAG(MonthIndicator,6) OVER (PARTITION BY UserKey,COALESCE(FeatureType,100),COALESCE(IsTxCountBiggerThan10,100) ORDER BY MonthIndicator), 1 ,NULL) IsActiveTxAlsoHad6MonthsAgo
	from RankingParameters
	group by MonthIndicator,UserKey, CUBE(FeatureType,IsTxCountBiggerThan10)
)--, UserLogins AS
--(
--	select
--		User_Key UserKey,DATEADD(DAY,1,EOMONTH(CreateDate,-1)) MonthIndicator,COUNT(Id) LoginCount
--	from [DWH_Papara].[DBO].[FACT_Logins] with (Nolock)
--sabah düşünülür...
--)
,Counting AS
(
	SELECT
		MonthIndicator
	   ,		   /*WITH CUBIC FLAG*/FeatureType
	   ,		   /*WITH CUBIC FLAG*/IsTxCountBiggerThan10
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(UserKey)						UU
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHadPreviousMonth) ActiveTxAlsoHadPreviousMonth
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHad2MonthsAgo	)   ActiveTxAlsoHad2MonthsAgo
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHad3MonthsAgo	)   ActiveTxAlsoHad3MonthsAgo
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHad4MonthsAgo	)   ActiveTxAlsoHad4MonthsAgo
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHad5MonthsAgo	)   ActiveTxAlsoHad5MonthsAgo
	   ,/*COUNTING ASSIGNED ELEMENTS*/COUNT(IsActiveTxAlsoHad6MonthsAgo	)   ActiveTxAlsoHad6MonthsAgo
	FROM ActivenessFlags
	GROUP BY MonthIndicator,FeatureType,IsTxCountBiggerThan10
),ReadyToInsertData AS
(
SELECT
		  MonthIndicator
	   ,/*FIXED FLAG*/FeatureType
	   ,/*FIXED FLAG*/IsTxCountBiggerThan10
	   ,  UU
	   ,  ActiveTxAlsoHadPreviousMonth
	   ,  ActiveTxAlsoHad2MonthsAgo
	   ,  ActiveTxAlsoHad3MonthsAgo
	   ,  ActiveTxAlsoHad4MonthsAgo
	   ,  ActiveTxAlsoHad5MonthsAgo
	   ,  ActiveTxAlsoHad6MonthsAgo
	   ,  MAX(ActiveTxAlsoHadPreviousMonth) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU)   OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHadPreviousMonth
	   ,  MAX(ActiveTxAlsoHad2MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,2) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHad2MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad3MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,3) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHad3MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad4MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,4) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHad4MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad5MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,5) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHad5MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad6MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,6) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) RetentionRateAlsoHad6MonthsAgo
	   ,1-MAX(ActiveTxAlsoHadPreviousMonth) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU)   OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHadPreviousMonth
	   ,1-MAX(ActiveTxAlsoHad2MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,2) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHad2MonthsAgo	
	   ,1-MAX(ActiveTxAlsoHad3MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,3) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHad3MonthsAgo	
	   ,1-MAX(ActiveTxAlsoHad4MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,4) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHad4MonthsAgo	
	   ,1-MAX(ActiveTxAlsoHad5MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,5) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHad5MonthsAgo	
	   ,1-MAX(ActiveTxAlsoHad6MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / LAG(UU,6) OVER (PARTITION BY				 FeatureType,IsTxCountBiggerThan10 ORDER BY MonthIndicator) ChurnRateAlsoHad6MonthsAgo
	   ,  MAX(ActiveTxAlsoHadPreviousMonth) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHadPreviousMonth
	   ,  MAX(ActiveTxAlsoHad2MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHad2MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad3MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHad3MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad4MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHad4MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad5MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHad5MonthsAgo	
	   ,  MAX(ActiveTxAlsoHad6MonthsAgo	  ) OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)*1.0 / MAX(UU)   OVER (PARTITION BY MonthIndicator,FeatureType,IsTxCountBiggerThan10)						  ActiveUURateAlsoHad6MonthsAgo

FROM  Counting
)
--INSERT INTO BI_Workspace..FACT_TransactionsFeatureTypeChurnAnalysis
SELECT FORMAT(MonthIndicator, 'yyyyMM') MonthKey
		,/*FIXED FLAG*/FeatureType
		,/*FIXED FLAG*/IsTxCountBiggerThan10
		,  UU
		,  ActiveTxAlsoHadPreviousMonth
		,  ActiveTxAlsoHad2MonthsAgo
		,  ActiveTxAlsoHad3MonthsAgo
		,  ActiveTxAlsoHad4MonthsAgo
		,  ActiveTxAlsoHad5MonthsAgo
		,  ActiveTxAlsoHad6MonthsAgo
		,		RetentionRateAlsoHadPreviousMonth
		,		RetentionRateAlsoHad2MonthsAgo
		,		RetentionRateAlsoHad3MonthsAgo
		,		RetentionRateAlsoHad4MonthsAgo
		,		RetentionRateAlsoHad5MonthsAgo
		,		RetentionRateAlsoHad6MonthsAgo
		,		ChurnRateAlsoHadPreviousMonth
		,		ChurnRateAlsoHad2MonthsAgo
		,		ChurnRateAlsoHad3MonthsAgo
		,		ChurnRateAlsoHad4MonthsAgo
		,		ChurnRateAlsoHad5MonthsAgo
		,		ChurnRateAlsoHad6MonthsAgo
		,		ActiveUURateAlsoHadPreviousMonth
		,		ActiveUURateAlsoHad2MonthsAgo
		,		ActiveUURateAlsoHad3MonthsAgo
		,		ActiveUURateAlsoHad4MonthsAgo
		,		ActiveUURateAlsoHad5MonthsAgo
		,		ActiveUURateAlsoHad6MonthsAgo
INTO #T3MP
FROM ReadyToInsertData where MonthIndicator >= '2023-06-01'/*@SPDate*/

/*TESTING*/
select * from #T3MP where FeatureType = 2
select * from FACT_TransactionsFeatureTypeChurnAnalysis with (nolock) where FeatureType = 2 and MonthKey = 202305