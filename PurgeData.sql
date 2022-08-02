USE [Proof]
GO

/****** Object:  StoredProcedure [dbo].[PurgeData]    Script Date: 7/30/2022 10:52:55 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		WTurner
-- Create date: 7/8/2018
-- Description:	This will DELETE data from the History Tables once it is xx years old.
-- =============================================
CREATE PROCEDURE [dbo].[PurgeData] 
	-- Add the parameters for the stored procedure here
	@Months	int = 36
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @PurgeDate Datetime;
	DECLARE @DeleteCtr INT = 0;
	DECLARE @BeforeCtr INT = 0;
	DECLARE @AfterCtr INT = 0;
	DECLARE @r INT = 1;

	SET @PurgeDate = DATEADD(MM,-1*abs(@Months), GETDATE());

	BEGIN TRY 
	--Delete ReviewLogHist
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[Incident];

		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) 
			  FROM [dbo].[Incident]
			 WHERE UpdatedOn < @PurgeDate
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[Incident];

		--Logging Purge Data for Company Records
		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[Incident]' );
		
	--Delete [dbo].[XrEvent] if no longer tied to an active Application
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[XrEvent];
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND (Note - Only deleting 50000 records at a time for efficiency & stability)		
			DELETE TOP (50000) A
	 		  FROM [dbo].[XrEvent] as A
				LEFT JOIN [dbo].[Incident] as B ON A.Id = B.EventId
			 WHERE A.EndDate < @PurgeDate
			   and b.EventId IS NULL;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[XrEvent];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[XrEvent]' );
		
		--Remove records older than As Of Date from the Purge Data Log
		DELETE A 
		  FROM _PurgeDataLog as A
		 WHERE PurgeRunDate < @PurgeDate;

		--Handle the Configuration tables
		--[dbo].[XrStatus] 
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[XrStatus];
		
		-- Set UpdatedBy to the last time the value was used
		Update A
		   set UpdatedOn = b.LastUsedDate
		  from [dbo].[XrStatus] as A
		  JOIN ( select StatusId, max(UpdatedOn) as LastUsedDate
				   from [dbo].[Incident]
				 Group by StatusId) as B on A.Id = b.StatusId
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) A
	 		  FROM [dbo].[XrStatus] as A
			 WHERE A.UpdatedOn < @PurgeDate;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[XrStatus];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[XrStatus]' );
		
		--[dbo].[XrPriority] 
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[XrPriority];
		
		-- Set UpdatedBy to the last time the value was used
		Update A
		   set UpdatedOn = b.LastUsedDate
		  from [dbo].[XrPriority] as A
		  JOIN ( select PriorityId, max(UpdatedOn) as LastUsedDate
				   from [dbo].[Incident]
				 Group by PriorityId) as B on A.Id = b.PriorityId
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) A
	 		  FROM [dbo].[XrPriority] as A
			 WHERE A.UpdatedOn < @PurgeDate;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[XrPriority];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[XrPriority]' );

		--[dbo].[XrConditions] 
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[XrConditions];
		
		-- Set UpdatedBy to the last time the value was used
		Update A
		   set UpdatedOn = b.LastUsedDate
		  from [dbo].[XrConditions] as A
		  JOIN ( select ConditionsId, max(UpdatedOn) as LastUsedDate
				   from [dbo].[Incident]
				 Group by ConditionsId) as B on A.Id = b.ConditionsId
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) A
	 		  FROM [dbo].[XrConditions] as A
			 WHERE A.UpdatedOn < @PurgeDate;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[XrConditions];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[XrConditions]' );
		
		
	--Delete [dbo].[AppAccess]
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[AppAccess];

		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) 
			  FROM [dbo].[AppAccess]
			 WHERE LastAccess < @PurgeDate
			   AND Active = 0
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[AppAccess];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[AppAccess]' );
		

		--Regions and Access Levels cannot be removed, so set to today
		
		--[dbo].[Regions] 
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[Regions];
		
		-- Set UpdatedBy to the last time the value was used
		Update [dbo].[Regions] set UpdatedOn = getdate()
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) A
	 		  FROM [dbo].[Regions] as A
			 WHERE A.UpdatedOn < @PurgeDate;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[Regions];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[Regions]' );
		
		
		--[dbo].[XrAccessLevel] 
		SELECT @r = 1, @BeforeCtr= 0, @DeleteCtr= 0, @AfterCtr = 0;
		SELECT @BeforeCtr = COUNT(*) FROM [dbo].[XrAccessLevel];
		
		-- Set UpdatedBy to the last time the value was used
		Update [dbo].[XrAccessLevel] set UpdatedOn = getdate()
				
		WHILE @r > 0
		BEGIN  
			--DELETE COMMAND		
			DELETE TOP (50000) A
	 		  FROM [dbo].[XrAccessLevel] as A
			 WHERE A.UpdatedOn < @PurgeDate;
		 
			SET @r = @@ROWCOUNT;
			SET @DeleteCtr = @DeleteCtr + @r;
		END;

		SELECT @AfterCtr = COUNT(*) FROM [dbo].[XrAccessLevel];

		INSERT INTO [dbo].[_PurgeDataLog] ( [PurgeRunDate], [BeforeCount], [RecordCount], [AfterCount], [TableName] )
		VALUES ( Getdate(), @BeforeCtr, @DeleteCtr, @AfterCtr, '[dbo].[XrAccessLevel]' );
		



	END TRY
	BEGIN CATCH
		SELECT 
			 ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,ERROR_PROCEDURE() AS ErrorProcedure
			,ERROR_LINE() AS ErrorLine
			,ERROR_MESSAGE() AS ErrorMessage;

		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION;
	END CATCH;
END
GO


