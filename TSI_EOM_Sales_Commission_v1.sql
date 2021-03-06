
/****** Object:  StoredProcedure [dbo].[sp_Reporting_DailyTransactions_Trintech_BATCH_v8]    Script Date: 8/9/2016 11:26:00 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
GO
/***************************************************************************************************************************
Author: Seth Low
Created Date: 4/7/2016
Description: Nightly extract for TSI that they are using to help detect billing errors. This should show them 
	all BATCH transactions done the previous day.

For full billing month dates should be set
	@CurrentWaterMark = 1/1/2016
	@NewWaterMark = 2/1/2016
***************************************************************************************************************************/
CREATE PROCEDURE [dbo].[sp_Reporting_EOM_SalesCommision_v1]
	@CurrentWaterMark DATETIME = NULL, --This is the begin time
	@NewWaterMark DATETIME = NULL OUTPUT --This is the end time
	

AS 
BEGIN



	IF(@CurrentWaterMark IS NULL)
		BEGIN
			SET @CurrentWaterMark = CONVERT(DATE,GETDATE(),101) --CurrentDate
		END
	SET @NewWaterMark = CONVERT(DATE,GETDATE(),101)




	/*
	For the time being this is being left as a function call, however a future version will bring 
	all the code from the function into this SP. This is being done to test and deploy a extract on 
	Grant's processor.
	*/

	SELECT *
	FROM dbo.TSI_CommSalesExport(
								DATEADD(DAY,1, EOMONTH(@CurrentWaterMark,-2))--Begining of Previous Month	
								, EOMONTH(GETDATE(),-1) -- End of Previous Month
								, 1
								, null
								)
	ORDER BY 
		TransactionDate
		, TxTransactionID
	

END
