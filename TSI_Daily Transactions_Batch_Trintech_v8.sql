
/***************************************************************************************************************************
Author: Seth Low
Created Date: 4/7/2016
Description: Nightly extract for TSI that they are using to help detect billing errors. This should show them 
	all BATCH transactions done the previous day.

For full billing month dates should be set
	@CurrentWaterMark = 1/1/2016
	@NewWaterMark = 2/1/2016
***************************************************************************************************************************/
CREATE PROCEDURE dbo.sp_Reporting_DailyTransactions_Trintech_BATCH_v8
	@CurrentWaterMark DATETIME = NULL, --This is the begin time
	@NewWaterMark DATETIME = NULL OUTPUT --This is the end time
	
	, @MosoPayTransactionCode VARCHAR(50) = NULL
	, @PaymentID INT = NULL
	, @Member VARCHAR(20) = NULL
	--, @MosoPayRef VARCHAR(50) = NULL

	, @Help BIT = 0

AS 
BEGIN


IF( OBJECT_ID('tempdb..#Str_PaymentTarget') IS NOT NULL) DROP TABLE #Str_PaymentTarget
;
	--DECLARE
	--	@CurrentWaterMark DATETIME = '10/1/2015',--NULL,
	--	@NewWaterMark DATETIME = '11/1/2015'-- NULL 
	
	----, @MosoPayTransactionCode VARCHAR(50) = NULL
	--, @PaymentID INT = 16910155
	----, @Member VARCHAR(20) = NULL
	----, @MosoPayRef VARCHAR(50) = NULL

	--, @Help BIT = 0

	IF(@CurrentWaterMark IS NULL)
		BEGIN
			SET @CurrentWaterMark = DATEADD(DAY, -1, CONVERT(DATE,GETDATE(),101))
		END
	SET @NewWaterMark = IIF(@NewWaterMark IS NULL ,DATEADD(d, 1, @CurrentWaterMark), @NewWaterMark)
 

;WITH 
	CTE_Paymenttarget
		AS 
			(

			SELECT p.TxPaymentID
						, p.PaymentProcessRequestId AS MOSOPayTransactionCode
						, p.PaymentProcessBatchId AS BatchID 
						, tp.TargetDate AS PaymentDate
						--, tp.Amount AS TotalPaymentAmount
						, tp.CreditCardTypeId
						, tp.TenderTypeID
						, t.Description
						, SUM(t.Amount) AS tr_Amount
						, t.IsAccountingCredit
						, t.TxInvoiceID
						, tp.IsDeclined
						, tp.PartyRoleId
				FROM dbo.PaymentProcessRequest p (NOLOCK) 
				INNER JOIN dbo.TxPayment tp (NOLOCK)  ON tp.TxPaymentId = p.TxPaymentID
				INNER JOIN dbo.PartyRole r (NOLOCK) ON r.PartyRoleID = tp.PartyRoleId 
				LEFT JOIN dbo.Txtransaction t (NOLOCK) ON t.ItemID = tp.TxPaymentID AND t.TxTypeId = 4
				LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging] es (NOLOCK) ON es.ReportName = 'TSI_Daily_Transaction_Batch_Trintech'
																							AND es.Deleted = 0
																							AND es.Criteria = 'TenderTypeID'
																							AND es.Value = tp.TenderTypeID
				WHERE 1=1
					AND es.ExclusionID IS NULL
					AND CONVERT(DATE,tp.TargetDATE,101) >= @CurrentWaterMark-- '12/1/2015'--
					AND CONVERT(DATE,tp.TargetDate,101) < @NewWaterMark --'1/1/2016'  --

					AND CAST(r.RoleID AS VARCHAR(20)) = IIF(@Member IS NULL, CAST(r.RoleID AS VARCHAR(20)), @Member) 
					AND p.TxPaymentID = IIF(@PaymentID IS NULL, p.TxPaymentID, @PaymentID)
					AND p.PaymentProcessRequestID = IIF(@MosoPayTransactionCode IS NULL, p.PaymentProcessRequestID, @MosoPayTransactionCode)
					--AND tp.Reference = IIF(@MosoPayRef IS NULL, tp.Reference, @MosoPayRef)
					
					--AND tp.TenderTypeID = 162
				GROUP BY 
					p.TxPaymentID
						, p.PaymentProcessRequestId 
						, p.PaymentProcessBatchId 
						, tp.TargetDate 
						, tp.CreditCardTypeId
						, tp.TenderTypeID
						, t.Description
						, t.Amount 
						, t.IsAccountingCredit
						, t.TxInvoiceID
						, tp.PartyRoleId 
						, tp.IsDeclined
			
			)


---->>> Report result query
		
SELECT --TOP 100 
		pr.RoleId
		, p.TxPaymentID
		, i.TxInvoiceID
		, b.name AS BusinessUnit
		, b.GLCodePrefix
		, b.Code
		, p.MOSOPayTransactionCode 
		, p.BatchID
		, 'Batch' AS Category
		, 'Payment' AS TransCode
		, res.ReferenceId
		, p.PaymentDate
		, i.TargetDate AS [InvoiceDate]
		, tt.Name AS [TenderType]
		, cct.Description AS [CC Description]
		, cct.Abbreviation AS [CC Abreviation]
		, ef.Token
		, ef.Mask 
		, ef.ExpirationDate
		, ef.MerchantCode
		, res.ResponseCode
		, res.ResponseMessage
		, IIF(p.IsAccountingCredit = 1,'TRUE','FALSE') AS [Debit/Credit Indicator]
		--, p.Py_Amount AS PaymentAmount
		, p.Tr_Amount AS TransactionAmount
		, g.Description AS CashGLAcct
		, CAST(mid.[VS/MC MOTO] AS VARCHAR(50))  AS [VS/MC MOTO]
		, CAST(mid.[VS/MC RETAIL] AS VARCHAR(50)) AS [VS/MC RETAIL]
		, CAST(mid.[AMEX MOTO] AS VARCHAR(50)) AS [AMEX MOTO]
		, CAST(mid.[AMEX RETAIL] AS VARCHAR(50)) AS [AMEX RETAIL] 
		, CAST(mid.[NEW MOTO SE#] AS VARCHAR(50)) AS [NEW MOTO SE#]
		, CAST(mid.[NEW RETAIL SE#] AS VARCHAR(50)) AS [NEW RETAIL SE#]
		, IIF((p.IsDeclined = 1 AND res.ResponseCode LIKE 'A%'), 1, 0) AS Chargeback_ind
FROM CTE_Paymenttarget p
	INNER JOIN dbo.TxInvoice i ON i.TxInvoiceid = p.TxInvoiceId
	INNER JOIN dbo.PaymentProcessResponse res ON res.PaymentProcessRequestId = p.MOSOPayTransactionCode
	LEFT JOIN dbo.TxPaymentEft ef ON ef.TxPaymentId = p.TxPaymentID  --->>> Converted to LEFT JOIN to address Chargeback reversals

	LEFT JOIN [METAALIAS].FocusMeta.dbo.CreditCardType as cct on p.CreditCardTypeId = cct.CreditCardTypeID 
	LEFT JOIN dbo.PartyRole pr ON pr.PartyRoleId = p.PartyRoleId
	LEFT JOIN dbo.TenderType tt ON p.TenderTypeID = tt.TenderTypeID
	LEFT JOIN dbo.GeneralLedgerCode g ON g.GeneralLedgerCodeId = tt.GeneralLedgerCodeId

	LEFT JOIN dbo.BusinessUnit b (NOLOCK) ON b.BusinessUnitId = i.TargetBusinessUnitID
	LEFT JOIN TSI_Tactical.dbo.Ref_TSI_MIDs_Reference mid (NOLOCK) ON  b.GLCodePrefix = mid.OrgID
WHERE 1=1
GROUP BY 
	pr.RoleId	
	, pr.PartyID
	, p.TxPaymentID
	, p.MOSOPayTransactionCode
	, res.ReferenceId
	, i.TxInvoiceID
	, p.PaymentDate
	, ef.Token
	, ef.ExpirationDate
	, ef.MerchantCode
	, res.ResponseCode
	, res.ResponseMessage
	, p.IsAccountingCredit
	--, p.Py_Amount
	, p.Tr_Amount
	, res.PaymentProcessRequestId
	, p.BatchID
	, cct.Description 
	, cct.Abbreviation
	, b.name
	, b.GLCodePrefix
	, b.Code
	, tt.Name
	, g.Description
	, ef.Mask
	, mid.[VS/MC MOTO]
	, mid.[VS/MC RETAIL]
	, mid.[AMEX MOTO]
	, mid.[AMEX RETAIL]
	, mid.[NEW MOTO SE#]
	, mid.[NEW RETAIL SE#]
	, i.TargetDate
	, p.IsDeclined 
	, p.Description


IF (@Help = 1)
	BEGIN
		PRINT N'Variables:'
		PRINT N'@CurrentWaterMark: This is a DATE field and should be the first date of the report inquestion'
		PRINT N'@NewWaterMark: (OPTIONAL) This is a DATE field and should be the day after the last day wanted in the report. For example, report ending on 1/31 should have a date of 2/1, for automated reports this will get auto-populated.'
		PRINT N'@MosoPayTransactionCode: (OPTIONAL) PaymentProcessRequestID of the transaction in question defaulted to NULL'
		PRINT N'@PaymentID: (OPTIONAL) The TxPaymentID for the transaction in question, defaulted to NULL'
		PRINT N'@Member: (OPTIONAL) The MemberID for the transaction in question, detaulted to NULL'
		PRINT N'@MosoPayRef: (OPTIONAL) The REFERENCE field on TxPayment for the transaction in question, defaulted to NULL'
	END
	

END
