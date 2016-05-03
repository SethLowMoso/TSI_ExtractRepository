
/***************************************************************************************************************************
Author: Seth Low
Created Date: 4/7/2016
Description: Nightly extract for TSI that they are using to help detect billing errors. This should show them 
	all POS transactions done the previous day.

For full billing month dates should be set
	@CurrentWaterMark = 1/1/2016
	@NewWaterMark = 2/1/2016
***************************************************************************************************************************/
CREATE PROCEDURE dbo.sp_Reporting_DailyTransactions_Trintech_POS_v8
	@CurrentWaterMark DATETIME = NULL, --This is the begin time
	@NewWaterMark DATETIME = NULL OUTPUT --This is the end time
	
	--, @MosoPayRef VARCHAR(50) = NULL --THIS HAS BEEN REMOVED FROM PRODUCTION BECAUSE IT CAUSED CERTAIN RESULTS TO DROP OUT
	, @PaymentID INT = NULL
	, @Member VARCHAR(20) = NULL

	, @Help BIT = 0

AS 
BEGIN

if (object_id('tempdb..#STR_PaymentData') is not null) drop table #STR_PaymentData;

--DECLARE
--	@CurrentWaterMark DATETIME = '10/1/2015',
--	@NewWaterMark DATETIME = '11/1/2015' --This is the end time

--	--, @MosoPayRef VARCHAR(50) = NULL --This has been commented out due to it causing certain results to drop out. 
--	, @PaymentID INT = 16864700
--	, @Member VARCHAR(20) = NULL
	IF(@CurrentWaterMark IS NULL)
		BEGIN
			SET @CurrentWaterMark = DATEADD(DAY, -1, CONVERT(DATE,GETDATE(),101))
		END
	SET @NewWaterMark = IIF(@NewWaterMark IS NULL ,DATEADD(d, 1, @CurrentWaterMark), @NewWaterMark)
	

;WITH
	CTE_PaymentData
		AS (

			SELECT p.PartyRoleId
					, p.LinkId
					, p.LinkTypeID
					, p.TxPaymentID
					, p.Reference AS [MOSOPay-Reference]
					, p.TargetDate AS [PaymentDate]
					, p.Amount AS TotalPaymentAmount
					, p.TenderTypeID 
					, p.TargetBusinessUnitId
					, p.CreditCardTypeId
					, p.IsDeclined
					, t.TxInvoiceID
					, SUM(t.Amount) AS TransactionAmount
					, t.IsAccountingCredit
					, t.TxTransactionID
					, t.WorkUnitId
					, ef.Token
					, ef.Mask 
					, ef.ExpirationDate
					, b.POSMerchantAccountCode AS MerchantCode
					, ef.ResponseCode
					, ef.ResponseMessage
					, pr.PaymentProcessRequestID AS MOSOPayTransactionCode
			FROM dbo.TxPayment p (NOLOCK)
			LEFT JOIN dbo.TxPaymentEft ef (NOLOCK) ON ef.TxPaymentId = p.TxPaymentID  --->>> Shifted to LEFT To include Chargeback reversals
			LEFT JOIN dbo.TxTransaction t (NOLOCK) ON t.ItemID = p.TxPaymentID AND t.TxTypeId = 4
			LEFT JOIN dbo.PaymentProcessRequest pr (NOLOCK) ON pr.TxPaymentID = p.TxPaymentID
			LEFT JOIN dbo.PartyRole r (NOLOCK) ON r.PartyRoleID = p.PartyRoleId
			LEFT JOIN dbo.BusinessUnit b (NOLOCK) ON b.BusinessUnitId = p.TargetBusinessUnitId
			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging] es (NOLOCK) ON es.ReportName = 'TSI_Daily_Transactions_POS_Trintech'
																							AND es.Deleted = 0
																							AND es.Criteria = 'TenderTypeID'
																							AND es.Value = p.TenderTypeID
			WHERE 1=1
				AND es.ExclusionID IS NULL
				AND CONVERT(DATE,p.TargetDATE,101) >= @CurrentWaterMark -- '11/1/2015' -- 
				AND CONVERT(DATE,p.TargetDate,101) <  @NewWaterMark  -- '12/1/2015' -- 
				AND pr.TxPaymentId  IS NULL
				AND CAST(r.RoleID AS VARCHAR(20)) = IIF(@Member IS NULL, CAST(r.RoleID AS VARCHAR(20)), @Member) 
				AND p.TxPaymentID = IIF(@PaymentID IS NULL, p.TxPaymentID, @PaymentID)
				--AND p.Reference = IIF(@MosoPayRef IS NULL, p.Reference, @MosoPayRef) 


			GROUP BY
					p.PartyRoleId
					, p.TxPaymentID
					, p.Reference
					, p.TargetDate
					, p.Amount
					, p.TenderTypeID 
					, t.Description
					, p.TargetBusinessUnitId
					, p.CreditCardTypeId
					, p.IsDeclined
					, t.TxInvoiceID
					, t.Amount
					, t.IsAccountingCredit
					, t.TxTransactionID
					, ef.Token
					, ef.Mask 
					, ef.ExpirationDate
					, b.POSMerchantAccountCode
					, ef.ResponseCode
					, ef.ResponseMessage
					, pr.PaymentProcessRequestID
					, t.WorkUnitId
	
					, p.LinkId
					, p.LinkTypeID
		
			
			)
					SELECT
							pr.RoleId
							, p.TxPaymentID

							, p.TxInvoiceID
							, b.name AS BusinessUnit
							, b.GLCodePrefix
							, b.Code
							, ISNULL(p.MOSOPayTransactionCode,'') AS MOSOPayTransactionCode
							, p.[MOSOPay-Reference]
							, '' AS BatchID
							, 'POS' AS Category
							, '' AS ReferenceID
							, 'Payment' AS TransCode--t.TxTypeId
							, p.[PaymentDate]
							, i.TargetDate AS [InvoiceDate]
							, tt.Name AS [TenderType]
							, cct.Description AS [CC Description]
							, cct.Abbreviation AS [CC Abreviation]
							, p.Token
							, p.Mask 
							, p.ExpirationDate
							, p.MerchantCode
							, p.ResponseCode
							, p.ResponseMessage
							, IIF(p.IsAccountingCredit = 1, 'TRUE', 'FALSE') AS [Debit/Credit Indicator]
							, p.TransactionAmount
							, g.Description AS CashGLAcct							
							
							, CAST(mid.[VS/MC MOTO] AS VARCHAR(50))  AS [VS/MC MOTO]
							, CAST(mid.[VS/MC RETAIL] AS VARCHAR(50)) AS [VS/MC RETAIL]
							, CAST(mid.[AMEX MOTO] AS VARCHAR(50)) AS [AMEX MOTO]
							, CAST(mid.[AMEX RETAIL] AS VARCHAR(50)) AS [AMEX RETAIL] 
							, CAST(mid.[NEW MOTO SE#] AS VARCHAR(50)) AS [NEW MOTO SE#]
							, CAST(mid.[NEW RETAIL SE#] AS VARCHAR(50)) AS [NEW RETAIL SE#]

							, IIF((p.IsDeclined = 1 AND p.ResponseCode LIKE 'A%'), 1, 0) AS Chargeback_ind

							
			--INTO #Temp
			FROM CTE_PaymentData p
				LEFT JOIN [METAALIAS].FocusMeta.dbo.CreditCardType as cct on p.CreditCardTypeId = cct.CreditCardTypeID 
				LEFT JOIN dbo.PartyRole pr ON pr.PartyRoleId = p.PartyRoleId
				LEFT JOIN dbo.TenderType tt ON p.TenderTypeID = tt.TenderTypeID
				LEFT JOIN dbo.GeneralLedgerCode g ON g.GeneralLedgerCodeId = tt.GeneralLedgerCodeId
				LEFT JOIN dbo.BusinessUnit b (NOLOCK) ON b.BusinessUnitId = p.TargetBusinessUnitID
				LEFT JOIN TSI_Tactical.dbo.Ref_TSI_MIDs_Reference mid (NOLOCK) ON  b.GLCodePrefix = mid.OrgID
				LEFT JOIN dbo.TxInvoice i ON i.TxInvoiceID = p.TxInvoiceId
			WHERE 1=1

			GROUP BY 
				pr.RoleId	
				, pr.PartyID
				, p.TxPaymentID
				, p.IsDeclined
				, p.TxInvoiceID
				, p.PaymentDate
				, b.Name
				, p.Token
				, p.Mask
				, g.Description
				, p.ExpirationDate
				, p.MerchantCode
				, p.ResponseCode
				, p.ResponseMessage	
				, b.GLCodePrefix
				, p.IsAccountingCredit
				, p.TransactionAmount
				, b.Code
				, cct.Description 
				, cct.Abbreviation
				, p.[MOSOPay-Reference]
				--, t.TxTypeId
				, tt.Name
				, mid.[VS/MC MOTO]
				, mid.[VS/MC RETAIL]
				, mid.[AMEX MOTO]
				, mid.[AMEX RETAIL]
				, mid.[NEW MOTO SE#]
				, mid.[NEW RETAIL SE#]
				, i.TargetDate
				, p.MOSOPayTransactionCode
				, p.TxTransactionID



--IF (@Help = 1)
--	BEGIN
--		PRINT N'Variables:'
--		PRINT N'@CurrentWaterMark: This is a DATE field and should be the first date of the report inquestion'
--		PRINT N'@NewWaterMark: (OPTIONAL) This is a DATE field and should be the day after the last day wanted in the report. For example, report ending on 1/31 should have a date of 2/1, for automated reports this will get auto-populated.'
--		PRINT N'@PaymentID: (OPTIONAL) The TxPaymentID for the transaction in question, defaulted to NULL'
--		PRINT N'@Member: (OPTIONAL) The MemberID for the transaction in question, detaulted to NULL'
--		PRINT N'@MosoPayRef: (OPTIONAL) The REFERENCE field on TxPayment for the transaction in question, defaulted to NULL'
--	END

END