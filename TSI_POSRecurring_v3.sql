
DROP PROCEDURE [dbo].[sp_Reporting_TSI_POSRecurringExport_V3]
/****** Object:  UserDefinedFunction [dbo].[TSI_POSRecurringExport_V2]    Script Date: 12/16/2014 4:44:00 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Chris Magers
-- Create date: 16-Nov, 2012
-- Modified:
--			16-Nov,2012 an: Initial Version
--			19-Nov,2012: Added getting UTC times and converting to business timezone per Ardeshir
--			02-Jan,2013: Excluded external agreements per TSI request
--			04-Jan,2013: Excluded tender type of 102.  Added JOIN in TD subquery
--			16-Jan,2013: Added Ardeshir's rewrite of the code
--			17-Jul,2013: Changed financial fields (GrossAmount, Discount, TotalTax, RecurringBillingDiscountAmountNoTaxes, BillingAmount) to only include transaction activity for reporting period
-- Description:	Dimension table for Freeze Reasons
-- =============================================
CREATE PROCEDURE [dbo].[sp_Reporting_TSI_POSRecurringExport_V3]
(
	@fromDateLocal DATE,
	@toDateLocal DATE,
	@userBusinessUnitId INT,
	@filterBusinessUnitId INT = NULL
	)
	
	AS
	BEGIN
			DECLARE @UTC_from DATE = Tenant_TSI.dbo.DateTimeLocalToUtc(@fromDateLocal,(SELECT TimeZoneName FROM Tenant_TSI.dbo.BusinessUnit WHERE BusinessUnitId = @userBusinessUnitId))
				, @UTC_toNextDay DATE = Tenant_TSI.dbo.DateTimeLocalToStartOfNextDayInUtc(@toDateLocal,(SELECT TimeZoneName FROM Tenant_TSI.dbo.BusinessUnit WHERE BusinessUnitId = @userBusinessUnitId))


			IF(OBJECT_ID('#GLTransactions_Root') IS NOT NULL) DROP TABLE #GLTransactions_Root;

			SELECT PartyRoleID
					, BusinessUnitId
					, TxInvoiceID
					, TxTransactionID
					, TXGroup
					, TxTypeId
					, TxPaymentId
					, LinkTypeId
					, MemberAgreementID
					, Quantity
					, SaleItemId
					, Time_UTC
			INTO #GLTransactions_Root
			FROM 	Tenant_TSI.dbo.TSI_GLTransactions_ROOT(	@fromDateLocal, DATEADD(DAY, 1, @toDateLocal), 	@userBusinessUnitId, @filterBusinessUnitId) GLTX

	

			SELECT	      DISTINCT  --DISTINCT is only being used as a temporary fix until the second signatures can be filtered. 
				  COALESCE(GM_BU.Code, '') AS BusinessUnitCode
				  ,COALESCE(GM_BU.GLCodePrefix, '') AS GLCodePrefix
				  ,COALESCE(PR.RoleID, '') AS MemberRoleId
				  ,COALESCE(SOLD_BU.Code, '') AS   PTBusinessUnitCode
				  ,COALESCE(SOLD_BU.GLCodePrefix, '') AS GLCodePrefixBusinessUnit
				  ,COALESCE(ITM.ItemRecordID, '') AS ItemCode            
				  ,(CASE
						WHEN PT_MAI.IsKeyItem = 1 THEN N'Y'
						WHEN PT_MAI.IsKeyItem = 0 THEN N'N'
						ELSE ''
				   END) AS KeyItem
				,(CASE 
					WHEN PT_MAIP.MemberAgreementItemPerpetualId IS NOT NULL THEN
							N'Recurring'
					ELSE
							N'POS'
					END) AS InvoiceOriginationType
				,(ISNULL(
						(SELECT SUM(CASE _tx.IsAccountingCredit 
										WHEN 0 THEN _tx.Amount
										ELSE -_tx.Amount
									END
									)
								FROM Tenant_TSI.dbo.TxTransaction _tx
								WHERE
									_tx.TargetDate_UTC between @UTC_from and @UTC_toNextDay 
									AND _tx.TxTypeId = 1
									AND _tx.TxInvoiceId = GLTX.TxInvoiceId
									AND _tx.GroupId = GLTX.TxGroup
								),
					0) ) AS GrossAmount
				,(ISNULL(
							(
									SELECT SUM(
												CASE _tx.IsAccountingCredit 
													WHEN 0 THEN _tx.Amount
													ELSE -_tx.Amount
												END
										)
									FROM Tenant_TSI.dbo.TxTransaction _tx
									WHERE
										_tx.TargetDate_UTC between @UTC_from and @UTC_toNextDay 	                                    
										AND _tx.TxTypeId = 3
										AND _tx.TxInvoiceId = GLTX.TxInvoiceId
										AND _tx.GroupId = GLTX.TxGroup
							),
							0)  ) AS Discount
				,(ISNULL(
							(
									SELECT SUM(
												CASE _tx.IsAccountingCredit 
													WHEN 0 THEN _tx.Amount
													ELSE -_tx.Amount
												END
										)
									FROM Tenant_TSI.dbo.TxTransaction _tx
									WHERE
										_tx.TargetDate_UTC between 	@UTC_from and @UTC_toNextDay 
										AND _tx.TxTypeId = 2
										AND _tx.TxInvoiceId = GLTX.TxInvoiceId
										AND _tx.GroupId = GLTX.TxGroup
							),0 ) ) AS TotalTax
				,GLTX.TxInvoiceId AS InvoiceNumber
				,GLTX.TxGroup AS TransactionGroupId
				,GLTX.TxTransactionId AS TransactionId
				,COALESCE((CASE 
							WHEN PT_MAIP.MemberAgreementItemPerpetualId IS NOT NULL THEN
									ISNULL((PT_MAIP.Quantity * PT_MAIP.Price), 0 )
							WHEN PT_MAI.MemberAgreementItemId IS NOT NULL THEN
									ISNULL(( SELECT TOP (1) _maip.Quantity * _maip.Price
												FROM Tenant_TSI.dbo.MemberAgreementItemPerpetual _maip
												WHERE _maip.MemberAgreementItemId = PT_MAI.MemberAgreementItemId
												ORDER BY _maip.Sequence),  0)
							END), 0) AS ReccuringBillingMinusDiscountsSalesTax
				,COALESCE((CASE 
							WHEN PT_MAI.MemberAgreementItemId IS NOT NULL THEN
								ISNULL(
										(
											SELECT SUM(
														CASE _tx.IsAccountingCredit 
																WHEN 0 THEN _tx.Amount
																ELSE -_tx.Amount
														END
													)
											FROM Tenant_TSI.dbo.TxTransaction _tx
											WHERE
												_tx.TargetDate_UTC between
													dbo.DateTimeLocalToUtc(@fromDateLocal,(SELECT TimeZoneName FROM BusinessUnit WHERE Tenant_TSI.dbo.BusinessUnitId = @userBusinessUnitId))
													and dbo.DateTimeLocalToStartOfNextDayInUtc(@toDateLocal,(SELECT TimeZoneName FROM Tenant_TSI.dbo.BusinessUnit WHERE BusinessUnitId = @userBusinessUnitId)) 
													AND _tx.TxTypeId = 3
													AND _tx.TxInvoiceId = GLTX.TxInvoiceId
													AND _tx.GroupId = GLTX.TxGroup
										), 0 )
							END ), 0) AS   RecurringBillingDiscountAmountNoTaxes
				,COALESCE((CASE 
							WHEN PT_MA.MemberAgreementId IS NOT NULL THEN
								ISNULL((SELECT SUM(	CASE _tx.IsAccountingCredit 
																WHEN 0 THEN _tx.Amount
																ELSE -_tx.Amount
														END	)
											FROM TxTransaction _tx
											WHERE
												_tx.TargetDate_UTC between
													dbo.DateTimeLocalToUtc(@fromDateLocal,(SELECT TimeZoneName FROM BusinessUnit WHERE Tenant_TSI.dbo.BusinessUnitId = @userBusinessUnitId))
													and dbo.DateTimeLocalToStartOfNextDayInUtc(@toDateLocal,(SELECT TimeZoneName FROM Tenant_TSI.dbo.BusinessUnit WHERE BusinessUnitId = @userBusinessUnitId)) 
													AND _tx.TxTypeId IN (1, 2, 3)
													AND _tx.TxInvoiceId = GLTX.TxInvoiceId
													AND _tx.GroupId = GLTX.TxGroup), 0)
							END ), 0) AS BillingAmount
				,COALESCE(PT_MA.StartDate,'') AS AgreementStartDate
				,COALESCE(PT_MA.ObligationDate,'') AS ObligationDate
				,COALESCE((SELECT TOP (1) _inv.TargetDate
								FROM Tenant_TSI.dbo.MemberAgreementInvoiceRequest
											_iri
									INNER JOIN Tenant_TSI.dbo.TxInvoice
											_inv
											ON (_iri.TxInvoiceId = _inv.TxInvoiceID)
								WHERE _iri.MemberAgreementId = PT_MA.MemberAgreementId
								ORDER BY _iri.BillDate_UTC),'') AS    InitialInvoiceDate
				,COALESCE((SELECT TOP (1) _pr.RoleID
								FROM Tenant_TSI.dbo.MemberAgreementSalesAdviser 
											_sa
									INNER JOIN Tenant_TSI.dbo.PartyRole
											_pr
											ON (_sa.PartyRoleId = _pr.PartyRoleID)
								WHERE _sa.MemberAgreementId = PT_MA.MemberAgreementId
								ORDER BY _sa.Sequence),'') AS InitialSalesRoleId
				,COALESCE(PT_MA.MemberAgreementId, '') AS PTMID
				,COALESCE(ACT.UnitsAcquired,'') AS UnitNumber
				,(SELECT _t.Name
						FROM METAALIAS.FocusMeta.dbo.ItemType _t
						WHERE _t.ItemTypeID = ITM.ItemTypeID
						) AS ItemType
				,CAT.CategoryName AS Category
				,COALESCE(CAT.SubCategoryName, '') AS SubCategory
				,(SELECT _gl.Code
						FROM Tenant_TSI.dbo.GeneralLedgerCode _gl
						WHERE _gl.GeneralLedgerCodeId = ITM.GLCodePOS
					) AS GLCodePOS
				,COALESCE((SELECT _gl.Code
						FROM Tenant_TSI.dbo.GeneralLedgerCode _gl
						WHERE _gl.GeneralLedgerCodeId = ITM.GLCodeRecurring
					), '') AS GLCodeRecurring
				,COALESCE((SELECT _gl.Code
						FROM Tenant_TSI.dbo.GeneralLedgerCode _gl
						WHERE _gl.GeneralLedgerCodeId = ITM.GLCodeDeferred
					),'') AS GLCodeDeferred
				,COALESCE((CASE ITM.ItemTypeID
					WHEN 2 THEN (SELECT _gl.Code
								FROM Tenant_TSI.dbo.AccessItem _ai
									INNER JOIN Tenant_TSI.dbo.GeneralLedgerCode _gl ON (_ai.RedemptionGLCode = _gl.GeneralLedgerCodeId)
								WHERE _ai.ItemID = ITM.ItemID)
					WHEN 3 THEN (SELECT _gl.Code
								FROM Tenant_TSI.dbo.ServiceItem _si
									INNER JOIN Tenant_TSI.dbo.GeneralLedgerCode _gl ON (_si.RedemptionGLCode = _gl.GeneralLedgerCodeId)
								WHERE _si.ItemID = ITM.ItemID)
								END),'') AS     RedemptioinGLCode
				,GLTX.Quantity AS Quantity,
				COALESCE((SELECT _b.Name
						FROM Tenant_TSI.dbo.Bundle _b
						WHERE _b.BundleId = PT_MAI.BundleId
					),'') AS  BundleName
				,GLTX.Time AS TransactionDate
				,GLTX.Time_Utc AS TransactionDate_UTC
				,GLTX.Time_ZoneFormat AS TransactionDate_ZoneFormat
				,GLTX.Description AS LineDescription
				,ITM.ItemTypeID AS  ItemTypeId
				,COALESCE(pt_ma.MemberAgreementId,'') AS [MemberAgreementID]
				,COALESCE(pt_ma.AgreementId,'') AS [AgreementID]
				,COALESCE(PT_MAI.BundleID,'') AS BundleID
				,CASE
					WHEN sd.SigID IS NULL THEN 0
					ELSE 1
					END AS Signed
				,COALESCE((SELECT MAX(txp.TargetDATE)
					FROM Tenant_TSI.dbo.TxPayment txp 
					INNER JOIN Tenant_TSI.dbo.TxTransaction txt ON txp.TxPaymentID = txt.ItemId
					WHERE txt.TxTypeID = 4
							AND txt.TxInvoiceId = GLTX.TxInvoiceId),'') AS PaymentDate
				--,pr.RoleID
				,COALESCE(ppr.[2],'') AS DateOfBirth
				,COALESCE(ppr.[1],'') AS Gender
				,CASE	
					WHEN pt_ma.LinkType = 1 AND PT_MA.LinkId != 0 THEN 1
					ELSE 0
					END AS [IsReWrite]
				,COALESCE(sch.Name,'') as ScheduleName
				,COALESCE(a.Name,'') AS [AgreementName]
				, txi.PaymentDueDate 
	  			,@fromDateLocal  AS [MOSOStartDate]
				,@toDateLocal AS [MOSOEndDate]
				,@userBusinessUnitId AS [MOSOUserBU]
				,COALESCE(@filterBusinessUnitId,'') AS [MOSOFilterBU]
			FROM
				  Tenant_TSI.dbo.TSI_GLTransactions_ROOT(
						@fromDateLocal, 
						DATEADD(DAY, 1, @toDateLocal), 
						@userBusinessUnitId, 
						@filterBusinessUnitId
						) GLTX
				  INNER JOIN Tenant_TSI.dbo.Item  ITM ON (GLTX.SaleItemId = ITM.ItemID)
				  CROSS APPLY Tenant_TSI.dbo.ItemCategoryAndSubCategory(ITM.ItemID) CAT
				  LEFT OUTER JOIN Tenant_TSI.dbo.PartyRole PR ON (GLTX.PartyRoleId = PR.PartyRoleID)
				  OUTER APPLY Tenant_TSI.dbo.GetFirstActiveExternalMemberAgreement(GLTX.PartyRoleId, GLTX.Time_UTC) GM_MA
				  LEFT OUTER JOIN Tenant_TSI.dbo.BusinessUnit GM_BU ON (GM_MA.BusinessUnitId = GM_BU.BusinessUnitId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.MemberAgreement PT_MA ON (GLTX.MemberAgreementId = PT_MA.MemberAgreementId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.BusinessUnit PT_BU ON (PT_MA.BusinessUnitId = PT_BU.BusinessUnitId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.MemberAgreementInvoiceRequestItem PT_IRI ON (GLTX.LinkTypeId = 2 AND GLTX.LinkId = PT_IRI.MemberAgreementInvoiceRequestItemId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.MemberAgreementItem PT_MAI ON (PT_IRI.MemberAgreementItemId = PT_MAI.MemberAgreementItemId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.MemberAgreementItemPerpetual PT_MAIP ON (PT_IRI.MemberAgreementItemPerpetualId = PT_MAIP.MemberAgreementItemPerpetualId)
				  LEFT JOIN Tenant_TSI.dbo.MemberAgreementItemPerpetualPaySource maips (nolock) on pt_maip.MemberAgreementItemPerpetualId = maips.MemberAgreementItemPerpetualId
				  LEFT JOIN Tenant_TSI.dbo.Agreement a ON a.AgreementID = pt_ma.AgreementId
				  LEFT JOIN Tenant_TSI.dbo.BillingSchedule bs (nolock) on maips.BillingScheduleId = bs.BillingScheduleId
				  LEFT JOIN Tenant_TSI.dbo.Schedule sch (nolock) on bs.ScheduleId = sch.ScheduleId
				  LEFT OUTER JOIN Tenant_TSI.dbo.Activity ACT ON (GLTX.TxTransactionId = ACT.TxTransactionId)
				  LEFT OUTER JOIN Tenant_TSI.dbo.BusinessUnit SOLD_BU ON (GLTX.BusinessUnitId = SOLD_BU.BusinessUnitId)
				  LEFT JOIN Tenant_TSI.dbo.PartyPropertiesReporting ppr ON pr.PartyID = ppr.PartyId
				  OUTER APPLY (SELECT TOP 1 * 
									FROM dbo.SignatureData sd 
									WHERE sd.SigTypeId = PT_MA.MemberAgreementId 
										AND sd.SigType = 2
										ORDER BY SigID DESC) AS SD
				  LEFT JOIN Tenant_TSI.dbo.TxPayment txp ON txp.TxPaymentID = gltx.TxPaymentId
				  LEFT JOIN Tenant_TSI.dbo.TxInvoice txi ON txi.TxInvoiceID = GLTX.TxInvoiceId
	
			WHERE
				  GLTX.TxTypeId = 1
				  --skip sales generated from import
				  AND NOT EXISTS(
						SELECT TOP (1) *
						FROM Tenant_TSI.dbo.TxTransaction 
										  _ptx
									INNER JOIN Tenant_TSI.dbo.TxPayment
										  _pay
										  ON (_ptx.TxTypeId = 4 AND _ptx.ItemId = _pay.TxPaymentID)
									INNER JOIN Tenant_TSI.dbo.TenderType
										  _tt
										  ON (_pay.TenderTypeID = _tt.TenderTypeID)
						WHERE
									_ptx.TxInvoiceId = GLTX.TxInvoiceId
									AND _ptx.GroupId = GLTX.TxGroup
									AND _tt.Name LIKE '%conversion%'
						)
	
	
	
	END