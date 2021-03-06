
/****** Object:  UserDefinedFunction [dbo].[TSI_GLFinancialExport]    Script Date: 7/7/2016 10:16:45 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 --=============================================
 --Author:		Ardeshir Namazi
 --Create date: 15-Dec,2012
 --Modified:

	--		20-Dec,2012 an: - change to GLFinancialExport params
	--		05-Jan,2013 an: - remove conversion tender filter
	--		14-Jan,2013 an: - populate SalesItemCode for discount transactions with item code that the discount is applied to
	--		18-Apr-2013 pb: - Populate Description, MosoGLDeferredId, GLDeferredFullCode for discount transactions with 
	--							Description, MosoGLDeferredId, GLDeferredFullCode that the discount is applied to
			

 --Description:	MOSO GL Financial export feed for TSI
	--column name and order is important
 --=============================================
ALTER PROCEDURE [dbo].[sp_Reporting_TSI_GLFinancialExport_v1]
(
	@fromDateLocal DATE = NULL, -- from data in local time-zone as defined by userBusinessUnit
	@toDateLocal DATE = NULL, -- to date in local time-zone as defined by userBusinessUnit
	@userBusinessUnitId INT = 1, -- user's business unit id, used for time-zone
	@filterBusinessUnitId INT = NULL -- NULL to get all transactions, or filter to business unit
)
			
AS 
BEGIN

		IF(@fromDateLocal IS NULL) SET @fromDateLocal = DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))
		IF(@toDateLocal IS NULL) SET @toDateLocal = DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))
		IF(@userBusinessUnitId IS NULL) SET	@userBusinessUnitId = 1
		IF(@filterBusinessUnitId IS NULL) SET @filterBusinessUnitId = NULL


				SELECT
					GLTX.MosoTxInvoiceId,			
					GLTX.MosoTxGroupId,			
					GLTX.MosoTxTransactionId,			
					GLTX.MosoActivityId,				
					GLTX.TransactionTime,			
					GLTX.TransactionTime_ZoneFormat,			
					GLTX.TransactionTime_Utc,			
					GLTX.MosoBusinessUnitId,			
					GLTX.BusinessUnitCode,			
					GLTX.MosoEndpointBusinessUnitId,			
					GLTX.EndpointBusinessUnitCode,			
					GLTX.MosoEndpointId,			
					GLTX.EndpointKey,			
					GLTX.MosoPosShiftId,			
					GLTX.MosoTxTypeId,			
					GLTX.MosoSalesItemId,			
						(
							CASE 
								WHEN GLTX.MosoDiscountCodeId IS NULL THEN GLTX.SalesItemCode
								ELSE(
										SELECT TOP (1) _itm.ItemRecordID
										FROM tenant_TSI.dbo.TxTransaction 	_tx
										INNER JOIN tenant_TSI.dbo.Item		_itm		ON (_tx.ItemId = _itm.ItemID)
										WHERE _tx.TxInvoiceId = GLTX.MosoTxInvoiceId
											AND _tx.GroupId = GLTX.MosoTxGroupId
											AND _tx.TxTypeId = 1
									)
						
							END
						) AS SalesItemCode,			
					GLTX.MosoDiscountCodeId,			
					GLTX.DiscountCode,			
					GLTX.Quantity,			
					GLTX.MosoTaxGroupId,			
					GLTX.MosoTxPaymentId,			
					GLTX.MosoTenderTypeId,			
					GLTX.MosoTenderInterfaceId,			
					GLTX.MosoCreditCardTypeId,			
					GLTX.MosoPartyRoleId,			
					GLTX.MosoPartyId,			
					GLTX.MemberRoleId,			
					GLTX.MosoMemberAgreementId,					
					GLTX.ActivityStart,			
					GLTX.ActivityStart_ZoneFormat,			
					GLTX.ActivityStart_Utc,			
					GLTX.ActivityEnd,			
					GLTX.ActivityEnd_ZoneFormat,			
					GLTX.ActivityEnd_Utc,			
					CASE 
						WHEN GLTX.SalesItemCode = '814|41430|039' THEN ''
						ELSE GLTX.ActivityUnits 
						END AS ActivityUnits,			
						(
							CASE 
								WHEN GLTX.MosoDiscountCodeId IS NULL  THEN GLTX.Description
								ELSE
									(
										SELECT TOP (1) _itm.Description
										FROM tenant_tsi.dbo.TxTransaction 		_tx
											INNER JOIN tenant_tsi.dbo.Item 		_itm 	ON (_tx.ItemId = _itm.ItemID)
										WHERE _tx.TxInvoiceId = GLTX.MosoTxInvoiceId
											AND _tx.GroupId = GLTX.MosoTxGroupId
											AND _tx.TxTypeId = 1
									)
							END	) AS Description,
					GLTX.MosoGLId,			
					GLTX.GLFullCode,			
						(
							CASE 
								WHEN GLTX.MosoDiscountCodeId IS NULL THEN GLTX.MosoGLDeferredId
								ELSE
									(
										SELECT TOP (1) _itm.GLCodeDeferred
										FROM tenant_tsi.dbo.TxTransaction 		_tx
											INNER JOIN tenant_tsi.dbo.Item		_itm	ON (_tx.ItemId = _itm.ItemID)
										WHERE _tx.TxInvoiceId = GLTX.MosoTxInvoiceId
											AND _tx.GroupId = GLTX.MosoTxGroupId
											AND _tx.TxTypeId = 1
									)
							END	) AS	MosoGLDeferredId,
						(
							CASE 
								WHEN GLTX.MosoDiscountCodeId IS NULL THEN GLTX.GLDeferredFullCode
								ELSE
									(
										SELECT TOP (1) ISNULL(_bu.GLCodePrefix,'') + _gl.Code
										FROM tenant_tsi.dbo.TxTransaction 			_tx
											INNER JOIN tenant_tsi.dbo.Item 			_itm		ON (_tx.ItemId = _itm.ItemID)
											INNER JOIN tenant_tsi.dbo.GeneralLedgerCode		_gl			ON (_itm.GLCodeDeferred = _gl.GeneralLedgerCodeId)
											INNER JOIN tenant_tsi.dbo.BusinessUnit			_bu		ON (_tx.TargetBusinessUnitId = _bu.BusinessUnitId)
										WHERE _tx.TxInvoiceId = GLTX.MosoTxInvoiceId
											AND _tx.GroupId = GLTX.MosoTxGroupId
											AND _tx.TxTypeId = 1
									)
						
							END) AS	GLDeferredFullCode,
					GLTX.Amount
					, ma.EditableStartDate AS EnrollmentDate
			
				FROM 
					tenant_TSI.dbo.GLFinancialExport(
						@fromDateLocal,
						DATEADD(DAY, 1, @toDateLocal),	-- to keep compatible with prev logic
						@userBusinessUnitId,
						@filterBusinessUnitId
						) 
						GLTX
				LEFT JOIN tenant_tsi.dbo.MemberAgreement ma ON ma.memberagreementid =	GLTX.MosoMemberAgreementId
		
END