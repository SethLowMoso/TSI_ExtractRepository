USE [tsi_tactical]
GO
/****** Object:  StoredProcedure [dbo].[sp_Reporting_TSI_GLActivityExportSpec_v1]    Script Date: 7/7/2016 10:30:34 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/* =============================================
-- Author:		Ardeshir Namazi
-- Create date: 22-Oct, 2012
-- Modified:
--		4/15/2016 - Updated to SP for new reporting process. This can be deployed to Tenant_TSI As well, but current is setup for TSI_Tactical
			This version also includes the enhancements to filter on the Exclusion table
--
-- Description:	GL activity export feed
-- ============================================= */
ALTER PROCEDURE [dbo].[sp_Reporting_TSI_GLActivityExportSpec_v1] 
	(
		@fromDateLocal DATE = NULL,
		@toDateLocal DATE = NULL,
		@userBusinessUnitId INT = NULL,
		@filterBusinessUnitId INT = NULL
	)

AS 
BEGIN

--DECLARE @fromDateLocal DATE = NULL,
--		@toDateLocal DATE = NULL,
--		@userBusinessUnitId INT = NULL,
--		@filterBusinessUnitId INT = NULL

		DECLARE @Date DATE = GETDATE(); -- This will set it to run the month previous unless someone defaults the date to something.

		IF(@fromDateLocal IS NULL) SET @fromDateLocal = DATEADD(MONTH,-1,DATEADD(month, DATEDIFF(month, 0, @date), 0) )
		IF(@toDateLocal IS NULL) SET @toDateLocal = DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))
		IF(@userBusinessUnitId IS NULL) SET	@userBusinessUnitId = 1
		IF(@filterBusinessUnitId IS NULL) SET @filterBusinessUnitId = NULL
	
	


		SELECT
				GLA.SaleTxInvoiceId 			AS MosoTxInvoiceId,			
				GLA.SaleTxGroupId 			AS MosoTxGroupId,
				GLA.SaleTxTransactionId 			AS MosoTxTransactionId,		
				GLA.ActivityId			AS MosoActivityId,
				EV.EventID 			AS MosoEventId,
				EV.EventCode 		AS EventCode,
				GLA.SaleBusinessUnitId 		AS MosoSaleBusinessUnitId,
				SBU.Code AS 	SaleBusinessUnitCode,
				GLA.BusinessUnitId 		AS MosoRedeemBusinessUnitId,
				BU.Code 			AS RedeemBusinessUnitCode,
				GLA.MemberPartyRoleId		AS MosoPartyRoleId,
				GLA.MemberPartyId			AS MosoPartyId,
				GLA.MemberRoleId			AS MemberRoleId,
				GLA.RealizeTime			AS TransactionTime,
				GLA.RealizeTime_ZoneFormat			AS TransactionTime_ZoneFormat,
				GLA.RealizeTime_UTC			AS TransactionTime_Utc,
				GLA.SaleItemId 			AS MosoItemId,
				ITM.ItemRecordID 			AS ItemCode,
				GLA.SaleQuantity 			AS Quantity,
				GLA.ActivityStart			AS ActivityStart,
				GLA.ActivityStart_ZoneFormat		AS ActivityStart_ZoneFormat,
				GLA.ActivityStart_UTC			AS ActivityStart_Utc,
				GLA.ActivityEnd			AS ActivityEnd,
				GLA.ActivityEnd_ZoneFormat			AS ActivityEnd_ZoneFormat,
				GLA.ActivityEnd_UTC			AS ActivityEnd_Utc,
				ISNULL(GLA.ActivityUnits,1)			AS ActivityUnits,
				TX.Description,			
				EP.EndpointId			AS MosoEndpointId,
				EP.EndpointKey			AS EndpointKey,
				GLA.ActivityTypeId		AS MosoActivityTypeId,
				GLA.GLRevenueId			AS MosoGLRevenueId,
				(
					CASE	
						WHEN GLREV.Code IS NOT NULL THEN ISNULL(SBU.GLCodePrefix,'') + GLREV.Code
					ELSE ''						
					END
				) 	AS GLRevenueFullCode,
				GLA.GLDeferredId 		AS MosoGLDeferredId,
				(
					CASE 
						WHEN GLDEF.Code IS NOT NULL THEN ISNULL(SBU.GLCodePrefix,'') + GLDEF.Code
					ELSE ''						
					END
				) 	AS GLDeferredFullCode,
			
				GLA.GLRedemptionId 			AS MosoGLRedemptionId,
				(
					CASE 
						WHEN GLRED.Code IS NOT NULL THEN ISNULL(BU.GLCodePrefix,'') + GLRED.Code
					ELSE ''						
					END
				) AS GLRedemptionFullCode,
				(
					CASE GLA.IsAccountingCredit
						WHEN 0 THEN GLA.Amount
					ELSE -GLA.Amount
					END
				) AS Amount
			
		FROM 
			dbo.GLActivityTransactions(@fromDateLocal, @toDateLocal, @userBusinessUnitId, @filterBusinessUnitId)		GLA
			LEFT OUTER JOIN dbo.BusinessUnit 				SBU				ON (GLA.SaleBusinessUnitId = SBU.BusinessUnitId)
			LEFT OUTER JOIN dbo.BusinessUnit				BU 				ON (GLA.BusinessUnitId = BU.BusinessUnitId)
			LEFT OUTER JOIN dbo.Item 				ITM				ON (GLA.SaleItemId = ITM.ItemID)
			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging]		RE	 (NOLOCK)	ON 	re.ReportName = 'TSI_GLActivity' AND re.Criteria = 'ItemCode' AND re.Deleted = 0 AND re.Value = ITM.ItemRecordID
			LEFT OUTER JOIN dbo.TxTransaction 				TX				ON (GLA.SaleTxTransactionId = TX.TxTransactionID)
			LEFT OUTER JOIN dbo.WorkUnit				WU				ON (GLA.WorkUnitId = WU.WorkUnitID)
			LEFT OUTER JOIN dbo.Endpoint				EP				ON (WU.EndpointId = EP.EndpointId)
			LEFT OUTER JOIN dbo.GeneralLedgerCode			GLREV			ON (GLA.GLRevenueId = GLREV.GeneralLedgerCodeId)
			LEFT OUTER JOIN dbo.GeneralLedgerCode			GLDEF			ON (GLA.GLDeferredId = GLDEF.GeneralLedgerCodeId)
			LEFT OUTER JOIN dbo.GeneralLedgerCode			GLRED			ON (GLA.GLRedemptionId = GLRED.GeneralLedgerCodeId)
			LEFT OUTER JOIN dbo.uschd_Appointments			APP				ON (GLA.AppointmentId = APP.AppointmentID)
			LEFT OUTER JOIN dbo.uschd_EventOccurrences		EO				ON (APP.EventOccurrenceID = EO.EventOccurrenceID)
			LEFT OUTER JOIN dbo.uschd_Events				EV				ON (EO.EventID = EV.EventID)
			LEFT OUTER JOIN dbo.ServiceItem					SITM			ON (ITM.ItemTypeID = 3 AND ITM.ItemID = SITM.ItemID)
			OUTER APPLY
				(
					SELECT TOP (1) _r.*
					FROM dbo.Redemption _r
					WHERE _r.RedeemID = SITM.RedeemID AND _r.InActive = 0
				) 
				RD
				
		WHERE

			(coalesce(rd.MemberAuthorizationRequired,0) = 1
			and GLA.ActivityTypeId NOT IN (10,11,9)) -- when mem-auth required, exclude cancel, schedule, sched-can, and adjustment
			or (coalesce(rd.MemberAuthorizationRequired,0) = 0
			and GLA.ActivityTypeId NOT IN (10,11))  -- when mem-auth NOT required, exclude cancel, schedule, sched-can

			and app.CreatedDate < [dbo].[DateTimeLocalToUtc](@fromDateLocal,bu.TimeZoneName)
			and re.ExclusionID IS NULL


END
