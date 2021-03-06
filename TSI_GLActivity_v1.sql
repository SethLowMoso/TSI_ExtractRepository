USE [tsi_tactical]
GO
/****** Object:  StoredProcedure [dbo].[sp_Reporting_TSI_GLActivityExport_v1]    Script Date: 7/7/2016 10:30:32 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*=============================================
-- Author:		Ardeshir Namazi
-- Create date: 22-Oct, 2012
-- Modified:
		4/15/2016 - Seth - Converting to SP - SP Targetted on TSI_Tactical, but can be deployed to TSIProduction.
				This version also includes the enhancements to filter on the Exclusion table
--
-- Description:	GL activity export feed
=============================================*/
ALTER PROCEDURE [dbo].[sp_Reporting_TSI_GLActivityExport_v1]
(
	@fromDateLocal DATE = NULL, --DATEADD(d,-1,CONVERT(DATE,GETDATE(),101)),
	@toDateLocal DATE = NULL, --CONVERT(DATE,GETDATE(),101),
	@userBusinessUnitId INT = 1,
	@filterBusinessUnitId INT = NULL
)

AS
BEGIN


		IF(@fromDateLocal IS NULL) SET @fromDateLocal = DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))
		IF(@toDateLocal IS NULL) SET @toDateLocal = DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))
		IF(@userBusinessUnitId IS NULL) SET	@userBusinessUnitId = 1
		IF(@filterBusinessUnitId IS NULL) SET @filterBusinessUnitId = NULL
	

---->>> Commented for testing purposes
--DROP TABLE #Temp;

	--DECLARE
	--	@fromDateLocal DATE =  '1/1/2016'--DATEADD(d,-1,CONVERT(DATE,GETDATE(),101))  -- '2/22/2016'  -- '1/1/2016'--
	--	,@toDateLocal DATE = '1/31/2016'--CONVERT(DATE,GETDATE(),101) -- '2/23/2016' -- '1/31/2016' --
	--	,@userBusinessUnitId INT = 1
	--	,@filterBusinessUnitId INT = NULL
		

		SELECT
				GLA.SaleTxInvoiceId 			AS MosoTxInvoiceId,
				GLA.SaleTxGroupId 			AS MosoTxGroupId,
				GLA.SaleTxTransactionId 			AS MosoTxTransactionId,
				GLA.ActivityId			AS MosoActivityId,
				EV.EventID 			AS MosoEventId,
				EV.EventCode 			AS EventCode,
				GLA.SaleBusinessUnitId 			AS MosoSaleBusinessUnitId,
				SBU.Code AS 			SaleBusinessUnitCode,
				GLA.BusinessUnitId 			AS MosoRedeemBusinessUnitId,
				BU.Code 			AS RedeemBusinessUnitCode,
				GLA.MemberPartyRoleId 			AS MosoPartyRoleId,
				GLA.MemberPartyId 			AS MosoPartyId,
				GLA.MemberRoleId			AS MemberRoleId,
				GLA.RealizeTime 			AS TransactionTime,
				GLA.RealizeTime_ZoneFormat		AS TransactionTime_ZoneFormat,
				GLA.RealizeTime_UTC			AS TransactionTime_Utc,
				GLA.SaleItemId 			AS MosoItemId,
				ITM.ItemRecordID 		AS ItemCode,
				GLA.SaleQuantity 		AS Quantity,
				GLA.ActivityStart		AS ActivityStart,
				GLA.ActivityStart_ZoneFormat		AS ActivityStart_ZoneFormat,
				GLA.ActivityStart_UTC		AS ActivityStart_Utc,
				GLA.ActivityEnd			AS ActivityEnd,
				GLA.ActivityEnd_ZoneFormat			AS ActivityEnd_ZoneFormat,
				GLA.ActivityEnd_UTC			AS ActivityEnd_Utc,
				ISNULL(GLA.ActivityUnits,1)			AS ActivityUnits,
				TX.Description,			
				EP.EndpointId			AS MosoEndpointId,
				EP.EndpointKey			AS EndpointKey,
				GLA.ActivityTypeId			AS MosoActivityTypeId,
				GLA.GLRevenueId			AS MosoGLRevenueId,
				(
					CASE 
						WHEN GLREV.Code IS NOT NULL  THEN ISNULL(SBU.GLCodePrefix,'') + GLREV.Code
						ELSE ''						
					END
				) 	AS GLRevenueFullCode,
				GLA.GLDeferredId		AS MosoGLDeferredId,
				(
					CASE 
						WHEN GLDEF.Code IS NOT NULL  THEN ISNULL(SBU.GLCodePrefix,'') + GLDEF.Code
					ELSE ''						
					END
				) 	AS GLDeferredFullCode,			
				GLA.GLRedemptionId		AS MosoGLRedemptionId,			
				(
					CASE 
						WHEN GLRED.Code IS NOT NULL THEN ISNULL(BU.GLCodePrefix,'') + GLRED.Code
						ELSE ''						
					END
				) 	AS GLRedemptionFullCode,
				(
					CASE GLA.IsAccountingCredit
						WHEN 0 THEN GLA.Amount
						ELSE -GLA.Amount
					END
				) AS Amount
		--INTO #Temp	
		FROM --#Root GLA
			Tenant_TSIDev.dbo.GLActivityTransactions(@fromDateLocal, @toDateLocal, @userBusinessUnitId, @filterBusinessUnitId) GLA
				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.BusinessUnit 			SBU (NOLOCK)				ON (GLA.SaleBusinessUnitId = SBU.BusinessUnitId)
			LEFT OUTER JOIN Tenant_TSIDev.dbo.BusinessUnit			BU (NOLOCK)				ON (GLA.BusinessUnitId = BU.BusinessUnitId)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.Item					ITM (NOLOCK)				ON (GLA.SaleItemId = ITM.ItemID)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.TxTransaction			TX (NOLOCK)				ON (GLA.SaleTxTransactionId = TX.TxTransactionID)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.WorkUnit				WU (NOLOCK)				ON (GLA.WorkUnitId = WU.WorkUnitID)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.Endpoint				EP (NOLOCK)				ON (WU.EndpointId = EP.EndpointId)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.GeneralLedgerCode		GLREV (NOLOCK)			ON (GLA.GLRevenueId = GLREV.GeneralLedgerCodeId)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.GeneralLedgerCode		GLDEF (NOLOCK)			ON (GLA.GLDeferredId = GLDEF.GeneralLedgerCodeId)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.GeneralLedgerCode		GLRED (NOLOCK)			ON (GLA.GLRedemptionId = GLRED.GeneralLedgerCodeId)				
			LEFT OUTER JOIN Tenant_TSIDev.dbo.uschd_Appointments		APP	 (NOLOCK)			ON (GLA.AppointmentId = APP.AppointmentID)
			LEFT OUTER JOIN Tenant_TSIDev.dbo.uschd_EventOccurrences	EO (NOLOCK)				ON (APP.EventOccurrenceID = EO.EventOccurrenceID)
			LEFT OUTER JOIN Tenant_TSIDev.dbo.uschd_Events			EV (NOLOCK)				ON (EO.EventID = EV.EventID)
			LEFT OUTER JOIN Tenant_TSIDev.dbo.ServiceItem				SITM (NOLOCK)			ON (ITM.ItemTypeID = 3 AND ITM.ItemID = SITM.ItemID)

			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging]		RE	 (NOLOCK)			
					ON 	re.ReportName = 'TSI_GLActivity' AND re.Criteria = 'ItemCode' AND re.Deleted = 0 AND re.Value = ITM.ItemRecordID

			OUTER APPLY
				(
					SELECT TOP (1) _r.*
					FROM Tenant_TSIDev.dbo.Redemption _r
					WHERE _r.RedeemID = SITM.RedeemID AND _r.InActive = 0
				) 	RD		

		WHERE 1=1
			AND re.[ExclusionID] IS NULL
			AND ((coalesce(rd.MemberAuthorizationRequired,0) = 1
					AND GLA.ActivityTypeId NOT IN (10,11,9)) -- when mem-auth required, exclude cancel, schedule, sched-can, and adjustment
				OR 
				(coalesce(rd.MemberAuthorizationRequired,0) = 0
					AND GLA.ActivityTypeId NOT IN (10,11)))  -- when mem-auth NOT required, exclude cancel, schedule, sched-can

END
