
--IF (object_id('Tenant_TSI.dbo.TSI_AR_SP_v4') IS NOT NULL)
--	DROP PROCEDURE Tenant_TSI.dbo.TSI_AR_SP_v4

--go
/* =============================================
---- Author: Paul Broomell / SETH LOW 
---- Create date: October 4, 2014
---- Description:      A new custom AR Script for TSI.
----	This script should wipe the AR Staging table and repopulate
		
---- ============================================= */
ALTER PROCEDURE [dbo].[TSI_AR_SP_v4] 
	@AsOfDate DATETIME = NULL
	
AS
BEGIN
       SET NOCOUNT ON;

------>>>These are for testing purposes
--			--DECLARE @AsOfDate DATETIME  = CONVERT(DATE,GETDATE(),101);


			SELECT @AsOfDate = ISNULL(@AsOfDate, GETDATE())

			DECLARE @TestBit bit = 0,
					@ScrapYard bit = 0,
					@RoleID VARCHAR(20) = NULL--'3820771' --null -- '5248798'; -- '1564653';
/***************************************************************************
-- All Invoices with Balance Calculated
***************************************************************************/

            if (object_id('tempdb..#OpenInv') is not null) drop table #OpenInv;
			if (object_id('tempdb..#InvoiceStaging') is not null) drop table #InvoiceStaging;

			SELECT t.TxInvoiceID
					, SUM( IIF(IsAccountingCredit = 1, Amount, -Amount) ) AS Balance
			INTO #InvoiceStaging
			FROM Tenant_TSI.dbo.TxTransaction t (NOLOCK)
				INNER JOIN Tenant_TSI.dbo.txInvoice i (NOLOCK) ON i.txInvoiceID = t.TxInvoiceID
				LEFT JOIN Tenant_TSI.dbo.partyrole p (NOLOCK)  on p.partyroleid = i.partyroleid
			WHERE 1=1
				AND p.RoleID = IIF(@RoleID IS NOT NULL, @RoleId, p.RoleID)
--				and p.businessunitid = 2
			GROUP BY T.TxInvoiceId
			HAVING SUM( IIF(IsAccountingCredit = 1, Amount, -Amount) ) != 0

			--SELECT * FROM #InvoiceStaging

              select
                     txi.TxInvoiceID
                     ,txt.GroupId
                     ,txi.PartyRoleId
                     ,prmem.PartyID as MemPartyId
                     ,prmem.RoleID as MemberID
                     ,prorg.PartyID as OrgPartyId
                     ,prorg.RoleID as OrganizationId
                     ,txi.TargetBusinessUnitId
                     ,txi.TargetDate
                     ,cast(txi.TargetDate as date) as TargetDateShort
                     ,txi.PaymentDueDate
                     ,cast(txi.PaymentDueDate as date) as PaymentDueDateShort
                     ,txi.ClientAccountId
                     ,txi.TxInvoiceStatusId
                     ,txi.BillingStatus
                     ,ca.IsExternal
                     ,DATEDIFF(day,cast(isnull(txi.TargetDate,@AsOfDate) as date),@AsOfDate) - 1 as ARAge
                     ,sum(case when txt.IsAccountingCredit = 0 then txt.Amount else -txt.Amount end) as Amount
					 , sm.Name AS [AgreementStatus]
					 , CASE 
							WHEN a.AgreementTypeID = 1 THEN 'MemberShip'
							WHEN a.AgreementTypeID = 2 THEN 'Service'
							ELSE 'Invalid'
							END AS AgreementType
					 , ma.MemberAgreementId
					 , ma.StartDate
                     ,@AsOfDate as ReportingDate
					 , GETDATE() AS ReportRunDate
              into #OpenInv
              from
                     Tenant_TSI.dbo.TxInvoice txi (nolock)
					 INNER JOIN #InvoiceStaging oi ON oi.TxInvoiceId = txi.TxInvoiceID
                     inner join Tenant_TSI.dbo.TxTransaction txt (nolock) on txi.TxInvoiceID = txt.TxInvoiceId and txt.TargetDate < @AsOfDate  -->>>  This gives you point and time by line item
                     inner join Tenant_TSI.dbo.ClientAccount ca (nolock) on txi.ClientAccountId = ca.ClientAccountId  and ca.IsExternal = 0 -->>> No CN Accounts
                     inner join Tenant_TSI.dbo.ClientAccountParty cap (nolock) on ca.ClientAccountId = cap.ClientAccountId  and cap.PrimaryParty = 1 
                     inner join Tenant_TSI.dbo.PartyRole prmem (NOLOCK)  on txi.PartyRoleId = prmem.PartyRoleID
                     left join Tenant_TSI.dbo.PartyRole prorg (NOLOCK)  on cap.PartyId = prorg.PartyID and prorg.PartyRoleTypeID = 7
					 LEFT JOIN Tenant_TSI.dbo.MemberAgreementInvoiceRequest mair (NOLOCK)  ON mair.TxInvoiceId = txi.TxInvoiceID 
					 LEFT JOIN Tenant_TSI.dbo.MemberAgreement ma (NOLOCK)  ON mair.MemberAgreementId = ma.MemberAgreementId
					 LEFT JOIN Tenant_TSI.[dbo].[StatusMap] sm (NOLOCK)   ON ma.Status = sm.[StatusId] AND sm.StatusMapType = 5
					 LEFT JOIN Tenant_TSI.dbo.Agreement a (NOLOCK)  ON ma.AgreementId = a.AgreementID
				WHERE 1=1
					--AND prmem.BusinessUnitID = 2
					--AND txi.TargetBusinessUnitId = 2
					--AND prmem.RoleID IN ('5248798')
						--AND prmem.RoleID IN ('5667639','5667639','5667639','8168693','8168693','8208433','5109925','5109925','5161377','5161377','5161377','5950093','5950093')
              group by
                     txi.TxInvoiceID
                     ,txt.GroupId
                     ,txi.PartyRoleId
                     ,prmem.PartyID
                     ,prmem.RoleID
                     ,prorg.PartyID 
                     ,prorg.RoleID 
                     ,txi.TargetBusinessUnitId
                     ,txi.TargetDate
                     ,cast(txi.TargetDate as date)
                     ,txi.PaymentDueDate
                     ,cast(txi.PaymentDueDate as date)
                     ,txi.ClientAccountId
                     ,txi.TxInvoiceStatusId
                     ,txi.BillingStatus
                     ,ca.IsExternal
                     ,DATEDIFF(day,cast(txi.TargetDate as date), @AsOfDate)
					 , sm.Name
					 , a.AgreementTypeID
					 , ma.MemberAgreementId
					 , ma.StartDate
					 
              having 
                     sum(case when txt.IsAccountingCredit = 0 then txt.Amount else -txt.Amount end)  != 0;
					 

--              --
--              -- Part 2
--              --

		------>>> Gather as much information about the member's most recent membership agreement
              if (object_id('tempdb..#ARAgreementInfo') is not null) drop table #ARAgreementInfo;

				SELECT 
						ROW_NUMBER() over (partition by oii.MemberID order by m.MemberAgreementID desc) as [ROW]
						, p.RoleID AS MemberID
						, m.MemberAgreementId
						, a.Name AS Agreement
						, s.Name AS AgreementStatus
						, c.Date AS CancellationDate
						, m.StartDate
						, CASE mgr.RoleType
								WHEN 1	THEN 'Primary'
								WHEN 2	THEN 'Add-on'
								WHEN 3	THEN 'ReWrite Primary'
								WHEN 4	THEN 'ReWrite Add-on'
								ELSE 'INVALID'
								END AS AgreementType
						, mgr.RoleType
						, CASE mgr.RoleType	
								WHEN 2	THEN m2.MemberAgreementId
								WHEN 4	THEN m2.MemberAgreementID
								ELSE m.MemberAgreementId
								END AS PrimaryAgreement 
						, CASE mgr.RoleType	
								WHEN 2 THEN p2.RoleID
								WHEN 4 THEN p2.RoleID
								ELSE p.RolEID
								END AS PrimaryAgreementMemberID
						, a2.Name AS [PrimaryAgreementName]
						, CASE 
							WHEN p2.PartyRoleID != p.PartyRoleID THEN pp.[31]
							--WHEN p2.PartyRoleID = p.PartyRoleID THEN ''
							ELSE ''
							END AS [CorporateName]
				INTO #ARAgreementInfo
				FROM #OpenInv oii
				INNER JOIN Tenant_TSI.dbo.PartyRole					p		 (NOLOCK) ON oii.MemberID = p.RoleID
				INNER JOIN Tenant_TSI.dbo.MemberAgreement				m		 (NOLOCK) ON m.PartyRoleId = p.PartyRoleID
				INNER JOIN Tenant_TSI.dbo.Agreement					a		 (NOLOCK) ON a.AgreementID = m.AgreementID AND a.AgreementTypeID = 1
				LEFT JOIN Tenant_TSI.dbo.StatusMap						s		 (NOLOCK) ON s.StatusId = m.Status AND s.StatusMapType = 5
				LEFT JOIN Tenant_TSI.dbo.Cancellation					c		 (NOLOCK) ON c.EntityId = m.MemberAgreementID AND c.EntityIdType = 1
				LEFT JOIN Tenant_TSI.dbo.MemberAgreementGroupRole		mgr		 (NOLOCK) ON mgr.MemberAgreementId = m.MemberAgreementId
				LEFT JOIN Tenant_TSI.dbo.MemberAgreementGroupRole		mgr2	 (NOLOCK) ON mgr.MemberAgreementGroupId = mgr2.MemberAgreementGroupId AND mgr2.RoleType = 1
				LEFT JOIN Tenant_TSI.dbo.MemberAgreement				m2		 (NOLOCK) ON m2.MemberAgreementId = mgr2.MemberAgreementId
				LEFT JOIN Tenant_TSI.dbo.PartyRole						p2		 (NOLOCK) ON p2.PartyRoleID = m2.PartyRoleId
				LEFT JOIN Tenant_TSI.dbo.Agreement						a2		 (NOLOCK) ON a2.AgreementID = m2.AgreementID
				LEFT JOIN Tenant_TSI.dbo.PartyPropertiesReporting		pp		 (NOLOCK) ON p2.PartyID = pp.PartyId
				WHERE 1=1
					--AND oii.MemberID = '3820771'
				GROUP BY p.RoleID
						, p2.RoleID
						, m.MemberAgreementID
						, a.name
						, s.name
						, c.date
						, m.StartDate
						, a2.Name
						, oii.MemberID
						, mgr.RoleType
						, m2.MemberAgreementID
						, p.PartyRoleID
						, p2.PartyRoleID
						, pp.[31]

--SELECT * FROM #ARAgreementInfo


		------>>> Gather information about the payment on the invoice
              if (object_id('tempdb..#ARPaymentTrans') is not null) drop table #ARPaymentTrans;

				SELECT  
					t.TxInvoiceId
					,MAX(t.TxTransactionID) AS LastTransactionID
				INTO #ARPaymentTrans
				FROM Tenant_TSI.dbo.TxTransaction t  (NOLOCK) 
				INNER JOIN #OpenInv oii ON t.TxInvoiceID = oii.TxInvoiceID
				WHERE 1=1
					AND t.TxTypeID = 4
				GROuP BY t.TxInvoiceId


		----->>> Invoice Cancellation Information
			



if (object_id('tempdb..#OpenInvByItem') is not null) drop table #OpenInvByItem;

              with 
					txSaleItem 
						AS (
							 select 
								   ROW_NUMBER() over (partition by t.TxInvoiceId, t.GroupId order by txTransactionId asc) as txSaleItemNo
								   ,t.*
							 from 
								   Tenant_TSI.dbo.TxTransaction t (nolock)
								   INNER JOIN #OpenInv i ON i.TxInvoiceID = t.TxInvoiceId
							 )
					, checkin_CTE (PartyID, LastCheckinID) 
						AS (
								SELECT s.MemPartyId, max(checkinID) AS [LastCheckInID]
									FROM #OpenInv s
									INNER JOIN Tenant_TSI.dbo.CheckIn c (NOLOCK)  ON s.MemPartyId = c.PartyId
									GROUP BY s.MemPartyId
								)
					, CTE_Cancellation
						AS (
								SELECT 
										c.EntityID AS MemberAgreementID,
										--c.Date,
										MAX(CancellationID) AS MaxID
									FROM Tenant_TSI.dbo.Cancellation c  (NOLOCK) 
									INNER JOIN #ARAgreementInfo a ON a.MemberAgreementID = c.EntityID
									WHERE 1=1
										AND c.EntityId = a.MemberAgreementId 
										AND c.EntityIdType = 1 
										AND c.StateId NOT IN (6,8)
									GROUP BY EntityID--, c.Date
									
							)
              select DISTINCT 
                     div.Name as Division
                     ,bu.name as Location
                     ,bu.GLCodePrefix as LocationGL
                     ,ar.MemberID as MemberID
                     ,coalesce(pprmem.[3],'') as FirstName
                     ,coalesce(pprmem.[5],'') as LastName
                     ,coalesce(pprorg.[31],'') as OrganizationName
					 --, ar.OrganizationID
					 , h.Address1
					 , h.Address2
					 , h.Address3
					 , h.City
					 , h.StateProvince
					 , h.PostalCode
					 , COALESCE(ph.PhoneNumber,'') AS HomePhone
					 , COALESCE(pw.PhoneNumber,'') AS WorkPhone
					 , COALESCE(pm.PhoneNumber,'') AS MobilePhone
					 , COALESCE(e.EmailAddress,'') AS Email
                     ,ar.ClientAccountId

                     ,ar.TxInvoiceID as InvoiceNumber
                     ,ar.GroupId as InvoiceLineItem
                     ,ar.TargetDateShort as InvoiceDate
                     ,ar.PaymentDueDateShort as DueDate

                     ,ar.ARAge ARDays
                     ,case 
                           when ar.IsExternal=  1 then 'CN'
                           else 'MoSo'
                     end as ARSource
                     ,sum(case
                           when ar.ARAge <= 30 then ar.Amount
                           else 0
                     end) as AgingCurrent
                     ,sum(case
                           when ar.ARAge between 31 and 60 then ar.Amount
                           else 0
                     end) as AR_31_to_60
                     ,sum(case
                           when ar.ARAge between 61 and 90 then ar.Amount
                           else 0
                     end) as AR_61_to_90
                     ,sum(case
                           when ar.ARAge between 91 and 120 then ar.Amount
                           else 0
                     end) as AR_91_to_120 
                     ,sum(case
                           when ar.ARAge > 120 then ar.Amount
                           else 0
                     end) as AR_121_and_Up
                     ,sum(ar.Amount) as TotalDue
-->> Item Information
                     ,coalesce(i.ItemRecordID,'') as ItemCode
                     ,coalesce(REPLACE(REPLACE(REPLACE(i.Description, CHAR(10), ''), CHAR(13), ''), CHAR(9), ''),'') as ItemDescription
                     ,coalesce(GLPOS.Code,'') as POSGL
					 , ib.POSGLCode AS [rpt_POSGLCode]
                     ,coalesce(GLDef.Code,'') as DeferredGL
                     ,coalesce(GLRec.Code,'') as RecurringGL
					 , COALESCE(ib.RecurringGLCode,'') AS [rpt_RecurringGL]
					 , COALESCE(IB.Category,'') AS Category
					 , COALESCE(IB.SubCategory,'') AS SubCategory

---->>CheckinDate and Time + Location					
					, c.CheckInDateTime
					, cct.LastCheckinID
					, c.CheckInID
					, cb.Name AS CheckinLocation


---->> Status/Type of Agreement Associated with the Invoice
					, ar.MemberAgreementId AS [InvoiceMemberAgreementID]
					, ar.AgreementStatus AS [InvoiceAgreementStatus]
					, ar.AgreementType
					, ar.StartDate
					, ca.Date AS [InvAgrCancelDate]

-->> Audit Columns
                     , ar.ReportingDate AS [ReportDate]
					 , ar.ReportRunDate
              into
                     #OpenInvByItem
              from 
                     #OpenInv ar
                     inner join Tenant_TSI.dbo.BusinessUnit			bu			 (NOLOCK) on ar.TargetBusinessUnitId = bu.BusinessUnitId
                     inner join Tenant_TSI.dbo.Division				div			 (NOLOCK) on bu.DivisionId = div.DivisionID
                     left join Tenant_TSI.dbo.PartyPropertiesReporting		pprmem		 (NOLOCK) on ar.memPartyID = pprmem.PartyId
                     left join Tenant_TSI.dbo.PartyPropertiesReporting		pprorg		 (NOLOCK) on ar.OrgPartyId = pprorg.PartyId
					 LEFT JOIN Tenant_TSI.dbo.PartyRole				pr			 (NOLOCK) ON ar.PartyRoleId = pr.PartyRoleID
					 LEFT JOIN Tenant_TSI.dbo.PartyRoleStatus			prs			 (NOLOCK) ON prs.PartyRoleId = pr.PartyRoleId 
					 LEFT JOIN Tenant_TSI.dbo.StatusMap				sm			 (NOLOCK) ON prs.Status = sm.StatusID AND sm.StatusMapType = 1

					--->>> Sale Item Information
					 left join txSaleItem					tsi			 (NOLOCK) on ar.TxInvoiceID = tsi.TxInvoiceId	and ar.GroupId = tsi.GroupId and tsi.txSaleItemNo = 1
					 LEFT JOIN Tenant_TSI.dbo.rpt_ItemBasic			IB			 (NOLOCK) ON ib.ItemID = tsi.ItemId
					 LEFT JOIN Tenant_TSI.dbo.Item						I			 (NOLOCK) ON i.ItemID = tsi.ItemId
                     left join Tenant_TSI.dbo.GeneralLedgerCode		GLDef		 (NOLOCK) on i.GLCodeDeferred = GLDef.GeneralLedgerCodeId
                     left join Tenant_TSI.dbo.GeneralLedgerCode		GLPOS		 (NOLOCK) on i.GLCodePOS = GLPOS.GeneralLedgerCodeId
					 left join Tenant_TSI.dbo.GeneralLedgerCode		GLRec		 (NOLOCK) on i.GLCodeRecurring = GLRec.GeneralLedgerCodeId
					
					--->>> Cancellation Invoice Agreement
					LEFT JOIN CTE_Cancellation				cn			 (NOLOCK) ON cn.MemberAgreementID = ar.MemberAgreementID
					LEFT JOIN Tenant_TSI.dbo.Cancellation					ca			 (NOLOCK) ON ca.CancellationID = cn.MaxID

					----->>>Checkin Status
					 LEFT JOIN CHECKIN_CTE					cct			 (NOLOCK) ON cct.PartyID = ar.MemPartyID
					 LEFT JOIN Tenant_TSI.dbo.CheckIn					c			 (NOLOCK) ON c.CheckInID = cct.LastCheckinID AND c.PartyID = ar.MemPartyID 
					 LEFT JOIN Tenant_TSI.dbo.BusinessUnit				cb			 (NOLOCK) ON c.BusinessUnitId = cb.BusinessUnitID 

					 --LEFT JOIN #ARAgreementInfo				aa			ON 

					--->> Out applies were used to handle multiple lines of data per instances, for example a member having 2 active home phones. Not certain how this is possible but it was occuring.
					OUTER APPLY (
									SELECT TOP 1
											e.EmailAddress
									FROM Tenant_TSI.dbo.EmailContactMechanism e (NOLOCK) 
									WHERE 1=1
										AND ar.memPartyId = e.PartyID
										AND e.InValid = 0
										AND e.ValidFrom <= GETDATE()
										AND (e.ValidThru >= GETDATE() OR e.ValidThru IS NULL)
										AND e.EmailContactMechanismTypeID = 1
									ORDER BY e.EmailContactMechanismID DESC
								)  e
					OUTER APPLY (	
									SELECT TOP 1 
											p.PhoneNumber
									FROM Tenant_TSI.dbo.PhoneContactMechanism p (NOLOCK) 
									WHERE 1=1
										AND ar.memPartyId = p.PartyID
										AND p.InValid = 0
										AND p.ValidFrom <= GETDATE()
										AND (p.ValidThru >= GETDATE() OR p.ValidThru IS NULL)
										AND p.PhoneContactMechanismTypeID = 1
									ORDER BY p.PhoneContactMechanismID DESC
								) ph
					OUTER APPLY (	
									SELECT TOP 1 
											p.PhoneNumber
									FROM Tenant_TSI.dbo.PhoneContactMechanism p (NOLOCK) 
									WHERE 1=1
										AND ar.memPartyId = p.PartyID
										AND p.InValid = 0
										AND p.ValidFrom <= GETDATE()
										AND (p.ValidThru >= GETDATE() OR p.ValidThru IS NULL)
										AND p.PhoneContactMechanismTypeID = 2
									ORDER BY p.PhoneContactMechanismID DESC
								) pw
					OUTER APPLY (	
									SELECT TOP 1 
											p.PhoneNumber
									FROM Tenant_TSI.dbo.PhoneContactMechanism p (NOLOCK) 
									WHERE 1=1
										AND ar.memPartyId = p.PartyID
										AND p.InValid = 0
										AND p.ValidFrom <= GETDATE()
										AND (p.ValidThru >= GETDATE() OR p.ValidThru IS NULL)
										AND p.PhoneContactMechanismTypeID = 3
									ORDER BY p.PhoneContactMechanismID DESC
								) pm
					OUTER APPLY (
									SELECT TOP 1
											p.AddressData1 AS Address1
											, p.AddressData2 AS Address2
											, p.AddressData3 AS Address3
											, p.CityCounty AS City
											, p.CountryCode
											, sp.Name AS StateProvince
											, p.PostalCode
									FROM Tenant_TSI.dbo.PostalContactMechanism p (NOLOCK) 
									LEFT JOIN FocusMeta.dbo.StateProvince sp  (NOLOCK) ON sp.StateProvinceID = p.StateProvince
									WHERE 1=1
										AND p.PartyID = ar.memPartyId
										AND p.InValid = 0
										AND p.ValidFrom <= GETDATE()
										AND (p.ValidThru >= GETDATE() OR p.ValidThru IS NULL)
										AND p.PostalContactMechanismTypeID = 1
								) h
			WHERE 1=1
				--AND ar.MemberID = '2437001'
              group by
                     div.Name
                     ,bu.name
                     ,bu.GLCodePrefix
                     ,ar.MemberID 
                     ,pprmem.[3] 
                     ,pprmem.[5] 
                     ,pprorg.[31]
					 , h.Address1
					 , h.Address2
					 , h.Address3
					 , h.City
					 , h.StateProvince
					 , h.PostalCode
					 , ph.PhoneNumber
					 , pw.PhoneNumber
					 , pm.PhoneNumber
					 , e.EmailAddress
                     , ar.ClientAccountId
                     , ar.TxInvoiceID
                     ,ar.GroupId
                     ,ar.TargetDateShort
                     ,ar.PaymentDueDateShort
                     ,case 
                           when ar.IsExternal=  1 then 'CN'
                           else 'MoSo'
                     end 
                     ,ar.ARAge
                     ,REPLACE(REPLACE(REPLACE(i.Description, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')
                     ,i.ItemRecordID
                     ,GLDef.Code
                     ,GLPOS.Code
                     ,GLRec.Code
					 ,ar.MemPartyID
					, ar.ReportRunDate
					, ar.ReportingDate
					, ar.MemberAgreementId
					, ar.AgreementStatus 
					, ar.AgreementType 
					, sm.Name 
					, c.CheckInDateTime
					, cb.Name
					, ar.StartDate
					, ib.Category
					, ib.SubCategory
					, ib.RecurringGLCode
					, ib.POSGLCode	
					, cct.LastCheckinID
					, c.CheckInID
					, ca.Date

					--SELECT * FROM #ARAgreementInfo

					--SELECT * FROM #OpenInvByItem

			if (object_id('tempdb..#FinalResults') is not null) drop table #FinalResults;


--SELECT * FROM #ARAgreementInfo a WHERE  a.MemberID = '1564653'

		;WITH
			CTE_Addon
				AS (
					SELECT ROW_NUMBER() over (partition by MemberID order by MemberAgreementID DESC) as ROW_Num  
							, *
					FROM #ARAgreementInfo a
					WHERE 1=1
						AND RoleType IN (2,4)
						--AND a.MemberID = '5248798'
						--AND a.MemberID = '1564653'
					)
			, CTE_Primary
				AS (
					SELECT ROW_NUMBER() over (partition by MemberID order by MemberAgreementID DESC) as ROW_Num  
							, *
					FROM #ARAgreementInfo a
					WHERE 1=1
						AND RoleType IN (1,3)
						--AND a.MemberID = '1564653'
					)

			SELECT 
					---->>>Demographic
					oii.Division
					, oii.Location
					, oii.LocationGL
					, oii.MemberID
					, oii.FirstName
					, oii.LastName
					--, ai2.RoleType
					--, CASE
					--	WHEN ai2.RoleType IS NOT NULL THEN ''--ai2.CorporateName
					--	ELSE oii.OrganizationName
					--	END AS OrganizationName
					, oii.OrganizationName
					, oii.Email
					, oii.MobilePhone
					, oii.WorkPhone
					, oii.HomePhone
					, oii.Address1
					, oii.Address2
					, oii.Address3
					, oii.City
					, oii.StateProvince
					, oii.PostalCode
					, oii.ClientAccountId

					---->>>INVOICE
					, oii.InvoiceNumber
					, oii.InvoiceDate
					, oii.DueDate
					, oii.InvoiceLineItem
					, oii.InvoiceMemberAgreementID
					, oii.InvAgrCancelDate
					, oii.StartDate
					, oii.AgreementType
					, oii.InvoiceAgreementStatus

					---->>>PAYMENT INFO
					, cc.Description AS CreditCardDescription
					, cc.Abbreviation AS CreditCardAbbreviation
					, E.Mask	AS CreditCardMask
					, E.ResponseCode	
					, E.ResponseMessage

					---->>>AR
					, oii.ARSource
					, oii.ARDays
					, oii.AgingCurrent
					, oii.AR_31_to_60
					, oii.AR_61_to_90
					, oii.AR_91_to_120
					, oii.AR_121_and_Up
					, oii.TotalDue

					---->>>ITEM 		
					, oii.ItemCode
					, oii.ItemDescription
					, oii.POSGL
					, oii.rpt_POSGLCode
					, oii.DeferredGL
					, oii.RecurringGL
					, oii.rpt_RecurringGL
					, oii.Category
					, oii.SubCategory

					---->>>Checkin 
					, oii.CheckInDateTime
					, oii.CheckInID
					, oii.LastCheckinID
					, oii.CheckinLocation

					---->>>AGREEMENT
					, aai.MemberAgreementId AS MostRecentMemberAgreement
					, aai.AgreementStatus AS MostRecentAgreementStatus
					, aai.Agreement	AS MostRecentAgreementName
					, aai.StartDate AS MostRecentStartDate
					, aai.CancellationDate AS CancelDate
					, aai.AgreementType AS [Primary/Add-on Agreement]
					--, ai2.RoleType
					, CASE	--ai2.RoleType
						WHEN ai2.RoleType IS NOT NULL THEN ai2.PrimaryAgreement
						ELSE aai.MemberAgreementID
						END AS PrimaryAgreement
					--, aai.PrimaryAgreement AS PrimaryAgreement_OLD
					, CASE 
						WHEN ai2.RoleType IS NOT NULL THEN ai2.PrimaryAgreementMemberID
						ELSE oii.MemberID
						END AS PrimaryAgreementMemberID
					--, aai.PrimaryAgreementMemberID AS PrimaryAgreementMemberID_OLD
					, CASE 
						WHEN ai2.RoleType IS NOT NULL THEN ai2.PrimaryAgreementName
						ELSE aai.Agreement
						END AS PrimaryAgreementName
					--, aai.PrimaryAgreementName AS PrimaryAgreementName_OLD
					, CASE
						WHEN ai2.RoleType IS NOT NULL THEN ai2.CorporateName
						ELSE oii.OrganizationName
						END AS CorporateName
					--, aai.CorporateName AS CorporateName_OLD

					---->>>AUDIT
					, oii.ReportDate
					, oii.ReportRunDate

			INTO #FinalResults
			FROM #OpenInvByItem							OII
			LEFT JOIN CTE_Primary					AAI ON aai.MemberID = oii.MemberID and aai.Roletype IN (1 , 3) AND aai.[ROW_Num] = 1
			LEFT JOIN CTE_Addon					AI2	ON ai2.MemberID = oii.MemberID AND ai2.RoleType IN ( 2 , 4) AND AI2.ROW_Num = 1
			LEFT JOIN #ARPaymentTrans					APT ON apt.TxInvoiceId = oii.InvoiceNumber
			LEFT JOIN Tenant_TSI.dbo.TxTransaction					T	 (NOLOCK) ON t.TxTransactionID = apt.LastTransactionID
			LEFT JOIN Tenant_TSI.dbo.TxPaymentEft					E	 (NOLOCK) ON t.ItemId = e.TxPaymentId
			LEFT JOIN Tenant_TSI.dbo.TxPayment						p	 (NOLOCK) ON t.ItemId = p.TxPaymentGroupId
			LEFT JOIN FocusMeta.dbo.CreditCardType		cc	 (NOLOCK) ON cc.CreditCardTypeID = p.CreditCardTypeId
			WHERE 1=1
				--AND oii.MemberID = '1564653'
			GROUP BY 
						---->>>Demographic
					oii.Division
					, oii.Location
					, oii.LocationGL
					, oii.MemberID
					, oii.FirstName
					, oii.LastName
					, oii.OrganizationName
					, oii.Email
					, oii.MobilePhone
					, oii.WorkPhone
					, oii.HomePhone
					, oii.Address1
					, oii.Address2
					, oii.Address3
					, oii.City
					, oii.StateProvince
					, oii.PostalCode
					, oii.ClientAccountId

					---->>>INVOICE
					, oii.InvoiceNumber
					, oii.InvoiceDate
					, oii.DueDate
					, oii.InvoiceLineItem
					, oii.InvoiceMemberAgreementID
					, oii.InvAgrCancelDate
					, oii.StartDate
					, oii.AgreementType
					, oii.InvoiceAgreementStatus

					---->>>PAYMENT INFO
					, cc.Description
					, cc.Abbreviation
					, E.Mask
					, E.ResponseCode	
					, E.ResponseMessage

					---->>>AR
					, oii.ARSource
					, oii.ARDays
					, oii.AgingCurrent
					, oii.AR_31_to_60
					, oii.AR_61_to_90
					, oii.AR_91_to_120
					, oii.AR_121_and_Up
					, oii.TotalDue

					---->>>ITEM 		
					, oii.ItemCode
					, oii.ItemDescription
					, oii.POSGL
					, oii.rpt_POSGLCode
					, oii.DeferredGL
					, oii.RecurringGL
					, oii.rpt_RecurringGL
					, oii.Category
					, oii.SubCategory

					---->>>Checkin 
					, oii.CheckInDateTime
					, oii.CheckInID
					, oii.LastCheckinID
					, oii.CheckinLocation

					---->>>AGREEMENT
					, aai.MemberAgreementId
					, aai.AgreementStatus
					, aai.Agreement	
					, aai.StartDate 
					, aai.CancellationDate 
					, aai.AgreementType 
					, aai.PrimaryAgreement
					, aai.PrimaryAgreementMemberID
					, aai.PrimaryAgreementName
					, aai.CorporateName
					, ai2.RoleType
					, ai2.PrimaryAgreement
					, ai2.PrimaryAgreementMemberID
					, ai2.PrimaryAgreementName
					, ai2.CorporateName

					---->>>AUDIT
					, oii.ReportDate
					, oii.ReportRunDate

					
					--, ai2.RoleType







/********************************************************************************************************************
-->> This section must be uncommented when this script is deployed to Production.
********************************************************************************************************************/
			DECLARE @UpdatedRecords INT = (	SELECT COUNT(*)	FROM #FinalResults 	WHERE ReportDate = @AsOfDate );
			
			IF (@UpdatedRecords > 0 AND @TestBit = 0)
				BEGIN 
					BEGIN TRAN
						--->>> Clean out table
						DELETE 
						FROM [dbo].[TSI_ARStagingv4]

						--->>> Insert New Data
						INSERT INTO [dbo].[TSI_ARStagingv4]
							--(Division, Location, LocationGL, MemberID, FirstName, LastName, OrganizationName, Email, MobilePhone, WorkPhone, HomePhone, Address1, Address2, Address3, City, StateProvince, PostalCode, ClientAccountId, InvoiceNumber, InvoiceDate, DueDate, InvoiceLineItem, InvoiceMemberAgreementID, InvAgrCancelDate, StartDate, AgreementType, InvoiceAgreementStatus, CreditCardDescription, CreditCardAbbreviation, CreditCardMask, ResponseCode, ResponseMessage, ARSource, ARDays, AgingCurrent, AR_31_to_60, AR_61_to_90, AR_91_to_120, AR_121_and_Up, TotalDue, ItemCode, ItemDescription, POSGL, rpt_POSGLCode, DeferredGL, RecurringGL, rpt_RecurringGL, Category, SubCategory, CheckInDateTime, CheckInID, LastCheckinID, CheckinLocation, ReportDate, ReportRunDate)
						SELECT *
						--INTO TSI_Tactical.dbo.Staging_BFX_ARStaging_v4
						FROM #FinalResults fr

						--DROP TABLE #ARAgreementInfo;
						DROP TABLE #ARPaymentTrans;
						DROP TABLE #OpenInv;
						DROP TABLE #OpenInvByItem;
						DROP TABLE #FinalResults;

					COMMIT TRAN
				END

			IF (@TestBit = 1)
				BEGIN
						SELECT *
						FROM #FinalResults fr

				END


			  
END


--go


/******** Scrap Yard ********

						IF (object_id('tempdb..#STR_Problem') IS NOT NULL) DROP TABLE #STR_Problem
						select
							   p.RoleID
							   , txp.TargetDate
							   , txp.TxPaymentID
							   , txp.Reference
							   , txp.Amount
							   , txpe.ResponseCode
							   , txpe.ResponseMessage
						INTO #STR_Problem
						from
						 Tenant_TSI.dbo.TxPayment txp (nolock)
						 inner join Tenant_TSI.dbo.TxPaymentEft txpe (nolock) on txp.TxPaymentID = txpe.TxPaymentId
						left join Tenant_TSI.dbo.TxTransaction txt (nolock) on txp.TxPaymentID = txt.ItemId
						LEFT JOIN Tenant_TSI.dbo.PartyRole p (NOLOCK) ON p.PartyRoleId = txp.PartyRoleId
						where 1=1
						and txp.TargetDate >= '8/24/2015'
						and txt.TxTransactionID is null
						and txp.Reference is not null
						order by
						 txp.TargetDate DESC
 
						SELECT fr.MemberID
								, fr.Invoicenumber
								, SUM(fr.TotalDue) AS Amount
								, p.Amount
						-- SELECT * 
						FROM #FinalResults fr
						INNER JOIN #STR_Problem p ON p.RoleID = fr.MemberID
						GROUP BY fr.MemberID, fr.InvoiceNumber, p.Amount

						SELECT * FROM #STR_Problem ORDER BY TargetDate DESC


*/