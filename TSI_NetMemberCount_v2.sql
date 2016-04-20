
--CREATE PROCEDURE sp_Reporting_TSI_NetMemberCount_v2
ALTER PROCEDURE sp_Reporting_TSI_NetMemberCount_v2 

/*
	These filters are standard with the extract however they are not required for this to function. The final extract will have no date of BU filters on it.
--	--@StartDate DATETIME = NULL,
--	--@EndDate DATETIME = NULL,
--	--@UserBusinessUnitID INT = 1,
--	--@FilterBusinessUnitID INT = NULL

----DECLARE 
----		@fromDateLocal DATE 
----		,@toDateLocal DATE 
----		,@userBusinessUnitId INT = 1
----		,@filterBusinessUnitId INT = NULL
*/
AS
BEGIN
	SET NOCOUNT ON;

if (object_id('tempdb..#Result') is not null) drop table #Result;

if (object_id('tempdb..#Result') is null) 
Create table #Result
(
	MemberAgreementId int,
	AgreementStatus varchar(50),
	PartyRoleId int,
	PartyId int,
	RoleId varchar(50),
	FirstName varchar(50),
	LastName varchar(50),
	BUName varchar(50),
	StartDate DateTime,
	EndDate DateTime,
	ObligationDate DateTime,
	CancelDate DateTime,
	FutureCancelDate DateTime,
	BusinessUnitId int,
	AgreementId int,
	Agreement varchar(50),
	BundleId int,
	BundleItemCode varchar(50),
	BundleName varchar(50),
	TxInvoiceId int, --Reference column to get other datapoints

	AuthenticatedUserId int, --Reference column to get the SALE_XXX info	
	PIN_UserID INT, --Reference column to collect the pin id from WorkUnit

	Sales_PartyId int, --Reference Column
	SALES_USERNAME varchar(50),
	SALES_ROLEID varchar(50),
	SALES_FIRSTNAME varchar(50),
	SALES_LASTNAME varchar(50),
	SALES_PAYROLLID varchar(50),
	SALES_WORKROLE varchar(50),
	
	REFERRAL_PARTYROLEID1 int,
	REFERRAL_PARTYID1 Int,
	REFERRAL_FIRSTNAME1 varchar(50),
	REFERRAL_LASTNAME1 varchar(50),
	REFERRAL_PAYROLLID varchar(50),
	REFERRAL_WORKROLE varchar(50),
	
	Advisor_PartyId int,
	ADVISOR_FIRSTNAME varchar(50),
	ADVISOR_LASTNAME varchar(50),
	ADVISOR_PAYROLLID varchar(50),
	ADVISOR_ROLEID varchar(50),
	ADVISOR_WORKROLE varchar(50),

	PIN_PARTYID Int,
	PIN_RoleID VARCHAR(50),
	PIN_FIRSTNAME varchar(50),
	PIN_LASTNAME varchar(50),
	PIN_Username VARCHAR(50),
	PIN_PAYROLLID varchar(50),

	AGREEMENT_MONTHLYDUES Decimal(12,2),
	FIRSTINVOICEDATE DateTime	
);

Insert into #Result
(
	MemberAgreementId,
	AgreementStatus,
	PartyRoleId,
	PartyId,
	RoleId,
	FirstName,
	LastName,
	BUName,
	StartDate,
	EndDate,
	ObligationDate,
	CancelDate,
	FutureCancelDate,
	BusinessUnitId,
	AgreementId,
	Agreement,
	BundleId,
	BundleItemCode,
	BundleName,
	REFERRAL_PARTYROLEID1
)
select ma.MemberAgreementId,
				sm.Name AS AgrementStatus,
				p.PartyRoleId,
				p.PartyID,
				p.RoleID AS MemberID,
				pv.[First Name] AS FirstName,
				pv.[Last Name] AS LastName,
				bu.Name AS BUName,
				ma.StartDate,
				ma.EndDate,
				ma.ObligationDate,
				cc.CancelDate AS CancelDate,
				cf.CancelDate AS FutureCancelDate,
				ma.BusinessUnitId,
				ma.AgreementId,
				a.Name AS Agreement,
				b.BundleId,
				b.Code AS BundleItemCode,
				b.Name AS BundleName,
				p.ReferredByPartyRoleID
				
			from Tenant_TSI.dbo.MemberAgreement ma (NOLOCK)
			LEFT JOIN Tenant_TSI.dbo.Agreement a ON a.AgreementID = ma.AgreementID
			LEFT JOIN Tenant_TSI.dbo.MemberAgreementItem mai (NOLOCK) ON mai.MemberAgreementId = ma.MemberAgreementId AND IsKeyItem = 1
			LEFT JOIN Tenant_TSI.dbo.Bundle b ON b.BundleID = mai.BundleId
			LEFT JOIN Tenant_TSI.dbo.StatusMap sm (NOLOCK) ON sm.StatusId = ma.Status AND sm.StatusMapType = 5
			LEFT JOIN Tenant_TSI.dbo.PartyRole p (NOLOCK) ON p.PartyRoleId = ma.PartyROleId
			LEFT JOIN Tenant_TSI.dbo.PartyPropertiesReportingView pv (NOLOCK) ON p.PartyID = pv.PartyID
			LEFT JOIN Tenant_TSI.dbo.BusinessUnit bu (NOLOCK) ON bu.BusinessUnitID = ma.BusinessUnitId

			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging]	 reb (NOLOCK) ON reb.ReportName = 'NewMemberCount'	AND reb.Deleted = 0		AND reb.Criteria = 'BundleID'		AND reb.Value = b.BundleID
			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging]	 rea (NOLOCK) ON rea.ReportName = 'NewMemberCount'	AND rea.Deleted = 0		AND rea.Criteria = 'AgreementID'	AND rea.Value = a.AgreementID
			LEFT JOIN [TSI_tactical].[dbo].[Staging_ReportExclusionStaging]	 rbu (NOLOCK) ON rbu.ReportName = 'NewMemberCount'	AND rbu.Deleted = 0		AND rbu.Criteria = 'BusinessUnitID'	AND rbu.Value = bu.BusinessUnitID

			OUTER APPLY 
				(	
					SELECT TOP 1 Date AS CancelDate
					FROM Tenant_TSI.dbo.Cancellation oc1
					WHERE oc1.EntityId = ma.MemberAgreementID 
						AND oc1.EntityIdType = 1
						AND oc1.StateID = 4
					ORDER BY CancellationID DESC
				) cc 
			OUTER APPLY 
				(	
					SELECT TOP 1
							Date AS CancelDate
					FROM Tenant_TSI.dbo.Cancellation oc2
					WHERE oc2.EntityId = ma.MemberAgreementID 
						AND oc2.EntityIdType = 1
						AND oc2.Date > GETDATE()
						AND oc2.StateID NOT IN (4,8,6)
					ORDER BY CancellationID DESC
				) cf 

			where 1=1 
				--AND p.RoleID IN ('8622477','771828')
				---Exclusion List
				AND reb.ExclusionID IS NULL
				AND rea.ExclusionID IS NULL
				AND rbu.ExclusionID IS NULL
	
				AND a.AgreementClassificationID IN (1,3)
				AND ma.Status <> 6-- agrementstatus <> 'expired'

				AND b.name not like '%trial%'
				AND b.name not like '%tryout%'
				AND b.name not like '%guest%'

			GROUP BY 
				ma.MemberAgreementId,
				sm.Name,
				p.PartyRoleId,
				p.PartyID,
				p.RoleID,
				pv.[First Name],
				pv.[Last Name],
				bu.Name,
				ma.StartDate,
				ma.EndDate,
				ma.ObligationDate,
				cc.CancelDate,
				cf.CancelDate,
				ma.BusinessUnitId,
				ma.AgreementId,
				a.Name,
				b.BundleId,
				b.Code,
				b.Name,
				p.ReferredByPartyRoleID
--0:00:40.000


			Update #Result
			set TxInvoiceId = mair.TxInvoiceId 
			from #Result r
			inner join Tenant_TSI.dbo.MemberAgreementInvoiceRequest mair on r.MemberAgreementId = mair.MemberAgreementId
			where mair.Status = 2;



			Update #Result
			set AuthenticatedUserId = wu.UserId--wu.AuthenticatedUserId FLIP HERE To Use PIN User
			from #Result r
			inner join Tenant_TSI.dbo.TxInvoice i on r.TxInvoiceId = i.TxInvoiceID
			inner join Tenant_TSI.dbo.WorkUnit wu on wu.WorkUnitID = i.WorkUnitId

--0:00:54.000


	--Update the demographic data for Sales_XXX
			Update #Result 
			set SALES_FIRSTNAME = e.[First Name],
				SALES_LASTNAME = e.[Last Name], 
				SALES_PAYROLLID = e.[Payroll ID],
				SALES_ROLEID = pr.RoleID,
				SALES_USERNAME = ua.Username,
				Sales_PartyId = pr.PartyID
			from #Result r
			inner join Tenant_TSI.dbo.UserAccount ua on ua.UserAccountId = r.AuthenticatedUserId
			inner join Tenant_TSI.dbo.PartyRole pr on pr.PartyID = ua.PartyID
			inner join Tenant_TSI.dbo.ReportingEmployeeCharacteristics e on e.PartyId = pr.PartyId
			
--0:00:25.000			
--------  PIN USER DATA
			Update #Result
			set PIN_UserID = wu.AuthenticatedUserId-- FLIP HERE To Use PIN User
			from #Result r
			inner join Tenant_TSI.dbo.TxInvoice i on r.TxInvoiceId = i.TxInvoiceID
			inner join Tenant_TSI.dbo.WorkUnit wu on wu.WorkUnitID = i.WorkUnitId

--0:00:04.000

			--Update the demographic data for PIN Owner
			Update #Result 
			set PIN_FIRSTNAME = e.[First Name],
				PIN_LASTNAME = e.[Last Name], 
				PIN_PAYROLLID = e.[Payroll ID],
				PIN_RoleID = pr.RoleID,
				PIN_USERNAME = ua.Username,
				PIN_PARTYID = pr.PartyID
			from #Result r
			inner join Tenant_TSI.dbo.UserAccount ua on ua.UserAccountId = r.PIN_UserID
			inner join Tenant_TSI.dbo.PartyRole pr on pr.PartyID = ua.PartyID
			inner join Tenant_TSI.dbo.ReportingEmployeeCharacteristics e on e.PartyId = pr.PartyID		
			
--0:00:28.000

			
			 
	--Handles the work roles for Sales_XXX
			;with 
				cte_WorkRoles 
						as (
							select First_Value(wr.Name) Over (Partition by ewr.PartyId Order by ewr.WorkRoleSourceLevel desc) Name, ewr.PartyId 
							from #Result r
								inner join Tenant_TSI.dbo.EmployeeWorkRole ewr on ewr.PartyId = r.Sales_PartyId
								left join Tenant_TSI.dbo.Division d on ewr.WorkRoleSourceId = d.DivisionID and ewr.WorkRoleSourceLevel = 2
								left Join Tenant_TSI.dbo.BusinessUnit bu on ewr.WorkRoleSourceId = bu.BusinessUnitId and ewr.WorkRoleSourceLevel = 3
									and bu.BusinessUnitId = r.BusinessUnitId
								inner join Tenant_TSI.dbo.WorkRole wr on wr.WorkRoleID = ewr.WorkRoleId)
			
			Update #Result
				set SALES_WORKROLE = wr.Name
			 from cte_WorkRoles wr
				inner join #Result r on r.Sales_PartyId = wr.PartyId;

--0:00:13.000


	--Handles Demographic information for Referrals
			;with 
				cte_SummaryData 
						as (
								select pr.PartyID, r.REFERRAL_PARTYROLEID1,   
									Coalesce(e.[First Name], m.[First Name]) FirstName, 
									Coalesce(e.[Last Name], m.[Last Name]) LastName,
									e.[Payroll ID],
									r.MemberAgreementId
								from #Result r
									inner join Tenant_TSI.dbo.PartyRole pr on pr.PartyRoleID = r.REFERRAL_PARTYROLEID1
									left join Tenant_TSI.dbo.ReportingEmployeeCharacteristics e on e.PartyId = pr.PartyID
									left join Tenant_TSI.dbo.ReportingMemberCharacteristics m on m.PartyID = pr.PartyID
								where REFERRAL_PARTYROLEID1 is not null),
								cte_GroupedSets as (
									Select PartyId, FirstName, LastName, [Payroll ID], REFERRAL_PARTYROLEID1 from cte_SummaryData
								Group by PartyId, FirstName, LastName, [Payroll ID], REFERRAL_PARTYROLEID1)
			
			
			Update #Result
				set REFERRAL_PARTYID1 = c.PartyID,
					REFERRAL_FIRSTNAME1 = c.FirstName,
					REFERRAL_LASTNAME1 = c.LastName,
					REFERRAL_PAYROLLID = c.[Payroll ID]
			--SELECT c.PartyID, c.Firstname, c.Lastname, c.[Payroll ID]
			from cte_GroupedSets c
				inner join #Result r on r.REFERRAL_PARTYROLEID1 = c.REFERRAL_PARTYROLEID1
			Where r.REFERRAL_PARTYROLEID1 is not null;

--0:00:11.000



	---Handles Work Roles for Referalls
			;with 
				cte_WorkRoles 
						as (
							select First_Value(wr.Name) Over (Partition by ewr.PartyId Order by ewr.WorkRoleSourceLevel desc) AS Name
								, ewr.PartyId 
							from #Result r
								inner join Tenant_TSI.dbo.EmployeeWorkRole ewr on ewr.PartyId = r.REFERRAL_PARTYID1
								left join Tenant_TSI.dbo.Division d on ewr.WorkRoleSourceId = d.DivisionID and ewr.WorkRoleSourceLevel = 2
								left Join Tenant_TSI.dbo.BusinessUnit bu on ewr.WorkRoleSourceId = bu.BusinessUnitId and ewr.WorkRoleSourceLevel = 3
									and bu.BusinessUnitId = r.BusinessUnitId
								inner join Tenant_TSI.dbo.WorkRole wr on wr.WorkRoleID = ewr.WorkRoleId
							where r.REFERRAL_PARTYID1 is not null)				
			
			Update #Result
				set REFERRAL_WORKROLE = wr.Name
			 from cte_WorkRoles wr
				inner join #Result r on r.REFERRAL_PARTYID1 = wr.PartyId
			where r.REFERRAL_PARTYID1 is not null;

--0:00:00.000


	---Update the demographic data for Advisor_XXX
			Update #Result 
			set ADVISOR_FIRSTNAME = e.[First Name],
				ADVISOR_LASTNAME = e.[Last Name], 
				ADVISOR_PAYROLLID = e.[Payroll ID],
				ADVISOR_ROLEID = pr.RoleID,				
				Advisor_PartyId = pr.PartyID
			from #Result r
			inner join Tenant_TSI.dbo.MemberAgreementSalesAdviser sa on r.MemberAgreementId = sa.MemberAgreementId
			inner join Tenant_TSI.dbo.PartyRole pr on pr.PartyRoleID = sa.PartyRoleId
			inner join Tenant_TSI.dbo.ReportingEmployeeCharacteristics e on e.PartyId = pr.PartyId
			where sa.Sequence = 1;

	
--0:00:25.000

	--Handles the work roles for Advisor_XXX
			;with 
				cte_WorkRoles 
						as (
									select First_Value(wr.Name) Over (Partition by ewr.PartyId Order by ewr.WorkRoleSourceLevel desc) AS Name
											, ewr.PartyId 
									from #Result r
										inner join Tenant_TSI.dbo.EmployeeWorkRole ewr on ewr.PartyId = r.Advisor_PartyId
										left join Tenant_TSI.dbo.Division d on ewr.WorkRoleSourceId = d.DivisionID and ewr.WorkRoleSourceLevel = 2
										left Join Tenant_TSI.dbo.BusinessUnit bu on ewr.WorkRoleSourceId = bu.BusinessUnitId and ewr.WorkRoleSourceLevel = 3
											and bu.BusinessUnitId = r.BusinessUnitId
										inner join Tenant_TSI.dbo.WorkRole wr on wr.WorkRoleID = ewr.WorkRoleId)
	
			Update #Result
				set ADVISOR_WORKROLE = wr.Name
			 from cte_WorkRoles wr
				inner join #Result r on r.Sales_PartyId = wr.PartyId;

--0:00:11.000


	---Calc first bill date
			;with cte_FirstInvoiceDate as(
			select Min(mair.BillDate) FirstBillDate, mai.MemberAgreementId from #Result r
				inner join Tenant_TSI.dbo.MemberAgreementItem mai on mai.MemberAgreementId = r.MemberAgreementId
				inner join Tenant_TSI.dbo.MemberAgreementInvoiceRequestItem mairi on mairi.MemberAgreementItemId = mai.MemberAgreementItemId
				inner join Tenant_TSI.dbo.MemberAgreementInvoiceRequest mair on mair.MemberAgreementInvoiceRequestId = mairi.MemberAgreementInvoiceRequestId
			where mai.IsKeyItem = 1 and mair.TxInvoiceId <> r.TxInvoiceId and IsNull(mair.ProcessType, 0) != 1
			group by mai.MemberAgreementId)
	
			Update #Result
				set FIRSTINVOICEDATE = FirstBillDate
			from cte_FirstInvoiceDate c 
				inner join #Result r on r.MemberAgreementId = c.MemberAgreementId

--0:00:44.000


	---Update Monthly Dues
			;with cte_FirstValues as (
										select FIRST_VALUE(maip.Price) Over (Partition by mai.MemberAgreementId Order by maip.Sequence) AS Price
												, mai.MemberAgreementId
										from #Result r
											inner join Tenant_TSI.dbo.MemberAgreementItem mai on mai.MemberAgreementId = r.MemberAgreementId	
											left join Tenant_TSI.dbo.MemberAgreementItemPerpetual maip on maip.MemberAgreementItemId = mai.MemberAgreementItemId 
										where mai.IsKeyItem = 1)
	
			Update #Result
				set AGREEMENT_MONTHLYDUES = c.Price
			from cte_FirstValues c
			inner join #Result r on r.MemberAgreementId = c.MemberAgreementId;

--0:00:15.000


			DECLARE @Count INT = ISNULL((SELECT COUNT(*) FROM #Result),0);

IF (@Count > 0)
				BEGIN
					TRUNCATE TABLE TSI_Tactical.dbo.Reporting_NetMemberCount_v2

					INSERT INTO TSI_Tactical.dbo.Reporting_NetMemberCount_v2
					select MemberAgreementId
						, AgreementStatus
						, PartyRoleId
						, PartyID
						, RoleId
						, FirstName
						, LastName
						, BUName
						, StartDate
						, EndDate
						, ObligationDate
						, CancelDate
						, FutureCancelDate
						, BusinessUnitId
						, AgreementId
						, Agreement
						, BundleId
						, BundleItemCode
						, BundleName
						, SALES_USERNAME
						, SALES_ROLEID
						, SALES_FIRSTNAME
						, SALES_LASTNAME
						, SALES_PAYROLLID
						, SALES_WORKROLE
						, REFERRAL_PARTYROLEID1
						, REFERRAL_PARTYID1
						, REFERRAL_FIRSTNAME1
						, REFERRAL_LASTNAME1
						, REFERRAL_PAYROLLID
						, REFERRAL_WORKROLE
						, ADVISOR_FIRSTNAME
						, ADVISOR_LASTNAME
						, ADVISOR_PAYROLLID
						, ADVISOR_ROLEID
						, ADVISOR_WORKROLE
						, PIN_FIRSTNAME
						, PIN_LASTNAME
						, PIN_PAYROLLID
						, PIN_RoleID
						, AGREEMENT_MONTHLYDUES
						, FIRSTINVOICEDATE
					--INTO TSI_Tactical.dbo.Reporting_NetMemberCount_v2
					From #Result
					
				END



END	