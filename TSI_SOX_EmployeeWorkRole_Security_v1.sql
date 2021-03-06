/****** Object:  StoredProcedure [dbo].[sp_Reporting_TSI_Employee-WorkRole_Security_SOX]    Script Date: 5/4/2016 11:58:53 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
--/*******************************************************************************************************************
--This View is getting generated for TSI inorder to ease their SOX compliance issues.  
--It will make this information more readily available so they can move on it faster. 
--*******************************************************************************************************************/
ALTER PROCEDURE [dbo].[sp_Reporting_TSI_Employee-WorkRole_Security_SOX]


AS
BEGIN
	select
		pr.RoleID as MemberID
		,pciee.Characteristic as PayrollID
		,pcifn.Characteristic as FirstName
		,pciln.Characteristic as Lastname
		,ua.Username
		,ua.LastLoginSuccess
		,ua.Sysadmin
		,ua.IsActive
		,ua.IsLocked
		,ua.ServiceOnly	
		,'WorkRole' as PermissionType
	   ,(Case 
			when ewr.WorkRoleSourceLevel = 1 then 'Enterprise'
			when ewr.WorkRoleSourceLevel = 2 then 'Division'
			when ewr.WorkRoleSourceLevel = 3 then 'BusinessUnit'
		End
		) as PermissionLevel
		,(Case 
			when ewr.WorkRoleSourceLevel = 1 then 'Enterprise'	
			when ewr.WorkRoleSourceLevel = 2 then 
				(select d.Name from tenant_tsi.dbo.Division d where ewr.WorkRoleSourceId = d.DivisionID )
			when ewr.WorkRoleSourceLevel = 3 then 
				(select bu.Name from tenant_tsi.dbo.BusinessUnit bu where ewr.WorkRoleSourceId = bu.BusinessUnitId)
		End
		) as PermissionLevelName
   		,wr.Name as PermissionName
		INTO #TEMP
	from
		tenant_tsi.dbo.PartyRole pr
		inner join tenant_tsi.dbo.UserAccount ua on pr.PartyID = ua.PartyID
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pcifn on (pr.PartyID = pcifn.PartyID) 
			and (getdate() between pcifn.ValidFrom and coalesce(pcifn.ValidThru,getdate())) and (pcifn.PartyCharacteristicTypeID = 3)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciln on (pr.PartyID = pciln.PartyID) 
			and (getdate() between pciln.ValidFrom and coalesce(pciln.ValidThru,getdate())) and (pciln.PartyCharacteristicTypeID = 5)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciee on (pr.PartyID = pciee.PartyID) 
			and (getdate() between pciee.ValidFrom and coalesce(pciee.ValidThru,getdate())) and (pciee.PartyCharacteristicTypeID = 9)
		left join tenant_tsi.dbo.EmployeeWorkRole ewr on pr.PartyID = ewr.PartyId
		left join tenant_tsi.dbo.WorkRole wr on ewr.WorkRoleId = wr.WorkRoleID
	where 
		pr.PartyRoleTypeID = 2

	union all

	---  Individual Permissions Assigned to EEs
	select
		pr.RoleID as MemberID
		,pciee.Characteristic as PayrollID
		,pcifn.Characteristic as FirstName
		,pciln.Characteristic as Lastname
		,ua.Username
		,ua.LastLoginSuccess
		,ua.Sysadmin
		,ua.IsActive
		,ua.IsLocked
		,ua.ServiceOnly	
		,'Individual' as PermissionType
		,'Enterprise' as PermissionLevel
		,'Enterprise' as PermissionLevelName
   		,fs.Name as PermissionName

	from
		tenant_tsi.dbo.PartyRole pr
		inner join tenant_tsi.dbo.UserAccount ua on pr.PartyID = ua.PartyID
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pcifn on (pr.PartyID = pcifn.PartyID) 
			and (getdate() between pcifn.ValidFrom and coalesce(pcifn.ValidThru,getdate())) and (pcifn.PartyCharacteristicTypeID = 3)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciln on (pr.PartyID = pciln.PartyID) 
			and (getdate() between pciln.ValidFrom and coalesce(pciln.ValidThru,getdate())) and (pciln.PartyCharacteristicTypeID = 5)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciee on (pr.PartyID = pciee.PartyID) 
			and (getdate() between pciee.ValidFrom and coalesce(pciee.ValidThru,getdate())) and (pciee.PartyCharacteristicTypeID = 9)
		left join tenant_tsi.dbo.AccessPredicate ap on ua.ObjectID = ap.PrincipalId
		left join tenant_tsi.dbo.Operation o on ap.OperationId = o.Id
		left join FocusMeta.dbo.FeatureSecurity fs on o.Id = fs.FeatureSecurityId
	where 
		pr.PartyRoleTypeID = 2



	
		SELECT *
		FROM #Temp a
		WHERE 1=1
			AND  a.PermissionType = 'WorkRole' --a.IsActive = 1
			AND a.MemberID != 'admin'

PRINT 
	'		
	select
		pr.RoleID as MemberID
		,pciee.Characteristic as PayrollID
		,pcifn.Characteristic as FirstName
		,pciln.Characteristic as Lastname
		,ua.Username
		,ua.LastLoginSuccess
		,ua.Sysadmin
		,ua.IsActive
		,ua.IsLocked
		,ua.ServiceOnly	
		,''WorkRole'' as PermissionType
	   ,(Case 
			when ewr.WorkRoleSourceLevel = 1 then ''Enterprise''
			when ewr.WorkRoleSourceLevel = 2 then ''Division''
			when ewr.WorkRoleSourceLevel = 3 then ''BusinessUnit''
		End
		) as PermissionLevel
		,(Case 
			when ewr.WorkRoleSourceLevel = 1 then ''Enterprise''	
			when ewr.WorkRoleSourceLevel = 2 then 
				(select d.Name from tenant_tsi.dbo.Division d where ewr.WorkRoleSourceId = d.DivisionID )
			when ewr.WorkRoleSourceLevel = 3 then 
				(select bu.Name from tenant_tsi.dbo.BusinessUnit bu where ewr.WorkRoleSourceId = bu.BusinessUnitId)
		End
		) as PermissionLevelName
   		,wr.Name as PermissionName
		INTO #TEMP
	from
		tenant_tsi.dbo.PartyRole pr
		inner join tenant_tsi.dbo.UserAccount ua on pr.PartyID = ua.PartyID
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pcifn on (pr.PartyID = pcifn.PartyID) 
			and (getdate() between pcifn.ValidFrom and coalesce(pcifn.ValidThru,getdate())) and (pcifn.PartyCharacteristicTypeID = 3)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciln on (pr.PartyID = pciln.PartyID) 
			and (getdate() between pciln.ValidFrom and coalesce(pciln.ValidThru,getdate())) and (pciln.PartyCharacteristicTypeID = 5)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciee on (pr.PartyID = pciee.PartyID) 
			and (getdate() between pciee.ValidFrom and coalesce(pciee.ValidThru,getdate())) and (pciee.PartyCharacteristicTypeID = 9)
		left join tenant_tsi.dbo.EmployeeWorkRole ewr on pr.PartyID = ewr.PartyId
		left join tenant_tsi.dbo.WorkRole wr on ewr.WorkRoleId = wr.WorkRoleID
	where 
		pr.PartyRoleTypeID = 2

	union all

	---  Individual Permissions Assigned to EEs
	select
		pr.RoleID as MemberID
		,pciee.Characteristic as PayrollID
		,pcifn.Characteristic as FirstName
		,pciln.Characteristic as Lastname
		,ua.Username
		,ua.LastLoginSuccess
		,ua.Sysadmin
		,ua.IsActive
		,ua.IsLocked
		,ua.ServiceOnly	
		,''Individual'' as PermissionType
		,''Enterprise'' as PermissionLevel
		,''Enterprise'' as PermissionLevelName
   		,fs.Name as PermissionName

	from
		tenant_tsi.dbo.PartyRole pr
		inner join tenant_tsi.dbo.UserAccount ua on pr.PartyID = ua.PartyID
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pcifn on (pr.PartyID = pcifn.PartyID) 
			and (getdate() between pcifn.ValidFrom and coalesce(pcifn.ValidThru,getdate())) and (pcifn.PartyCharacteristicTypeID = 3)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciln on (pr.PartyID = pciln.PartyID) 
			and (getdate() between pciln.ValidFrom and coalesce(pciln.ValidThru,getdate())) and (pciln.PartyCharacteristicTypeID = 5)
		left join tenant_tsi.dbo.PartyCharacteristicIndexed pciee on (pr.PartyID = pciee.PartyID) 
			and (getdate() between pciee.ValidFrom and coalesce(pciee.ValidThru,getdate())) and (pciee.PartyCharacteristicTypeID = 9)
		left join tenant_tsi.dbo.AccessPredicate ap on ua.ObjectID = ap.PrincipalId
		left join tenant_tsi.dbo.Operation o on ap.OperationId = o.Id
		left join FocusMeta.dbo.FeatureSecurity fs on o.Id = fs.FeatureSecurityId
	where 
		pr.PartyRoleTypeID = 2



	
		SELECT *
		FROM #Temp a
		WHERE 1=1
			AND  a.PermissionType = ''WorkRole'' --a.IsActive = 1
			AND a.MemberID != ''admin''

	'

END
