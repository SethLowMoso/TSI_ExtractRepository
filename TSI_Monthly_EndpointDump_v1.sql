USE [TSI_tactical]
GO

/****** Object:  StoredProcedure [dbo].[sp_Reporting_TSI_Monthly_EndpointDump_v1]    Script Date: 4/15/2016 10:20:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[sp_Reporting_TSI_Monthly_EndpointDump_v1]

AS 
BEGIN


		SELECT 
				e.*
			, b.Name AS BUName
			,(select count(*) from Tenant_TSI.dbo.EndpointAccessType eat where eat.EndpointId = e.EndpointId) as Access
			, w.Name AS WorkRole
		FROM Tenant_TSI.dbo.Endpoint e
		INNER JOIN Tenant_TSI.MOSOSec.EndpointSecurity es (NOLOCK) ON es.EndpointID = e.EndpointID
		INNER JOIN Tenant_TSI.dbo.WorkRole w (NOLOCK) ON w.WorkRoleID = es.WorkRoleID
		INNER JOIN Tenant_TSI.dbo.BusinessUnit b (NOLOCK) ON b.BusinessUnitID = e.BusinessUnitID
		ORDER BY e.BusinessUnitID



END


GO


