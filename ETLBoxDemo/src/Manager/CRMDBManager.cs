using ALE.ETLBox;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public static class CRMDBManager
    {
        public static string GetCRMConnectionString()
        {
            return string.Format(CompanySettings.ConnectionStringFormat_Sql, CompanySettings.SP_ServerName, CompanySettings.SP_DatabaseName, CompanySettings.SP_DBUser, CompanySettings.SP_DBPassword);
        }

        public static readonly string SQL_CreateSourceCode = 
            @"DECLARE @propertycode VARCHAR(50); 
              SET @propertycode = (SELECT TOP 1 propertycode FROM dbo.D_PROPERTY WHERE CenAdminCompanyID = {0}) ;
              IF NOT EXISTS ( SELECT  1 FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}' ) 
              BEGIN
                  INSERT  dbo.L_DATA_SOURCE_CODE ( PropertyCode, SourceName, SubSourceName, SourceNameDes, SubSourceNameDes, IsShowdropdown, DedupPriority, ETLProcess )
                    VALUES  (
                        @propertycode, -- PropertyCode - varchar(20)
                        '{1}', -- SourceName - nvarchar(100)
                        '{2}', -- SubSourceName - nvarchar(100)
                        '', -- SourceNameDes - nvarchar(200)
                        '', -- SubSourceNameDes - nvarchar(200)
                        {3}, -- IsShowdropdown - bit
                        {4}, -- DedupPriority - int
                        {5}  -- ETLProcess - int
		            ); 
                 SELECT TOP 1 SourceID FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}';  
             END
             ELSE 
                SELECT  TOP 1 SourceID FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}'; ";

        public static readonly string SQL_DeleteGlobalSpecialRequestsForUpdatedProfiles = @"DELETE  FROM dbo.CENRES_SpecialRequests
                                                                            WHERE   RequestCode NOT IN ( SELECT RequestCode
                                                                                                         FROM   dbo.SUR_PMS_SpecialRequest_Mapping )

                                                                            DECLARE @item INT
                                                                            SET @item = ( SELECT MIN(item_id) FROM dbo.SUR_PMS_SpecialRequest_Mapping )

                                                                            WHILE @item IS NOT NULL
                                                                            BEGIN

	                                                                            IF ( SELECT item_type_id FROM dbo.SUR_ITEM WHERE  item_id = @item ) = 3 
		                                                                            BEGIN
			                                                                            DELETE  FROM dbo.PMS_SpecialRequests
			                                                                            WHERE   ResortField IS NULL
					                                                                            AND EXISTS ( SELECT 1
								                                                                             FROM   dbo.SUR_PMS_SpecialRequest_Mapping
								                                                                             WHERE  RequestType = PMS_SpecialRequests.RequestType
										                                                                            AND RequestCode = PMS_SpecialRequests.RequestCode
										                                                                            AND item_id = @item )
					                                                                            AND FK_Profiles IN (
					                                                                            SELECT  FK_Profiles
					                                                                            FROM    dbo.CENRES_SpecialRequests
					                                                                            WHERE   RequestCode IN (
							                                                                            SELECT  RequestCode
							                                                                            FROM    dbo.SUR_PMS_SpecialRequest_Mapping
							                                                                            WHERE   item_id = @item ) )

			                                                                            SET @item = ( SELECT MIN(item_id)
						                                                                              FROM dbo.SUR_PMS_SpecialRequest_Mapping
						                                                                              WHERE item_id > @item )
		                                                                            END
	                                                                            ELSE
		                                                                            BEGIN
			                                                                            SET @item = ( SELECT MIN(item_id)
						                                                                              FROM dbo.SUR_PMS_SpecialRequest_Mapping
						                                                                              WHERE item_id > @item )
		                                                                            END
                                                                            END";

        public static readonly string SQL_GetDataFromPMS_SpecialRequests = @"SELECT ISNULL(sr.FK_Reservations, '00000000-0000-0000-0000-000000000000') AS FK_Reservations ,ISNULL(sr.FK_Profiles, '00000000-0000-0000-0000-000000000000') AS FK_Profiles
                                    FROM PMS_SpecialRequests sr WITH ( NOLOCK )
                                    INNER JOIN dbo.ETL_TEMP_Remove_SpecialRequests AS er WITH ( NOLOCK ) 
                                    ON sr.FK_Reservations = er.FK_Reservations AND sr.FK_Profiles = er.FK_Profiles
                                                                               AND 1 = (SELECT ISNULL(ConfigurationSettingValue, '1') AS OverwriteExistingSpecialRequests
                                                                                        FROM   eInsightCRM_ConfigurationSettings WITH ( NOLOCK )
                                                                                        WHERE  ConfigurationSettingName = 'OverwriteExistingSpecialRequests')
                                    ";

        public static readonly string SQL_DeleteSpecialRequests = @"DELETE FROM PMS_SpecialRequests WHERE FK_Reservations = @FK_Reservations AND FK_Profiles = @FK_Profiles";

        public static readonly string SQL_RemoveResortFieldForGlobalRequest = @"UPDATE s SET s.ResortField = NULL
                                                                FROM dbo.PMS_SpecialRequests s
                                                                INNER JOIN dbo.SUR_PMS_SpecialRequest_Mapping m
                                                                ON s.RequestType = m.RequestType AND s.RequestCode = m.RequestCode
                                                                WHERE ResortField IS NOT NULL";

        public static readonly string SQL_DeletePMS_SpecialRequests = @"DELETE FROM dbo.PMS_SpecialRequests
                                    WHERE PK_SpecialRequests NOT IN (SELECT PK_SpecialRequests FROM dbo.CENRES_SpecialRequests WITH(NOLOCK))
                                    AND CRMSourceActionType = 'CENRES' AND ResortField IS NOT NULL";
        public static readonly string SQL_DeleteCENRES_SpecialRequests = @"DELETE FROM dbo.CENRES_SpecialRequests";

        public static readonly string SQL_InsertPMSProfileMappingForBiltmore = @"insert into PMS_PROFILE_MAPPING
                                                (FK_Profiles, FK_GlobalProfile, CustomerID, GlobalCustomerID, ExternalProfileID,CendynPropertyID, PMSProfileID, IsCenRes)

                                                select C.PK_Profiles as FK_Profiles, C.PK_Profiles as FK_GlobalProfile, C.CustomerID, C.CustomerID as GlobalCustomerID,
                                                P.ExternalProfileID, P.CendynPropertyID, P.PMSProfileID, 1 as IsCenRes
                                                from D_Customer C with (nolock) inner join PMS_Profiles P with (nolock)
                                                on C.PK_Profiles = P.PK_Profiles
                                                where C.SourceID IN(?, ?, ?)
                                                and not exists (select 1 from PMS_PROFILE_MAPPING m with (nolock) where m.FK_Profiles = C.PK_Profiles)";

        public static readonly string SQL_InsertPMSProfileMapping = @"INSERT INTO PMS_PROFILE_MAPPING (   FK_Profiles ,
                                                                                        FK_GlobalProfile ,
                                                                                        CustomerID ,
                                                                                        GlobalCustomerID ,
                                                                                        ExternalProfileID ,
                                                                                        CendynPropertyID ,
                                                                                        PMSProfileID ,
                                                                                        IsCenRes
                                                                                    )
                                                    SELECT DISTINCT C.PK_Profiles AS FK_Profiles ,
                                                                    C.PK_Profiles AS FK_GlobalProfile ,
                                                                    C.CustomerID ,
                                                                    C.CustomerID AS GlobalCustomerID ,
                                                                    P.ExternalProfileID ,
                                                                    P.CendynPropertyID ,
                                                                    P.PMSProfileID ,
                                                                    1 AS IsCenRes
                                                    FROM   D_CUSTOMER C WITH ( NOLOCK )
                                                            INNER JOIN PMS_Profiles P WITH ( NOLOCK ) ON C.PK_Profiles = P.PK_Profiles
                                                    WHERE  NOT EXISTS (   SELECT 1
                                                                            FROM   dbo.PMS_PROFILE_MAPPING AS pm WITH ( NOLOCK )
                                                                            WHERE  pm.FK_Profiles = P.PK_Profiles );";






        public static string CreateSourceCode(string companyId, string sourceName, string subSourceName, byte isShowdropdown, int dedupPriority, int ETLProcess)
        {
            string sql = string.Format(SQL_CreateSourceCode, companyId, sourceName, subSourceName, isShowdropdown, dedupPriority, ETLProcess);
            return SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlCreateSourceCode_{subSourceName.Trim()}", sql, null).FirstOrDefault();
        }

        public static string GetLastCheckTime(string sourceTable)
        {
            string sqlGetLastCheckTime = $@"SELECT ISNULL(MAX(DriverExecutionDate), DATEADD(yy,-10,GETDATE())) AS LastCheckTime FROM ETL_PACKAGE_LOG WHERE Component = '{sourceTable}' AND EndTime IS NOT NULL";
            List<string> sqlGetLastCheckTimeList = SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlGet{sourceTable}LastCheckTime", sqlGetLastCheckTime, null);
            return sqlGetLastCheckTimeList.FirstOrDefault();
        }

        public static void DeleteGlobalSpecialRequestsForUpdatedProfiles()
        {
            SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteGlobalSpecialRequestsForUpdatedProfiles", SQL_DeleteGlobalSpecialRequestsForUpdatedProfiles, null);
        }

        public static void MoveExistingSpecialRequests()
        {
            List<string> SQL_GetDataFromPMS_SpecialRequestsList = SQLHelper.GetDbValues(GetCRMConnectionString(), "SQL_GetDataFromPMS_SpecialRequests", SQL_GetDataFromPMS_SpecialRequests, null);
            for (int i = 0; i < SQL_GetDataFromPMS_SpecialRequestsList.Count; i++)
            {
                String[] strArr = SQL_GetDataFromPMS_SpecialRequestsList[i].Split(",");
                List<QueryParameter> DeleteParameter = new List<QueryParameter>()
                    {   new QueryParameter("FK_Reservations", "string", strArr[0]),
                        new QueryParameter("FK_Profiles", "string", strArr[1])
                    };
                SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteSpecialRequests", SQL_DeleteSpecialRequests, DeleteParameter);
            }
        }

        public static void RemoveResortFieldForGlobalRequest()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_RemoveResortFieldForGlobalRequest", SQL_RemoveResortFieldForGlobalRequest, null);
        }

        public static void UpdatePMSProfileMappingForBiltmore()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_InsertPMSProfileMappingForBiltmore", SQL_InsertPMSProfileMappingForBiltmore, null);
        }

        public static void UpdatePMSProfileMapping()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_InsertPMSProfileMapping", SQL_InsertPMSProfileMapping, null);
        }




        public static void DeleteOrphanRecords()
        {
            SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeletePMS_SpecialRequests", SQL_DeletePMS_SpecialRequests, null);
            SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteCENRES_SpecialRequests", SQL_DeleteCENRES_SpecialRequests, null);
        }

        public static void TruncateTable(params string[] tableNames)
        {
            SQLHelper.TruncateTable(GetCRMConnectionString(), "TruncateETLCRMTEMPTable: " + string.Join(", ", tableNames), tableNames);
        }

    }
}
