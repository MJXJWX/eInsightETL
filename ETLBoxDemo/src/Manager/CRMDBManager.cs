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

        public static readonly string SQL_UpdatePreferredlanguageUnderD_Customer =
                @"UPDATE d SET d.Languages = p.PrimaryLanguage
                        FROM dbo.D_CUSTOMER AS d
                        INNER JOIN dbo.PMS_Profiles AS p With(Nolock)
                        ON d.PK_Profiles = p.PK_Profiles
                        INNER JOIN dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS ed With(Nolock)
                        ON ed.PK_Profiles = p.PK_Profiles";

        public static readonly string SQL_UpdateAllAddressFieldsToBlank =
                @"UPDATE d SET d.Address1 = '',
                                d.Address2 = '',
                                d.City = '',
                                d.StateProvinceCode = '',
                                d.CountryCode = '',
	                            d.ZipCode = '',
	                            d.AddressStatus = 0,
	                            d.AddressType = '',
	                            d.DivisionCode = '',
	                            d.RegionCode = '',
	                            d.ZipCodePlus4 = ''
                            FROM dbo.D_CUSTOMER AS d
                                INNER JOIN dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS e WITH (NOLOCK)
                                    ON d.PK_Profiles = e.PK_Profiles;";

        public static readonly string SQL_GetInactiveEmailQuery =
                @"SELECT  cm.FK_Profiles, 2 as EmailStatus, '' as Email FROM  dbo.PMS_ContactMethod cm with (nolock) 
                            inner join dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS e WITH (NOLOCK) 
                            ON cm.FK_Profiles = e.PK_Profiles 
                            WHERE cm.CMType = 'IP' 
                            and cm.CMCategory = 'Email' 
                            and IsPrimary = 1 
                            and cm.RecordStatus = 'Inactive'";

        public static readonly string SQL_UpdateAddressTypeUnderD_Customer =
                @"UPDATE dbo.D_CUSTOMER
                            SET AddressType = 'W'
                            WHERE AddressType = 'U' AND SourceID = 1";

        public static readonly string SQL_UpdateAllPhoneFieldsToBlank =
                @"UPDATE d SET d.PhoneNumber = '',
                        d.HomePhoneNumber = '',
                        d.CellPhoneNumber = '',
                        d.FaxNumber = '',
                        d.BusinessPhoneNumber = ''
                        FROM dbo.D_CUSTOMER AS d
                            INNER JOIN dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS e WITH (NOLOCK)
                                ON d.PK_Profiles = e.PK_Profiles;";

        public static readonly string SQL_UpdatePhonesUnderD_Customer =
                @"update C 
                    set C.HomePhoneNumber = P.PhoneValue, DedupeCheck = 0, UpdateDate = getdate()
                    from D_CUSTOMER C INNER JOIN ETL_TEMP_D_CUSTOMER_PHONE P on C.CustomerID = P.CustomerID
                    and P.PhoneType = 'HomePhoneNumber'

                    update C 
                    set C.PhoneNumber = P.PhoneValue, DedupeCheck = 0, UpdateDate = getdate()
                    from D_CUSTOMER C INNER JOIN ETL_TEMP_D_CUSTOMER_PHONE P on C.CustomerID = P.CustomerID
                    and P.PhoneType = 'PhoneNumber'

                    update C 
                    set C.CellPhoneNumber = P.PhoneValue, DedupeCheck = 0, UpdateDate = getdate()
                    from D_CUSTOMER C INNER JOIN ETL_TEMP_D_CUSTOMER_PHONE P on C.CustomerID = P.CustomerID
                    and P.PhoneType = 'CellPhoneNumber'

                    update C 
                    set C.BusinessPhoneNumber = P.PhoneValue
                    from D_CUSTOMER C INNER JOIN ETL_TEMP_D_CUSTOMER_PHONE P on C.CustomerID = P.CustomerID
                    and P.PhoneType = 'BusinessPhoneNumber'

                    update C 
                    set C.FaxNumber = P.PhoneValue
                    from D_CUSTOMER C INNER JOIN ETL_TEMP_D_CUSTOMER_PHONE P on C.CustomerID = P.CustomerID
                    and P.PhoneType = 'FaxNumber'
                    ";

        public static readonly string SQL_GetGlobalCustomerIDUnderPMS_Profile_Mapping =
                @"SELECT pm.CustomerID ,
                           pm.GlobalCustomerID ,
                           pm.FK_Profiles
                    FROM   PMS_PROFILE_MAPPING AS pm WITH ( NOLOCK )
                           INNER JOIN dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS p WITH ( NOLOCK ) ON pm.FK_Profiles = p.PK_Profiles;";

        public static readonly string SQL_GetDataUnderPMS_Profile_Mapping =
                @"Select CustomerID, GlobalCustomerID, FK_Profiles from PMS_PROFILE_MAPPING with (nolock)";

        public static readonly string SQL_GetContactMethodInactiveRecords =
                @"SELECT  cm.PK_ContactMethod FROM  dbo.PMS_ContactMethod cm with (nolock)  WHERE cm.RecordStatus = 'Inactive'  and (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00')  ";

        public static readonly string SQL_DeleteD_Customer_Email = @"DELETE FROM D_Customer_Email WHERE PK_ContactMethod = @PK_ContactMethod";

        public static readonly string SQL_GetDataFromETL_D_Customer_Email_Staging =
                @"SELECT e.Customerid ,
                           e.email ,
                           e.emailstatus ,
                           e.emailtype ,
                           e.EmailDomainHash ,
                           em.id AS email_id
                    FROM   dbo.ETL_D_CUSTOMER_EMAIL_STAGING AS e WITH ( NOLOCK )
                           LEFT JOIN dbo.email AS em WITH ( NOLOCK ) ON ISNULL(e.email, '') = ISNULL(
                                                                                                  em.email_address ,
                                                                                                  '');";

        public static readonly string SQL_UpdateOptInOptOut =
                @"IF OBJECT_ID('Tempdb..#customers_optout') IS NOT NULL
                        DROP TABLE #customers_optout;
                    IF OBJECT_ID('Tempdb..#customers_optin') IS NOT NULL
                        DROP TABLE #customers_optin;

                    SELECT DISTINCT d.CustomerID
                    INTO   #customers_optout
                    FROM   dbo.D_CUSTOMER AS d WITH ( NOLOCK )
                    WHERE  d.SourceID = 1
                            AND EXISTS (   SELECT 1
                                            FROM   dbo.PMS_ProfilePolicies WITH ( NOLOCK )
                                            WHERE  FK_PolicyTypes = 3
                                                    AND AttributeName = 'AllowEmail'
                                                    AND IntegerValue = 0
                                                    AND d.PK_Profiles = FK_Profiles
                                                    AND d.EmailStatus IN ( 1, 2, 4 ));

                    CREATE NONCLUSTERED INDEX IX_customers_optout_CustomerID
                        ON #customers_optout ( CustomerID );

                    UPDATE d
                    SET    d.EmailStatus = 5
                    FROM   dbo.D_CUSTOMER AS d
                            INNER JOIN #customers_optout AS c WITH ( NOLOCK ) ON d.CustomerID = c.CustomerID;

                    UPDATE e
                    SET    e.EmailStatus = 5
                    FROM   dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN #customers_optout AS c WITH ( NOLOCK ) ON e.CustomerID = c.CustomerID;

                    SELECT DISTINCT d.CustomerID
                    INTO   #customers_optin
                    FROM   dbo.D_CUSTOMER AS d WITH ( NOLOCK )
                    WHERE  d.SourceID = 1
                            AND EXISTS (   SELECT 1
                                            FROM   dbo.PMS_ProfilePolicies WITH ( NOLOCK )
                                            WHERE  FK_PolicyTypes = 3
                                                    AND AttributeName = 'AllowEmail'
                                                    AND IntegerValue = 1
                                                    AND d.PK_Profiles = FK_Profiles
                                                    AND d.EmailStatus IN ( 5 ))
                            AND NOT EXISTS (   SELECT 1
                                                FROM   dbo.ECONTACT_CUSTOMER_UNSUBSCRIBED WITH ( NOLOCK )
                                                WHERE  d.Email = EmailAddress );

                    CREATE NONCLUSTERED INDEX IX_customers_optin_CustomerID
                        ON #customers_optin ( CustomerID );

                    UPDATE d
                    SET    d.EmailStatus = 1
                    FROM   dbo.D_CUSTOMER AS d
                            INNER JOIN #customers_optin AS c WITH ( NOLOCK ) ON d.CustomerID = c.CustomerID;

                    UPDATE e
                    SET    e.EmailStatus = 1
                    FROM   dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN #customers_optin AS c WITH ( NOLOCK ) ON e.CustomerID = c.CustomerID;";

        public static readonly string SQL_GetDataETL_Temp_ProfilesAndD_Customer =
                @"select d.CustomerID, d.PK_Profiles from D_Customer as d with(nolock) 
                    inner join dbo.ETL_Temp_Profiles_D_Customer as p with(nolock) 
                    ON d.PK_Profiles = p.PK_Profiles";

        public static readonly string SQL_UpdateVIPLevel =
                @"UPDATE d
                    SET d.VIPLevel = ISNULL(l.VIPCode, ISNULL(u.UDFFieldValue, ''))
                    FROM dbo.D_CUSTOMER AS d
                        INNER JOIN dbo.D_CUSTOMER_UDFFIELDS AS u WITH (NOLOCK)
                            ON d.CustomerID = u.CustomerID
                               AND u.UDFFieldName = 'UDFC31'
                        LEFT JOIN V_L_VIPLevel AS l WITH (NOLOCK)
                            ON l.VIPName = u.UDFFieldValue
                    WHERE (
                              ISNULL(d.VIPLevel, '') <> ISNULL(l.VIPCode, ISNULL(u.UDFFieldValue, ''))
                              AND
                              (
                                  d.InsertDate >= DATEADD(HOUR, -6, GETDATE())
                                  AND d.InsertDate <= GETDATE()
                              )
                              OR
                              (
                                  d.UpdateDate >= DATEADD(HOUR, -6, GETDATE())
                                  AND d.UpdateDate <= GETDATE()
                              )
                              OR
                              (
                                  u.InsertDate >= DATEADD(HOUR, -6, GETDATE())
                                  AND u.InsertDate <= GETDATE()
                              )
                              OR
                              (
                                  u.UpdateDate >= DATEADD(HOUR, -6, GETDATE())
                                  AND u.UpdateDate <= GETDATE()
                              )
                          );";

        public static string SQL_UpdateRateTypeDictionary = 
            @"INSERT INTO dbo.L_DATA_DICTIONARY
        (PropertyCode,
         FieldName,
         FieldValue,
         Description,
         ManualUpdate)
SELECT DISTINCT s.PropertyCode, 
         'RateType', 
         s.RateType, 
         s.RateType, 
         1  
FROM dbo.D_CUSTOMER_STAY s WITH(NOLOCK)
WHERE ISNULL(PropertyCode, '') <> '' AND ISNULL(RateType, '') <> ''
AND NOT EXISTS (SELECT 1 FROM dbo.L_DATA_DICTIONARY WITH(NOLOCK)
				WHERE FieldName = 'RateType' 
				AND PropertyCode = s.PropertyCode
				AND FieldValue = s.RateType)";

        public static string SQL_UpdateRoomTypeDictionary = 
            @"INSERT INTO dbo.L_DATA_DICTIONARY
        (PropertyCode,
         FieldName,
         FieldValue,
         Description,
         ManualUpdate)
SELECT DISTINCT s.PropertyCode, 
		'RoomType',
		s.RoomTypeCode,
		s.RoomTypeCode,
		1
FROM dbo.D_CUSTOMER_STAY s WITH(NOLOCK)
WHERE ISNULL(PropertyCode, '') <> '' AND ISNULL(RoomTypeCode, '') <> ''
AND NOT EXISTS (SELECT 1 FROM dbo.L_DATA_DICTIONARY WITH(NOLOCK)
					WHERE FieldName = 'RoomType' 
					AND PropertyCode = s.PropertyCode
					AND FieldValue = s.RoomTypeCode)";

        public static string SQL_UpdateChannelCodeDictionary =
            @"INSERT  INTO dbo.L_DATA_DICTIONARY
        ( PropertyCode ,
          FieldName ,
          FieldValue ,
          ManualUpdate
        )
        SELECT DISTINCT
                s.PropertyCode ,
                'ChannelCode' ,
                s.Channel ,
                1
        FROM    dbo.D_CUSTOMER_STAY s WITH ( NOLOCK )
        WHERE   ISNULL(s.PropertyCode, '') <> ''
                AND ISNULL(s.Channel, '') <> ''
                AND NOT EXISTS ( SELECT 1
                                 FROM   dbo.L_DATA_DICTIONARY WITH ( NOLOCK )
                                 WHERE  FieldName = 'ChannelCode'
                                        AND PropertyCode = s.PropertyCode
                                        AND FieldValue = s.Channel );";

        public static string SQL_UpdateRoomCodeDictionary =
            @"INSERT  INTO dbo.L_DATA_DICTIONARY
        ( PropertyCode ,
          FieldName ,
          FieldValue ,
          Description ,
          ManualUpdate
        )
        SELECT DISTINCT
                s.PropertyCode ,
                'RoomCode' ,
                s.RoomCode ,
                s.RoomCode ,
                1
        FROM    dbo.D_CUSTOMER_STAY s WITH ( NOLOCK )
        WHERE   ISNULL(PropertyCode, '') <> ''
                AND ISNULL(RoomCode, '') <> ''
                AND NOT EXISTS ( SELECT 1
                                 FROM   dbo.L_DATA_DICTIONARY WITH ( NOLOCK )
                                 WHERE  FieldName = 'RoomCode'
                                        AND PropertyCode = s.PropertyCode
                                        AND FieldValue = s.RoomCode );";

        public static string SQL_UpdateNotesForRateType =
            @"UPDATE dbo.L_DATA_DICTIONARY
SET Notes = '<B>Group Rate</B><BR>- Room Only Reservation.<BR> *This rate does not include Biltmore House admission or gratuities.<BR>'
WHERE FieldName = 'Ratetype'
      AND ISNUMERIC(SUBSTRING(FieldValue, 2, 1)) = 1
      AND ISNULL(Notes, '') = ''
      AND PropertyCode IN ( 'A', 'B', 'V' );";

        public static string SQL_UpdateTargetTableName1AndTargetFieldName1ForRateType = 
            @"UPDATE dbo.L_DATA_DICTIONARY
            SET TargetTableName1 = 'D_Customer_Stay',
            TargetFieldName1 = 'MainRateCode'
            WHERE FieldName = 'RateType'";

        public static string SQL_GetRateTypesForBiltmore =
            @"SELECT  DISTINCT
        p.PropertyCode ,
        CONVERT(VARCHAR(50), p.CendynPropertyID) AS CendynPropertyID ,
        CONVERT(VARCHAR(100), 'RateType') AS FieldName ,
        l.FieldValue ,
        CONVERT(NVARCHAR(50), p.PropertySeparatorValue) AS BuildingCode
        FROM    dbo.L_DATA_PROPERTY_SEPARATOR AS p WITH ( NOLOCK )
        INNER JOIN dbo.L_DATA_DICTIONARY AS l WITH ( NOLOCK ) ON p.PropertyCode = l.PropertyCode
        AND l.FieldName = 'RateType';";

        public static string SQL_GetRateTypesForSHGroup =
            @"SELECT  DISTINCT
        p.PropertyCode ,
        CONVERT(VARCHAR(50), p.CendynPropertyID) AS CendynPropertyID ,
        CONVERT(VARCHAR(100), 'RateType') AS FieldName ,
        l.FieldValue 
        FROM    dbo.D_PROPERTY AS p WITH ( NOLOCK )
        INNER JOIN dbo.L_DATA_DICTIONARY AS l WITH ( NOLOCK ) ON p.PropertyCode = l.PropertyCode
        AND l.FieldName = 'RateType';";

        public static string SQL_GetRoomTypesForSHGroup =
            @"SELECT  DISTINCT
        p.PropertyCode ,
        CONVERT(VARCHAR(50), p.CendynPropertyID) AS CendynPropertyID ,
        CONVERT(VARCHAR(100), 'RoomType') AS FieldName ,
        l.FieldValue 
        FROM    dbo.D_PROPERTY AS p WITH ( NOLOCK )
        INNER JOIN dbo.L_DATA_DICTIONARY AS l WITH ( NOLOCK ) ON p.PropertyCode = l.PropertyCode
        AND l.FieldName = 'RoomType';";

        public static string SQL_GetRateTypesFor12951 =
            @"SELECT  DISTINCT
        p.PropertyCode ,
        CONVERT(VARCHAR(50), p.CendynPropertyID) AS CendynPropertyID ,
        CONVERT(VARCHAR(100), 'RateType') AS FieldName ,
        l.FieldValue 
        FROM    dbo.D_PROPERTY AS p WITH ( NOLOCK )
        INNER JOIN dbo.L_DATA_DICTIONARY AS l WITH ( NOLOCK ) ON p.PropertyCode = l.PropertyCode
        AND l.FieldName = 'RateType';";

        public static string SQL_GetRateTypesForAquaAston =
            @"SELECT  DISTINCT
        p.PropertyCode ,
        CONVERT(VARCHAR(50), p.CendynPropertyID) AS CendynPropertyID ,
        CONVERT(VARCHAR(100), 'RateType') AS FieldName ,
        l.FieldValue 
        FROM    dbo.D_PROPERTY AS p WITH ( NOLOCK )
        INNER JOIN dbo.L_DATA_DICTIONARY AS l WITH ( NOLOCK ) ON p.PropertyCode = l.PropertyCode
        AND l.FieldName = 'RateType';";

        public static string SQL_GetShowAlphaCodeAndShowName =
            @"SELECT 'BILTMORE' AS PropertyCode, 'ShowAlphaCode' AS FieldName, Convert(NVARCHAR(100), x.ShowAlphaCode) AS FieldValue, Convert(NVARCHAR(4000), x.ShowName) AS Description 
            FROM   (   SELECT   ShowName ,
                    ShowAlphaCode ,
                    ROW_NUMBER() OVER ( PARTITION BY ShowAlphaCode
                                        ORDER BY CASE WHEN eInsightCRM_UpdateDate IS NOT NULL THEN
                                                          eInsightCRM_UpdateDate
                                                      ELSE eInsightCRM_InsertDate
                                                 END DESC
                                      ) AS rn
           FROM     dbo.D_Customer_Omni_TicketLog WITH ( NOLOCK )
           WHERE    ISNULL(ShowAlphaCode, '') <> ''
            ) x
           WHERE  x.rn = 1;";

        public static string CreateSourceCode(string companyId, string sourceName, string subSourceName, byte isShowdropdown, int dedupPriority, int ETLProcess)
        {
            string sql = string.Format(SQL_CreateSourceCode, companyId, sourceName, subSourceName, isShowdropdown, dedupPriority, ETLProcess);
            return SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlCreateSourceCode_{subSourceName.Trim()}", sql, null).FirstOrDefault()?["SourceID"];
        }

        public static string GetLastCheckTime(string sourceTable)
        {
            string sqlGetLastCheckTime = $@"SELECT ISNULL(MAX(DriverExecutionDate), DATEADD(yy,-10,GETDATE())) AS LastCheckTime FROM ETL_PACKAGE_LOG WHERE Component = '{sourceTable}' AND EndTime IS NOT NULL";
            List<Dictionary<string, string>> sqlGetLastCheckTimeList = SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlGet{sourceTable}LastCheckTime", sqlGetLastCheckTime, null);
            return sqlGetLastCheckTimeList.FirstOrDefault()?["LastCheckTime"];
        }

        public static void DeleteGlobalSpecialRequestsForUpdatedProfiles()
        {
            SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteGlobalSpecialRequestsForUpdatedProfiles", SQL_DeleteGlobalSpecialRequestsForUpdatedProfiles, null);
        }

        public static void MoveExistingSpecialRequests()
        {
            List<Dictionary<string, string>> SQL_GetDataFromPMS_SpecialRequestsList = SQLHelper.GetDbValues(GetCRMConnectionString(), "SQL_GetDataFromPMS_SpecialRequests", SQL_GetDataFromPMS_SpecialRequests, null);
            for (int i = 0; i < SQL_GetDataFromPMS_SpecialRequestsList.Count; i++)
            {
                Dictionary<string, string> dictionary = SQL_GetDataFromPMS_SpecialRequestsList[i];
                List<QueryParameter> DeleteParameter = new List<QueryParameter>()
                    {   new QueryParameter("FK_Reservations", "string", dictionary["FK_Reservations"]),
                        new QueryParameter("FK_Profiles", "string", dictionary["FK_Profiles"])
                    };
                SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteSpecialRequests", SQL_DeleteSpecialRequests, DeleteParameter);
            }
        }

        public static void D_CustomerEmailMaintenance()
        {
            List<Dictionary<string, string>> SQL_GetContactMethodInactiveRecordsList = SQLHelper.GetDbValues(GetCRMConnectionString(), "SQL_GetContactMethodInactiveRecords", SQL_GetContactMethodInactiveRecords, null);
            for (int i = 0; i < SQL_GetContactMethodInactiveRecordsList.Count; i++)
            {
                Dictionary<string, string> dictionary = SQL_GetContactMethodInactiveRecordsList[i];
                List<QueryParameter> DeleteParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ContactMethod", "string", dictionary["PK_ContactMethod"])
                    };
                SQLHelper.DeleteTableValue(GetCRMConnectionString(), "SQL_DeleteSpecialRequests", SQL_DeleteD_Customer_Email, DeleteParameter);
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

        public static void UpdatePreferredlanguageUnderD_Customer()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdatePreferredlanguageUnderD_Customer", SQL_UpdatePreferredlanguageUnderD_Customer, null);
        }

        public static void UpdateAllAddressFieldsToBlank()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateAllAddressFieldsToBlank", SQL_UpdateAllAddressFieldsToBlank, null);
        }

        public static void UpdateAddressTypeUnderD_Customer()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateAddressTypeUnderD_Customer", SQL_UpdateAddressTypeUnderD_Customer, null);
        }

        public static void UpdateAllPhoneFieldsToBlank()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateAllPhoneFieldsToBlank", SQL_UpdateAllPhoneFieldsToBlank, null);
        }

        public static void UpdatePhonesUnderD_Customer()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdatePhonesUnderD_Customer", SQL_UpdatePhonesUnderD_Customer, null);
        }

        public static void UpdateOptInOptOut()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateOptInOptOut", SQL_UpdateOptInOptOut, null);
        }

        public static void UpdateVIPLevel()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateVIPLevel", SQL_UpdateVIPLevel, null);
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

        public static void UpdateRateTypeDictionary()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateRateTypeDictionary", SQL_UpdateRateTypeDictionary, null);
        }

        public static void UpdateRoomCodeDictionary()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateRoomCodeDictionary", SQL_UpdateRoomCodeDictionary, null);
        }

        public static void UpdateChannelCodeDictionary()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateChannelCodeDictionary", SQL_UpdateChannelCodeDictionary, null);
        }

        public static void UpdateRoomTypeDictionary()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateRoomTypeDictionary", SQL_UpdateRoomTypeDictionary, null);
        }

        public static void UpdateNotesForRateType()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateNotesForRateTypeWithSecondCharacterAsNumber", SQL_UpdateNotesForRateType, null);
        }

        public static void UpdateTargetTableName1AndTargetFieldName1ForRateType()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateTargetTableName1AndTargetFieldName1ForRateType_RedLion", SQL_UpdateTargetTableName1AndTargetFieldName1ForRateType, null);
        }
    }
}
