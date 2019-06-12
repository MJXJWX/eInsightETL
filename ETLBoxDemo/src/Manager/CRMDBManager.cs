using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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
                @"SELECT  cm.PK_ContactMethod FROM  dbo.PMS_ContactMethod cm with (nolock)  WHERE cm.RecordStatus = 'Inactive'  and (cm.LastUpdated >= '{0}' OR cm.DateInserted >= '{0}')  ";

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

        public static readonly string SQL_EmailVerifyWithBounceRules =
                @"insert into D_CUSTOMER_EMAIL_SUMMARY
                (PK_Email, EmailInsertDate, EmailHash, EmailDomainHash)
                select distinct email, GETDATE() as EmailInsertDate, CHECKSUM(Email) as EmailHash, 
                CHECKSUM(LTRIM(RTRIM(SUBSTRING(Email,CHARINDEX('@',Email)+(1),LEN(Email))))) as EmailDomainHash from D_Customer with (nolock)
                where EmailStatus = 0 and Email like '%@%' and
                Email not in (select PK_Email from D_CUSTOMER_EMAIL_SUMMARY with (nolock))

                --Emails that need reprocessing of EmailStatus
                IF OBJECT_ID('tempdb..#EmailsToUpdate') IS NOT NULL 
                DROP TABLE #EmailsToUpdate

                select PK_Email into #EmailsToUpdate from D_CUSTOMER_EMAIL_SUMMARY with (nolock)
                where ETLDeliverDataUpdated = 1 or ETLBounceDataUpdated = 1 or EmailStatus = 0

                --Pass 1: Flag all bad emails that have been gone through email validation service and has either bad domain or bad syntax.
                Update D_CUSTOMER_EMAIL_SUMMARY
                set EmailStatus = 4, EmailSummaryUpdateDate = GETDATE()
                where EmailValidationStatus = 2 and EmailStatus <> 4

                --Run by emailverify function as well
                UPDATE  CES
                SET     CES.EmailStatus = 4, EmailSummaryUpdateDate = GETDATE()   
                FROM    D_CUSTOMER_EMAIL_SUMMARY AS CES
                WHERE   CES.EmailStatus = 0 and dbo.IsValidEmail(CES.PK_Email) = 0

                --Pass 2: Flag emails that are invalid due to bounce rules
                Update es
                set es.EmailStatus = 8, EmailSummaryUpdateDate = GETDATE()
                from D_Customer_Email_Summary es with (nolock)
                inner join #EmailsToUpdate eu with (nolock) on es.PK_Email = eu.PK_Email
                where exists (
                select 1 from D_Customer_Email_Bounce_Summary ebs with (nolock)
                inner join Cendyn_CRM_Email_Bounce_Rules r with (nolock) 
                on ebs.BounceReason = r.BounceReason
                and ISNULL(ebs.BouncesAfterLastDeliver, TotalBounces) >= r.MaxBouncesAllowed 
                and es.PK_Email = ebs.FK_Email
                and r.BounceAction = 'Invalidate Email')
                and es.EmailStatus not in (4,8)

                --Pass 3: Invalid due to bounce rules (unsubscribe)
                Update es
                set es.EmailStatus = 9, EmailSummaryUpdateDate = GETDATE()
                from D_Customer_Email_Summary es with (nolock)
                inner join #EmailsToUpdate eu with (nolock) on es.PK_Email = eu.PK_Email
                where exists (
                select 1 from D_Customer_Email_Bounce_Summary ebs with (nolock)
                inner join Cendyn_CRM_Email_Bounce_Rules r with (nolock) 
                on ebs.BounceREason = r.BounceReason
                and ISNULL(ebs.BouncesAfterLastDeliver, TotalBounces) >= r.MaxBouncesAllowed 
                and es.PK_Email = ebs.FK_Email
                and r.BounceAction = 'Unsubscribe Email')
                and es.EmailStatus not in (4,8,9)

                --Pass 4: Flag all emails that have been manually suppressed.
                UPDATE CS
                SET CS.EmailStatus = 7, EmailSummaryUpdateDate = GETDATE()
                FROM dbo.D_CUSTOMER_EMAIL_SUMMARY CS with (nolock)
                INNER JOIN dbo.eInsight_SuppressEmails S with (nolock)
                ON CS.PK_Email = S.Email
                WHERE CS.EmailStatus not in (4,7,8,9)

                --Pass 5: Flag unsubscribed emails
                UPDATE  CES
                SET     CES.EmailStatus = 5, EmailSummaryUpdateDate = GETDATE()
                FROM    D_Customer_Email_Summary AS CES
                WHERE   CES.EmailStatus IN (0,1)
                        AND EXISTS ( SELECT 1
                                        FROM   ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH (NOLOCK)
                                        WHERE U.EmailAddress = CES.PK_Email ) 
                     
                --Abuse Unsubscribes
                UPDATE  CES
                SET     IsAbuseUnsubscribed = 1
                FROM    D_Customer_Email_Summary AS CES WITH (NOLOCK) 
                INNER JOIN ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH (NOLOCK)
                ON U.EmailAddress = CES.PK_Email
                WHERE U.Method = 'Abuse Complaint' and CES.IsAbuseUnsubscribed = 0

                --Global Unsubscribes
                UPDATE  CES
                SET     IsGlobalUnsubscribed = 1
                FROM    D_Customer_Email_Summary AS CES WITH (NOLOCK) 
                INNER JOIN ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH (NOLOCK)
                ON U.EmailAddress = CES.PK_Email
                WHERE U.Method <> 'Abuse Complaint' and CES.IsGlobalUnsubscribed = 0

                --Pass 6: Flag suppress emails that come from PMS (AllowEmail = 0)
                Update CES
                SET    CES.EmailStatus = 5, EmailSummaryUpdateDate = GETDATE()
                FROM   D_Customer_Email_Summary CES with (nolock) inner join D_CUSTOMER C with (nolock)
                on CES.PK_Email = C.Email 
                WHERE CES.EmailStatus in (0,1) and ISNULL(C.AllowEmail, 1) = 0 and 
                EXISTS (select 1 from D_PROPERTY P with (nolock) where P.PropertyCode = C.PropertyCode and P.UseAllowEmail = 1)

                --Pass 7: Reset Status for emails that were bounced before but got delivered or just got delived as we know those are good.
                Update D_CUSTOMER_EMAIL_SUMMARY
                set EmailStatus = 1 ,EmailSummaryUpdateDate = GETDATE()
                where ETLDeliverDataUpdated = 1 and EmailStatus in (0,8)

                --Pass 8: Remaining Emails as good
                Update D_CUSTOMER_EMAIL_SUMMARY
                set EmailStatus = 1
                where EmailValidationStatus = 1 and EmailStatus = 0

                --Pass 9: emails that have not been validated yet by emailvalidation service will be validated by old verify
                UPDATE  CES
                SET     CES.EmailStatus = CASE WHEN dbo.IsValidEmail(CES.PK_Email) = 0 THEN 4 ELSE 1 END              
                FROM    D_CUSTOMER_EMAIL_SUMMARY AS CES
                WHERE   EmailStatus = 0

                /* Update EmailStatus in D_Customer and D_Customer_Email_Summary if setting is ON */

                /* Add setting when bounce rules go live
                insert into eInsightCRM_ConfigurationSettings (ConfigurationSettingName, ConfigurationSettingValue, Created)
                select 'UseEmailVerifyBounceRules', 'Y', GETDATE()
                where not exists (select 1 from eInsightCRM_ConfigurationSettings where ConfigurationSettingName = 'UseEmailVerifyBounceRules')
                */

                IF exists (select 1 from eInsightCRM_ConfigurationSettings where ConfigurationSettingName = 'UseEmailVerifyBounceRules' 
                and ConfigurationSettingValue = 'Y')  
                begin 

                /* 4 Statuses in D_Customer and D_Customer_Email. Blank/NULL (Status 2),  Valid (Status 1), Invalid (Status 4) and Unsubscribe (Status 5) */
                ----------D_Customer----------
                --NULL or Blank
                Update C
                Set C.EmailStatus = 2
                From D_CUSTOMER C
                WHERE (ltrim(rtrim(C.Email)) = '' OR C.Email IS NULL)
                and C.EmailStatus <> 2
                --Invalid
                Update C
                set C.EmailStatus = 4
                from D_CUSTOMER C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 4 and ES.EmailStatus in (4,7,8,9)
                --Unsubscribed
                Update C
                set C.EmailStatus = 5
                from D_CUSTOMER C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 5 and ES.EmailStatus = 5
                --Valid
                Update C
                set C.EmailStatus = 1
                from D_CUSTOMER C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 1 and ES.EmailStatus = 1

                ----------D_Customer_Email----------
                --Invalid
                Update C
                set C.EmailStatus = 4
                from D_CUSTOMER_EMAIL C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 4 and ES.EmailStatus in (4,7,8,9)
                --Unsubscribed
                Update C
                set C.EmailStatus = 5
                from D_CUSTOMER_EMAIL C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 5 and ES.EmailStatus = 5
                --Valid
                Update C
                set C.EmailStatus = 1
                from D_CUSTOMER_EMAIL C with (nolock) inner join D_CUSTOMER_EMAIL_SUMMARY ES with (nolock)
                on C.Email = ES.PK_Email
                where C.EmailStatus <> 1 and ES.EmailStatus = 1
                print 'Updates performed on D_Customer and D_Customer_Email as setting is enabled' 
                end 
                else 
                begin print 'No updates performed on D_Customer and D_Customer_Email as setting is not enabled' 
                end 

                --Reset ETL Deliver and Bounce flags after emailverification is complete
                Update es
                set es.ETLBounceDataUpdated = 0, es.ETLDeliverDataUpdated = 0
                from D_CUSTOMER_EMAIL_SUMMARY es with (nolock) inner join #EmailsToUpdate eu with (nolock)
                on es.PK_Email = eu.PK_Email";

        public static readonly string SQL_EmailVerifyOnD_Customer =
                @"IF EXISTS
                    (
                    SELECT 1
                    FROM eInsightCRM_ConfigurationSettings WITH
                    (NOLOCK)
                    WHERE ConfigurationSettingName = 'UseEmailVerifyBounceRules'
                    AND ConfigurationSettingValue = 'Y'
                    )
                    GOTO skip_emailverify;
                    SET NOCOUNT ON;

                    DECLARE @ProcessStartTime DATETIME2;
                    DECLARE @ProcessMessage VARCHAR(MAX);
                    DECLARE @RowCount INT = 0;

                    RAISERROR('Starting NONCONSENT Update', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME(); -- Start Timing Section
                    SET @RowCount = 0;

                    DECLARE @ETL_RowVersion_SettingKey VARCHAR(50) = 'ETL_RowVersion';
                    DECLARE @ETL_EmailSubscriptionEvent_ID_SettingKey VARCHAR(50) = 'ETL_EmailSubscriptionEvent_Key';
                    DECLARE @Starting_DBTS ROWVERSION = @@DBTS;
                    DECLARE @Starting_EmailSubEventID INT;
                    DECLARE @ETL_RowVersion ROWVERSION;
                    DECLARE @ETL_EmailSubscriptionEvent_LastId INT;
                    DECLARE @ETL_EmailVerify_CustomerID_SettingKey VARCHAR(50) = 'ETL_EmailVerify_CustomerID';
                    DECLARE @Starting_ETL_EmailVerify_CustomerID INT;
                    DECLARE @ETL_EmailVerify_CustomerID INT;

                    -- Get the last CustomerID
                    SELECT @Starting_ETL_EmailVerify_CustomerID = ISNULL(MAX(CustomerID), 0)
                    FROM D_CUSTOMER WITH
                    (NOLOCK);
                    IF (NOT EXISTS
                    (
                    SELECT 1
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_EmailVerify_CustomerID_SettingKey
                    )
                    )
                    BEGIN
                    INSERT INTO [eInsightCRM_ConfigurationSettings]
                    (
                    [ConfigurationSettingName],
                    [ConfigurationSettingValue],
                    [Created],
                    [LastUpdated]
                    )
                    VALUES
                    (@ETL_EmailVerify_CustomerID_SettingKey, @Starting_ETL_EmailVerify_CustomerID, GETDATE(), GETDATE());
                    END;
                    SELECT @ETL_EmailVerify_CustomerID = CONVERT(INT, [ConfigurationSettingValue])
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_EmailVerify_CustomerID_SettingKey;

                    SELECT @Starting_EmailSubEventID = ISNULL(MAX([id]), 0)
                    FROM [email_subscription_event] WITH
                    (NOLOCK);
                    IF (NOT EXISTS
                    (
                    SELECT 1
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_EmailSubscriptionEvent_ID_SettingKey
                    )
                    )
                    BEGIN
                    INSERT INTO [eInsightCRM_ConfigurationSettings]
                    (
                    [ConfigurationSettingName],
                    [ConfigurationSettingValue],
                    [Created],
                    [LastUpdated]
                    )
                    VALUES
                    (@ETL_EmailSubscriptionEvent_ID_SettingKey, @Starting_EmailSubEventID, GETDATE(), GETDATE());
                    END;
                    SELECT @ETL_EmailSubscriptionEvent_LastId = CONVERT(INT, [ConfigurationSettingValue])
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_EmailSubscriptionEvent_ID_SettingKey;

                    -- Get the last RowVersion from previous run
                    IF (NOT EXISTS
                    (
                    SELECT 1
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_RowVersion_SettingKey
                    )
                    )
                    BEGIN
                    INSERT INTO [eInsightCRM_ConfigurationSettings]
                    (
                    [ConfigurationSettingName],
                    [ConfigurationSettingValue],
                    [Created],
                    [LastUpdated]
                    )
                    VALUES
                    (@ETL_RowVersion_SettingKey, CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), @Starting_DBTS), 1), GETDATE(), GETDATE());
                    END;
                    SELECT @ETL_RowVersion = CONVERT(VARBINARY(8), [ConfigurationSettingValue], 1)
                    FROM [eInsightCRM_ConfigurationSettings] WITH
                    (NOLOCK)
                    WHERE [ConfigurationSettingName] = @ETL_RowVersion_SettingKey;

                    RAISERROR('    Last Subscription Event ID: %s', 0, 1, @ETL_EmailSubscriptionEvent_ID_SettingKey) WITH NOWAIT;
                    DECLARE @ETL_RowVersion_VC VARCHAR(50) = CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), @ETL_RowVersion), 1);
                    RAISERROR('    Last RowVersion: %s', 0, 1, @ETL_RowVersion_VC) WITH NOWAIT;
                    RAISERROR('    Last CustomerID: %d', 0, 1, @ETL_EmailVerify_CustomerID) WITH NOWAIT;

                    CREATE TABLE #ChangedEmails
                    (
                    [Email] VARCHAR(255)
                    );
                    CREATE CLUSTERED INDEX email ON #ChangedEmails ([Email]);

                    IF (@Starting_ETL_EmailVerify_CustomerID > @ETL_EmailVerify_CustomerID)
                    BEGIN
                    INSERT INTO #ChangedEmails
                    (
                    [Email]
                    )
                    SELECT DISTINCT
                    C.[Email]
                    FROM [D_CUSTOMER] C WITH
                    (NOLOCK)
                    WHERE C.[CustomerID]
                    BETWEEN @ETL_EmailVerify_CustomerID AND @Starting_ETL_EmailVerify_CustomerID
                    AND C.[EmailStatus] IN ( 1 )
                    AND NOT EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE C.Email = Email
                    );
                    END;

                    IF (@Starting_ETL_EmailVerify_CustomerID > @ETL_EmailVerify_CustomerID)
                    BEGIN
                    INSERT INTO #ChangedEmails
                    (
                    [Email]
                    )
                    SELECT DISTINCT
                    C.[Email]
                    FROM [D_CUSTOMER_EMAIL] C WITH
                    (NOLOCK)
                    WHERE C.[CustomerID]
                    BETWEEN @ETL_EmailVerify_CustomerID AND @Starting_ETL_EmailVerify_CustomerID
                    AND C.[EmailStatus] IN ( 1 )
                    AND DB_NAME() LIKE '%Rosewood%'
                    AND NOT EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE C.Email = Email
                    );
                    END;

                    INSERT INTO #ChangedEmails
                    (
                    [Email]
                    )
                    SELECT DISTINCT
                    C.[Email]
                    FROM [D_CUSTOMER] C WITH
                    (NOLOCK)
                    INNER JOIN [L_DATA_SOURCE_CODE] SC WITH
                    (NOLOCK)
                    ON C.[SourceID] = SC.[SourceID]
                    WHERE SC.[RowVersion] > @ETL_RowVersion
                    AND NOT EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE C.Email = Email
                    );

                    INSERT INTO #ChangedEmails
                    (
                    [Email]
                    )
                    SELECT DISTINCT E.[Email]
                    FROM [email_consent_status] E WITH
                    (NOLOCK)
                    WHERE [RowVersion] > @ETL_RowVersion
                    AND NOT EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE E.Email = Email
                    );

                    INSERT INTO #ChangedEmails
                    (
                    [Email]
                    )
                    SELECT DISTINCT
                    E.[email_address]
                    FROM [email] E WITH
                    (NOLOCK)
                    INNER JOIN [email_subscription_event] ESE WITH
                    (NOLOCK)
                    ON E.[id] = ESE.[email_id]
                    WHERE ESE.[id] > @ETL_EmailSubscriptionEvent_LastId
                    AND NOT EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE E.email_address = Email
                    );

                    IF (EXISTS (SELECT TOP (1) 1 FROM #ChangedEmails))
                    BEGIN
                    BEGIN TRAN RESETEMAILSTATUS;
                    -- Reset Valid,NonConsent Emails back to Unknown in 
                    -- D_CUSTOMER & D_CUSTOMER_EMAIL
                    UPDATE CE
                    SET CE.[EmailStatus] = 0,
                    CE.[UpdateDate] = GETDATE()
                    FROM [D_CUSTOMER_EMAIL] CE
                    WHERE CE.[EmailStatus] IN ( 1, 8 )
                    AND EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE CE.[Email] = [Email]
                    ) 
                    AND DB_NAME() LIKE '%Rosewood%';

                    SET @RowCount = @RowCount + @@ROWCOUNT;

                    UPDATE C
                    SET C.[EmailStatus] = 0,
                    C.[UpdateDate] = GETDATE()
                    FROM [D_CUSTOMER] C
                    WHERE C.[EmailStatus] IN ( 1, 8 )
                    AND EXISTS
                    (
                    SELECT 1 FROM #ChangedEmails WITH (NOLOCK) WHERE C.[Email] = [Email]
                    );

                    SET @RowCount = @RowCount + @@ROWCOUNT;

                    COMMIT TRAN RESETEMAILSTATUS;
                    END;

                    DROP TABLE #ChangedEmails;

                    UPDATE [eInsightCRM_ConfigurationSettings]
                    SET [ConfigurationSettingValue] = @Starting_EmailSubEventID
                    WHERE [ConfigurationSettingName] = @ETL_EmailSubscriptionEvent_ID_SettingKey;

                    UPDATE [eInsightCRM_ConfigurationSettings]
                    SET [ConfigurationSettingValue] = CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), @Starting_DBTS), 1)
                    WHERE [ConfigurationSettingName] = @ETL_RowVersion_SettingKey;

                    UPDATE [eInsightCRM_ConfigurationSettings]
                    SET [ConfigurationSettingValue] = @Starting_ETL_EmailVerify_CustomerID
                    WHERE [ConfigurationSettingName] = @ETL_EmailVerify_CustomerID_SettingKey
                    -- End of ETL Settings

                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    RAISERROR('Starting EMAILSTATUSUPDATE', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    --check for new statuses
                    DECLARE @CountEmailStatus INT;
                    SET @CountEmailStatus =
                    (
                    SELECT COUNT(1) FROM dbo.D_CUSTOMER WITH (NOLOCK) WHERE EmailStatus = 0
                    );

                    BEGIN TRAN EMAILSTATUSUPDATE;

                    IF @CountEmailStatus > 0
                    BEGIN
                    UPDATE C
                    SET C.EmailStatus = CASE --NULL/Blank Emails 
                                    WHEN LTRIM(RTRIM(C.Email)) = ''
                                            OR C.Email IS NULL THEN
                                        2
                                    --Custom query to flag certain emails as status 7 added 8/23/2010 AK
                                    WHEN C.Email LIKE '%@expedia.com' THEN
                                        7
                                    --Bad Emails
                                    WHEN dbo.IsValidEmail(C.Email) = 0 THEN
                                        4
                                    --Has Consent?
                                    WHEN dbo.fn_HasConsent(C.Email) = 0 THEN
                                        8
                                    -- 
                                    ELSE
                                        1
                                END,
                    C.EmailHash = CHECKSUM(C.Email),
                    C.EmailDomainHash = CHECKSUM(LTRIM(RTRIM(SUBSTRING(C.Email, CHARINDEX('@', C.Email) + (1), LEN(C.Email)))))
                    FROM D_CUSTOMER AS C
                    WHERE EmailStatus = 0;

                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    END;

                    COMMIT TRAN EMAILSTATUSUPDATE;   
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    --Update to custom exclusion status for email addresses in eInsight_SupressEmails table. Added by AK-06/12/2013

                    RAISERROR('Starting CUSTOM', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN CUSTOM;

                    UPDATE CS
                    SET CS.EmailStatus = 7
                    FROM dbo.D_CUSTOMER CS
                    INNER JOIN dbo.eInsight_SuppressEmails S WITH
                    (NOLOCK)
                    ON CS.Email = S.Email
                    WHERE CS.EmailStatus <> 7;

                    SET @RowCount = @RowCount + @@ROWCOUNT;

                    UPDATE CS
                    SET CS.EmailStatus = 7
                    FROM dbo.D_CUSTOMER CS
                    INNER JOIN dbo.email AS e WITH
                    (NOLOCK)
                    ON CS.Email = e.email_address
                    INNER JOIN dbo.eInsight_DomainExclusion AS d WITH
                    (NOLOCK)
                    ON d.ExcludeDomain = e.domain_name
                    WHERE CS.EmailStatus <> 7;

                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN CUSTOM;
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    --check for bounces
                    RAISERROR('Starting BOUNCES', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN BOUNCES;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER AS CB
                    WHERE CB.EmailStatus IN ( 1, 8 )
                    AND EXISTS
                    (
                    SELECT 1
                    FROM ECONTACT_CONTACT_BOUNCE_REPORTS AS B WITH
                    (NOLOCK)
                    WHERE B.BounceReason NOT IN ( 'Auto Reply', 'General Soft Bounce', 'Mailbox FULL', 'DNS Failure',
                                            'Transient Failure', 'Spam Detected', 'General Mail Block'
                                        )
                    AND B.BounceDetail NOT LIKE '%postmaster.info.aol.com/errors/554rlyb1%'
                    AND B.BounceDetail NOT LIKE '%postmaster.yahoo.com/421-ts03%'
                    AND B.BounceDetail NOT LIKE '%smtp;554%Comcast block for spam. Please see http://postmaster.comcast.net/smtp-error-codes.php#BL000000%'
                    AND B.BounceDetail NOT LIKE '%smtp;421 mtain-mb03.r1000.mx.aol.com Service unavailable - try again later%'
                    AND B.BounceDetail NOT LIKE '%smtp;421%.mx.aol.com Service unavailable - try again later%'
                    AND B.EmailAddress = CB.Email
                    );
                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN BOUNCES;
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;


                    RAISERROR('Starting BOUNCES1', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN BOUNCES1;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER AS CB
                    WHERE CB.EmailStatus IN ( 1, 8 )
                    AND EXISTS
                    (
                    SELECT 1
                    FROM [dbo].[SM_REPORT_BOUNCED] AL WITH
                    (NOLOCK)
                    WHERE AL.Category IN ( '2', '8', '9' )
                    AND AL.Email = CB.Email
                    );

                    SET @RowCount = @RowCount + @@ROWCOUNT;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER AS CB
                    WHERE CB.EmailStatus IN ( 0, 1 )
                    AND EXISTS
                    (
                    SELECT 1
                    FROM dbo.SM_REPORT_BOUNCED WITH
                    (NOLOCK)
                    WHERE MailServiceSource = 'Maropost'
                    AND BounceType = 'hard_bounce'
                    AND Email = CB.Email
                    );

                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN BOUNCES1;
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    --check for re-subscribes. This should be the last check to ensure re-subscribed emails are not flagged as unsubscribed
                    RAISERROR('Starting SUB', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN SUB;

                    UPDATE CU
                    SET CU.EmailStatus = 1
                    FROM D_CUSTOMER AS CU
                    WHERE CU.EmailStatus IN ( 5 )
                    AND NOT EXISTS
                    (
                    SELECT 1
                    FROM ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH
                    (NOLOCK)
                    WHERE U.EmailAddress = CU.Email
                    )
                    AND
                    (
                    (
                        (ISNULL(CU.AllowEMail, 1) = 1)
                        AND (
                            (
                                SELECT UseAllowEmail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
                    )
                    OR
                    (
                        (ISNULL(CU.AllowMail, 1) = 1)
                        AND (
                            (
                                SELECT UseAllowMail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
                    )
                    );
                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN SUB;
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    --check for unsubscribes. This should be the last check to ensure unsubscribed emails are not flagged as anything else
                    RAISERROR('Starting UNSUB', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN UNSUB;

                    UPDATE CU
                    SET CU.EmailStatus = 5
                    FROM D_CUSTOMER AS CU
                    WHERE CU.EmailStatus IN ( 1, 8 )
                    AND
                    (
                    EXISTS
                    (
                    SELECT 1
                    FROM ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH
                    (NOLOCK)
                    WHERE U.EmailAddress = CU.Email
                    )
                    OR
                    (
                        (ISNULL(CU.AllowEMail, 1) = 0)
                        AND (
                            (
                                SELECT UseAllowEmail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
	                    AND DB_NAME() NOT LIKE '%FIRMDALE%'
                    )
                    OR
                    (
                        (ISNULL(CU.AllowEMail, 1) = 0)
                        AND (
                            (
                                SELECT UseAllowEmail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
	                    AND CU.InsertDate >= '{0}'
	                    AND DB_NAME() LIKE '%FIRMDALE%'
                    )
                    OR
                    (
                        (ISNULL(CU.AllowMail, 1) = 0)
                        AND (
                            (
                                SELECT UseAllowMail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
	                    AND DB_NAME() NOT LIKE '%FIRMDALE%'
                    )
                    OR
                    (
                        (ISNULL(CU.AllowMail, 1) = 0)
                        AND (
                            (
                                SELECT UseAllowMail
                                FROM D_PROPERTY WITH
                                    (NOLOCK)
                                WHERE PropertyCode = CU.PropertyCode
                            ) = 1
                            )
	                    AND CU.InsertDate >= '{0}'
	                    AND DB_NAME() LIKE '%FIRMDALE%'
                    )
                    );
                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN UNSUB;
                    SET @ProcessMessage
                    = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;

                    SET NOCOUNT OFF;

                    skip_emailverify:
                    IF EXISTS
                    (
                    SELECT 1
                    FROM eInsightCRM_ConfigurationSettings WITH
                    (NOLOCK)
                    WHERE ConfigurationSettingName = 'UseEmailVerifyBounceRules'
                    AND ConfigurationSettingValue = 'Y'
                    )
                    BEGIN
                    PRINT 'Skipping legacy emailverify as new rules have been enabled';
                    END;
                    ELSE
                    BEGIN
                    PRINT 'Running legacy emailverify';
                    END;";

        public static readonly string SQL_EmailVerifyOnD_CUSTOMER_EMAIL_Rosewood =
                @"IF EXISTS
                    (
                        SELECT 1
                        FROM eInsightCRM_ConfigurationSettings WITH
                            (NOLOCK)
                        WHERE ConfigurationSettingName = 'UseEmailVerifyBounceRules'
                              AND ConfigurationSettingValue = 'Y'
                    )
                        GOTO skip_emailverify;

                    DECLARE @ProcessStartTime DATETIME2;
                    DECLARE @ProcessMessage VARCHAR(MAX);
                    DECLARE @RowCount INT = 0;

                    RAISERROR('check for new statuses', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    DECLARE @CountEmailStatus INT;
                    SET @CountEmailStatus =
                    (
                        SELECT COUNT(1)
                        FROM dbo.D_CUSTOMER_EMAIL WITH
                            (NOLOCK)
                        WHERE EmailStatus = 0
                    );

                    BEGIN TRAN EMAILSTATUSUPDATE;

                    IF @CountEmailStatus > 0
                    BEGIN
                        UPDATE C
                        SET C.EmailStatus = CASE --NULL/Blank Emails 
                                                WHEN LTRIM(RTRIM(C.Email)) = ''
                                                     OR C.Email IS NULL THEN
                                                    2
                                                --Custom query to flag certain emails as status 7 added 8/23/2010 AK
                                                WHEN C.Email LIKE '%@expedia.com' THEN
                                                    7
                                                --Bad Emails
                                                WHEN dbo.IsValidEmail(C.Email) = 0 THEN
                                                    4
							                    --Has Consent?
                                                WHEN dbo.fn_HasConsent(C.Email) = 0 AND C.EmailType NOT IN('UDFC30') THEN
                                                    8
                                                --Good Emails
                                                ELSE
                                                    1
                                            END,
                            C.EmailDomainHash = CHECKSUM(LTRIM(RTRIM(SUBSTRING(C.Email, CHARINDEX('@', C.Email) + (1), LEN(C.Email)))))
                        FROM D_CUSTOMER_EMAIL AS C
                        WHERE EmailStatus = 0;

                        SET @RowCount = @@ROWCOUNT;
                    END;

                    COMMIT TRAN EMAILSTATUSUPDATE;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    RAISERROR('Update to custom exclusion status for email addresses in eInsight_SupressEmails table', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN CUSTOM;

                    UPDATE CS
                    SET CS.EmailStatus = 7
                    FROM dbo.D_CUSTOMER_EMAIL CS
                        INNER JOIN dbo.eInsight_SuppressEmails S WITH
                        (NOLOCK)
                            ON CS.Email = S.Email
                    WHERE CS.EmailStatus <> 7;

                    SET @RowCount = @@ROWCOUNT;

                    UPDATE CS
                    SET CS.EmailStatus = 7
                    FROM dbo.D_CUSTOMER_EMAIL CS
                        INNER JOIN dbo.email AS e WITH
                        (NOLOCK)
                            ON CS.Email = e.email_address
                        INNER JOIN dbo.eInsight_DomainExclusion AS d WITH
                        (NOLOCK)
                            ON d.ExcludeDomain = e.domain_name
                    WHERE CS.EmailStatus <> 7;

                    SET @RowCount = @@ROWCOUNT;

                    COMMIT TRAN CUSTOM;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    RAISERROR('check for bounces(1/3)', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN BOUNCES;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER_EMAIL AS CB
                    WHERE CB.EmailStatus IN ( 0, 1, 8 )
                          AND EXISTS
                    (
                        SELECT 1
                        FROM ECONTACT_CONTACT_BOUNCE_REPORTS AS B WITH
                            (NOLOCK)
                        WHERE B.BounceReason NOT IN ( 'Auto Reply', 'General Soft Bounce', 'Mailbox FULL', 'DNS Failure',
                                                      'Transient Failure', 'Spam Detected', 'General Mail Block'
                                                    )
                              AND B.BounceDetail NOT LIKE '%postmaster.info.aol.com/errors/554rlyb1%'
                              AND B.BounceDetail NOT LIKE '%postmaster.yahoo.com/421-ts03%'
                              AND B.BounceDetail NOT LIKE '%smtp;554%Comcast block for spam. Please see http://postmaster.comcast.net/smtp-error-codes.php#BL000000%'
                              AND B.BounceDetail NOT LIKE '%smtp;421 mtain-mb03.r1000.mx.aol.com Service unavailable - try again later%'
                              AND B.BounceDetail NOT LIKE '%smtp;421%.mx.aol.com Service unavailable - try again later%'
                              AND B.EmailAddress = CB.Email
                    );

                    SET @RowCount = @@ROWCOUNT;
                    COMMIT TRAN BOUNCES;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    RAISERROR('check for bounces(2/3)', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN BOUNCES1;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER_EMAIL AS CB
                    WHERE CB.EmailStatus IN ( 1, 8 )
                          AND EXISTS
                    (
                        SELECT 1
                        FROM [dbo].[SM_REPORT_BOUNCED] AL WITH
                            (NOLOCK)
                        WHERE AL.Category IN ( '2', '8', '9' )
                              AND AL.Email = CB.Email
                    );

                    SET @RowCount = @@ROWCOUNT;
                    COMMIT TRAN BOUNCES1;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;
                    RAISERROR('check for bounces(3/3)', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN BOUNCES2;

                    UPDATE CB
                    SET CB.EmailStatus = 6
                    FROM D_CUSTOMER_EMAIL AS CB
                    WHERE CB.EmailStatus IN ( 1, 8 )
                          AND EXISTS
                    (
                        SELECT 1
                        FROM dbo.SM_REPORT_BOUNCED WITH
                            (NOLOCK)
                        WHERE MailServiceSource = 'Maropost'
                              AND BounceType = 'hard_bounce'
                              AND Email = CB.Email
                    );

                    SET @RowCount = @@ROWCOUNT;
                    COMMIT TRAN BOUNCES2;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;

                    RAISERROR('Starting SUB', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN SUB;

                    UPDATE CU
                    SET CU.EmailStatus = 1
                    FROM D_CUSTOMER_EMAIL AS CU
                    INNER JOIN D_CUSTOMER AS D WITH
                        (NOLOCK)
                            ON CU.CustomerID = D.CustomerID
                    WHERE CU.EmailStatus IN ( 5 )
                          AND NOT EXISTS
                    (
                        SELECT 1
                        FROM ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH
                            (NOLOCK)
                        WHERE U.EmailAddress = CU.Email
                    )
                          AND
                          (
                              (
                                  (ISNULL(D.AllowEMail, 1) = 1)
                                  AND (
                                      (
                                          SELECT UseAllowEmail
                                          FROM D_PROPERTY WITH
                                              (NOLOCK)
                                          WHERE PropertyCode = D.PropertyCode
                                      ) = 1
                                      )
                              )
                              OR
                              (
                                  (ISNULL(D.AllowMail, 1) = 1)
                                  AND (
                                      (
                                          SELECT UseAllowMail
                                          FROM D_PROPERTY WITH
                                              (NOLOCK)
                                          WHERE PropertyCode = D.PropertyCode
                                      ) = 1
                                      )
                              )
                          );
                    SET @RowCount = @RowCount + @@ROWCOUNT;
                    COMMIT TRAN SUB;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;

                    RAISERROR('check for unsubscribes', 0, 1) WITH NOWAIT;
                    SET @ProcessStartTime = SYSDATETIME();
                    SET @RowCount = 0;
                    BEGIN TRAN UNSUB;

                    UPDATE CU
                    SET CU.EmailStatus = 5
                    FROM D_CUSTOMER_EMAIL AS CU
                        INNER JOIN D_CUSTOMER AS D WITH
                        (NOLOCK)
                            ON CU.CustomerID = D.CustomerID
                    WHERE CU.EmailStatus IN ( 1, 8 )
                          AND
                          (
                              EXISTS
                    (
                        SELECT 1
                        FROM ECONTACT_CUSTOMER_UNSUBSCRIBED AS U WITH
                            (NOLOCK)
                        WHERE U.EmailAddress = CU.Email
                    )
                              OR
                              (
                                  (ISNULL(D.AllowEMail, 1) = 0)
                                  AND (
                                      (
                                          SELECT UseAllowEmail
                                          FROM D_PROPERTY WITH
                                              (NOLOCK)
                                          WHERE PropertyCode = D.PropertyCode
                                      ) = 1
                                      )
                              )
		                      OR
                              (
                                  (ISNULL(D.AllowMail, 1) = 0)
                                  AND (
                                      (
                                          SELECT UseAllowMail
                                          FROM D_PROPERTY WITH
                                              (NOLOCK)
                                          WHERE PropertyCode = D.PropertyCode
                                      ) = 1
                                      )
                              )
                          );


                    SET @RowCount = @@ROWCOUNT;
                    COMMIT TRAN UNSUB;
                    SET @ProcessMessage
                        = 'Time Elapsed: ' + CAST(DATEDIFF(MILLISECOND, @ProcessStartTime, SYSDATETIME()) / 1000.00 AS VARCHAR) + 's';
                    RAISERROR('    %i rows affected', 0, 1, @RowCount) WITH NOWAIT;
                    RAISERROR('    %s', 0, 1, @ProcessMessage) WITH NOWAIT;

                    skip_emailverify:
                    IF EXISTS
                    (
                        SELECT 1
                        FROM eInsightCRM_ConfigurationSettings WITH
                            (NOLOCK)
                        WHERE ConfigurationSettingName = 'UseEmailVerifyBounceRules'
                              AND ConfigurationSettingValue = 'Y'
                    )
                    BEGIN
                        PRINT 'Skipping legacy emailverify as new rules have been enabled';
                    END;
                    ELSE
                    BEGIN
                        PRINT 'Running legacy emailverify';
                    END;";

        public static readonly string SQL_GetDataFromD_CustomerToMoveEmails =
                @"SELECT DISTINCT d.CustomerID ,
                       'Email' AS EmailType ,
                       ISNULL(d.Email, '') AS Email ,
                       d.EmailStatus ,
                       NEWID() AS PK_ContactMethod ,
                       em.id AS email_id
                FROM   dbo.D_CUSTOMER AS d WITH ( NOLOCK )
                       INNER JOIN dbo.ETL_TEMP_D_Customer_For_Email AS e WITH ( NOLOCK ) ON d.CustomerID = e.CustomerID
                       LEFT JOIN email AS em WITH ( NOLOCK ) ON ISNULL(em.email_address, '') = ISNULL(d.Email ,'')
                WHERE  ISNULL(d.Email, '') LIKE '%@%'";

        public static readonly string SQL_FillUpMissingDataUnderD_Customer_Email =
                @"IF OBJECT_ID('Tempdb..#missing_emails') IS NOT NULL
                    DROP TABLE #missing_emails;
                SELECT DISTINCT d.CustomerID ,
                                d.Email ,
                                d.EmailStatus ,
                                d.InsertDate
                INTO   #missing_emails
                FROM   D_CUSTOMER AS d WITH ( NOLOCK )
                WHERE  d.Email LIKE '%@%'
                       AND d.SourceID = 1
                       AND NOT EXISTS (   SELECT 1
                                          FROM   D_CUSTOMER_EMAIL WITH ( NOLOCK )
                                          WHERE  d.CustomerID = CustomerID
                                                 AND SourceStayID IS NULL )
                       AND DB_NAME() NOT LIKE '%Rosewood%';

                INSERT INTO dbo.D_CUSTOMER_EMAIL ( CustomerID ,
                                                   SourceStayID ,
                                                   EmailType ,
                                                   Email ,
                                                   EmailStatus ,
                                                   InsertDate ,
                                                   PK_ContactMethod ,
                                                   email_id )
                            SELECT DISTINCT d.CustomerID ,
                                            NULL AS SourceStayID ,
                                            'Email' AS EmailType ,
                                            ISNULL(d.Email, '') AS Email ,
                                            d.EmailStatus ,
                                            d.InsertDate ,
                                            NEWID() AS PK_ContactMethod ,
                                            em.id AS email_id
                            FROM   #missing_emails d
                                   LEFT JOIN dbo.email AS em WITH ( NOLOCK ) ON d.Email = em.email_address
                            WHERE  d.EmailStatus NOT IN ( 2, 4 );


                IF OBJECT_ID('Tempdb..#missing_stay_emails') IS NOT NULL
                    DROP TABLE #missing_stay_emails;
                SELECT DISTINCT s.CustomerID ,
                                s.SourceStayID ,
                                d.Email ,
                                d.EmailStatus ,
                                d.InsertDate ,
                                d.PK_Profiles
                INTO   #missing_stay_emails
                FROM   D_CUSTOMER AS d WITH ( NOLOCK )
                       INNER JOIN dbo.D_CUSTOMER_STAY AS s WITH ( NOLOCK ) ON d.CustomerID = s.CustomerID
                WHERE  d.Email LIKE '%@%'
                       AND d.SourceID = 1
                       AND NOT EXISTS (   SELECT 1
                                          FROM   D_CUSTOMER_EMAIL WITH ( NOLOCK )
                                          WHERE  s.CustomerID = CustomerID
                                                 AND s.SourceStayID = SourceStayID
                                                 AND SourceStayID IS NOT NULL )
                       AND DB_NAME() NOT LIKE '%Rosewood%';

                INSERT INTO dbo.D_CUSTOMER_EMAIL ( CustomerID ,
                                                   SourceStayID ,
                                                   EmailType ,
                                                   Email ,
                                                   EmailStatus ,
                                                   InsertDate ,
                                                   PK_ContactMethod ,
                                                   email_id )
                            SELECT DISTINCT d.CustomerID ,
                                            d.SourceStayID ,
                                            'UDFC30' AS EmailType ,
                                            ISNULL(d.Email, '') AS Email ,
                                            d.EmailStatus ,
                                            d.InsertDate ,
                                            NEWID() AS PK_ContactMethod ,
                                            em.id AS email_id
                            FROM   #missing_stay_emails d
                                   LEFT JOIN dbo.email AS em WITH ( NOLOCK ) ON d.Email = em.email_address
                            WHERE  d.EmailStatus NOT IN ( 2, 4 )
                                   AND NOT EXISTS (   SELECT 1
                                                      FROM   dbo.D_CUSTOMER AS c WITH ( NOLOCK )
                                                             INNER JOIN dbo.PMS_CONTACTMETHOD AS pcm WITH ( NOLOCK ) ON c.PK_Profiles = pcm.FK_Profiles
                                                                                                                        AND c.Email = SUBSTRING(
                                                                                                                                          LTRIM(
                                                                                                                                              RTRIM(
                                                                                                                                                  pcm.CMData)) ,
                                                                                                                                          1 ,
                                                                                                                                          70)
                                                      WHERE  pcm.CMType = 'IP'
                                                             AND pcm.CMCategory = 'Email'
                                                             AND pcm.RecordStatus = 'Inactive'
                                                             AND NOT EXISTS (   SELECT 1
                                                                                FROM   dbo.PMS_CONTACTMETHOD WITH ( NOLOCK )
                                                                                WHERE  FK_Profiles = pcm.FK_Profiles
                                                                                       AND RecordStatus = 'Active'
                                                                                       AND CMType = 'IP'
                                                                                       AND CMCategory = 'Email'
                                                                                       AND SUBSTRING(
                                                                                               LTRIM(
                                                                                                   RTRIM(
                                                                                                       CMData)) ,
                                                                                               1 ,
                                                                                               70) = c.Email )
                                                             AND c.CustomerID = d.CustomerID );";

        public static readonly string SQL_FixEmailsDataUnderD_Customer_Email =
                @"UPDATE e
                        SET e.Email = d.Email,
                            e.EmailStatus = d.EmailStatus,
                            e.EmailDomainHash = d.EmailDomainHash
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN dbo.D_CUSTOMER AS d WITH (NOLOCK)
                                ON e.CustomerID = d.CustomerID
                        WHERE (
                                  ISNULL(e.Email, '') <> ISNULL(d.Email, '')
                                  OR e.EmailStatus <> d.EmailStatus
                              )
                              AND e.EmailType IN ( 'Email', 'UDFC30' )
                              AND DB_NAME() NOT LIKE '%Rosewood%';";

        public static readonly string SQL_FixEmail_IDUnderD_Customer_Email =
                @"IF DB_NAME() NOT LIKE '%Rosewood%'
                       AND DB_NAME() NOT LIKE '%OmniHotels%'
                    BEGIN

                        UPDATE e
                        SET e.email_id = em.id
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN dbo.email AS em WITH (NOLOCK)
                                ON ISNULL(e.Email, '') = ISNULL(em.email_address, '')
                        WHERE ISNULL(e.Email, '') LIKE '%@%'
                              AND e.email_id IS NULL;

                        UPDATE e
                        SET e.email_id = em2.id
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN dbo.email AS em1 WITH (NOLOCK)
                                ON ISNULL(e.Email, '') <> ISNULL(em1.email_address, '')
                                   AND e.email_id = em1.id
                            INNER JOIN dbo.email AS em2 WITH (NOLOCK)
                                ON ISNULL(e.Email, '') = ISNULL(em2.email_address, '')
                                   AND e.email_id <> em2.id
                        WHERE ISNULL(e.Email, '') LIKE '%@%';

                        UPDATE e
                        SET e.email_id = NULL
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                        WHERE (
                                  ISNULL(e.Email, '') = ''
                                  OR ISNULL(e.Email, '') NOT LIKE '%@%'
                              )
                              AND e.email_id IS NOT NULL;

                    END;

                    IF DB_NAME() LIKE '%OmniHotels%'
                    BEGIN

                        BEGIN TRAN Trans_01;

                        UPDATE e
                        SET e.email_id = em.id
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN dbo.email AS em WITH (NOLOCK)
                                ON ISNULL(e.Email, '') = ISNULL(em.email_address, '')
                        WHERE ISNULL(e.Email, '') LIKE '%@%'
                              AND e.email_id IS NULL;

                        COMMIT TRAN Trans_01;

                        BEGIN TRAN Trans_02;

                        -- Determin the last @@DBTS / ROWVERSION since last time ran 
                        DECLARE @Now DATETIME = GETDATE();
                        DECLARE @D_Customer_Email_Fix_Email_ID_RowVersion_SettingKey VARCHAR(50)
                            = 'D_Customer_Email_Fix_Email_ID_RowVersion';
                        DECLARE @Current_DBTS ROWVERSION = @@DBTS;
                        DECLARE @D_Customer_Email_Fix_Email_ID_RowVersion ROWVERSION;
                        --Get the last rowversion from previours run 
                        SELECT @D_Customer_Email_Fix_Email_ID_RowVersion = CONVERT(VARBINARY(8), [ConfigurationSettingValue], 1)
                        FROM [dbo].[eInsightCRM_ConfigurationSettings] eiccs WITH (NOLOCK)
                        WHERE eiccs.ConfigurationSettingName = @D_Customer_Email_Fix_Email_ID_RowVersion_SettingKey;
                        IF (@D_Customer_Email_Fix_Email_ID_RowVersion IS NULL)
                        BEGIN
                            SELECT @D_Customer_Email_Fix_Email_ID_RowVersion = 0;
                        END;

                        --Only get back the records that has been changed in D_Customer_Email table

                        UPDATE e
                        SET e.email_id = em2.id
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                            INNER JOIN dbo.email AS em1 WITH (NOLOCK)
                                ON ISNULL(e.Email, '') <> ISNULL(em1.email_address, '')
                                   AND e.email_id = em1.id
                            INNER JOIN dbo.email AS em2 WITH (NOLOCK)
                                ON ISNULL(e.Email, '') = ISNULL(em2.email_address, '')
                                   AND e.email_id <> em2.id
                        WHERE ISNULL(e.Email, '') LIKE '%@%'
                              AND e.RV > @D_Customer_Email_Fix_Email_ID_RowVersion
                              AND e.RV <= @Current_DBTS;

                        IF EXISTS
                        (
                            SELECT 1
                            FROM eInsightCRM_ConfigurationSettings
                            WHERE [ConfigurationSettingName] = @D_Customer_Email_Fix_Email_ID_RowVersion_SettingKey
                        )
                        BEGIN
                            UPDATE [eInsightCRM_ConfigurationSettings]
                            SET [ConfigurationSettingValue] = CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), @Current_DBTS), 1)
                            WHERE [ConfigurationSettingName] = @D_Customer_Email_Fix_Email_ID_RowVersion_SettingKey;
                        END;
                        ELSE
                        BEGIN
                            INSERT INTO [eInsightCRM_ConfigurationSettings]
                            (
                                [ConfigurationSettingName],
                                [ConfigurationSettingValue],
                                [Created],
                                [LastUpdated]
                            )
                            VALUES
                            (@D_Customer_Email_Fix_Email_ID_RowVersion_SettingKey,
                             CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), @Current_DBTS), 1), GETDATE(), GETDATE());
                        END;
                        COMMIT TRAN Trans_02;

                        BEGIN TRAN Trans_03;

                        UPDATE e
                        SET e.email_id = NULL
                        FROM dbo.D_CUSTOMER_EMAIL AS e
                        WHERE (
                                  ISNULL(e.Email, '') = ''
                                  OR ISNULL(e.Email, '') NOT LIKE '%@%'
                              )
                              AND e.email_id IS NOT NULL;

                        COMMIT TRAN Trans_03;

                    END;";

        public static readonly string SQL_UpdateUnifocusScorePerCustomer =
                @"UPDATE  d
                    SET     d.UNIFOCUS_SCORE =  ROUND(ISNULL(zzz.AverageUNIFOCUSScore, 0), 0)
                    FROM    dbo.D_CUSTOMER AS d
                            INNER JOIN ( SELECT b.GuestNumber ,
                                                zz.AverageUNIFOCUSScore
                                         FROM   dbo.UNIFOCUS_SurveyResultsHeader AS b WITH ( NOLOCK )
                                                INNER JOIN ( SELECT z.FK_SurveyResultsHeader ,
                                                                    AVG(z.GroupValue) AS AverageUNIFOCUSScore
                                                             FROM   ( SELECT    x.*
                                                                      FROM      ( SELECT
                                                                                  a.* ,
                                                                                  ROW_NUMBER() OVER ( PARTITION BY a.GuestNumber ORDER BY a.SurveyTakenDate DESC ) AS rn
                                                                                  FROM
                                                                                  dbo.UNIFOCUS_SurveyResultsHeader
                                                                                  AS a WITH ( NOLOCK )
                                                                                ) x
                                                                      WHERE     x.rn = 1
                                                                    ) y
                                                                    INNER JOIN dbo.UNIFOCUS_QuestionGroupValues
                                                                    AS z WITH ( NOLOCK ) ON y.PK_SurveyResultsHeader = z.FK_SurveyResultsHeader
												                    WHERE z.GroupID IN(1, 56)
                                                             GROUP BY z.FK_SurveyResultsHeader
                                                           ) AS zz ON b.PK_SurveyResultsHeader = zz.FK_SurveyResultsHeader
                                       ) zzz ON d.CustomerID = zzz.GuestNumber";

        public static readonly string SQL_InsertEndTimeIntoLogTable =
                @"UPDATE  ETL_PACKAGE_LOG 
                    SET EndTime = GETDATE(), RecordCount = {0}
                    WHERE Id = {1} AND Component = 'D_CUSTOMER'
                    ";

        public static readonly string SQL_GetDataFromETL_TEMP_PROFILES_D_CUSTOMER_Insert =
                @"SELECT DISTINCT PK_Profiles FROM dbo.ETL_TEMP_PROFILES_D_CUSTOMER_Insert With(Nolock)";

        public static readonly string SQL_GetDataFromPMS_Profile_Mapping =
                @"select DISTINCT pm.FK_Profiles, pm.CustomerID, pm.GlobalCustomerID from PMS_Profile_Mapping as pm with(nolock) 
                    inner join dbo.ETL_TEMP_Profiles_D_Customer_Update as e with(nolock) 
                    on pm.FK_Profiles = e.PK_Profiles";

        public static readonly string SQL_GetDataFromD_Property =
            @"SELECT CendynPropertyId, PropertyCode FROM dbo.D_PROPERTY  with(nolock) 
                    WHERE CendynPropertyID IN(SELECT CendynPropertyID FROM dbo.D_PROPERTY  with(nolock) 
                    GROUP BY CendynPropertyID
                    HAVING COUNT(*) = 1 AND ISNULL(CendynPropertyID, '0') <> '0' AND CendynPropertyID <> '')
                    UNION 
                    select TOP 1 CendynPropertyID, PropertyCode from D_Property with(nolock)
                    WHERE CendynPropertyID IN(SELECT CendynPropertyID FROM dbo.D_PROPERTY  with(nolock) 
                    GROUP BY CendynPropertyID
                    HAVING COUNT(*) > 1 AND ISNULL(CendynPropertyID, '0') <> '0' AND CendynPropertyID <> '')
                    UNION 
					Select 0 as CendynPropertyID, '' as PropertyCode ;";

        public static readonly string SQL_GetDataFromD_customerAndETL_TEMP_Profiles_D_Customer =
                @"select DISTINCT d.PK_Profiles, d.CustomerID from dbo.d_customer as d with(nolock) 
                    inner join ETL_TEMP_Profiles_D_Customer as e with(nolock) 
                    on d.PK_Profiles = e.PK_Profiles";

        public static readonly string SQL_GetDataFromPMS_PROFILE_MAPPING =
                @"SELECT pm.CustomerID ,
                           pm.GlobalCustomerID ,
                           pm.FK_Profiles
                    FROM   PMS_PROFILE_MAPPING AS pm WITH ( NOLOCK )
                           INNER JOIN dbo.ETL_TEMP_PROFILES_D_CUSTOMER AS p WITH ( NOLOCK ) ON pm.FK_Profiles = p.PK_Profiles;";

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


        public static readonly string SQL_UpdatePMS_Address = @"UPDATE PMS_Address SET AddressTypeCode = 'W' WHERE AddressTypeCode = 'U'";

        public static string CreateSourceCode(string companyId, string sourceName, string subSourceName, byte isShowdropdown, int dedupPriority, int ETLProcess)
        {
            string sql = string.Format(SQL_CreateSourceCode, companyId, sourceName, subSourceName, isShowdropdown, dedupPriority, ETLProcess);
            return SQLHelper.GetDbValues(GetCRMConnectionString(), $"SQL_CreateSourceCode_{subSourceName.Trim()}", sql, null).FirstOrDefault()?["SourceID"];
        }

        public static string GetLastCheckTime(string sourceTable)
        {
            string sqlGetLastCheckTime = $@"SELECT ISNULL(MAX(DriverExecutionDate), DATEADD(yy,-10,GETDATE())) AS LastCheckTime FROM ETL_PACKAGE_LOG WHERE Component = '{sourceTable}' AND EndTime IS NOT NULL";
            List<Dictionary<string, string>> sqlGetLastCheckTimeList = SQLHelper.GetDbValues(GetCRMConnectionString(), $"SQL_Get{sourceTable}LastCheckTime", sqlGetLastCheckTime, null);
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
            List<Dictionary<string, string>> SQL_GetContactMethodInactiveRecordsList = SQLHelper.GetDbValues(GetCRMConnectionString(), "SQL_GetContactMethodInactiveRecords", string.Format(SQL_GetContactMethodInactiveRecords, CompanySettings.StartDate), null);
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

        public static void EmailVerifyWithBounceRules()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_EmailVerifyWithBounceRules", SQL_EmailVerifyWithBounceRules, null);
        }

        public static void EmailVerifyOnD_Customer()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_EmailVerifyOnD_Customer", string.Format(SQL_EmailVerifyOnD_Customer, CompanySettings.StartDate), null);
        }

        public static void EmailVerifyOnD_CUSTOMER_EMAIL_Rosewood()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_EmailVerifyOnD_CUSTOMER_EMAIL_Rosewood", SQL_EmailVerifyOnD_CUSTOMER_EMAIL_Rosewood, null);
        }

        public static void FillUpMissingDataUnderD_Customer_Email()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_FillUpMissingDataUnderD_Customer_Email", SQL_FillUpMissingDataUnderD_Customer_Email, null);
        }

        public static void FixEmailsDataUnderD_Customer_Email()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_FixEmailsDataUnderD_Customer_Email", SQL_FixEmailsDataUnderD_Customer_Email, null);
        }

        public static void FixEmail_IDUnderD_Customer_Email()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_FixEmail_IDUnderD_Customer_Email", SQL_FixEmail_IDUnderD_Customer_Email, null);
        }

        public static void UpdateUnifocusScorePerCustomer()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdateUnifocusScorePerCustomer", SQL_UpdateUnifocusScorePerCustomer, null);
        }

        public static void InsertEndTimeIntoLogTable()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_InsertEndTimeIntoLogTable", string.Format(SQL_InsertEndTimeIntoLogTable,"",""), null);
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
        
        public static void UpdateAddressTypeCodeUnderPMS_Address()
        {
            SQLHelper.InsertOrUpdateDbValue(GetCRMConnectionString(), "SQL_UpdatePMS_Address", SQL_UpdatePMS_Address, null);
        }
        
        public static void BulkInsertTempTable(DataTable data, string tableName, SqlConnection conn)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                for (int i = 0; i < data.Columns.Count; i++)
                {
                    bulkCopy.ColumnMappings.Add(data.Columns[i].ColumnName, data.Columns[i].ColumnName);
                }
                bulkCopy.WriteToServer(data);
            }

        }

        public static void UpsertData(DataTable data, string tableName, List<string> keys, List<string> updateFields, string lookUpSql = "", Dictionary<string, string> lKeys = null, Dictionary<string, string> lMappings = null)
        {
            try
            {
                var logger = new LogTask()
                {
                    Message = "Data Flow Task: Upsert Data",
                    ActionType = "Start"
                };
                logger.Info();
                var conn = new SqlConnection(GetCRMConnectionString());
                conn.Open();

            StringBuilder columns = new StringBuilder();
            for (int i = 0; i < data.Columns.Count; i++)
            {
                if (columns.Length > 0)
                    columns.Append(", ");
                columns.Append($"{data.Columns[i].ColumnName} {(data.Columns[i].DataType.Name == "Guid" ? "uniqueidentifier" : "nvarchar(2000)")}");
            }
            var createTempTableCommand = $"Create Table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} ({columns.ToString()})";

            //Execute the command to make a temp table
            SqlCommand cmd = new SqlCommand(createTempTableCommand, conn);
            cmd.ExecuteNonQuery();
            
            BulkInsertTempTable(data, $"#temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)}", conn);

            var insertKeys = new List<string>();
            updateFields.ForEach(f => {
                if (!insertKeys.Contains(f))
                    insertKeys.Add(f);
            });

            keys.ForEach(k => {
                if (updateFields.Contains(k))
                    updateFields.Remove(k);
                if (!insertKeys.Contains(k))
                    insertKeys.Add(k);
            });

            StringBuilder mergeJoin = new StringBuilder();
            if (!string.IsNullOrEmpty(lookUpSql) && lMappings != null && lMappings.Count > 0)
            {
                List<string> tkeys = new List<string>();
                foreach (var key in lKeys.Keys)
                {
                    tkeys.Add($" L.{key} = T.{lKeys[key]} ");
                    //lMappings.TryAdd(key, lKeys[key]); 
                }

                List<string> tMaps = new List<string>();
                foreach (var key in lMappings.Keys)
                {
                    tMaps.Add($" LT.{key} as {lKeys[key]} ");
                }

                mergeJoin.Append($" as T Inner join ( SELECT {string.Join(" ,", tMaps)} FROM ( {lookUpSql} ) LT ) as L ON {string.Join(" AND ", tkeys)} ");
            }

            //use the merge command to upsert from the temp table to the destination table
            string mergeSql = $"merge into {tableName} as Target  using (Select * from #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} {mergeJoin.ToString()}) as Source  on  {string.Join(" AND ", keys.ConvertAll(k => k = $"Target.{k} = Source.{k} "))} when matched then update set {string.Join(" , ", updateFields.ConvertAll(c => c = $"Target.{c} = Source.{c}"))} when not matched then insert ({string.Join(",", updateFields)}) values (Source.{string.Join(", Source.", updateFields)});";

            cmd.CommandText = mergeSql;
            var rowsAffected = cmd.ExecuteNonQuery();

            //Clean up the temp table
            cmd.CommandText = $"drop table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)};";
            cmd.ExecuteNonQuery();
                ControlFlow.STAGE = (int.Parse(ControlFlow.STAGE) + 1) + "";
                logger.Message += ", Rows Affected: "+ rowsAffected;
                logger.ActionType = "END";
                logger.Info();
                ControlFlow.STAGE = (int.Parse(ControlFlow.STAGE) - 1) + "";
            }
            catch (Exception ex)
            {

            }
        }
    }
}
