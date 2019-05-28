using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public static class PMSDBManager
    {
        public static string GetPMSConnectionString()
        {
            return string.Format(CompanySettings.ConnectionStringFormat_Sql, CompanySettings.ETL_CENRES_SERVERNAME, CompanySettings.ETL_CENRES_DATABASENAME, CompanySettings.ETL_CENRES_DBUSER, CompanySettings.ETL_CENRES_DBPASSWORD);
        }

        #region Sql Statement
        public static readonly string SQL_GetDataFromProfileDocument = 
            @"SELECT Id ,
                    FK_Profile ,
                    DocType ,
                    DocSource ,
                    CodeOnDocument ,
                    DocNotes ,
                    DocId_PII ,
                    NameOnDocument_PII ,
                    DocumentBody_PII ,
                    NationalityOnDocument ,
                    EffectiveDate ,
                    ExpirationDate ,
                    PII_StoredAs ,
                    PII_Algorithm ,
                    PII_Key ,
                    PII_KeyId ,
                    Issuer ,
                    IssuerAddress1 ,
                    IssuerAddress2 ,
                    IssuerCity ,
                    IssuerStateProv ,
                    IssuerPostalCode ,
                    IssuerCountry ,
                    IsPrimary ,
                    InactiveDate ,
                    DateCreated ,
                    LastUpdated FROM dbo.ProfileDocuments With(Nolock)
            WHERE (LastUpdated >= '{0}' OR DateCreated >= '{0}') 
            AND LastUpdated <= '{1}' and DateCreated <= '{1}'";
        
        public static readonly string SQL_GetDataFromProfilePolicies =
            @"SELECT PK_ProfilePolicies ,
                FK_Profiles ,
                FK_PolicyTypes ,
                AttributeName ,
                IntegerValue ,
                StringValue ,
                StartDate ,
                ExpirationDate ,
                Comments ,
                DateInserted ,
                LastUpdated FROM dbo.ProfilePolicies With(Nolock)
            WHERE (LastUpdated >=  '{0}' OR DateInserted >=  '{0}') 
            AND LastUpdated <= '{1}' and DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromContactMethodPolicies =
            @"SELECT PK_ContactMethodPolicies ,
                FK_ContactMethod ,
                FK_PolicyTypes ,
                AttributeName ,
                IntegerValue ,
                StringValue ,
                StartDate ,
                ExpirationDate ,
                Comments ,
                DateInserted ,
                LastUpdated FROM dbo.ContactMethodPolicies With(Nolock)
            WHERE (LastUpdated >= '{0}' OR DateInserted >= '{0}') 
            AND LastUpdated <= '{1}' and DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromContactMethod =
            @"SELECT  cm.PK_ContactMethod,
                cm.FK_Reservations,
                cm.FK_Profiles,
                cm.CMStatusId,
                cm.CMType,
                cm.CMData,
                cm.CMCategory,
                cm.CMOptOut,
                cm.CMSourceDate,
                cm.DateInserted,
                cm.LastUpdated,
                cm.Checksum,cm.IsDirty, 
                cm.IsPrimary, 
                cm.Confirmation, 
                cm.CMExtraData, 
                cm.InactiveDate, 
                cm.RecordStatus FROM dbo.V_ContactMethod cm with (nolock) 
            WHERE  (cm.LastUpdated >= '{0}' OR cm.DateInserted >= '{0}') and cm.LastUpdated <= '{1}' and cm.DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromActionComments =
            @"SELECT PK_ActionComments, 
            FK_Internal, 
            KeyTable, 
            CommentType, 
            ExternalID, 
            ActionType, 
            ActionText, 
            ActionTypeID, 
            ActionDate, 
            InactiveDate, 
            GuestViewable, 
            PMSCreatorCode, 
            DatePMSCommentCreated, 
            DateInserted, 
            LastUpdated, 
            Checksum, 
            IsDirty, 
            CommentTitle, 
            CRMSourceActionType, 
            RecordStatus, 
            CommentClass, 
            ResortID FROM dbo.[V_PMS_ActionComments] p 
            WHERE (p.LastUpdated >= '{0}' OR p.DateInserted >= '{0}') and p.LastUpdated <= '{1}' and p.DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromAddress =
            @"SELECTA.PK_Address, 
                A.FK_Profiles, 
                A.AddressTypeCode, 
                A.SourceAddressType, 
                A.RecordStatus, 
                A.AddressStatus, 
                A.Attn, 
                A.Address1, 
                A.Address2, 
                A.City, 
                A.StateProvince, 
                A.PostalCode, 
                A.CountryCode, 
                A.DateInserted, 
                A.LastUpdated, 
                A.Checksum, 
                A.IsDirty, 
                A.IsPrimary, 
                A.AddressCleansed, 
                A.AddressLanguage FROM dbo.V_Address A with(nolock) 
            WHERE RecordStatus = 'Active' AND (A.LastUpdated >= '{0}' OR A.DateInserted >= '{0}') and A.LastUpdated <= '{1}' and A.DateInserted <= '{1}'";
        
        public static readonly string SQL_GetSourceNameFromContactMethod =
            @"SELECT cm.PK_ContactMethod, 
                cm.SourceName FROM dbo.ContactMethod cm with (nolock) 
            WHERE (cm.LastUpdated >= '{0}' OR cm.DateInserted >= '{0}') and cm.LastUpdated <= '{1}' and cm.DateInserted <='{1}'";
        
        public static readonly string SQL_UpdatePMS_Address = @"UPDATE PMS_Address SET AddressTypeCode = 'W' WHERE AddressTypeCode = 'U'";
        
        public static readonly string SQL_GetDataFromV_SpecialRequests =
            @"SELECT sr.PK_SpecialRequests ,
                    sr.FK_Reservations ,
                    sr.FK_Profiles ,
                    sr.ExternalRPH ,
                    sr.RequestType ,
                    sr.RequestCode ,
                    sr.RequestComments ,
                    sr.ResortField ,
                    sr.ActionTypeCode ,
                    sr.SourceActionType ,
                    sr.CRMSourceActionType ,
                    sr.InactiveDate ,
                    sr.DateInserted ,
                    sr.LastUpdated ,
                    sr.Checksum ,
                    sr.IsDirty ,
                    sr.Quantity FROM dbo.V_SpecialRequests AS sr With(Nolock) WHERE  (sr.LastUpdated >= '{0}' OR sr.DateInserted >= '{0}') and sr.LastUpdated <= '{1}' and sr.DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromSpecialRequests =
            @"SELECT [PK_SpecialRequests],
                        [FK_Profiles],
                        [RequestType],
                        [RequestCode],
                        CAST( REPLACE(sr.RequestComments, '?', '') AS VARCHAR(1000) )  AS RequestComments  FROM SpecialRequests sr WITH(NOLOCK)  WHERE FK_Profiles IS NOT NULL AND  (sr.LastUpdated >= '{0}' OR sr.DateInserted >= '{0}') and sr.LastUpdated <= '{1}' and sr.DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromProfiles = 
            @"SELECT ''AS PropertyCode,
                    p.PK_Profiles AS PK_Profiles,
                    CAST(p.ExternalProfileID AS VARCHAR(50)) AS SourceGuestID,
                    LTRIM(RTRIM(ISNULL(p.FirstName, ''))) AS FirstName,
                    p.MiddleName AS MiddleName, 
                    LTRIM(RTRIM(ISNULL(p.LastName, ''))) AS LastName,
                    p.Salutation AS Salutation,
                    convert(varchar(50), p.Salutation) as ShortTitle,
                    CAST(p.gendercode AS NVARCHAR(1)) AS GenderCode, 
                    p.CompanyName AS Company,
                    p.CompanyName AS CompanyTitle,
                    p.JobTitle,	
                    CAST(p.PrimaryLanguage AS NVARCHAR(50)) AS Languages,
                    CASE WHEN 0 = 7375 AND p.ProfileTypeCategory = 'OpenTable' Then 0 WHEN 0 = 7375 AND p.ExternalProfileID2 = 'CounterPoint' Then 0 ELSE 0 END as SourceID, 
                    0 AS DedupeCheck,
                    p.DatePMSProfileUpdated AS DatePMSProfileUpdated, 
                    ISNULL(AllowEMail, 1) as AllowEMail, 
                    ISNULL(AllowMail, 1) AllowMail,
                    ISNULL(AllowSMS, 1) as AllowSMS, 
                    ISNULL(AllowPhone, 1) as AllowPhone,
                    ISNULL(Membership, '') as Membership, 
                    p.ExternalProfileID2, 
                    VIPID, 
                    VIPCode, 
                    p.Nationality FROM dbo.Profiles p WITH (NOLOCK) INNER JOIN ETL_TEMP_Profiles AS e WITH(NOLOCK) ON p.PK_Profiles = e.PK_Profiles 
                                                                    WHERE p.RecordStatus = 'Active' AND p.ExternalProfileID <> ''";

        public static readonly string SQL_GetDataFromProfilesToUpdatePropertyCode = 
            @"SELECT DISTINCT 
                p.PK_Profiles AS PK_Profiles, 
                p.CendynPropertyID 
                FROM dbo.Profiles p WITH (NOLOCK)
                INNER JOIN dbo.ETL_TEMP_Profiles as e with(nolock) on p.PK_Profiles = e.PK_Profiles 
                WHERE p.RecordStatus = 'Active' AND p.ExternalProfileID <> ''";

        public static readonly string SQL_GetDataFromProfilesToUpsertPMS_Profiles = 
            @"SELECT p.PK_Profiles,
                        p.RecordStatus, 
                        p.SourceTypeId, 
                        p.ExternalProfileID, 
                        p.PMSProfileID, 
                        p.PMSNameCode, 
                        p.CendynPropertyID, 
                        p.ProfileTypeID, 
                        p.ProfileTypeCode,
                        p.SourceProfileType, 
                        p.SourceProfileNameCode, 
                        p.ProfileTypeCategory, 
                        p.JobTitle, 
                        p.Salutation, 
                        LTRIM(RTRIM(ISNULL(p.FirstName, ''))) as FirstName,
                        LTRIM(RTRIM(ISNULL(p.FamiliarName, ''))) as FamiliarName, 
                        p.MiddleName, LTRIM(RTRIM(ISNULL(p.LastName, ''))) as LastName, 
                        LTRIM(RTRIM(ISNULL(p.FullName, ''))) as FullName,
                        p.Suffix,  
                        p.Membership, 
                        p.VIPID, 
                        p.VIPCode, 
                        p.FreqFlyerNum, 
                        p.PrimaryLanguage, 
                        p.GenderCode, 
                        p.DOBYear, 
                        p.DOBMonth, 
                        p.DOBDayOfMonth, 
                        p.CompanyName,
                        p.FK_CompanyProfile, 
                        p.AllowMail, 
                        p.AllowEMail, 
                        p.GuestPriv, 
                        p.AllowPhone, 
                        p.AllowSMS, 
                        p.AllowHistory, 
                        p.AllowMarketResearch, 
                        p.AllowThirdParty, 
                        p.PMSCreatorCode,
                        p.PMSLastUpdaterCode, 
                        p.DatePMSProfileCreated, 
                        p.DatePMSProfileUpdated, 
                        p.DateInserted, 
                        p.LastUpdated, 
                        p.Checksum, 
                        0 as IsDirty, 
                        p.ImportSource,
                        p.ImportSourceID,
                        p.ARNumber, 
                        p.SourceXML, 
                        p.ResortID, 
                        p.ExternalProfileID2,
                        p.Nationality
                        FROM Profiles p with(nolock) inner join dbo.ETL_TEMP_Profiles as e with(nolock) ON p.PK_Profiles = e.PK_Profiles WHERE p.RecordStatus = 'Active' AND p.ExternalProfileId <> ''";

        public static readonly string SQL_GetDataFromProfilesExt =
            @"SELECT e.PK_ProfilesExt, 
                        e.FK_Profiles, 
                        e.RecordStatus,
                        e.PriorityCode,
                        e.RoomsPotential,
                        e.SalesScope,
                        e.ScopeCity,
                        e.ActionCode,
                        e.BusinessSegment,
                        e.AccountType, 
                        e.SalesSource,
                        e.IndustryCode,
                        e.CompetitionCode,
                        e.InfluenceCode, 
                        e.DateInserted,
                        e.LastUpdated,
                        e.Checksum,
                        e.IsDirty,
                        e.Salutation2,
                        e.FirstName2,
                        e.LastName2,
                        e.FamiliarName2,
                        e.CompanyName2,
                        e.PrimaryLanguage2,
                        e.Blacklist,
                        e.BlacklistMessage,
                        e.AnonymizationStatus
                        FROM dbo.Profiles_Ext e With(Nolock) WHERE  (e.LastUpdated >='{0}' OR e.DateInserted >= '{0}') and e.LastUpdated <= '{1}' and e.DateInserted <= '{1}'";

		public static readonly string SQL_MoveBirthday =
            @"SELECT p.PK_Profiles, 
	               Convert(nvarchar(20), LTRIM(RTRIM(p.DOBMonth)) + '/' + LTRIM(RTRIM(p.DOBDayOfMonth)) + '/' + LTRIM(RTRIM(p.DOBYear))) as DOB
                    FROM dbo.V_Profiles p WITH (NOLOCK) 
                    inner join dbo.ETL_TEMP_Profiles as e with(nolock) on e.PK_Profiles = p.PK_Profiles 
                    WHERE p.RecordStatus = 'Active' 
                    AND p.ExternalProfileID <> '' 
                    AND p.DOBDayOfMonth IS NOT NULL AND p.DOBMonth IS NOT NULL AND p.DOBYear IS NOT null";

        public static readonly string SQL_GetPreferredLanguage =
            @"SELECT p.PK_Profiles,
                    CONVERT(NVARCHAR(50), p.PrimaryLanguage) AS PrimaryLanguage
                    FROM dbo.V_Profiles AS p WITH (NOLOCK)
                    INNER JOIN dbo.ETL_TEMP_PROFILES AS ep WITH (NOLOCK)
                    ON p.PK_Profiles = ep.PK_Profiles;";

        public static readonly string SQL_GetEmailQuery =
              @"SELECT cm.FK_Profiles AS PK_Profiles, 
                CASE WHEN cm.CMData LIKE '%@%'  and  'PROD'  IN ('QA','TeamDev','BETA') 
                THEN Convert(varchar(70), SUBSTRING(LTRIM(RTRIM(cm.CMData)), 1, CHARINDEX('@', LTRIM(RTRIM(cm.CMData))))+'cendyn17.com') 
                ELSE Convert(varchar(70), LTRIM(RTRIM(cm.CMData))) END as Email, 
                0 as EmailStatus,
                CHECKSUM(cm.CMData) as EmailHash, 
                CHECKSUM(LTRIM(RTRIM(SUBSTRING(cm.CMData, CHARINDEX('@',cm.CMData)+(1),LEN(cm.CMData))))) as EmailDomainHash 
                FROM    dbo.V_ContactMethod cm with (nolock) 
                inner join dbo.ETL_TEMP_Profiles as p with(nolock) ON cm.FK_Profiles = p.PK_Profiles 
                WHERE  isnull(cm.recordstatus,'Active') = 'Active' and isnull(cm.Isprimary,1) = 1 and cm.CMType = 'IP'";

        public static readonly string SQL_GetPhoneQuery =
            @"SELECT cm1.FK_Profiles AS PK_Profiles,
                        CAST( left(cm1.PhonePrimary,20)  AS NVARCHAR(50)) as PhoneNumber,
                        CAST( left(cm1.PhoneOther,20)  AS NVARCHAR(50)) as CellPhoneNumber,
                        CAST( left(cm1.PhoneHome,20)  AS NVARCHAR(50)) as HomePhoneNumber,
                        CAST( left(cm1.PhoneFax, 20)  AS NVARCHAR(50)) as FaxNumber,
                        CAST(left(cm1.PhoneBusiness, 20) AS NVARCHAR(50)) AS BusinessPhoneNumber FROM  (SELECT  FK_Profiles,
                                [PhonePrimary],
		                        [PhoneOther],
                                [PhoneFax],
                                [PhoneBusiness],
                                [PhoneHome]  FROM ( SELECT    cm.FK_Profiles,
                                            ISNULL(cm.CMType, '') + ISNULL(cm.CMCategory, '') AS CMTypeCMCategory,
                                            cm.CMData FROM    dbo.V_ContactMethod cm with (nolock) 
					                        inner join dbo.ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles
                        WHERE  isnull(cm.recordstatus,'Active') = 'Active' and cm.CMType = 'Phone' and cm.CMCategory IN ('Primary', 'Home','Other','Business','Fax') ) AS Unpivoted 
                                PIVOT 
                                ( MAX(CMData) FOR CMTypeCMCategory IN ( [PhonePrimary], [PhoneOther], [PhoneFax], [PhoneBusiness], [PhoneHome] ) ) 
                                AS ";

        public static readonly string SQL_GetPhoneExtQuery =
                @"SELECT cm.FK_Profiles AS PK_Profiles, CAST(left(cm.CMExtraData,10) AS NVARCHAR(10)) as PhoneExtention
                    FROM    dbo.V_ContactMethod cm with (nolock) 
                    inner join ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles 
                    WHERE  isnull(cm.recordstatus,'Active') = 'Active' and cm.CMType = 'Phone' and cm.CMCategory IN ('Primary') ";

        public static readonly string SQL_GetEmailAndPhoneQuery =
                @"SELECT cm1.FK_Profiles,
                        CAST( left(cm1.PhonePhone,20)  AS NVARCHAR(50)) as PhoneNumber,
                        CAST( left(cm1.PhoneMobile,20)  AS NVARCHAR(50)) as CellPhoneNumber,
                        CAST( left(cm1.PhoneHome,20)  AS NVARCHAR(50)) as HomePhoneNumber,
                        CAST( left(cm1.PhoneFax, 20)  AS NVARCHAR(50)) as FaxNumber,
                        CAST(left(cm1.PhoneBusiness, 20) AS NVARCHAR(50)) AS BusinessPhoneNumber,
                        CASE WHEN cm1.Email LIKE '%@%'  and  'PROD'  IN ('QA','TeamDev','BETA') THEN Convert(varchar(70), SUBSTRING(LTRIM(RTRIM(cm1.Email)), 1, CHARINDEX('@', LTRIM(RTRIM(cm1.Email))))+'cendyn17.com') ELSE Convert(varchar(70), LTRIM(RTRIM(cm1.Email))) END as Email, 0 as EmailStatus , CHECKSUM(cm1.Email) as EmailHash, 
                        CHECKSUM(LTRIM(RTRIM(SUBSTRING(cm1.Email,CHARINDEX('@',cm1.Email)+(1),LEN(cm1.Email))))) as EmailDomainHash, 0 as DedupeCheck FROM  (SELECT  FK_Profiles,
                                isnull([PhoneHome],isnull([PhonePhone],isnull([PhoneMobile],isnull([PhoneCELL PHONE],[PhoneCELL])))) as PhonePhone,
		                        isnull([PhoneMobile],isnull([PhoneCELL PHONE],[PhoneCELL])) as PhoneMobile,
                                [PhoneFax],
                                Replace([IPEmail], ' ', '') as Email,
                                isnull([PhoneBusiness],[PhoneWork]) as PhoneBusiness,
                                isnull([PhoneHome], [PhonePhone]) as PhoneHome   FROM ( SELECT    cm.FK_Profiles,
                                            ISNULL(cm.CMType, '') + 'Email' AS CMTypeCMCategory,
                                            cm.CMData FROM    dbo.V_ContactMethod cm with (nolock) 
					                        inner join ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles
                        WHERE  isnull(cm.recordstatus,'Active') = 'Active' and isnull(cm.Isprimary,1) = 1 and cm.CMType = 'IP' 
                        and '6543' = '0' UNION SELECT    cm.FK_Profiles,
                                            ISNULL(cm.CMType, '') + ISNULL(cm.CMCategory, '') AS CMTypeCMCategory,
                                            cm.CMData FROM    dbo.V_ContactMethod cm with (nolock) 
					                        inner join ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles
                        WHERE  isnull(cm.recordstatus,'Active') = 'Active' and isnull(cm.Isprimary,1) = 1 and cm.CMType = 'IP' and cm.CMCategory = 'Email' and '6543' <> '0' UNION SELECT    cm.FK_Profiles,
                                            ISNULL(cm.CMType, '') + ISNULL(cm.CMCategory, '') AS CMTypeCMCategory,
                                            cm.CMData FROM    dbo.V_ContactMethod cm with (nolock) 
					                        inner join ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles
                        WHERE  DB_NAME() NOT LIKE '%MinorHotel%' AND isnull(cm.recordstatus,'Active') = 'Active' and cm.CMType = 'Phone' and cm.CMCategory IN ('Phone','Home','CELL','CELL PHONE','Mobile','Business','Work','Fax') UNION SELECT    cm.FK_Profiles,
                                            ISNULL(cm.CMType, '') + ISNULL(cm.CMCategory, '') AS CMTypeCMCategory,
                                            cm.CMData FROM    dbo.V_ContactMethod_Primary cm with (nolock) 
					                        inner join ETL_TEMP_Profiles as p with(nolock) on cm.FK_Profiles = p.PK_Profiles
                        WHERE  DB_NAME() LIKE '%MinorHotel%' AND isnull(cm.recordstatus,'Active') = 'Active' and cm.CMType = 'Phone' and cm.CMCategory IN ('Phone','Home','CELL','CELL PHONE','Mobile','Business','Work','Fax')) AS Unpivoted 
                                PIVOT 
                        ( MAX(CMData) FOR CMTypeCMCategory IN ( [PhonePhone], [PhoneMobile], [PhoneFax], [IPEmail], [PhoneBusiness], [PhoneHome], [PhoneCELL],[PhoneCELL PHONE],[PhoneWork] )) 
                        AS Pivoted) cm1";

        public static readonly string SQL_GetD_Customer_EmailQuery =
                @"SELECT  A.PK_ContactMethod, A.FK_Profiles, A.EmailType,  Convert(varchar(70), LEFT(A.Email,70)) as 'Email', A.EmailStatus FROM
                    (SELECT cm.PK_ContactMethod, cm.FK_Profiles,
                            UPPER(cm.CMCategory) AS EmailType,
                             CASE WHEN 'PROD' IN ('QA','TeamDev','BETA') 
                     THEN SUBSTRING(cm.CMData, 1, CHARINDEX('@', cm.CMData))+'cendyn17.com' ELSE
                     SUBSTRING(LTRIM(RTRIM(cm.CMData)),1,70) END AS Email, 0 AS EmailStatus,  CASE WHEN cm.CMCategory = 'EMAIL' THEN isnull(cm.IsPrimary,1) ELSE 1 END AS 'IsPrimaryCustom' FROM    dbo.V_ContactMethod cm WITH (NOLOCK)
                    WHERE CMData LIKE '%@%' AND ISNULL(FK_Reservations,  '00000000-0000-0000-0000-000000000000') 
					                    = '00000000-0000-0000-0000-000000000000'  AND ISNULL(RecordStatus,'Active') = 'Active' and  (cm.LastUpdated >= '{0}' OR cm.DateInserted >= '{0}') and cm.LastUpdated <= '{1}' and cm.DateInserted <= '{1}') A WHERE A.isprimaryCustom = 1";

        public static readonly string SQL_EmailListTypeOfData =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'EmailList'";

        public static readonly string SQL_UDF31TypeOfData =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC31'";

        public static readonly string SQL_KanaLastNameTypeOfData =
                @"SELECT u.FK_Internal ,
                           u.ColumnName ,
                           u.UDFValue
                    FROM   dbo.ETL_TEMP_PROFILES AS e WITH ( NOLOCK )
                           INNER JOIN dbo.UDFData AS u WITH ( NOLOCK ) ON e.PK_Profiles = u.FK_Internal
                    WHERE  u.RecordStatus = 'Active'
                           AND u.ColumnName = '01KANALASTNAME';";

        public static readonly string SQL_KanaFirstNameTypeOfData =
                @"SELECT u.FK_Internal ,
                           u.ColumnName ,
                           u.UDFValue
                    FROM   dbo.ETL_TEMP_PROFILES AS e WITH ( NOLOCK )
                           INNER JOIN dbo.UDFData AS u WITH ( NOLOCK ) ON e.PK_Profiles = u.FK_Internal
                    WHERE  u.RecordStatus = 'Active'
                           AND u.ColumnName = '03KANAFIRSTNAME';";

        public static readonly string SQL_NKanaLastNameTypeOfData =
                @"SELECT u.FK_Internal ,
                           u.ColumnName ,
                           u.UDFValue
                    FROM   dbo.ETL_TEMP_PROFILES AS e WITH ( NOLOCK )
                           INNER JOIN dbo.UDFData AS u WITH ( NOLOCK ) ON e.PK_Profiles = u.FK_Internal
                    WHERE  u.RecordStatus = 'Active'
                           AND u.ColumnName = '02NKANALASTNAME';";

        public static readonly string SQL_NKanaFirstNameTypeOfData =
                @"SELECT u.FK_Internal ,
                           u.ColumnName ,
                           u.UDFValue
                    FROM   dbo.ETL_TEMP_PROFILES AS e WITH ( NOLOCK )
                           INNER JOIN dbo.UDFData AS u WITH ( NOLOCK ) ON e.PK_Profiles = u.FK_Internal
                    WHERE  u.RecordStatus = 'Active'
                           AND u.ColumnName = '04NKANAFIRSTNAME';";

        public static readonly string SQL_NKanaNameTypeOfData =
                @"SELECT u.FK_Internal ,
                           u.ColumnName ,
                           u.UDFValue
                    FROM   dbo.ETL_TEMP_PROFILES AS e WITH ( NOLOCK )
                           INNER JOIN dbo.UDFData AS u WITH ( NOLOCK ) ON e.PK_Profiles = u.FK_Internal
                    WHERE  u.RecordStatus = 'Active'
                           AND u.ColumnName = '05NKANANAME';";

        public static readonly string SQL_UDFC37TypeOfData =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC37'";

        public static readonly string SQL_UDFDataForAlways_Email_Folio =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                    INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                    ON u.FK_Internal = e.PK_Profiles
                    where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'ALWAYS_EMAIL_FOLIO'";

        public static readonly string SQL_UDFDataForGHA_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'GHA_EMAIL'";

        public static readonly string SQL_UDFDataForHotel_Offer_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'HOTEL_OFFER_EMAIL'";

        public static readonly string SQL_UDFDataForMarketing_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'MARKETING_EMAIL'";

        public static readonly string SQL_UDFDataForMarketing_Print =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'MARKETING_PRINT'";

        public static readonly string SQL_UDFDataForMokara_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'MOKARA_EMAIL'";

        public static readonly string SQL_UDFDataForNew_Card_Request =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'NEW_CARD_REQUEST'";

        public static readonly string SQL_UDFDataForNewsLetter_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'NEWSLETTER_EMAIL'";

        public static readonly string SQL_UDFDataForSG_Account_Summary =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'SG_ACCOUNT_SUMMARY'";

        public static readonly string SQL_UDFDataForTransaction_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'TRANSACTION_EMAIL'";

        public static readonly string SQL_UDFDataForSG_Email =
                @"SELECT u.FK_Internal, u.ColumnName, u.UDFValue FROM dbo.V_UDFData AS u WITH(NOLOCK) 
                            INNER JOIN dbo.ETL_TEMP_PROFILES AS e With(Nolock)
                            ON u.FK_Internal = e.PK_Profiles
                            where u.RecordStatus = 'Active' and u.TableName = 'Profiles' and u.ColumnName = 'SG_Email'";

        public static readonly string SQL_GetUDFData =
                @"SELECT u.PK_UDFData,
                           u.RecordStatus,
                           u.CendynPropertyID,
                           u.FK_Internal,
                           u.ModuleName,
                           CASE
                               WHEN u.TableName = 'NAME' THEN
                                   'Profiles'
                               ELSE
                                   u.TableName
                           END AS TableName,
                           um.Description AS ColumnName,
                           u.UDFValue,
                           u.DateInserted,
                           u.LastUpdated,
                           u.Checksum,
                           u.IsDirty
                    FROM dbo.UDFData AS u WITH (NOLOCK)
                        INNER JOIN dbo.UDFField_Mapping AS um WITH (NOLOCK)
                            ON u.ColumnName = um.UDFFieldName
                        INNER JOIN dbo.ETL_TEMP_PROFILES AS p WITH (NOLOCK)
                            ON u.FK_Internal = p.PK_Profiles
                    UNION
                    SELECT u.PK_UDFData,
                           u.RecordStatus,
                           u.CendynPropertyID,
                           u.FK_Internal,
                           u.ModuleName,
                           CASE
                               WHEN u.TableName = 'NAME' THEN
                                   'Profiles'
                               ELSE
                                   u.TableName
                           END AS TableName,
                           um.Description AS ColumnName,
                           u.UDFValue,
                           u.DateInserted,
                           u.LastUpdated,
                           u.Checksum,
                           u.IsDirty
                    FROM dbo.UDFData AS u WITH (NOLOCK)
                        INNER JOIN dbo.UDFField_Mapping AS um WITH (NOLOCK)
                            ON u.ColumnName = um.Description
                        INNER JOIN dbo.ETL_TEMP_PROFILES AS p WITH (NOLOCK)
                            ON p.PK_Profiles = u.FK_Internal
                    UNION
                    SELECT u.PK_UDFData,
                           u.RecordStatus,
                           u.CendynPropertyID,
                           u.FK_Internal,
                           u.ModuleName,
                           u.TableName,
                           u.ColumnName,
                           u.UDFValue,
                           u.DateInserted,
                           u.LastUpdated,
                           u.Checksum,
                           u.IsDirty
                    FROM dbo.UDFData AS u WITH (NOLOCK)
                        INNER JOIN dbo.ETL_TEMP_PROFILES AS e WITH (NOLOCK)
                            ON u.FK_Internal = e.PK_Profiles
                    WHERE NOT EXISTS
                    (
                        SELECT 1
                        FROM dbo.UDFField_Mapping WITH (NOLOCK)
                        WHERE u.ColumnName = UDFFieldName
                              OR u.ColumnName = Description
                    );";

        public static readonly string SQL_UDFDataForSalesRep =
                @"SELECT FK_Internal, 'Sales Rep' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'grep'";

        public static readonly string SQL_UDFDataForEstrate =
                @"SELECT FK_Internal, 'Est Rate' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'revest'";

        public static readonly string SQL_UDFDataForDepart =
                @"SELECT FK_Internal, 'Depart' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'gdepart'";

        public static readonly string SQL_UDFDataForArrival =
                @"SELECT FK_Internal, 'Arrival' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'garrive'";

        public static readonly string SQL_UDFDataForBlockRef =
                @"SELECT FK_Internal, 'Block Ref' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'gref'";

        public static readonly string SQL_UDFDataForMarketSegment =
                @"SELECT FK_Internal, 'Market Segment' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'gmrkt'";

        public static readonly string SQL_UDFDataForMarketSubSegment =
                @"SELECT FK_Internal, 'Market SubSegment' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'gsubmkt''";

        public static readonly string SQL_UDFDataForSource =
                @"SELECT FK_Internal, 'Source' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'source'";

        public static readonly string SQL_UDFDataForNotes =
                @"SELECT FK_Internal, 'Notes' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'remarks'";

        public static readonly string SQL_UDFDataForBanned =
                @"SELECT FK_Internal, 'Banned' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'blaklisted'";

        public static readonly string SQL_UDFDataForAddress3 =
                @"SELECT FK_Internal, 'Address 3' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'address3'";

        public static readonly string SQL_UDFDataForAlternate =
                @"SELECT FK_Internal, 'Alternate' as ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and TableName = 'Profiles' and ColumnName = 'altname'";

        public static readonly string SQL_GetDataFromAddressToUpdateCompanyName =
                @"SELECT x.FK_Profiles, convert(nvarchar(100), x.CompanyName) as CompanyName FROM (SELECT A.FK_Profiles, A.Address1 as CompanyName, 
                    ROW_NUMBER() OVER (PARTITION BY A.FK_Profiles ORDER BY A.DateInserted, A.LastUpdated DESC) rn
                    FROM dbo.Address A WITH (NOLOCK)
                    WHERE isnull(A.recordstatus, 'Active') = 'Active'
                    and ISNULL(A.Address1, '') <> '' and ISNULL(A.Address2, '') <> '' 
                    and (A.LastUpdated >= '{0}' OR A.DateInserted >= '{0}') 
                    and A.LastUpdated <= '{1}' 
                    and A.DateInserted <= '{1}') x
                    WHERE x.rn = 1";

        public static readonly string SQL_UDFDataForCNotes =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'cnotes' )
                            AND ModuleName = 'OpenTable'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCount_Cancel =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'count_cancel' )
                            AND ModuleName = 'OpenTable'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCount_NoShow =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'count_noshow' )
                            AND ModuleName = 'OpenTable'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCount_Resos =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'count_resos' )
                            AND ModuleName = 'OpenTable'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCustcodes =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'custcodes' )
                            AND ModuleName = 'OpenTable'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForBAPExpiration =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'BAPExpiration' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForChildMembership =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'ChildMembership' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForComment =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'Comment' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCreatedOnline =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'CreatedOnline' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForCustomerCode =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'CustomerCode' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForHasOnlineProfile =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'HasOnlineProfile' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForLastOnlineOrderDate =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'LastOnlineOrderDate' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForLastOnlineOrderNumber =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'LastOnlineOrderNumber' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForOmniPassBarcode =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'OmniPassBarcode' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForOmniPassID =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'OmniPassID' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForOnlineCustomerNumber =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'OnlineCustomerNumber' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForProductDelivery =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'ProductDelivery' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForPROF_COD_5 =
                @"SELECT  FK_Internal ,
                            ColumnName ,
                            UDFValue
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   ColumnName IN ( 'PROF_COD_5' )
                            AND ModuleName = 'Counterpoint'
                            AND TableName = 'Profiles'
                            AND RecordStatus = 'Active';";

        public static readonly string SQL_UDFDataForUDFC20 =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC20'";

        public static readonly string SQL_UDFDataForUDFC22 =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC22'";

        public static readonly string SQL_UDFDataForUDFC23 =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC23'";

        public static readonly string SQL_UDFDataForUDFC33 =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'UDFC33'";

        public static readonly string SQL_GetDataFromProfilesToUpdateTwoStatus =
                @"SELECT  p.PK_Profiles, p.RecordStatus, 0 as D_Customer_RecordStatus FROM  dbo.Profiles p with (nolock)  
                    WHERE p.RecordStatus = 'Inactive' 
                    AND (p.LastUpdated >= '{0}' OR p.DateInserted >= '{0}') 
                    and p.LastUpdated <= '{1}' and p.DateInserted <= '{1}'";

        public static readonly string SQL_GetDataFromV_Address =
                @"SELECT FK_Profiles ,
                           AddressTypeCode ,
                           Address1 ,
                           Address2 ,
                           City ,
                           StateProvinceCode ,
                           ZipCode ,
                           CountryCode ,
                           DivisionCode ,
                           RegionCode ,
                           ZipCodePlus4 ,
                           DedupeCheck ,
                           AddressStatus
                    FROM   (   SELECT a.FK_Profiles ,
                                      a.AddressTypeCode ,
                                      CASE WHEN ISNULL(a.Address1, '') = ''
                                                AND ISNULL(a.Address2, '') <> '' THEN a.Address2
                                           ELSE a.Address1
                                      END AS Address1 ,
                                      CASE WHEN ISNULL(a.Address1, '') = '' THEN ''
                                           ELSE a.Address2
                                      END AS Address2 ,
                                      CAST(a.PostalCode AS NVARCHAR(30)) AS ZipCode ,
                                      a.City AS City ,
                                      a.StateProvince AS StateProvinceCode ,
                                      CAST(a.CountryCode AS NVARCHAR(20)) AS CountryCode ,
                                      0 AS DivisionCode ,
                                      0 AS RegionCode ,
                                      0 AS DedupeCheck ,
                                      0 AS AddressStatus ,
                                      NULL AS ZipCodePlus4 ,
                                      RANK() OVER ( PARTITION BY a.FK_Profiles
                                                  ORDER BY  CASE WHEN a.IsPrimary = 1 THEN 'a' ELSE  AddressTypeCode END ASC ) AS AddressRank
                               FROM   dbo.V_Address a WITH ( NOLOCK )
                                      INNER JOIN dbo.ETL_TEMP_PROFILES AS p WITH ( NOLOCK ) ON a.FK_Profiles = p.PK_Profiles
                               WHERE  a.RecordStatus = 'Active'
                                      AND a.AddressTypeCode IN ( 'H', 'U', 'W', 'L' )) b
                    WHERE  b.AddressRank = 1;";

        public static readonly string SQL_GetAnniversaryData =
                @"SELECT  FK_Internal ,
                            CONVERT(NVARCHAR(50), SUBSTRING(ISNULL(UDFValue, ''), 1, 50)) AS Anniversary
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   RecordStatus = 'Active'
                            AND TableName = 'Profiles'
                            AND ColumnName = 'AnniversaryDate'";

        public static readonly string SQL_GetSpouseBirthData =
                @"SELECT  FK_Internal ,
                            CONVERT(NVARCHAR(50), SUBSTRING(ISNULL(UDFValue, ''), 1, 20)) AS SpouseBirthDate 
                    FROM    dbo.UDFData WITH ( NOLOCK )
                    WHERE   RecordStatus = 'Active'
                            AND TableName = 'Profiles'
                            AND ColumnName = 'SpouseBirthDate'";

        public static readonly string SQL_GetRateTypeForBiltmore =
            @"SELECT CONVERT(VARCHAR(100), 'RateType') AS FieldName,
               CONVERT(NVARCHAR(100), rh.RatePlanName) AS FieldValue,
               CONVERT(NVARCHAR(4000), rh.ShortDescription) AS Description,
               CONVERT(NVARCHAR(4000), rh.LongDescription) AS Notes,
               rh.CendynPropertyID,
               rh.BuildingCode
            FROM dbo.RateHeader AS rh WITH (NOLOCK)
            WHERE rh.RecordStatus = 'Active'
              AND rh.RatePlanName NOT LIKE 'A%'
              AND rh.RatePlanName NOT LIKE 'B%'
              AND rh.RatePlanName NOT LIKE 'V%'
              AND rh.RatePlanGroup IN ( 'CRES' );";

        public static readonly string SQL_GetRateTypeDataForSHGroup =
            @"SELECT DISTINCT
       CONVERT(VARCHAR(100), 'RateType') AS FieldName,
       CONVERT(NVARCHAR(100), rh.RateCode) AS FieldValue,
       CONVERT(NVARCHAR(4000), ISNULL(rh.LongDescription, rh.RateCode)) AS Description,
       CONVERT(NVARCHAR(4000), ISNULL(rh.LongDescription, rh.RateCode)) AS Notes,
       rh.HotelCode AS CendynPropertyID
        FROM dbo.RateTypes AS rh WITH (NOLOCK);";

        public static readonly string SQL_GetRoomTypeDataForSHGroup =
            @"SELECT DISTINCT
       CONVERT(VARCHAR(100), 'RoomType') AS FieldName,
       CONVERT(NVARCHAR(100), rh.RoomCode) AS FieldValue,
       CONVERT(NVARCHAR(4000), ISNULL(rh.ShortDescription, rh.RoomCode)) AS Description,
       CONVERT(NVARCHAR(4000), ISNULL(rh.LongDescription, rh.ShortDescription)) AS Notes,
       convert(varchar(50), rh.HotelCode) AS CendynPropertyID
        FROM dbo.RoomTypes AS rh WITH (NOLOCK);";

        public static readonly string SQL_GetRateTypeDataFor12951 =
            @"SELECT DISTINCT
       CONVERT(VARCHAR(100), 'RateType') AS FieldName,
       CONVERT(NVARCHAR(100), rh.RateCode) AS FieldValue,
       CONVERT(NVARCHAR(4000), ISNULL(rh.DisplayText, rh.RateCode)) AS [Description],
       CONVERT(NVARCHAR(4000), ISNULL(rh.LongDescription, rh.ShortDescription)) AS Notes,
       rh.HotelCode AS CendynPropertyID
        FROM dbo.RateTypes AS rh WITH (NOLOCK);";

        public static readonly string SQL_GetRateTypeDataForAquaAston =
            @"SELECT DISTINCT CONVERT(VARCHAR(100), 'RateType') AS FieldName ,
       CONVERT(NVARCHAR(100), rh.RatePlanName) AS FieldValue ,
       CONVERT(NVARCHAR(4000), rh.ShortDescription) AS [Description] ,
       CONVERT(
                  NVARCHAR(4000) ,
                  ISNULL(rh.LongDescription, rh.ShortDescription)
              ) AS Notes ,
       rh.CendynPropertyID
        FROM   dbo.RateHeader AS rh WITH ( NOLOCK )
        WHERE  rh.RecordStatus = 'Active';";

        #endregion

        public static void TruncateTable(params string[] tableNames)
        {
            SQLHelper.TruncateTable(GetPMSConnectionString(), "TruncateETLPMSTEMPTable: " + string.Join(", ", tableNames), tableNames);
        }

        public static void UpdateAddressTypeCodeUnderPMS_Address()
        {
            SQLHelper.InsertOrUpdateDbValue(GetPMSConnectionString(), "sqlUpdatePMS_Address", SQL_UpdatePMS_Address, null);
        }

    }
}
