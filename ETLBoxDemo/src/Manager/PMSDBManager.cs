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
            WHERE (LastUpdated >= '2012-03-12 20:50:00' OR DateInserted >= '2012-03-12 20:50:00') 
            AND LastUpdated <= '2012-01-24 11:06:00' and DateInserted <= '2012-01-24 11:06:00'";

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
            WHERE (LastUpdated >= '2012-03-12 20:50:00' OR DateInserted >= '2012-03-12 20:50:00') 
            AND LastUpdated <= '2012-01-24 11:06:00' and DateInserted <= '2012-01-24 11:06:00'";

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
            WHERE  (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00') and cm.LastUpdated <= '2012-01-24 11:06:00' and cm.DateInserted <= '2012-01-24 11:06:00'";

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
            WHERE (p.LastUpdated >= '2012-03-12 20:50:00' OR p.DateInserted >= '2012-03-12 20:50:00') and p.LastUpdated <= '2012-01-24 11:06:00' and p.DateInserted <= '2012-01-24 11:06:00'";

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
            WHERE RecordStatus = 'Active' AND (A.LastUpdated >= '2012-03-12 20:50:00' OR A.DateInserted >= '2012-03-12 20:50:00') and A.LastUpdated <= '2012-01-24 11:06:00' and A.DateInserted <= '2012-01-24 11:06:00'";
        
        public static readonly string SQL_GetSourceNameFromContactMethod = 
            @"SELECT cm.PK_ContactMethod, 
                cm.SourceName FROM dbo.ContactMethod cm with (nolock) 
            WHERE (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00') and cm.LastUpdated <= '2012-01-24 11:06:00' and cm.DateInserted <= '2012-01-24 11:06:00'";
        
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
                    sr.Quantity FROM dbo.V_SpecialRequests AS sr With(Nolock) WHERE  (sr.LastUpdated >= '@LastUpdated' OR sr.DateInserted >= '@DateInserted') and sr.LastUpdated <= '@LastUpdated' and sr.DateInserted <= '@DateInserted'";

        public static readonly string SQL_GetDataFromSpecialRequests = 
            @"SELECT [PK_SpecialRequests],
                        [FK_Profiles],
                        [RequestType],
                        [RequestCode],
                        CAST( REPLACE(sr.RequestComments, '?', '') AS VARCHAR(1000) )  AS RequestComments  FROM SpecialRequests sr WITH(NOLOCK)  WHERE FK_Profiles IS NOT NULL AND  (sr.LastUpdated >= '@LastUpdated' OR sr.DateInserted >= '@DateInserted') and sr.LastUpdated <= '@LastUpdated' and sr.DateInserted <= '@DateInserted'";

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
                        FROM dbo.Profiles_Ext e With(Nolock) WHERE  (e.LastUpdated >= '2012-03-12 20:50:00' OR e.DateInserted >= '2012-03-12 20:50:00') and e.LastUpdated <= '2012-01-24 11:06:00' and e.DateInserted <= '2012-01-24 11:06:00'";

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
					                    = '00000000-0000-0000-0000-000000000000'  AND ISNULL(RecordStatus,'Active') = 'Active' and  (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00') and cm.LastUpdated <= '2012-01-24 11:06:00' and cm.DateInserted <= '2012-01-24 11:06:00') A WHERE A.isprimaryCustom = 1";

        public static readonly string SQL_EmailListTypeOfData =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
                    where RecordStatus = 'Active' and ColumnName = 'EmailList'";

        public static readonly string SQL_UDF31TypeOfData =
                @"SELECT FK_Internal, ColumnName, UDFValue FROM dbo.UDFData WITH(NOLOCK) 
wher            e RecordStatus = 'Active' and ColumnName = 'UDFC31'";

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
