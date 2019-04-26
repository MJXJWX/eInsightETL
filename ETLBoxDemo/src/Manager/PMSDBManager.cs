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
