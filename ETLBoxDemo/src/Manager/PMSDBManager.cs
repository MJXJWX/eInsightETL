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
        #endregion

        public static void TruncateTable(params string[] tableNames)
        {
            SQLHelper.TruncateTable(GetPMSConnectionString(), "TruncateETLPMSTEMPTable: " + string.Join(", ", tableNames), "ETL_TEMP_Profiles");
        }

        public static void UpdateAddressTypeCodeUnderPMS_Address()
        {
            SQLHelper.InsertOrUpdateDbValue(GetPMSConnectionString(), "sqlUpdatePMS_Address", SQL_UpdatePMS_Address, null);
        }

    }
}
