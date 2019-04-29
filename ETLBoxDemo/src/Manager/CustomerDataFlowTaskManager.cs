using ETLBoxDemo.src.Utility;
using System;
using System.Collections.Generic;
using System.Text;
using ETLBoxDemo.src.Modules.Customer;
using ETLBoxDemo.Common;
using System.Data.SqlClient;

namespace ETLBoxDemo.src.Manager
{
    class CustomerDataFlowTaskManager
    {
        public static void TestDataFlowTask()
        {
            string sC = "data source=QHB-CRMDB001.centralservices.local;initial catalog=eInsightCRM_OceanProperties_QA;uid=eInsightCRM_eContact_OceanProperties;pwd=Tv3CxdZwA%9;MultipleActiveResultSets=True";
            string dC = "data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True";

            string dT = "dbo.D_Customer";
            string sql = "SELECT TOP 20 CustomerID, FirstName, LastName, Email, PropertyCode, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer with(Nolock);";

            //new DataFlowTask<D_Customer>().runTask(sC, dC, dT, sql, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });

            var mapping = new Dictionary<string, string>();
            mapping.Add("CustomerID", "CustomerID");
            mapping.Add("FirstName", "FirstName");
            mapping.Add("LastName", "LastName");
            mapping.Add("Email", "Email");
            mapping.Add("PropertyCode", "PropertyCode");
            mapping.Add("InsertDate", "InsertDate");
            mapping.Add("SourceID", "SourceID");
            mapping.Add("AddressStatus", "AddressStatus");
            mapping.Add("DedupeCheck", "DedupeCheck");
            mapping.Add("AllowEMail", "AllowEMail");
            mapping.Add("Report_Flag", "Report_Flag");
            mapping.Add("UNIFOCUS_SCORE", "UNIFOCUS_SCORE");
            //new DataFlowTask<D_Customer, D_Customer>().runTask(sC, dC, dT, sql, mapping, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });

            string lSql = "select top 20 ID, PropertyCode, FieldName, FieldValue, Description from L_DATA_DICTIONARY with(nolock);";
            var keys = new Dictionary<string, string>();
            keys.Add("ID", "CustomerID");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("FieldValue", "ShortTitle");
            new DataFlowTask<D_Customer, D_Customer, L_Data_Dictionary>().runTask(sC, dC, dC, dT, sql, lSql, keys, lMapping, mapping, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });

        }

        public static void DFT_MoveProfileDocument()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ProfileDocuments";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromProfileDocument, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "FK_Profile" };
            var properties = new List<string>() { "Id", "FK_Profile", "DocType", "DocSource", "CodeOnDocument", "DocNotes", "DocId_PII", "NameOnDocument_PII", "DocumentBody_PII", "NationalityOnDocument", "EffectiveDate", "ExpirationDate", "PII_StoredAs", "PII_Algorithm", "PII_Key", "PII_KeyId", "Issuer ", "IssuerAddress1", "IssuerAddress2", "IssuerCity", "IssuerStateProv", "IssuerPostalCode", "IssuerCountry", "IsPrimary", "InactiveDate", "DateCreated", "LastUpdated" };

            new DataFlowTask<PMS_ProfileDocuments>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveProfilePolicies()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ProfilePolicies";
            var sql = PMSDBManager.SQL_GetDataFromProfilePolicies;
            var primaryKeys = new List<string>() { "PK_ProfilePolicies" };
            var properties = new List<string>() { "PK_ProfilePolicies", "FK_Profiles", "FK_PolicyTypes", "AttributeName", "IntegerValue", "StringValue", "StartDate", "ExpirationDate", "Comments" };

            new DataFlowTask<PMS_ProfilePolicies>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveContactMethodPolicies()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethodPolicies";
            var sql = PMSDBManager.SQL_GetDataFromContactMethodPolicies;
            var primaryKeys = new List<string>() { "PK_ContactMethodPolicies" };
            var properties = new List<string>() { "PK_ContactMethodPolicies", "FK_ContactMethod", "FK_PolicyTypes", "AttributeName", "IntegerValue", "StringValue", "StartDate", "ExpirationDate", "Comments" };

            new DataFlowTask<PMS_ContactMethodPolicies>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveContactMethod()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethod";
            var sql = PMSDBManager.SQL_GetDataFromContactMethod;
            var primaryKeys = new List<string>() { "PK_ContactMethod" };
            var properties = new List<string>() { "PK_ContactMethod", "FK_Reservations", "FK_Profiles", "CMStatusId", "CMType", "CMData", "CMCategory", "CMOptOut", "CMSourceDate", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "IsPrimary", "Confirmation", "CMExtraData", "InactiveDate", "RecordStatus" };

            new DataFlowTask<PMS_ContactMethod>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_UpdateSourceNameUnderContactMethod()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethod";
            var sql = PMSDBManager.SQL_GetSourceNameFromContactMethod;
            var primaryKeys = new List<string>() { "PK_ContactMethod" };
            var properties = new List<string>() { "PK_ContactMethod", "SourceName" };

            new DataFlowTask<PMS_ActionComments>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveActionComments()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ActionComments";
            var sql = PMSDBManager.SQL_GetDataFromActionComments;
            var primaryKeys = new List<string>() { "PK_ActionComments" };
            var properties = new List<string>() { "PK_ActionComments", "FK_Internal", "KeyTable", "CommentType", "ExternalID", "ActionType", "ActionText", "ActionTypeID", "ActionDate", "InactiveDate", "GuestViewable", "PMSCreatorCode", "DatePMSCommentCreated", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "CommentTitle", "CRMSourceActionType", "RecordStatus", "CommentClass", "ResortID" };

            new DataFlowTask<PMS_ActionComments>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveAddress()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ADDRESS";
            var sql = PMSDBManager.SQL_GetDataFromAddress;
            var primaryKeys = new List<string>() { "PK_Address" };
            var properties = new List<string>() { "PK_Address", "FK_Profiles", "AddressTypeCode", "SourceAddressType", "RecordStatus", "AddressStatus", "Attn", "Address1", "Address2", "City", "StateProvince", "PostalCode", "CountryCode", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "IsPrimary", "AddressCleansed", "AddressLanguage" };

            new DataFlowTask<PMS_Address>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveSpecialRequests_Omni()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_SpecialRequests";
            var sql = PMSDBManager.SQL_GetDataFromV_SpecialRequests;
            var primaryKeys = new List<string>() { "PK_SpecialRequests" };
            var properties = new List<string>() {"PK_SpecialRequests", "FK_Reservations", "FK_Profiles", "ExternalRPH", "RequestType", "RequestCode", "RequestComments",
                                                  "ResortField", "ActionTypeCode", "SourceActionType", "CRMSourceActionType", "InactiveDate", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "Quantity" };

            new DataFlowTask<PMS_SpecialRequests>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveUpdatedProfilesIntoTempTable()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.CENRES_SpecialRequests";
            var sql = PMSDBManager.SQL_GetDataFromSpecialRequests;
            var primaryKeys = new List<string>() { "PK_SpecialRequests" };
            var properties = new List<string>() { "PK_SpecialRequests", "FK_Profiles", "RequestType", "RequestCode", "RequestComments" };

            new DataFlowTask<CENRES_SpecialRequests>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveSpecialRequests()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_SpecialRequests";
            var sql = PMSDBManager.SQL_GetDataFromV_SpecialRequests;
            var primaryKeys = new List<string>() { "PK_SpecialRequests" };
            var properties = new List<string>() { "PK_SpecialRequests", "FK_Profiles", "RequestType", "RequestCode", "RequestComments" };

            new DataFlowTask<CENRES_SpecialRequests>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_InsertOrUpdateD_CustomerWithoutMembership()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = PMSDBManager.SQL_GetDataFromProfiles;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PropertyCode","PK_Profiles","SourceGuestID","FirstName","MiddleName","LastName","Salutation","ShortTitle","GenderCode",
                                                                                            "Company","CompanyTitle","JobTitle","Languages","SourceID","DedupeCheck","DatePMSProfileUpdated","AllowEMail","AllowMail",
                                                                                            "AllowSMS","AllowPhone","ExternalProfileID2","VIPID","VIPCode","Nationality" };

            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_InsertOrUpdateD_CustomerWithMembership()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = PMSDBManager.SQL_GetDataFromProfiles;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PropertyCode","PK_Profiles","SourceGuestID","FirstName","MiddleName","LastName","Salutation","ShortTitle","GenderCode",
                                                                                            "Company","CompanyTitle","JobTitle","Languages","SourceID","DedupeCheck","DatePMSProfileUpdated","AllowEMail","AllowMail",
                                                                                            "AllowSMS","AllowPhone","Membership","ExternalProfileID2","VIPID","VIPCode","Nationality" };

            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        //issue
        public static void DFT_UpdatePropertyCode()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = PMSDBManager.SQL_GetDataFromProfilesToUpdatePropertyCode;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PropertyCode","PK_Profiles"};

            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveProfilesToPMS_ProfilesAndPMS_PROFILE_MAPPING()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_Profiles";
            var sql = PMSDBManager.SQL_GetDataFromProfilesToUpsertPMS_Profiles;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles", "RecordStatus", "SourceTypeId", "ExternalProfileID", "PMSProfileID", "PMSNameCode", "CendynPropertyID", "ProfileTypeID", "ProfileTypeCode",
                                                    "SourceProfileType", "SourceProfileNameCode", "ProfileTypeCategory", "JobTitle", "Salutation", "FirstName", "FamiliarName", "MiddleName", "LastName", "FullName",
                                                    "Suffix", "Membership", "VIPID", "VIPCode", "FreqFlyerNum", "PrimaryLanguage", "GenderCode", "DOBYear", "DOBMonth", "DOBDayOfMonth", "CompanyName",
                                                    "FK_CompanyProfile", "AllowMail", "AllowEMail", "GuestPriv", "AllowPhone", "AllowSMS", "AllowHistory", "AllowMarketResearch", "AllowThirdParty", "PMSCreatorCode",
                                                    "PMSLastUpdaterCode", "DatePMSProfileCreated", "DatePMSProfileUpdated", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "ImportSource", "ImportSourceID", "ARNumber", "SourceXML", "ResortID", "ExternalProfileID2", "Nationality" };

            new DataFlowTask<PMS_Profiles>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveProfiles_ExtToPMS_Profiles_Ext()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_Profiles_Ext";
            var sql = PMSDBManager.SQL_GetDataFromProfilesExt;
            var primaryKeys = new List<string>() { "PK_ProfilesExt" };
            var properties = new List<string>() { "PK_ProfilesExt", "FK_Profiles", "RecordStatus", "PriorityCode", "RoomsPotential", "SalesScope", "ScopeCity", "ActionCode", "BusinessSegment",
                                                    "AccountType", "SalesSource", "IndustryCode", "CompetitionCode", "InfluenceCode", "DateInserted", "LastUpdated", "Checksum",
                                                    "IsDirty", "Salutation2", "FirstName2", "LastName2", "FamiliarName2", "CompanyName2", "PrimaryLanguage2", "Blacklist", "BlacklistMessage", "AnonymizationStatus" };
            new DataFlowTask<PMS_Profiles_Ext>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);

        }


    }
}
