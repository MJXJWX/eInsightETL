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
            string sql = "SELECT TOP 10 CustomerID, FirstName, LastName, Email, PropertyCode, PK_Profiles, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer with(Nolock);";

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

            string lSql = "select top 30 ID, PropertyCode, FieldName, FieldValue, Description from L_DATA_DICTIONARY with(nolock);";
            var keys = new Dictionary<string, string>();
            keys.Add("ID", "CustomerID");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("FieldValue", "ShortTitle");
            new DataFlowTask<D_Customer, D_Customer, L_Data_Dictionary>().runTask(sC, dC, dC, dT, sql, lSql, keys, lMapping, null, true, false, new List<string>() { "FirstName", "LastName" }, new List<string>() { "AllowEMail", "PropertyCode", "ShortTitle" });

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
            var sql = string.Format(PMSDBManager.SQL_GetDataFromProfilePolicies, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ProfilePolicies" };
            var properties = new List<string>() { "PK_ProfilePolicies", "FK_Profiles", "FK_PolicyTypes", "AttributeName", "IntegerValue", "StringValue", "StartDate", "ExpirationDate", "Comments" };

            new DataFlowTask<PMS_ProfilePolicies>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveContactMethodPolicies()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethodPolicies";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromContactMethodPolicies, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ContactMethodPolicies" };
            var properties = new List<string>() { "PK_ContactMethodPolicies", "FK_ContactMethod", "FK_PolicyTypes", "AttributeName", "IntegerValue", "StringValue", "StartDate", "ExpirationDate", "Comments" };

            new DataFlowTask<PMS_ContactMethodPolicies>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveContactMethod()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethod";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromContactMethod, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ContactMethod" };
            var properties = new List<string>() { "PK_ContactMethod", "FK_Reservations", "FK_Profiles", "CMStatusId", "CMType", "CMData", "CMCategory", "CMOptOut", "CMSourceDate", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "IsPrimary", "Confirmation", "CMExtraData", "InactiveDate", "RecordStatus" };

            new DataFlowTask<PMS_ContactMethod>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_UpdateSourceNameUnderContactMethod()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ContactMethod";
            var sql = string.Format(PMSDBManager.SQL_GetSourceNameFromContactMethod, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ContactMethod" };
            var properties = new List<string>() { "PK_ContactMethod", "SourceName" };

            new DataFlowTask<PMS_ActionComments>().runTask(sourceCon, destinationCon, tableName, sql, true, false, primaryKeys, properties);
        }

        public static void DFT_MoveActionComments()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ActionComments";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromActionComments, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ActionComments" };
            var properties = new List<string>() { "PK_ActionComments", "FK_Internal", "KeyTable", "CommentType", "ExternalID", "ActionType", "ActionText", "ActionTypeID", "ActionDate", "InactiveDate", "GuestViewable", "PMSCreatorCode", "DatePMSCommentCreated", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "CommentTitle", "CRMSourceActionType", "RecordStatus", "CommentClass", "ResortID" };

            new DataFlowTask<PMS_ActionComments>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveAddress()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ADDRESS";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromAddress, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_Address" };
            var properties = new List<string>() { "PK_Address", "FK_Profiles", "AddressTypeCode", "SourceAddressType", "RecordStatus", "AddressStatus", "Attn", "Address1", "Address2", "City", "StateProvince", "PostalCode", "CountryCode", "DateInserted", "LastUpdated", "Checksum", "IsDirty", "IsPrimary", "AddressCleansed", "AddressLanguage" };

            new DataFlowTask<PMS_Address>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveSpecialRequests_Omni()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_SpecialRequests";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromV_SpecialRequests, CompanySettings.StartDate, CompanySettings.EndDate);
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
            var sql = string.Format(PMSDBManager.SQL_GetDataFromSpecialRequests, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_SpecialRequests" };
            var properties = new List<string>() { "PK_SpecialRequests", "FK_Profiles", "RequestType", "RequestCode", "RequestComments" };

            new DataFlowTask<CENRES_SpecialRequests>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveSpecialRequests()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_SpecialRequests";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromV_SpecialRequests, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_SpecialRequests" };
            var properties = new List<string>() { "PK_SpecialRequests", "FK_Profiles", "RequestType", "RequestCode", "RequestComments" };

            new DataFlowTask<CENRES_SpecialRequests>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_InsertOrUpdateD_CustomerWithoutMembership()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = CRMDBManager.SQL_GetDataFromETL_TEMP_PROFILES_D_CUSTOMER_Insert;
            var lsql = PMSDBManager.SQL_GetDataFromProfiles;

            var keys = new Dictionary<string, string>();
            keys.Add("PK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            lMapping.Add("SourceGuestID", "SourceGuestID");
            lMapping.Add("FirstName", "FirstName");
            lMapping.Add("MiddleName", "MiddleName");
            lMapping.Add("LastName", "LastName");
            lMapping.Add("Salutation", "Salutation");
            lMapping.Add("ShortTitle", "ShortTitle");
            lMapping.Add("GenderCode", "GenderCode");
            lMapping.Add("Company", "Company");
            lMapping.Add("CompanyTitle", "CompanyTitle");
            lMapping.Add("JobTitle", "JobTitle");
            lMapping.Add("Languages", "Languages");
            lMapping.Add("SourceID", "SourceID");
            lMapping.Add("DedupeCheck", "DedupeCheck");
            lMapping.Add("DatePMSProfileUpdated", "DatePMSProfileUpdated");
            lMapping.Add("AllowEMail", "AllowEMail");
            lMapping.Add("AllowMail", "AllowMail");
            lMapping.Add("AllowSMS", "AllowSMS");
            lMapping.Add("AllowPhone", "AllowPhone");
            lMapping.Add("ExternalProfileID2", "ExternalProfileID2");
            lMapping.Add("VIPID", "VIPID");
            lMapping.Add("VIPCode", "VIPCode");
            lMapping.Add("Nationality", "Nationality");
            new DataFlowTask<D_Customer, D_Customer, D_Customer>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "PK_Profiles" });
         }
        
        public static void DFT_InsertOrUpdateD_CustomerWithoutMembershipColumnUpdate()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = CRMDBManager.SQL_GetDataFromPMS_Profile_Mapping;
            var lsql = PMSDBManager.SQL_GetDataFromProfiles;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            lMapping.Add("SourceGuestID", "SourceGuestID");
            lMapping.Add("FirstName", "FirstName");
            lMapping.Add("MiddleName", "MiddleName");
            lMapping.Add("LastName", "LastName");
            lMapping.Add("Salutation", "Salutation");
            lMapping.Add("ShortTitle", "ShortTitle");
            lMapping.Add("GenderCode", "GenderCode");
            lMapping.Add("Company", "Company");
            lMapping.Add("CompanyTitle", "CompanyTitle");
            lMapping.Add("JobTitle", "JobTitle");
            lMapping.Add("Languages", "Languages");
            lMapping.Add("SourceID", "SourceID");
            lMapping.Add("DedupeCheck", "DedupeCheck");
            lMapping.Add("DatePMSProfileUpdated", "DatePMSProfileUpdated");
            lMapping.Add("AllowEMail", "AllowEMail");
            lMapping.Add("AllowMail", "AllowMail");
            lMapping.Add("AllowSMS", "AllowSMS");
            lMapping.Add("AllowPhone", "AllowPhone");
            lMapping.Add("ExternalProfileID2", "ExternalProfileID2");
            lMapping.Add("VIPID", "VIPID");
            lMapping.Add("VIPCode", "VIPCode");
            lMapping.Add("Nationality", "Nationality");
            new DataFlowTask<D_Customer, D_Customer, D_Customer>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }
        

        public static void DFT_InsertOrUpdateD_CustomerWithMembershipColumnUpdate()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = CRMDBManager.SQL_GetDataFromPMS_Profile_Mapping;
            var lsql = PMSDBManager.SQL_GetDataFromProfiles;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            lMapping.Add("SourceGuestID", "SourceGuestID");
            lMapping.Add("FirstName", "FirstName");
            lMapping.Add("MiddleName", "MiddleName");
            lMapping.Add("LastName", "LastName");
            lMapping.Add("Salutation", "Salutation");
            lMapping.Add("ShortTitle", "ShortTitle");
            lMapping.Add("GenderCode", "GenderCode");
            lMapping.Add("Company", "Company");
            lMapping.Add("CompanyTitle", "CompanyTitle");
            lMapping.Add("JobTitle", "JobTitle");
            lMapping.Add("Languages", "Languages");
            lMapping.Add("SourceID", "SourceID");
            lMapping.Add("DedupeCheck", "DedupeCheck");
            lMapping.Add("DatePMSProfileUpdated", "DatePMSProfileUpdated");
            lMapping.Add("AllowEMail", "AllowEMail");
            lMapping.Add("AllowMail", "AllowMail");
            lMapping.Add("AllowSMS", "AllowSMS");
            lMapping.Add("AllowPhone", "AllowPhone");
            lMapping.Add("ExternalProfileID2", "ExternalProfileID2");
            lMapping.Add("VIPID", "VIPID"); 
            lMapping.Add("VIPCode", "VIPCode");
            lMapping.Add("Nationality", "Nationality");
            lMapping.Add("Membership", "Membership");
            new DataFlowTask<D_Customer, D_Customer, D_Customer>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }
        
        public static void DFT_UpdatePropertyCode()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_CUSTOMER";
            var sql = PMSDBManager.SQL_GetDataFromProfilesToUpdatePropertyCode;
            var lsql = CRMDBManager.SQL_GetDataFromD_Property;

            var keys = new Dictionary<string, string>();
            keys.Add("CendynPropertyId", "CendynPropertyID");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            new DataFlowTask<D_Customer, D_Customer, D_Property>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "PK_Profiles" });

        }

        //issue
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
            var sql = string.Format(PMSDBManager.SQL_GetDataFromProfilesExt, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_ProfilesExt" };
            var properties = new List<string>() { "PK_ProfilesExt", "FK_Profiles", "RecordStatus", "PriorityCode", "RoomsPotential", "SalesScope", "ScopeCity", "ActionCode", "BusinessSegment",
                                                    "AccountType", "SalesSource", "IndustryCode", "CompetitionCode", "InfluenceCode", "DateInserted", "LastUpdated", "Checksum",
                                                    "IsDirty", "Salutation2", "FirstName2", "LastName2", "FamiliarName2", "CompanyName2", "PrimaryLanguage2", "Blacklist", "BlacklistMessage", "AnonymizationStatus" };
            new DataFlowTask<PMS_Profiles_Ext>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);

        }

		public static void DFT_MoveBirthday()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_Profile";
            var sql = CRMDBManager.SQL_GetDataFromD_customerAndETL_TEMP_Profiles_D_Customer;
            var lsql = PMSDBManager.SQL_MoveBirthday;

            var keys = new Dictionary<string, string>();
            keys.Add("PK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("DOB", "DOB");
            new DataFlowTask<D_Customer_Profile, D_Customer_Profile, D_Customer>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }


        public static void DFT_MoveAnniversaryIntoD_Customer_Profile()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_Profile";
            var sql = CRMDBManager.SQL_GetDataFromD_customerAndETL_TEMP_Profiles_D_Customer;
            var lsql = PMSDBManager.SQL_GetAnniversaryData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("Anniversary", "Anniversary");
            new DataFlowTask<D_Customer_Profile, D_Customer_Profile, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID" });
        }

        public static void DFT_MoveSpouseBirthDateIntoD_Customer_Profile()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_Profile";
            var sql = CRMDBManager.SQL_GetDataFromD_customerAndETL_TEMP_Profiles_D_Customer;
            var lsql = PMSDBManager.SQL_GetSpouseBirthData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("SpouseBirthDate", "SpouseBirthDate");
            new DataFlowTask<D_Customer_Profile, D_Customer_Profile, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID" });
        }

        public static void DFT_UpdatePreferredLanguageUnderPMS_Profiles()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_Profiles";
            var sql = PMSDBManager.SQL_GetPreferredLanguage;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles", "PrimaryLanguage" };

            new DataFlowTask<PMS_Profiles>().runTask(sourceCon, destinationCon, tableName, sql, true, false, primaryKeys, properties);
        }

        public static void DFT_RemoveInactiveEmailRecords()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = CRMDBManager.SQL_GetInactiveEmailQuery;
            var primaryKeys = new List<string>() { "FK_Profiles" };
            var properties = new List<string>() { "FK_Profiles", "EmailStatus", "Email" };

            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_UpdateD_CustomerForAddress()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer_Profile";
            var sql = PMSDBManager.SQL_GetDataFromV_Address;
            var lsql = CRMDBManager.SQL_GetDataFromPMS_PROFILE_MAPPING;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "FK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            //lMapping.Add("Address1", "Address1");
            //lMapping.Add("AddressTypeCode", "AddressTypeCode");
            //lMapping.Add("Address2", "Address2");
            //lMapping.Add("City", "City");
            //lMapping.Add("StateProvinceCode", "StateProvinceCode");
            //lMapping.Add("ZipCode", "ZipCode");
            //lMapping.Add("CountryCode", "CountryCode");
            //lMapping.Add("DivisionCode", "DivisionCode");
            //lMapping.Add("RegionCode", "RegionCode");
            //lMapping.Add("ZipCodePlus4", "ZipCodePlus4");
            //lMapping.Add("DedupeCheck", "DedupeCheck");
            //lMapping.Add("AddressStatus", "AddressStatus");
            new DataFlowTask<D_Customer, D_Customer, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });

        }

        public static void DFT_UpdateEmailUnderD_Customer()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = PMSDBManager.SQL_GetEmailQuery;
            var lsql = CRMDBManager.SQL_GetGlobalCustomerIDUnderPMS_Profile_Mapping;
            
             var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            new DataFlowTask<D_Customer, D_Customer, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });

        }

        public  static void DFT_UpdatePhoneNumbersUnderD_Customer()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = PMSDBManager.SQL_GetPhoneQuery;
            var lsql = CRMDBManager.SQL_GetGlobalCustomerIDUnderPMS_Profile_Mapping;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            new DataFlowTask<D_Customer, D_Customer, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }

        public static void DFT_UpdatePhoneExtUnderD_Customer()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = PMSDBManager.SQL_GetPhoneExtQuery;
            var lsql = CRMDBManager.SQL_GetGlobalCustomerIDUnderPMS_Profile_Mapping;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            new DataFlowTask<D_Customer, D_Customer, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }

        public static void DFT_UpdateEmailAndPhoneUnderD_Customer()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = PMSDBManager.SQL_GetEmailAndPhoneQuery;
            var lsql = CRMDBManager.SQL_GetGlobalCustomerIDUnderPMS_Profile_Mapping;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            new DataFlowTask<D_Customer, D_Customer, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lsql, keys, lMapping, null, true, false, new List<string>() { "CustomerID" });
        }

        public static void DFT_UpdateD_Customer_Email()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = string.Format(PMSDBManager.SQL_GetD_Customer_EmailQuery, CompanySettings.StartDate, CompanySettings.EndDate);
            var lsql = CRMDBManager.SQL_GetDataUnderPMS_Profile_Mapping;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Profiles", "FK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("GlobalCustomerID", "CustomerID");
            new DataFlowTask<D_Customer_Email, D_Customer_Email, PMS_Profile_Mapping>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "PK_ContactMethod" });
        }

        public static void DFT_UpSertDataIntoD_Customer_Email()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer_Email";
            var sql = CRMDBManager.SQL_GetDataFromETL_D_Customer_Email_Staging;
            var primaryKeys = new List<string>() { "CustomerID", "EmailType" };
            var properties = new List<string>() { "CustomerID", "EmailType", "EmailStatus", "Email", "EmailDomainHash", "email_id" };

            new DataFlowTask<D_Customer_Email>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveEmailListTypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_EmailListTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID" });
        }

        public static void DFT_MoveUDF31TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDF31TypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveKanaLastName()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_KanaLastNameTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveKanaFirstName()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_KanaFirstNameTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveNKanaLastName()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_NKanaLastNameTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveNKanaFirstName()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_NKanaFirstNameTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveNKanaName()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_NKanaNameTypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveUDFC37TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFC37TypeOfData;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveAlways_Email_DolioData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForAlways_Email_Folio;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveGHA_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForGHA_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveHotel_Offer_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForHotel_Offer_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveMarketing_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForMarketing_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveMarketing_PrintData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForMarketing_Print;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveMokara_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForMokara_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveNew_Card_RequestData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForNew_Card_Request;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveNewsLetter_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForNewsLetter_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveSG_Account_SummaryData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForSG_Account_Summary;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveTransaction_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForTransaction_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveSG_EmailData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForSG_Email;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveUDFData()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.UDFData ";
            var sql = PMSDBManager.SQL_GetUDFData;
            var primaryKeys = new List<string>() { "PK_UDFData" };
            var properties = new List<string>() { "PK_UDFData", "RecordStatus", "CendynPropertyID", "FK_Internal", "ModuleName", "TableName", "ColumnName", "UDFValue", "DateInserted", "LastUpdated", "Checksum", "IsDirty"};

            new DataFlowTask<PMS_UDFData>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_MoveSalesRepUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForSalesRep;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveEstrateUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForEstrate;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveDepartUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForDepart;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveArrivalUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForArrival;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveBlockRefUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForBlockRef;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveMarketSegmentUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForMarketSegment;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveMarketSubSegmentUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForMarketSubSegment;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveSourceUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForSource;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveNotesUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForNotes;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveBannedUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForBanned;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveAddress3UDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForAddress3;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveAlternateUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForAlternate;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        
        public static void DFT_UpdateCompanyNameFromContactMethod()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromAddressToUpdateCompanyName, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles","Company" };
            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, false, primaryKeys, properties);
        }
        
        public static void DFT_MoveCNotesUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCNotes;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCount_CancelUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCount_Cancel;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCount_NoshowUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCount_NoShow;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCount_ResosUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCount_Resos;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCustcodesUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCustcodes;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveBAPExpirationUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForBAPExpiration;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveChildMembershipUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForChildMembership;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCommentUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForComment;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCreatedOnlineUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCreatedOnline;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveCustomerCodeUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForCustomerCode;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveHasOnlineProfileUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForHasOnlineProfile;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveLastOnlineOrderDateUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForLastOnlineOrderDate;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveLastOnlineOrderNumberUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForLastOnlineOrderNumber;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveOmniPassBarcodeUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForOmniPassBarcode;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveOmniPassIDUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForOmniPassID;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveOnlineCustomerNumberUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForOnlineCustomerNumber;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveProductDeliveryUDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForProductDelivery;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MovePROF_COD_5UDFData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForPROF_COD_5;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveUDFC20TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForUDFC20;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveUDFC22TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForUDFC22;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveUDFC23TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForUDFC23;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }

        public static void DFT_MoveUDFC33TypeOfData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var lookUpCon = PMSDBManager.GetPMSConnectionString();
            var tableName = "dbo.D_Customer_UDFFields";
            var sql = CRMDBManager.SQL_GetDataETL_Temp_ProfilesAndD_Customer;
            var lsql = PMSDBManager.SQL_UDFDataForUDFC33;

            var keys = new Dictionary<string, string>();
            keys.Add("FK_Internal", "PK_Profiles");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("ColumnName", "UDFFieldName");
            lMapping.Add("UDFValue", "UDFFieldValue");
            new DataFlowTask<D_Customer_UDFFields, D_Customer_UDFFields, UDFData>().runTask(sourceCon, destinationCon, lookUpCon, tableName, sql, lsql, keys, lMapping, null, true, true, new List<string>() { "CustomerID", "UDFFieldName" });
        }
        
        public static void DFT_MoveEmailsFromD_CustomerToD_Customer_Email_NonRosewood()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer_Email";
            var sql = CRMDBManager.SQL_GetDataFromD_CustomerToMoveEmails;
            var primaryKeys = new List<string>() { "CustomerID", "EmailType" };
            var properties = new List<string>() { "CustomerID", "EmailType", "EmailStatus", "Email", "EmailDomainHash", "email_id" };

            new DataFlowTask<D_Customer_Email>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }
        
        public static void DFT_UpdateRecordStatusUnderPMS_Profiles()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_Profiles";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromProfilesToUpdateTwoStatus, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles", "RecordStatus"};

            new DataFlowTask<PMS_Profiles>().runTask(sourceCon, destinationCon, tableName, sql, true, false, primaryKeys, properties);
        }
        
        public static void DFT_UpdateRecordStatusUnderD_Customer()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer";
            var sql = string.Format(PMSDBManager.SQL_GetDataFromProfilesToUpdateTwoStatus, CompanySettings.StartDate, CompanySettings.EndDate);
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles", "RecordStatus" };

            new DataFlowTask<D_Customer>().runTask(sourceCon, destinationCon, tableName, sql, true, false, primaryKeys, properties);
        }


    }
}
