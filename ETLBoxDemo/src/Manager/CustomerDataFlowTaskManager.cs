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

		public static void DFT_MoveBirthday()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.D_Customer_Profile";
            var sql = PMSDBManager.SQL_MoveBirthday;
            var primaryKeys = new List<string>() { "CustomerID" };
            var properties = new List<string>() { "CustomerID", "DOB"};

            new DataFlowTask<D_Customer_Profile>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

        public static void DFT_UpdatePreferredLanguageUnderPMS_Profiles()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_Profiles";
            var sql = PMSDBManager.SQL_GetPreferredLanguage;
            var primaryKeys = new List<string>() { "PK_Profiles" };
            var properties = new List<string>() { "PK_Profiles", "PrimaryLanguage" };

            new DataFlowTask<PMS_Profiles>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
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
            var sql = PMSDBManager.SQL_GetD_Customer_EmailQuery;
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

    }
}
