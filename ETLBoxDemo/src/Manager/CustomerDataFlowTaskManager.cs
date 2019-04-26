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

            new DataFlowTask<D_Customer>().runTask(sC, dC, dT, sql, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });

            //string dT = "dbo.eInsight_L_Languages";
            //string sql = "select ID, Language, Language_en, Globalization from dbo.eInsight_L_Languages with(nolock);";
            //new DataFlowTask<eInsight_L_Languages>().runTask(sC, dC, dT, sql);
        }

        public static void DFT_MoveProfileDocument()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.PMS_ProfileDocuments";
            var sql = PMSDBManager.SQL_GetDataFromProfileDocument;
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

            new DataFlowTask<PMS_ADDRESS>().runTask(sourceCon, destinationCon, tableName, sql, true, true, primaryKeys, properties);
        }

    }
}
