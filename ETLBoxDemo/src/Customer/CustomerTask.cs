using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.src.Manager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Customer
{
    public class CustomerTask
    {
        private Dictionary<string, object> dictionarySettings;
        public CustomerTask()
        {
        }
        public CustomerTask(Dictionary<string, object> dictionarySettings)
        {
            this.dictionarySettings = dictionarySettings;
        }
        public void Start()
        {
            //Get Last CheckTime
            string strConnectionString = (string)dictionarySettings["User::streInsightCRMCONNECTSTRING"];
            string lastCheckTime = CRMDBManager.GetLastCheckTime(strConnectionString, "D_CUSTOMER");

            //Truncate Table ETL_TEMP_Profiles
            string strPMSConnectionString = (string)dictionarySettings["User::strPMSCONNECTSTRING"];
            SQLHelper.TruncateTable(strPMSConnectionString, "TruncateETLProfilesTEMPTable", "ETL_TEMP_Profiles");

            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_INSERT;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE;
            //TRUNCATE TABLE ETL_TEMP_D_CUSTOMER_For_Email;
            //SQLHelper.TruncateTable(strPMSConnectionString, "TruncateETLCustomerTempTable", "ETL_TEMP_PROFILES_D_CUSTOMER", "ETL_TEMP_PROFILES_D_CUSTOMER_INSERT", "ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE", "ETL_TEMP_D_CUSTOMER_For_Email");

            var companyId = dictionarySettings["CompanyID"] + "";
            if ("7375".Equals(companyId))
            {
                // Create Source Code For Open Table
                string openTableSourceCode = CRMDBManager.CreateSourceCode(strConnectionString, companyId, "CLIENT", "Open Table", 1, 99, 99);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCodeForOpenTable", sqlCreateSourceCodeForOpenTable, null);

                // Create Source Code For Counter Point
                string counterPointSourceCode = CRMDBManager.CreateSourceCode(strConnectionString, companyId, "CLIENT", "Counter Point", 1, 99, 99);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCodeForCounterPoint", sqlCreateSourceCodeForCounterPoint, null);
            }

            //create source code
            string pmsSourceCode = CRMDBManager.CreateSourceCode(strConnectionString, companyId, "PMS", "PMS", 0, 1, 1);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCode", sqlCreateSourceCode, null);

            // Move ProfileDocument
            List<string> profileDocuments = PMSDBManager.GetDataFromProfileDocuments(strPMSConnectionString);
            for (int i = 0; i < profileDocuments.Count; i++)
            {
                String[] strArr = profileDocuments[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ProfileDocuments = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_ProfileDocuments WHERE Id = @Id)
                                                                                    INSERT INTO dbo.PMS_ProfileDocuments (Id,FK_Profile,DocType,DocSource,CodeOnDocument,
                                                                                    DocNotes,DocId_PII,NameOnDocument_PII,DocumentBody_PII,NationalityOnDocument,EffectiveDate,ExpirationDate,PII_StoredAs,PII_Algorithm,PII_Key,PII_KeyId,Issuer ,
                                                                                    IssuerAddress1,IssuerAddress2,IssuerCity,IssuerStateProv,IssuerPostalCode,IssuerCountry,IsPrimary,InactiveDate,DateCreated,LastUpdated) 
                                                                                    VALUES (@Id,@FK_Profile,@DocType,@DocSource,@CodeOnDocument,@DocNotes,@DocId_PII,@NameOnDocument_PII,@DocumentBody_PII,@NationalityOnDocument,@EffectiveDate,@ExpirationDate,@PII_StoredAs,
                                                                                    @PII_Algorithm,@PII_Key,@PII_KeyId,@Issuer,@IssuerAddress1,@IssuerAddress2,@IssuerCity,@IssuerStateProv,@IssuerPostalCode,@IssuerCountry,@IsPrimary,@InactiveDate,@DateCreated,@LastUpdated)
                                                                    ELSE
                                                                          UPDATE dbo.PMS_ProfileDocuments SET FK_Profile = @FK_Profile,DocType = @DocType,DocSource = @DocSource,CodeOnDocument = @CodeOnDocument,DocNotes = @DocNotes,DocId_PII = @DocId_PII,
                                                                                                NameOnDocument_PII = @NameOnDocument_PII,DocumentBody_PII = @DocumentBody_PII,NationalityOnDocument = @NationalityOnDocument,EffectiveDate = @EffectiveDate,
                                                                                                ExpirationDate = @ExpirationDate,PII_StoredAs = @PII_StoredAs,PII_Algorithm = @PII_Algorithm, PII_Key = @PII_Key,PII_KeyId = @PII_KeyId,Issuer = @Issuer,
                                                                                                IssuerAddress1 = @IssuerAddress1,IssuerAddress2 = @IssuerAddress2,IssuerCity = @IssuerCity,IssuerStateProv = @IssuerStateProv,IssuerPostalCode = @IssuerPostalCode,
                                                                                                IssuerCountry = @IssuerCountry,IsPrimary = @IsPrimary,InactiveDate = @InactiveDate,DateCreated = @DateCreated, LastUpdated = @LastUpdated WHERE Id = @Id          
                                                                    ";
               
                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("Id", "string", strArr[0]),
                        new QueryParameter("FK_Profile", "string", strArr[1]),
                        new QueryParameter("DocType", "string", strArr[2]),
                        new QueryParameter("DocSource", "string", strArr[3]),
                        new QueryParameter("CodeOnDocument", "string", strArr[4]),
                        new QueryParameter("DocNotes", "string", strArr[5]),
                        new QueryParameter("DocId_PII", "string", strArr[6]),
                        new QueryParameter("NameOnDocument_PII", "string", strArr[7]),
                        new QueryParameter("DocumentBody_PII", "string", strArr[8]),
                        new QueryParameter("NationalityOnDocument", "string", strArr[9]),
                        new QueryParameter("EffectiveDate", "string", strArr[10]),
                        new QueryParameter("ExpirationDate", "string", strArr[11]),
                        new QueryParameter("PII_StoredAs", "string", strArr[12]),
                        new QueryParameter("PII_Algorithm", "string", strArr[13]),
                        new QueryParameter("PII_Key", "string", strArr[14]),
                        new QueryParameter("PII_KeyId", "string", strArr[15]),
                        new QueryParameter("Issuer", "string", strArr[16]),
                        new QueryParameter("IssuerAddress1", "string", strArr[17]),
                        new QueryParameter("IssuerAddress2", "string", strArr[18]),
                        new QueryParameter("IssuerCity", "string", strArr[19]),
                        new QueryParameter("IssuerStateProv", "string", strArr[20]),
                        new QueryParameter("IssuerPostalCode", "string", strArr[21]),
                        new QueryParameter("IssuerCountry", "string", strArr[22]),
                        new QueryParameter("IsPrimary", "string", strArr[23]),
                        new QueryParameter("InactiveDate", "string", strArr[24]),
                        new QueryParameter("DateCreated", "string", strArr[25]),
                        new QueryParameter("LastUpdated", "string", strArr[26])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ProfileDocuments", sqlInsertOrUpdateTablePMS_ProfileDocuments, InsertOrUpdateParameter);
            }

            // Move ProfilePolicies
            string sqlGetDataFromProfilePolicies = @"SELECT PK_ProfilePolicies ,
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

            List<string> sqlGetDataFromProfilePoliciesList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetDataFromProfilePolicies", sqlGetDataFromProfilePolicies, null);
            for (int i = 0; i < sqlGetDataFromProfilePoliciesList.Count; i++)
            {
                String[] strArr = sqlGetDataFromProfilePoliciesList[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ProfilePolicies = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_ProfilePolicies WHERE PK_ProfilePolicies = @PK_ProfilePolicies)
                                                                                    INSERT INTO dbo.PMS_ProfilePolicies (PK_ProfilePolicies,FK_Profiles,FK_PolicyTypes,AttributeName,IntegerValue,StringValue,StartDate,ExpirationDate,Comments)
                                                                                    VALUES (@PK_ProfilePolicies,@FK_Profiles,@FK_PolicyTypes,@AttributeName,@IntegerValue,@StringValue,@StartDate,@ExpirationDate,@Comments)
                                                                     ELSE
                                                                            UPDATE dbo.PMS_ProfilePolicies SET FK_Profiles = @FK_Profiles, FK_PolicyTypes = @FK_PolicyTypes, AttributeName = @AttributeName,
                                                                            IntegerValue = @IntegerValue, StringValue = @StringValue, StartDate = @StartDate, ExpirationDate = @ExpirationDate, Comments = @Comments WHERE PK_ProfilePolicies = @PK_ProfilePolicies        
                                                                    ";

                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ProfilePolicies", "string", strArr[0]),
                        new QueryParameter("FK_Profiles", "string", strArr[1]),
                        new QueryParameter("FK_PolicyTypes", "string", strArr[2]),
                        new QueryParameter("AttributeName", "string", strArr[3]),
                        new QueryParameter("IntegerValue", "string", strArr[4]),
                        new QueryParameter("StringValue", "string", strArr[5]),
                        new QueryParameter("StartDate", "string", strArr[6]),
                        new QueryParameter("ExpirationDate", "string", strArr[7]),
                        new QueryParameter("Comments", "string", strArr[8])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ProfilePolicies", sqlInsertOrUpdateTablePMS_ProfilePolicies, InsertOrUpdateParameter);
            }

            //Move ContactMethodPolicies
            string sqlGetDataFromContactMethodPolicies = @"SELECT PK_ContactMethodPolicies ,
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
            List<string> sqlGetDataFromContactMethodPoliciesList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetDataFromContactMethodPolicies", sqlGetDataFromContactMethodPolicies, null);
            for (int i = 0; i < sqlGetDataFromContactMethodPoliciesList.Count; i++)
            {
                String[] strArr = sqlGetDataFromContactMethodPoliciesList[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ContactMethodPolicies = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_ContactMethodPolicies WHERE PK_ContactMethodPolicies = @PK_ContactMethodPolicies)
                                                                                         INSERT INTO dbo.PMS_ContactMethodPolicies (PK_ContactMethodPolicies,FK_ContactMethod,FK_PolicyTypes,AttributeName,IntegerValue,StringValue,StartDate,ExpirationDate,Comments)
                                                                                         VALUES (@PK_ContactMethodPolicies,@FK_ContactMethod,@FK_PolicyTypes,@AttributeName,@IntegerValue,@StringValue,@StartDate,@ExpirationDate,@Comments)
                                                                            ELSE
                                                                                        UPDATE dbo.PMS_ContactMethodPolicies SET FK_ContactMethod = @FK_ContactMethod, FK_PolicyTypes = @FK_PolicyTypes, AttributeName = @AttributeName,
                                                                                        IntegerValue = @IntegerValue, StringValue = @StringValue, StartDate = @StartDate, ExpirationDate = @ExpirationDate, Comments = @Comments WHERE PK_ContactMethodPolicies = @PK_ContactMethodPolicies    
                                                                        ";
                
                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ProfilePolicies", "string", strArr[0]),
                        new QueryParameter("FK_Profiles", "string", strArr[1]),
                        new QueryParameter("FK_PolicyTypes", "string", strArr[2]),
                        new QueryParameter("AttributeName", "string", strArr[3]),
                        new QueryParameter("IntegerValue", "string", strArr[4]),
                        new QueryParameter("StringValue", "string", strArr[5]),
                        new QueryParameter("StartDate", "string", strArr[6]),
                        new QueryParameter("ExpirationDate", "string", strArr[7]),
                        new QueryParameter("Comments", "string", strArr[8])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ContactMethodPolicies", sqlInsertOrUpdateTablePMS_ContactMethodPolicies, InsertOrUpdateParameter);
            }

            //Move ContactMethod
            string sqlGetDataFromContactMethod = @"SELECT  cm.PK_ContactMethod,cm.FK_Reservations,cm.FK_Profiles,cm.CMStatusId,cm.CMType,cm.CMData,cm.CMCategory,cm.CMOptOut,cm.CMSourceDate,cm.DateInserted,
                                                           cm.LastUpdated,cm.Checksum,cm.IsDirty, cm.IsPrimary, cm.Confirmation, cm.CMExtraData, cm.InactiveDate, cm.RecordStatus 
                                                            FROM dbo.V_ContactMethod cm with (nolock) WHERE  (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00') and cm.LastUpdated <= '2012-01-24 11:06:00' and cm.DateInserted <= '2012-01-24 11:06:00'";
            List<string> sqlGetDataFromContactMethodList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetDataFromContactMethod", sqlGetDataFromContactMethod, null);
            for (int i = 0; i < sqlGetDataFromContactMethodList.Count; i++)
            {
                String[] strArr = sqlGetDataFromContactMethodList[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ContactMethod = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_CONTACTMETHOD WHERE PK_ContactMethod = @PK_ContactMethod)
                                                                                         INSERT INTO dbo.PMS_CONTACTMETHOD (PK_ContactMethod,FK_Reservations,FK_Profiles,CMStatusId,CMType,CMData,CMCategory,CMOptOut,CMSourceDate,DateInserted,
                                                                                         LastUpdated,Checksum,IsDirty, IsPrimary, Confirmation, CMExtraData, InactiveDate, RecordStatus)
                                                                                         VALUES (@PK_ContactMethod,@FK_Reservations,@FK_Profiles,@CMStatusId,@CMType,@CMData,@CMCategory,@CMOptOut,@CMSourceDate,@DateInserted,
                                                                                         @LastUpdated,@Checksum,@IsDirty, @IsPrimary, @Confirmation, @CMExtraData, @InactiveDate, @RecordStatus)
                                                                            ELSE
                                                                                        UPDATE dbo.PMS_CONTACTMETHOD SET FK_Reservations=@FK_Reservations,FK_Profiles=@FK_Profiles,CMStatusId=@CMStatusId,CMType=@CMType,CMData=@CMData,CMCategory=@CMCategory,CMOptOut=@CMOptOut,CMSourceDate=@CMSourceDate,DateInserted=@DateInserted,
                                                                                         LastUpdated=@LastUpdated,Checksum=@Checksum,IsDirty=@IsDirty, IsPrimary=@IsPrimary, Confirmation=@Confirmation, CMExtraData=@CMExtraData, InactiveDate=@InactiveDate, RecordStatus=@RecordStatus WHERE PK_ContactMethod = @PK_ContactMethod    
                                                                        ";

                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ContactMethod", "string", strArr[0]),
                        new QueryParameter("FK_Reservations", "string", strArr[1]),
                        new QueryParameter("FK_Profiles", "string", strArr[2]),
                        new QueryParameter("CMStatusId", "string", strArr[3]),
                        new QueryParameter("CMType", "string", strArr[4]),
                        new QueryParameter("CMData", "string", strArr[5]),
                        new QueryParameter("CMCategory", "string", strArr[6]),
                        new QueryParameter("CMOptOut", "string", strArr[7]),
                        new QueryParameter("CMSourceDate", "string", strArr[8]),
                        new QueryParameter("DateInserted", "string", strArr[9]),
                        new QueryParameter("LastUpdated", "string", strArr[10]),
                        new QueryParameter("Checksum", "string", strArr[11]),
                        new QueryParameter("IsDirty", "string", strArr[12]),
                        new QueryParameter("IsPrimary", "string", strArr[13]),
                        new QueryParameter("Confirmation", "string", strArr[14]),
                        new QueryParameter("CMExtraData", "string", strArr[15]),
                        new QueryParameter("InactiveDate", "string", strArr[16]),
                        new QueryParameter("RecordStatus", "string", strArr[17])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ContactMethod", sqlInsertOrUpdateTablePMS_ContactMethod, InsertOrUpdateParameter);
            }

            //Update SourceName under ContactMethod
            string sqlGetSourceNameFromContactMethod = @"SELECT cm.PK_ContactMethod, cm.SourceName FROM dbo.ContactMethod cm with (nolock) WHERE (cm.LastUpdated >= '2012-03-12 20:50:00' OR cm.DateInserted >= '2012-03-12 20:50:00') and cm.LastUpdated <= '2012-01-24 11:06:00' and cm.DateInserted <= '2012-01-24 11:06:00'";
            List<string> sqlGetSourceNameFromContactMethodList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetSourceNameFromContactMethod", sqlGetSourceNameFromContactMethod, null);
            for (int i = 0; i < sqlGetSourceNameFromContactMethodList.Count; i++)
            {
                String[] strArr = sqlGetSourceNameFromContactMethodList[i].Split(",");
                string sqlUpdateSourceNameUnderTablePMS_ContactMethod = @"UPDATE dbo.PMS_CONTACTMETHOD SET SourceName=@SourceName WHERE PK_ContactMethod = @PK_ContactMethod";

                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ContactMethod", "string", strArr[0]),
                        new QueryParameter("SourceName", "string", strArr[1])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlUpdateSourceNameUnderTablePMS_ContactMethod", sqlUpdateSourceNameUnderTablePMS_ContactMethod, InsertOrUpdateParameter);
            }

            //Move Action comments
            string sqlGetDataFromActionComments = @"SELECT PK_ActionComments, FK_Internal ,KeyTable ,CommentType ,ExternalID ,ActionType,ActionText , ActionTypeID, ActionDate ,InactiveDate ,
                                                    GuestViewable ,PMSCreatorCode ,DatePMSCommentCreated, DateInserted ,LastUpdated, Checksum, IsDirty, CommentTitle, CRMSourceActionType, RecordStatus, CommentClass, ResortID  FROM dbo.[V_PMS_ActionComments] p 
                                                    WHERE (p.LastUpdated >= '2012-03-12 20:50:00' OR p.DateInserted >= '2012-03-12 20:50:00') and p.LastUpdated <= '2012-01-24 11:06:00' and p.DateInserted <= '2012-01-24 11:06:00'";
            List<string> sqlGetDataFromActionCommentsList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetDataFromActionComments", sqlGetDataFromActionComments, null);
            for (int i = 0; i < sqlGetDataFromActionCommentsList.Count; i++)
            {
                String[] strArr = sqlGetDataFromActionCommentsList[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ActionComments = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_ActionComments WHERE PK_ActionComments = @PK_ActionComments)
                                                                                         INSERT INTO dbo.PMS_ActionComments (PK_ActionComments, FK_Internal ,KeyTable ,CommentType ,ExternalID ,ActionType,ActionText , ActionTypeID, ActionDate ,InactiveDate ,
                                                                                         GuestViewable ,PMSCreatorCode ,DatePMSCommentCreated, DateInserted ,LastUpdated, Checksum, IsDirty, CommentTitle, CRMSourceActionType, RecordStatus, CommentClass, ResortID)
                                                                                         VALUES (@PK_ActionComments, @FK_Internal ,@KeyTable ,@CommentType ,@ExternalID ,@ActionType,@ActionText , @ActionTypeID, @ActionDate ,@InactiveDate ,
                                                                                         @GuestViewable ,@PMSCreatorCode ,@DatePMSCommentCreated, @DateInserted ,@LastUpdated, @Checksum, @IsDirty, @CommentTitle, @CRMSourceActionType, @RecordStatus, @CommentClass, @ResortID)
                                                                            ELSE
                                                                                        UPDATE dbo.PMS_ActionComments SET FK_Internal=@FK_Internal ,KeyTable=@KeyTable ,CommentType=@CommentType ,ExternalID=@ExternalID ,ActionType=@ActionType,ActionText=@ActionText , ActionTypeID=@ActionTypeID, ActionDate=@ActionDate ,InactiveDate=@InactiveDate ,GuestViewable=@GuestViewable ,PMSCreatorCode=@PMSCreatorCode ,DatePMSCommentCreated=@DatePMSCommentCreated,
                                                                                        DateInserted=@DateInserted ,LastUpdated=@LastUpdated, Checksum=@Checksum, IsDirty=@IsDirty, CommentTitle=@CommentTitle, CRMSourceActionType=@CRMSourceActionType, RecordStatus=@RecordStatus, CommentClass=@CommentClass, ResortID=@ResortID WHERE PK_ActionComments = @PK_ActionComments    
                                                                        ";

                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_ActionComments", "string", strArr[0]),
                        new QueryParameter("FK_Internal", "string", strArr[1]),
                        new QueryParameter("KeyTable", "string", strArr[2]),
                        new QueryParameter("CommentType", "string", strArr[3]),
                        new QueryParameter("ExternalID", "string", strArr[4]),
                        new QueryParameter("ActionType", "string", strArr[5]),
                        new QueryParameter("ActionText", "string", strArr[6]),
                        new QueryParameter("ActionTypeID", "string", strArr[7]),
                        new QueryParameter("ActionDate", "string", strArr[8]),
                        new QueryParameter("InactiveDate", "string", strArr[9]),
                        new QueryParameter("GuestViewable", "string", strArr[10]),
                        new QueryParameter("PMSCreatorCode", "string", strArr[11]),
                        new QueryParameter("DatePMSCommentCreated", "string", strArr[12]),
                        new QueryParameter("DateInserted", "string", strArr[13]),
                        new QueryParameter("LastUpdated", "string", strArr[14]),
                        new QueryParameter("Checksum", "string", strArr[15]),
                        new QueryParameter("IsDirty", "string", strArr[16]),
                        new QueryParameter("CommentTitle", "string", strArr[17]),
                        new QueryParameter("CRMSourceActionType", "string", strArr[18]),
                        new QueryParameter("RecordStatus", "string", strArr[19]),
                        new QueryParameter("CommentClass", "string", strArr[20]),
                        new QueryParameter("ResortID", "string", strArr[21])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ActionComments", sqlInsertOrUpdateTablePMS_ActionComments, InsertOrUpdateParameter);
            }

            //Move Address
            string sqlGetDataFromAddress = @"SELECT A.PK_Address, A.FK_Profiles, A.AddressTypeCode, A.SourceAddressType, A.RecordStatus, A.AddressStatus, A.Attn, A.Address1, A.Address2,
                                                    A.City, A.StateProvince, A.PostalCode, A.CountryCode, A.DateInserted, A.LastUpdated,
                                                    A.Checksum, A.IsDirty, A.IsPrimary, A.AddressCleansed, A.AddressLanguage FROM    dbo.V_Address A with (nolock)
                                                    WHERE RecordStatus = 'Active' AND (A.LastUpdated >= '2012-03-12 20:50:00' OR A.DateInserted >= '2012-03-12 20:50:00') and A.LastUpdated <= '2012-01-24 11:06:00' and A.DateInserted <= '2012-01-24 11:06:00'";
            List<string> sqlGetDataFromAddressList = SQLHelper.GetDbValues(strPMSConnectionString, "sqlGetDataFromAddress", sqlGetDataFromAddress, null);
            for (int i = 0; i < sqlGetDataFromAddressList.Count; i++)
            {
                String[] strArr = sqlGetDataFromAddressList[i].Split(",");
                string sqlInsertOrUpdateTablePMS_ADDRESS = @"IF NOT EXISTS (SELECT 1 FROM dbo.PMS_ADDRESS WHERE PK_Address = @PK_Address)
                                                                                         INSERT INTO dbo.PMS_ADDRESS (PK_Address, FK_Profiles, AddressTypeCode, SourceAddressType, RecordStatus, AddressStatus, Attn, Address1, Address2,
                                                                                         City, StateProvince, PostalCode, CountryCode, DateInserted, LastUpdated,
                                                                                         Checksum, IsDirty, IsPrimary, AddressCleansed, AddressLanguage)
                                                                                         VALUES (@PK_Address, @FK_Profiles, @AddressTypeCode, @SourceAddressType, @RecordStatus, @AddressStatus, @Attn, @Address1, @Address2,
                                                                                         @City, @StateProvince, @PostalCode, @CountryCode, @DateInserted, @LastUpdated,
                                                                                         @Checksum, @IsDirty, @IsPrimary, @AddressCleansed, @AddressLanguage)
                                                                            ELSE
                                                                                        UPDATE dbo.PMS_ADDRESS SET FK_Profiles=@FK_Profiles, AddressTypeCode=@AddressTypeCode, SourceAddressType=@SourceAddressType, RecordStatus=@RecordStatus, AddressStatus=@AddressStatus, Attn=@Attn, Address1=@Address1, Address2=@Address2,
                                                                                         City=@City, StateProvince=@StateProvince, PostalCode=@PostalCode, CountryCode=@CountryCode, DateInserted=@DateInserted, LastUpdated=@LastUpdated,
                                                                                         Checksum=@Checksum, IsDirty=@IsDirty, IsPrimary=@IsPrimary, AddressCleansed=@AddressCleansed, AddressLanguage=@AddressLanguage WHERE PK_Address = @PK_Address    
                                                                        ";

                List<QueryParameter> InsertOrUpdateParameter = new List<QueryParameter>()
                    {   new QueryParameter("PK_Address", "string", strArr[0]),
                        new QueryParameter("FK_Profiles", "string", strArr[1]),
                        new QueryParameter("AddressTypeCode", "string", strArr[2]),
                        new QueryParameter("SourceAddressType", "string", strArr[3]),
                        new QueryParameter("RecordStatus", "string", strArr[4]),
                        new QueryParameter("AddressStatus", "string", strArr[5]),
                        new QueryParameter("Attn", "string", strArr[6]),
                        new QueryParameter("Address1", "string", strArr[7]),
                        new QueryParameter("Address2", "string", strArr[8]),
                        new QueryParameter("City", "string", strArr[9]),
                        new QueryParameter("StateProvince", "string", strArr[10]),
                        new QueryParameter("PostalCode", "string", strArr[11]),
                        new QueryParameter("CountryCode", "string", strArr[12]),
                        new QueryParameter("DateInserted", "string", strArr[13]),
                        new QueryParameter("LastUpdated", "string", strArr[14]),
                        new QueryParameter("Checksum", "string", strArr[15]),
                        new QueryParameter("IsDirty", "string", strArr[16]),
                        new QueryParameter("IsPrimary", "string", strArr[17]),
                        new QueryParameter("AddressCleansed", "string", strArr[18]),
                        new QueryParameter("AddressLanguage", "string", strArr[19])
                    };
                SQLHelper.InsertOrUpdateDbValue(strConnectionString, "sqlInsertOrUpdateTablePMS_ADDRESS", sqlInsertOrUpdateTablePMS_ADDRESS, InsertOrUpdateParameter);
            }

            //Move



        }
    }
}