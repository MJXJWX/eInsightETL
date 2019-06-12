using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using ETLBoxDemo.src.Manager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Tasks
{
    public class CustomerTask
    {
        public void Start()
        {
            ControlFlow.STAGE = "0";
            var logger = new LogTask()
            {
                Message = "D_Customer Task",
                ActionType = "Start"
            };
            logger.Info();

            //Get Last CheckTime
            ControlFlow.STAGE = (int.Parse(ControlFlow.STAGE) + 1) + "";
            string lastCheckTime = CRMDBManager.GetLastCheckTime("D_CUSTOMER");

            //Truncate Table ETL_TEMP_Profiles
            //PMSDBManager.TruncateTable("ETL_TEMP_Profiles");


            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_INSERT;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE;
            //TRUNCATE TABLE ETL_TEMP_D_CUSTOMER_For_Email;
            //CRMDBManager.TruncateTable("ETL_TEMP_PROFILES_D_CUSTOMER", "ETL_TEMP_PROFILES_D_CUSTOMER_INSERT", "ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE", "ETL_TEMP_D_CUSTOMER_For_Email");

            var companyId = CompanySettings.CompanyID;
            if ("7375".Equals(companyId))
            {
                // Create Source Code For Open Table
                string openTableSourceCode = CRMDBManager.CreateSourceCode(companyId, "CLIENT", "Open Table", 1, 99, 99);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCodeForOpenTable", sqlCreateSourceCodeForOpenTable, null);

                // Create Source Code For Counter Point
                string counterPointSourceCode = CRMDBManager.CreateSourceCode(companyId, "CLIENT", "Counter Point", 1, 99, 99);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCodeForCounterPoint", sqlCreateSourceCodeForCounterPoint, null);
            }

            //create source code
            string pmsSourceCode = CRMDBManager.CreateSourceCode(companyId, "PMS", "PMS", 0, 1, 1);//SQLHelper.GetDbValues(strConnectionString, "sqlCreateSourceCode", sqlCreateSourceCode, null);

            // Move ProfileDocument
            CustomerDataFlowTaskManager.DFT_MoveProfileDocument();

            // Move ProfilePolicies
            CustomerDataFlowTaskManager.DFT_MoveProfilePolicies();

            //Move ContactMethodPolicies
            CustomerDataFlowTaskManager.DFT_MoveContactMethodPolicies();

            //Move ContactMethod
            CustomerDataFlowTaskManager.DFT_MoveContactMethod();

            //Update SourceName under ContactMethod
            CustomerDataFlowTaskManager.DFT_UpdateSourceNameUnderContactMethod();

            //Move Action comments
            CustomerDataFlowTaskManager.DFT_MoveActionComments();

            //Move Address
            CustomerDataFlowTaskManager.DFT_MoveAddress();

            //Update AddressTypeCode under table PMS_Address
            CRMDBManager.UpdateAddressTypeCodeUnderPMS_Address();

            //Move special Requests - Omni
            if ("12123".Equals(companyId) || "13358".Equals(companyId) || "13298".Equals(companyId))
            {
                CustomerDataFlowTaskManager.DFT_MoveSpecialRequests_Omni();
            }
            else
            {
                //Truncate CENRES_Specialrequests 
                CRMDBManager.TruncateTable("CENRES_SpecialRequests", "ETL_TEMP_Remove_SpecialRequests");

                //Move Updated Profiles into Temp table 
                CustomerDataFlowTaskManager.DFT_MoveUpdatedProfilesIntoTempTable();

                //Delete Global Special Requests for Updated Profiles
                CRMDBManager.DeleteGlobalSpecialRequestsForUpdatedProfiles();

                //Move the existing Special Requests

                //Move Existing Special Requests
                CRMDBManager.MoveExistingSpecialRequests();

                //Move Special Requests
                CustomerDataFlowTaskManager.DFT_MoveSpecialRequests_Omni();

                //Remove ResortField for Global Request
                CRMDBManager.RemoveResortFieldForGlobalRequest();

                //Truncate CENRES_Specialrequests
                CRMDBManager.TruncateTable("CENRES_SpecialRequests");

                //Move Special Requests into temp table

                //Delete Orphan records that don't match CenRes from PMS_SpecialRequests table
                CRMDBManager.DeleteOrphanRecords();
            }

            
            if ("7375".Equals(companyId))
            {
                // Update PMS Profile Mapping for Biltmore
                CRMDBManager.UpdatePMSProfileMappingForBiltmore();
            }
            else
            {
                //Update PMS Profile Mapping
                CRMDBManager.UpdatePMSProfileMapping();
            }

            // Move the New or Updates in tehe profiles in the Temp Table

            // Separate the Insert and Update into 2 temp tables

            //Insert profiles in D_Customer without membership
            CustomerDataFlowTaskManager.DFT_InsertOrUpdateD_CustomerWithoutMembership();

            if ("1338".Equals(companyId) || "1475".Equals(companyId) || "6245".Equals(companyId) || "7902".Equals(companyId) || "8009".Equals(companyId) || "7692".Equals(companyId) || "11733".Equals(companyId) || "11761".Equals(companyId))
            {
                //Update profiles into D_Customer with membership
                CustomerDataFlowTaskManager.DFT_InsertOrUpdateD_CustomerWithMembershipColumnUpdate();
            }
            else
            {
                //Update profiles into D_Customer without membership Column Update
                CustomerDataFlowTaskManager.DFT_InsertOrUpdateD_CustomerWithoutMembershipColumnUpdate();
            }

            //Update property code 
            CustomerDataFlowTaskManager.DFT_UpdatePropertyCode();

            //issue Move Profiles to PMS_Profiles and PMS_PROFILE_MAPPING
            CustomerDataFlowTaskManager.DFT_MoveProfilesToPMS_ProfilesAndPMS_PROFILE_MAPPING();

            //Move Profiles_Ext to PMS_Profiles_Ext 
            CustomerDataFlowTaskManager.DFT_MoveProfiles_ExtToPMS_Profiles_Ext();

			//Move Birthday into D_Customer_Profile
            CustomerDataFlowTaskManager.DFT_MoveBirthday();

            //Move Anniversary into D_Customer_Profile
            CustomerDataFlowTaskManager.DFT_MoveAnniversaryIntoD_Customer_Profile();

            //Move SpouseBirthDate into D_Customer_Profile
            CustomerDataFlowTaskManager.DFT_MoveSpouseBirthDateIntoD_Customer_Profile();

            if ("11312".Equals(companyId))
            {
                //Update Preferred Language in PMS_Profiles table - Minor
                CustomerDataFlowTaskManager.DFT_UpdatePreferredLanguageUnderPMS_Profiles();

                //Update preferred language in D_Customer table - Minor
                CRMDBManager.UpdatePreferredlanguageUnderD_Customer();
            }
            else
            {
                //Truncate temp tables
                CRMDBManager.TruncateTable("ETL_TEMP_D_CUSTOMER_PHONE");
            }

            if ("12123".Equals(companyId) || "13298".Equals(companyId) || "13358".Equals(companyId))
            {
                //Update all address fields to blank - Omni
                CRMDBManager.UpdateAllAddressFieldsToBlank();
            }

            //Update D_Customer table for Address
            CustomerDataFlowTaskManager.DFT_UpdateD_CustomerForAddress();

            if ("7375".Equals(companyId))
            {
                // Update Email in D_Customer for Biltmore
                CustomerDataFlowTaskManager.DFT_UpdateEmailUnderD_Customer();

                // Update Phone Numbers in D_Customer for Biltmore
                CustomerDataFlowTaskManager.DFT_UpdatePhoneNumbersUnderD_Customer();

                // Update Phone Ext in D_Customer for Biltmore 
                CustomerDataFlowTaskManager.DFT_UpdatePhoneExtUnderD_Customer();
            }
            else
            {
                //Remove inactive email records
                CustomerDataFlowTaskManager.DFT_RemoveInactiveEmailRecords();

                if ("10738".Equals(companyId))
                {
                    //Update AddressType as W if it is U for Rydges under D_Customer table
                    CRMDBManager.UpdateAddressTypeUnderD_Customer();
                }

                //Update Email And Phone In D_Customer
                CustomerDataFlowTaskManager.DFT_UpdateEmailAndPhoneUnderD_Customer();



                if ("12123".Equals(companyId) || "13298".Equals(companyId) || "13358".Equals(companyId))
                {
                    //Update all phone fields to blank - Omni
                    CRMDBManager.UpdateAllPhoneFieldsToBlank();
                }

                //Update Phones in D_Customer
                CRMDBManager.UpdatePhonesUnderD_Customer();

            }

            // D_Customer Email maintenance
            CRMDBManager.D_CustomerEmailMaintenance();

            if ("6543".Equals(companyId))
            {
                //Update D_Customer_Email Rosewood
                CustomerDataFlowTaskManager.DFT_UpdateD_Customer_Email();
            }

            //Fill Email table


            //Move data from ETL_D_Customer_Email_Staging table to D_Customer_Email table
            CustomerDataFlowTaskManager.DFT_UpSertDataIntoD_Customer_Email();

            //Truncate table ETL_D_CUSTOMER_EMAIL_STAGING
            CRMDBManager.TruncateTable("ETL_D_CUSTOMER_EMAIL_STAGING");

            if ("10960".Equals(companyId))
            {
                //Update OptIn OptOut based on ProfilePolicies Data
                CRMDBManager.UpdateOptInOptOut();
            }

            //Move EmailList type of data
            CustomerDataFlowTaskManager.DFT_MoveEmailListTypeOfData();

            //Move UDF31 Type Of Data
            CustomerDataFlowTaskManager.DFT_MoveUDF31TypeOfData();

            //Update VIPLevel based on UDFC31 type of data
            CRMDBManager.UpdateVIPLevel();

            if ("11757".Equals(companyId) || "12988".Equals(companyId) || "13006".Equals(companyId))
            {
                //Move KanaLastName
                CustomerDataFlowTaskManager.DFT_MoveKanaLastName();

                //Move KanaFirstName
                CustomerDataFlowTaskManager.DFT_MoveKanaFirstName();

                //Move NKanaLastName
                CustomerDataFlowTaskManager.DFT_MoveNKanaLastName();

                //Move NKanaFirstName
                CustomerDataFlowTaskManager.DFT_MoveNKanaFirstName();

                //Move NKanaName
                CustomerDataFlowTaskManager.DFT_MoveNKanaName();

                //Move UDFC37 Type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC37TypeOfData();

            }
            else if ("12123".Equals(companyId) || "13298".Equals(companyId) || "13358".Equals(companyId))
            {
                //Move Always_Email_Folio data
                CustomerDataFlowTaskManager.DFT_MoveAlways_Email_DolioData();

                //Move GHA_Email Data
                CustomerDataFlowTaskManager.DFT_MoveGHA_EmailData();

                //Move TOTEL_OFFER_EMAIL Data
                CustomerDataFlowTaskManager.DFT_MoveHotel_Offer_EmailData();

                //Move Marketing_Email Data
                CustomerDataFlowTaskManager.DFT_MoveMarketing_EmailData();

                //Move Marketing_Print Data
                CustomerDataFlowTaskManager.DFT_MoveMarketing_PrintData();

                //Move Mokara_Email Data
                CustomerDataFlowTaskManager.DFT_MoveMokara_EmailData();

                //Move New_Card_Request Data
                CustomerDataFlowTaskManager.DFT_MoveNew_Card_RequestData();

                //Move NewsLetter_Email Data
                CustomerDataFlowTaskManager.DFT_MoveNewsLetter_EmailData();

                //Move SG_ACCOUNT_SUMMARY Data
                CustomerDataFlowTaskManager.DFT_MoveSG_Account_SummaryData();

                //Move Transaction_Email Data
                CustomerDataFlowTaskManager.DFT_MoveTransaction_EmailData();

                //Move SG_Email Data
                CustomerDataFlowTaskManager.DFT_MoveSG_EmailData();

                //Move UDFData
                CustomerDataFlowTaskManager.DFT_MoveUDFData();
            }
            else
            {
                //Move UDFC37 Type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC37TypeOfData();
            }
            
            if ("7375".Equals(companyId))
            {
                //Move Sales Rep UDF Data
                CustomerDataFlowTaskManager.DFT_MoveSalesRepUDFData();

                //Move Est Rate UDF Data
                CustomerDataFlowTaskManager.DFT_MoveEstrateUDFData();

                //Move Depart UDF Data
                CustomerDataFlowTaskManager.DFT_MoveDepartUDFData();

                //Move Arrival UDF Data
                CustomerDataFlowTaskManager.DFT_MoveArrivalUDFData();

                //Move Block Ref UDF Data
                CustomerDataFlowTaskManager.DFT_MoveBlockRefUDFData();

                //Move Market Segment UDF Data
                CustomerDataFlowTaskManager.DFT_MoveMarketSegmentUDFData();
 
                //Move Market SubSegment UDF Data
                CustomerDataFlowTaskManager.DFT_MoveMarketSubSegmentUDFData();

                //Move Source UDF Data
                CustomerDataFlowTaskManager.DFT_MoveSourceUDFData();

                //Move Notes UDF Data
                CustomerDataFlowTaskManager.DFT_MoveNotesUDFData();

                //Move Banned UDF Data
                CustomerDataFlowTaskManager.DFT_MoveBannedUDFData();

                //Move Address3 UDF Data
                CustomerDataFlowTaskManager.DFT_MoveAddress3UDFData();

                //Move Alternate UDF Data
                CustomerDataFlowTaskManager.DFT_MoveAlternateUDFData();

                //Update CompanyName from ContactMethod table
                CustomerDataFlowTaskManager.DFT_UpdateCompanyNameFromContactMethod();

                //Move cnotes UDF Data for OpenTable
                CustomerDataFlowTaskManager.DFT_MoveCNotesUDFData();

                //Move count_cancel UDF Data for OpenTable
                CustomerDataFlowTaskManager.DFT_MoveCount_CancelUDFData();

                //Move count_noshow UDF Data for OpenTable
                CustomerDataFlowTaskManager.DFT_MoveCount_NoshowUDFData();
                
                //Move count_resos UDF Data for OpenTable
                CustomerDataFlowTaskManager.DFT_MoveCount_ResosUDFData();
               
                //Move custcodes UDF Data for OpenTable
                CustomerDataFlowTaskManager.DFT_MoveCustcodesUDFData();

                //Move BAPExpiration UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveBAPExpirationUDFData();

                //Move ChildMembership UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveChildMembershipUDFData();

                //Move Comment UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveCommentUDFData();

                //Move CreatedOnline UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveCreatedOnlineUDFData();

                //Move CustomerCode UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveCustomerCodeUDFData();

                //Move HasOnlineProfile UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveHasOnlineProfileUDFData();

                //Move LastOnlineOrderDate UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveLastOnlineOrderDateUDFData();

                //Move LastOnlineOrderNumber UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveLastOnlineOrderNumberUDFData();

                //Move OmniPassBarcode UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveOmniPassBarcodeUDFData();

                //Move OmniPassID UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveOmniPassIDUDFData();

                //Move OnlineCustomerNumber UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveOnlineCustomerNumberUDFData();

                //Move ProductDelivery UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MoveProductDeliveryUDFData();

                //Move PROF_COD_5 UDF Data for counterpoint
                CustomerDataFlowTaskManager.DFT_MovePROF_COD_5UDFData();
            }

            if ("11964".Equals(companyId))
            {
                //Move UDFC20 type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC20TypeOfData();

                //Move UDFC22 type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC22TypeOfData();

                //Move UDFC23 type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC23TypeOfData();

                //Move UDFC33 type of data
                CustomerDataFlowTaskManager.DFT_MoveUDFC33TypeOfData();
            }

            //DTF Connector

            //EmailVerify With Bounce Rules
            CRMDBManager.EmailVerifyWithBounceRules();

            // EmailVerify on D_Customer
            CRMDBManager.EmailVerifyOnD_Customer();

            if ("6543".Equals(companyId))
            {
                //EmailVerify on D_CUSTOMER_EMAIL - Rosewood
                CRMDBManager.EmailVerifyOnD_CUSTOMER_EMAIL_Rosewood();
            }
            else
            {
                //Move Emails From D_Customer To D_Customer_Email _NonRosewood
                CustomerDataFlowTaskManager.DFT_MoveEmailsFromD_CustomerToD_Customer_Email_NonRosewood();

                //Fill up missing data under D_Customer_Email table
                CRMDBManager.FillUpMissingDataUnderD_Customer_Email();

                //Fix emails which are not matching under D_Customer_Email table
                CRMDBManager.FixEmailsDataUnderD_Customer_Email();

                //Fix email_id under D_Customer_Email table
                CRMDBManager.FixEmail_IDUnderD_Customer_Email();
            }

            if ("12123".Equals(companyId) || "13298".Equals(companyId) || "13358".Equals(companyId))
            {
                //Update Inactive profiles record status - omni
                CustomerDataFlowTaskManager.DFT_UpdateRecordStatusUnderPMS_Profiles();
                CustomerDataFlowTaskManager.DFT_UpdateRecordStatusUnderD_Customer();
            }

            if ("1".Equals(CompanySettings.ETL_HASUNIFOCUSDATA))//@HasUNIFOCUSData == 1
            {
                //Update Unifocus score per customer
                CRMDBManager.UpdateUnifocusScorePerCustomer();
            }

            // Insert EndTime into Log Table
            //CRMDBManager.InsertEndTimeIntoLogTable();

            logger.ActionType = "END";
            logger.Info();
            ControlFlow.STAGE = (int.Parse(ControlFlow.STAGE) - 1) + "";
        }
    }
}