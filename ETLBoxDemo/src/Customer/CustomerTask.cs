using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
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
        public void Start()
        {
            //Get Last CheckTime
            string lastCheckTime = CRMDBManager.GetLastCheckTime("D_CUSTOMER");

            //Truncate Table ETL_TEMP_Profiles
            PMSDBManager.TruncateTable("ETL_TEMP_Profiles");


            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_INSERT;
            //TRUNCATE TABLE ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE;
            //TRUNCATE TABLE ETL_TEMP_D_CUSTOMER_For_Email;
            PMSDBManager.TruncateTable("ETL_TEMP_PROFILES_D_CUSTOMER", "ETL_TEMP_PROFILES_D_CUSTOMER_INSERT", "ETL_TEMP_PROFILES_D_CUSTOMER_UPDATE", "ETL_TEMP_D_CUSTOMER_For_Email");

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
            PMSDBManager.UpdateAddressTypeCodeUnderPMS_Address();



        }
    }
}