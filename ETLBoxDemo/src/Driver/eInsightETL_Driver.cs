using ETLBoxDemo.Common;
using ETLBoxDemo.src.Manager;
using ETLBoxDemo.src.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.eInsightETL_Driver
{
    public class eInsightETL_Driver
    {
        private Dictionary<string, object> requestSettings;
        public eInsightETL_Driver(Dictionary<string, object> requestSettings)
        {
            this.requestSettings = requestSettings;
        }
        public void start()
        {
            eContactDBManager.GetCompanySetting(requestSettings);

            //!!
            var logResult = SSISLOGDBManager.LogPackageStart("ETL_Driver");
            var BatchLogID = logResult["BatchLogID"];
            var PackageLogID = logResult["PackageLogID"];
            var EndBatchAudit = logResult["EndBatchAudit"];

            if ("1".Equals(CompanySettings.ETL_ECONCIERGEEXPORT) && "1.0".Equals(CompanySettings.ETL_ECONCIERGE_TYPE))
            {
                new eConciergeETLTask().Start();
            }

            if ("1".Equals(CompanySettings.ETL_HASNEWETL))
            {
                //DFT Driver Run ETL Packages !!

                new DataDictionaryTask().Start();

                new CustomerTask().Start();

                if ("1".Equals(CompanySettings.ETL_HASVENGADATA))
                {
                    new CustomerVengaTask().Start();
                }

                if ("1".Equals(CompanySettings.ETL_HASSIRIUSWAREDATA))
                {
                    new CustomerSiriuswareTask().Start();

                    if ("1".Equals(CompanySettings.ETL_HASCENRESWEBFORMS))
                    {
                        new CustomerWebFormsTask().Start();

                        if ("1".Equals(CompanySettings.ETL_HASCENRESOMNI))
                        {
                            new CustomerOmniTask().Start();
                        }
                    }
                }

                new CustomerStayTask().Start();

                if ("1".Equals(CompanySettings.HasStayOneToManyStayDetailHeader))
                {
                    new CustomerStayHeaderTask().Start();
                }

                new CustomerStayRateTask().Start();

                if ("1".Equals(CompanySettings.ETL_HASUNIFOCUSDATA))
                {
                    new CustomerUnifocusTask().Start();
                }

                if ("1".Equals(CompanySettings.ETL_HASMDSDATA))
                {
                    new CustomerMDSTask().Start();
                }

                if ("1".Equals(CompanySettings.ETL_HASCONDODATA))
                {
                    new CustomerCondoTask().Start();
                }

                if ("1".Equals(CompanySettings.ETL_HASCLUBESSENTIALSDATA))
                {
                    new CustomerClubEssentialsTask().Start();
                }

                if (new string[] { "8009", "6245", "10971", "11746" }.Contains(CompanySettings.CompanyID))
                {
                    new CustomerLeadsTask().Start();
                }

                if ("1".Equals(CompanySettings.HasStayActivities))
                {
                    new CustomerStayActivitiesTask().Start();
                }

                if ("1".Equals(CompanySettings.HasPropertyUnsubscribe) || "1".Equals(CompanySettings.HasBrandUnsubscribe) && !"10960".Equals(CompanySettings.CompanyID))
                {
                    new UnsubscribePropertyEmailTask().Start();
                }

                //Data Flow Task !!

                if ("1".Equals(CompanySettings.ETL_ECONCIERGEEXPORT) && !"1.0".Equals(CompanySettings.ETL_ECONCIERGE_TYPE))
                {
                    new eConciergeETLTask().Start();

                }

                if ("7375".Equals(CompanySettings.CompanyID))
                {
                    new MicrosTask().Start();
                }

                if ("1".Equals(CompanySettings.ETL_HASMEMBERSHIPS) || "1".Equals(CompanySettings.ETL_HASMEMBERSHIPSDATA) || "1".Equals(CompanySettings.HAS_PMSMemberships))
                {
                    new MembershipsTask().Start();

                    if ("1".Equals(CompanySettings.HAS_PMSMemberships))
                    {
                        new PMSObjectsGlobalProfileMaintenanceTask().Start();
                    }
                }

                new PostETLUpdatesTask().Start();

                //DFT Driver ALL CRM ETL TASKS !!

                new RealTimeCampaignTask().Start();

            }
            else
            {
                //DFT Driver Legacy Systems !!

                if ("isRealTime".Equals("1"))
                {
                    new RealTimeCampaignTask().Start();
                }

            }

            // !!
            SSISLOGDBManager.LogPackageEnd(PackageLogID, BatchLogID, EndBatchAudit);

        }
    }
}