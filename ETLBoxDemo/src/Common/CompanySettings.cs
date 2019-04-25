using System;

namespace ETLBoxDemo.Common
{
    public class CompanySettings
    {
        public static string CompanyID { get; set;}
        public static string ETL_HASNEWETL { get; set; }
        public static string StartDate { get; set; }
        public static string EndDate { get; set; }
        public static string PK_Profiles { get; set; }

        public static string ETL_CENRES_SERVERNAME { get; set;}
        public static string ETL_CENRES_DBUSER { get; set;}
        public static string ETL_CENRES_DATABASENAME { get; set;}
        public static string ETL_CENRES_DBPASSWORD { get; set;}

        public static string SP_DatabaseName { get; set; }
        public static string SP_ServerName { get; set; }
        public static string SP_DBUser { get; set; }
        public static string SP_DBPassword { get; set; }

        public static string ETL_SSIS_Logs_SERVERNAME { get; set; }
        public static string ETL_SSIS_Logs_DBUSER { get; set; }
        public static string ETL_SSIS_Logs_DATABASENAME { get; set; }
        public static string ETL_SSIS_Logs_DBPASSWORD { get; set; }

        public static string ETL_MDS_SERVERNAME { get; set; }
        public static string ETL_MDS_DBUSER { get; set; }
        public static string ETL_MDS_DATABASENAME { get; set; }
        public static string ETL_MDS_DBPASSWORD { get; set; }

        public static string ETL_CLUBESSENTIALS_SERVERNAME { get; set; }
        public static string ETL_CLUBESSENTIALS_DBUSER { get; set; }
        public static string ETL_CLUBESSENTIALS_DATABASENAME { get; set; }
        public static string ETL_CLUBESSENTIALS_DBPASSWORD { get; set; }

        public static string ETL_CENRES_WEBFORMS_SERVERNAME{ get; set; }
        public static string ETL_CENRES_WEBFORMS_DBUSER{ get; set; }
        public static string ETL_CENRES_WEBFORMS_DATABASENAME{ get; set; }
        public static string ETL_CENRES_WEBFORMS_DBPASSWORD{ get; set; }

        public static string ETL_CENRES_OMNI_SERVERNAME{ get; set; }
        public static string ETL_CENRES_OMNI_DBUSER{ get; set; }
        public static string ETL_CENRES_OMNI_DATABASENAME{ get; set; }
        public static string ETL_CENRES_OMNI_DBPASSWORD{ get; set; }

        public static string ETL_UNIFOCUS_SERVERNAME{ get; set; }
        public static string ETL_UNIFOCUS_DBUSER{ get; set; }
        public static string ETL_UNIFOCUS_DATABASENAME{ get; set; }
        public static string ETL_UNIFOCUS_DBPASSWORD{ get; set; }

        public static string ETL_CONDO_SERVERNAME{ get; set; }
        public static string ETL_CONDO_DBUSER{ get; set; }
        public static string ETL_CONDO_DATABASENAME{ get; set; }
        public static string ETL_CONDO_DBPASSWORD{ get; set; }

        public static string ETL_AUTODEDUPE { get; set; }
        public static string ETL_HASMEMBERSHIPS { get; set; }
        public static string ETL_PMSDEDUPE { get; set; }
        public static string ETL_ECONCIERGEEXPORT { get; set; }
        public static string ETL_HASUNIFOCUSDATA { get; set; }
        public static string ETL_HASMDSDATA { get; set; }
        public static string ETL_HASDOB { get; set; }
        public static string ETL_AUTOMATICALLY_MAP_MARKET_SEG { get; set; }
        public static string ETL_HASTRANSACTIONSDATA { get; set; }
        public static string ETL_HASCENRESWEBFORMS { get; set; }
        public static string ETL_HASCENRESOMNI { get; set; }
        public static string ETL_HASCONDODATA { get; set; }
        public static string ETL_DEDUPE { get; set; }
        public static string ETL_HASCLUBESSENTIALSDATA { get; set; }
        public static string ETL_HASVENGADATA { get; set; }
        public static string ETL_VENGASERVERNAME { get; set; }
        public static string ETL_VENGADATABASENAME { get; set; }
        public static string ETL_VENGADBUSER { get; set; }
        public static string ETL_VENGADBPASSWORD { get; set; }
        public static string ETL_HASSIRIUSWAREDATA { get; set; }
        public static string ETL_HASMEMBERSHIPSDATA { get; set; }
        public static string ETL_ECONCIERGE_TYPE { get; set; }
        public static string ETL_HASVOUCHERSSDATA { get; set; }
        public static string ETL_HAS_EC_SURVEY_RESPONSE_DATA { get; set; }
        public static string HAS_PMSMemberships { get; set; }
        public static string HasPropertyUnsubscribe { get; set; }
        public static string HasStayActivities { get; set; }
        public static string HasStayOneToManyStayDetailHeader { get; set; }
        public static string BringRateTypeDescFromCenRes { get; set; }
        public static string ETL_Has_RelatedTravelers_Info { get; set; }
        public static string HasBrandUnsubscribe { get; set; }
    }
}