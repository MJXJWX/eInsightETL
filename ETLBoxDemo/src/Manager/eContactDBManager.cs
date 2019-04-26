using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Text;
using ETLBoxDemo.Common;
using System.Linq;
using System.Data;
using System.Reflection;

namespace ETLBoxDemo.src.Manager
{
    public static class eContactDBManager
    {
        public static readonly string SQL_GeteContactSettings =@"SELECT DISTINCT SettingName,SettingValue
                                                                FROM dbo.eContact_Settings WITH (NOLOCK)
                                                                WHERE (settingname LIKE 'ETL%' OR settingname IN (
                                                                    'SP_DatabaseName'
                                                                    ,'SP_ServerName'
                                                                    ,'SP_DBUser'
                                                                    ,'SP_DBPassword'
                                                                    ,'ETL_AUTODEDUPE'
                                                                    ,'ETL_HASMEMBERSHIPS'
                                                                    ,'ETL_PMSDEDUPE'
                                                                    ,'ETL_ECONCIERGEEXPORT'
                                                                    ,'ETL_HASUNIFOCUSDATA'
			                                                        ,'ETL_HASMDSDATA'
                                                                    ,'ETL_HASDOB'
                                                                    ,'ETL_AUTOMATICALLY_MAP_MARKET_SEG'
                                                                    ,'ETL_HASTRANSACTIONSDATA'
                                                                    ,'ETL_HASCENRESWEBFORMS'
                                                                    ,'ETL_HASCENRESOMNI'
                                                                    ,'ETL_HASCONDODATA'
                                                                    ,'ETL_DEDUPE'
                                                                    ,'ETL_HASCLUBESSENTIALSDATA'
                                                                    ,'ETL_HASVENGADATA'
                                                                    ,'ETL_VENGASERVERNAME'
                                                                    ,'ETL_VENGADATABASENAME'
                                                                    ,'ETL_VENGADBUSER'
                                                                    ,'ETL_VENGADBPASSWORD'
                                                                    ,'ETL_HASSIRIUSWAREDATA'
                                                                    ,'ETL_HASMEMBERSHIPSDATA'
                                                                    ,'ETL_ECONCIERGE_TYPE'
                                                                    ,'ETL_HASVOUCHERSSDATA'
                                                                    ,'ETL_HAS_EC_SURVEY_RESPONSE_DATA'
                                                                    ,'HAS_PMSMemberships'
                                                                    )) AND CompanyID = @CompanyID

                                                                UNION

                                                                SELECT DISTINCT SettingName, SettingValue
                                                                FROM dbo.eContact_ParentCompany_Settings AS a WITH (NOLOCK)
                                                                INNER JOIN dbo.Company AS c WITH (NOLOCK) 
                                                                    ON c.ParentCompany = a.CompanyID
                                                                WHERE a.SettingName IN ('HasPropertyUnsubscribe', 'HasStayActivities', 'HasStayOneToManyStayDetailHeader', 'BringRateTypeDescFromCenRes', 'ETL_Has_RelatedTravelers_Info', 'HasBrandUnsubscribe')
                                                                AND c.CompanyID = @CompanyID";

        public static void GetCompanySetting(Dictionary<string, object> settings)
        {
            if (settings != null && settings.Count > 0)
            {
                foreach (var key in settings.Keys)
                {
                    if (typeof(CompanySettings).GetProperty(key) != null)
                    {
                        typeof(CompanySettings).GetProperty(key).SetValue(key, settings[key] + "");
                    }
                }
            }
            int companyId;
            int.TryParse(CompanySettings.CompanyID, out companyId);
            if (companyId == 0)
            {
                throw new KeyNotFoundException("Missing CompanyID in Request.");
            }

            string eContactConnectionString = System.Configuration.ConfigurationManager.AppSettings["econtact-db-sql"];
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(eContactConnectionString));
            List<QueryParameter> parameter = new List<QueryParameter>() { new QueryParameter("CompanyID", "int", companyId) };
            new SqlTask("Select", SQL_GeteContactSettings, parameter)
            {
                Actions = new List<Action<object>>() {
                    result => {
                        if (result != null && typeof(CompanySettings).GetProperty(DbTask.GetValueFromReader(result, "SettingName") + "") != null)
                        {
                            typeof(CompanySettings).GetProperty(DbTask.GetValueFromReader(result, "SettingName") + "").SetValue(DbTask.GetValueFromReader(result, "SettingName") + "", DbTask.GetValueFromReader(result, "SettingValue") ?? "0");
                        }
                    }
                }
            }.ExecuteReader();
        }

    }
}
