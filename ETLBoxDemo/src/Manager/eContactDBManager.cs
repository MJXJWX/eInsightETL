﻿using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Text;
using ETLBoxDemo.Common;
using System.Linq;

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

        public static void GetCompanySetting(int companyId)
        {
            string eContactConnectionString = System.Configuration.ConfigurationManager.AppSettings["econtact-db-sql"];
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(eContactConnectionString));
            List<string> DatabaseValues = new List<string>();
            List<QueryParameter> parameter = new List<QueryParameter>() { new QueryParameter("CompanyID", "int", companyId) };
            System.Reflection.PropertyInfo[] properties = typeof(CompanySettings).GetProperties();
            string settingName = "", settingValue = "";
            new SqlTask("Select", SQL_GeteContactSettings, parameter)
            {
                BeforeRowReadAction = () => { settingName = settingValue = ""; },
                AfterRowReadAction = () => {
                    //properties.First(p => p.Name == settingName).SetValue(settingName, settingValue ?? "0");
                    for (int j = 0; j < properties.Length; j++)
                    {
                        if (properties[j].Name.Equals(settingName))
                        {
                            properties[j].SetValue(settingName, settingValue ?? "0");
                        }
                    }
                },
                Actions = new List<Action<object>>() {
                    sName => settingName = (string)sName,
                    sValue => settingValue = (string)sValue
                }
            }.ExecuteReader();
            //for (int i = 0; i < DatabaseValues.Count; i++)
            //{
            //    String[] strArr = DatabaseValues[i].Split(",");
            //    for (int j = 0; j< properties.Length; j++)
            //    {
            //        if (properties[j].Name.Equals(strArr[0]))
            //        {
            //            properties[j].SetValue(strArr[0],strArr[1]??"0");
            //        }
            //    }
            //}
            CompanySettings.CompanyID = companyId.ToString();
            
        }

    }
}
