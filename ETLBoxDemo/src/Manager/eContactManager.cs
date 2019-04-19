using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public static class eContactManager
    {
        public static string SQL_GeteContactSettings()
        {
            return @"SELECT DISTINCT SettingName,SettingValue
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
        }

        public static Dictionary<string, object> GetNecessarySetting(int companyId)
        {
            Dictionary<string, object> necessarySettings = new Dictionary<string, object>();
            var queryString = SQL_GeteContactSettings();
            string eContactConnectionString = System.Configuration.ConfigurationManager.AppSettings["econtact-db-sql"];
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(eContactConnectionString));
            List<string> DatabaseValues = new List<string>();
            List<QueryParameter> parameter = new List<QueryParameter>() { new QueryParameter("CompanyID", "int", companyId) };
            new SqlTask("Select", queryString, parameter)
            {
                Actions = new List<Action<object>>() {
                    n => DatabaseValues.Add((string)n)
                }
            }.ExecuteReader();
            Dictionary<string, string> datasettings = new Dictionary<string, string>();
            for (int i = 0; i < DatabaseValues.Count; i++)
            {
                String[] strArr = DatabaseValues[i].Split(",");
                datasettings.Add(strArr[0], strArr[1]);
            }
            
            necessarySettings.Add("CompanyID", companyId);
            if (datasettings.Count > 0 && datasettings["ETL_HASNEWETL"] != null)
            {
                //eInsight OLEDB CRM and eInsight ADO.NET CRM
                if (datasettings.ContainsKey("SP_ServerName") && datasettings["SP_ServerName"] != null && datasettings.ContainsKey("SP_DatabaseName") && datasettings["SP_DatabaseName"] != null && datasettings.ContainsKey("SP_DBUser") && datasettings["SP_DBUser"] != null && datasettings.ContainsKey("SP_DBPassword") && datasettings["SP_DBPassword"] != null)
                {
                    //eInsight OLEDB CRM
                    necessarySettings.Add("User::streInsightCRMCONNECTSTRING",
                        string.Format("Data Source = {0}; initial catalog = {2}; uid = {1}; pwd = {3}; MultipleActiveResultSets = True;",
                        datasettings["SP_ServerName"],
                        datasettings["SP_DBUser"],
                        datasettings["SP_DatabaseName"],
                        datasettings["SP_DBPassword"]));

                    //eInsight ADO.NET CRM
                    //necessarySettings.Add("User::strADONETeInsightCRMCONNECTSTRING",
                    //    string.Format("Data Source={0};User ID={1};Initial Catalog={2};Persist Security Info=True;Application Name=ADOeInsightCRM;Password={3};",
                    //    datasettings["SP_ServerName"],
                    //    datasettings["SP_DBUser"],
                    //    datasettings["SP_DatabaseName"],
                    //    datasettings["SP_DBPassword"]));
                }
                else
                {
                    throw new KeyNotFoundException("Company is Missing CRM Database Connection Setting.");
                }

                //CENRES
                if (datasettings.ContainsKey("ETL_CENRES_SERVERNAME") && datasettings["ETL_CENRES_SERVERNAME"] != null && datasettings.ContainsKey("ETL_CENRES_DATABASENAME") && datasettings["ETL_CENRES_DATABASENAME"] != null && datasettings.ContainsKey("ETL_CENRES_DBUSER") && datasettings["ETL_CENRES_DBUSER"] != null && datasettings.ContainsKey("ETL_CENRES_DBPASSWORD") && datasettings["ETL_CENRES_DBPASSWORD"] != null)
                {
                    necessarySettings.Add("User::strPMSCONNECTSTRING",
                        string.Format("Data Source = {0}; initial catalog = {2}; uid = {1}; pwd = {3}; MultipleActiveResultSets = True;",
                        datasettings["ETL_CENRES_SERVERNAME"],
                        datasettings["ETL_CENRES_DBUSER"],
                        datasettings["ETL_CENRES_DATABASENAME"],
                        datasettings["ETL_CENRES_DBPASSWORD"]));
                }
                else
                {
                    throw new KeyNotFoundException("Company is Missing CenRes Database Connection Setting.");
                }

                //SSIS Logs
                if (datasettings.ContainsKey("ETL_SSIS_Logs_SERVERNAME") && datasettings["ETL_SSIS_Logs_SERVERNAME"] != null && datasettings.ContainsKey("ETL_SSIS_Logs_DATABASENAME") && datasettings["ETL_SSIS_Logs_DATABASENAME"] != null && datasettings.ContainsKey("ETL_SSIS_Logs_DBUSER") && datasettings["ETL_SSIS_Logs_DBUSER"] != null && datasettings.ContainsKey("ETL_SSIS_Logs_DBPASSWORD") && datasettings["ETL_SSIS_Logs_DBPASSWORD"] != null)
                {
                    necessarySettings.Add("User::streInsight_SSIS_LogsCONNECTSTRING",
                        string.Format("Data Source = {0}; initial catalog = {2}; uid = {1}; pwd = {3}; MultipleActiveResultSets = True;",
                        datasettings["ETL_SSIS_Logs_SERVERNAME"],
                        datasettings["ETL_SSIS_Logs_DBUSER"],
                        datasettings["ETL_SSIS_Logs_DATABASENAME"],
                        datasettings["ETL_SSIS_Logs_DBPASSWORD"]));
                }
                else
                {
                    throw new KeyNotFoundException("Company is Missing SSIS Logs Database Connection Setting.");
                }

                //ServerName and DatabaseName
                necessarySettings.Add("User::DatabaseName", datasettings["SP_DatabaseName"]);
                necessarySettings.Add("User::ServerName", datasettings["SP_ServerName"]);

                string tempValue;
                //check if property has new ETL
                datasettings.TryGetValue("ETL_HASNEWETL", out tempValue);
                necessarySettings.Add("User::HasNewETL", tempValue ?? "0");

                //check if Company has property unsubscribe
                //necessarySettings.Add("User::HasPropertyUnsubscribe", datasettings.ContainsKey("HasBrandUnsubscribe") ? datasettings["HasBrandUnsubscribe"] : "0");

                //check if property has auto dedupe
                datasettings.TryGetValue("ETL_AUTODEDUPE", out tempValue);
                necessarySettings.Add("User::ETLAutoDedupe", Int32.Parse(tempValue ?? "0"));
                datasettings.TryGetValue("ETL_PMSDEDUPE", out tempValue);
                necessarySettings.Add("User::ETLPMSDedupe", Int32.Parse(tempValue ?? "0"));

                //check if property has new dedupe model enabled
                datasettings.TryGetValue("ETL_DEDUPE", out tempValue);
                necessarySettings.Add("User::HAS_ETL_DEDUPE", Int32.Parse(tempValue ?? "0"));

                //check if property has loyalty program
                datasettings.TryGetValue("ETL_HASMEMBERSHIPS", out tempValue);
                necessarySettings.Add("User::HasMemberships", Int32.Parse(tempValue ?? "0"));

                //check if property has econcierge export
                datasettings.TryGetValue("ETL_ECONCIERGEEXPORT", out tempValue);
                necessarySettings.Add("User::ETLExportForeConcierge", Int32.Parse(tempValue ?? "0"));

                //check if property has econcierge type
                datasettings.TryGetValue("ETL_ECONCIERGE_TYPE", out tempValue);
                necessarySettings.Add("User::streConciergeType", tempValue ?? "1.0");
                
                //check if client has HasPropertyUnsubscribe enabled
                datasettings.TryGetValue("HasPropertyUnsubscribe", out tempValue);
                necessarySettings.Add("User::HasPropertyUnsubscribe", (tempValue + "").ToUpper() == "Y" ? 1 : 0);

                //check if client has HasBrandUnsubscribe enabled
                datasettings.TryGetValue("HasBrandUnsubscribe", out tempValue);
                necessarySettings.Add("User::HasBrandUnsubscribe", (tempValue + "").ToUpper() == "Y" ? 1 : 0);

                //check if client has ETL_HASVOUCHERSSDATA enabled
                datasettings.TryGetValue("ETL_HASVOUCHERSSDATA", out tempValue);
                necessarySettings.Add("User::intHasVouchersData", (tempValue + "").ToUpper() == "Y" ? 1 : 0);
                
                //check if client has ETL_HAS_EC_SURVEY_RESPONSE_DATA enabled
                datasettings.TryGetValue("ETL_HAS_EC_SURVEY_RESPONSE_DATA", out tempValue);
                necessarySettings.Add("User::intHASECSURVEYRESPONSEDATA", (tempValue + "").ToUpper() == "Y" ? 1 : 0);
                

                //check if client has HasStayActivities enabled
                datasettings.TryGetValue("HasStayActivities", out tempValue);
                necessarySettings.Add("User::HasStayActivities", (tempValue + "").ToUpper() == "1" ? 1 : 0);
                
                //check if client has HasStayOneToManyStayDetailHeader enabled
                datasettings.TryGetValue("HasStayOneToManyStayDetailHeader", out tempValue);
                necessarySettings.Add("User::HasStayOneToManyStayDetailHeader", (tempValue + "").ToUpper() == "1" ? 1 : 0);
                
                //check if client has ETL_Has_RelatedTravelers_Info enabled
                datasettings.TryGetValue("ETL_Has_RelatedTravelers_Info", out tempValue);
                necessarySettings.Add("User::ETL_Has_RelatedTravelers_Info", (tempValue + "").ToUpper() == "1" ? 1 : 0);
                
                //check if property has MDS data
                if (datasettings.ContainsKey("ETL_HASMDSDATA"))
                {
                    necessarySettings.Add("User::HasMDSData", Int32.Parse(datasettings["ETL_HASMDSDATA"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_MDS_SERVERNAME") && datasettings["ETL_MDS_SERVERNAME"] != null && datasettings.ContainsKey("ETL_MDS_DATABASENAME") && datasettings["ETL_MDS_DATABASENAME"] != null && datasettings.ContainsKey("ETL_MDS_DBUSER") && datasettings["ETL_MDS_DBUSER"] != null && datasettings.ContainsKey("ETL_MDS_DBPASSWORD") && datasettings["ETL_MDS_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_MDS_SERVERNAME"],
                            datasettings["ETL_MDS_DBUSER"],
                            datasettings["ETL_MDS_DATABASENAME"],
                            datasettings["ETL_MDS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasMDSData", 0); 
                    necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING", string.Empty);
                }

                //check if property has Club Essentials data
                if (datasettings.ContainsKey("ETL_HASCLUBESSENTIALSDATA"))
                {
                    necessarySettings.Add("User::HasClubEssentialsData", Int32.Parse(datasettings["ETL_HASCLUBESSENTIALSDATA"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_CLUBESSENTIALS_SERVERNAME") && datasettings["ETL_CLUBESSENTIALS_SERVERNAME"] != null && datasettings.ContainsKey("ETL_CLUBESSENTIALS_DATABASENAME") && datasettings["ETL_CLUBESSENTIALS_DATABASENAME"] != null && datasettings.ContainsKey("ETL_CLUBESSENTIALS_DBUSER") && datasettings["ETL_CLUBESSENTIALS_DBUSER"] != null && datasettings.ContainsKey("ETL_CLUBESSENTIALS_DBPASSWORD") && datasettings["ETL_CLUBESSENTIALS_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_CLUBESSENTIALS_SERVERNAME"],
                            datasettings["ETL_CLUBESSENTIALS_DBUSER"],
                            datasettings["ETL_CLUBESSENTIALS_DATABASENAME"],
                            datasettings["ETL_CLUBESSENTIALS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasClubEssentialsData", 0); 
                    necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING", string.Empty);
                }

                //check if property has UNIFOCUS data
                if (datasettings.ContainsKey("ETL_HASUNIFOCUSDATA"))
                {
                    necessarySettings.Add("User::HasUNIFOCUSData", Int32.Parse(datasettings["ETL_HASUNIFOCUSDATA"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_UNIFOCUS_SERVERNAME") && datasettings["ETL_UNIFOCUS_SERVERNAME"] != null && datasettings.ContainsKey("ETL_UNIFOCUS_DATABASENAME") && datasettings["ETL_UNIFOCUS_DATABASENAME"] != null && datasettings.ContainsKey("ETL_UNIFOCUS_DBUSER") && datasettings["ETL_UNIFOCUS_DBUSER"] != null && datasettings.ContainsKey("ETL_UNIFOCUS_DBPASSWORD") && datasettings["ETL_UNIFOCUS_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=UNIFOCUS;Password={3};",
                            datasettings["ETL_UNIFOCUS_SERVERNAME"],
                            datasettings["ETL_UNIFOCUS_DBUSER"],
                            datasettings["ETL_UNIFOCUS_DATABASENAME"],
                            datasettings["ETL_UNIFOCUS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasUNIFOCUSData", 0); 
                    necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING", string.Empty);
                }

                //check if property has VENGA data
                if (datasettings.ContainsKey("ETL_HASVENGADATA"))
                {
                    necessarySettings.Add("User::HasVengaData", Int32.Parse(datasettings["ETL_HASVENGADATA"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_VENGASERVERNAME") && datasettings["ETL_VENGASERVERNAME"] != null && datasettings.ContainsKey("ETL_VENGADBUSER") && datasettings["ETL_VENGADBUSER"] != null && datasettings.ContainsKey("ETL_VENGADATABASENAME") && datasettings["ETL_VENGADATABASENAME"] != null && datasettings.ContainsKey("ETL_VENGADBPASSWORD") && datasettings["ETL_VENGADBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_VENGASERVERNAME"],
                            datasettings["ETL_VENGADBUSER"],
                            datasettings["ETL_VENGADATABASENAME"],
                            datasettings["ETL_VENGADBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasVengaData", 0); 
                    necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING", string.Empty);
                }

                //check if property has Date of Birth data
                datasettings.TryGetValue("ETL_HASDOB", out tempValue);
                necessarySettings.Add("User::HasDOB", Int32.Parse(tempValue ?? "0"));

                //check if property has A flag ETL_AUTOMATICALLY_MAP_MARKET_SEG set to 1
                datasettings.TryGetValue("ETL_AUTOMATICALLY_MAP_MARKET_SEG", out tempValue);
                necessarySettings.Add("User::AUTOMATICALLY_MAP_MARKET_SEG", Int32.Parse(tempValue ?? "0"));
                
                //check if property has A flag ETL_HASTRANSACTIONSDATA set to 1
                datasettings.TryGetValue("ETL_HASTRANSACTIONSDATA", out tempValue);
                necessarySettings.Add("User::HasTransactionsData", Int32.Parse(tempValue ?? "0"));

                //check if has cenres web forms data
                if (datasettings.ContainsKey("ETL_HASCENRESWEBFORMS"))
                {
                    necessarySettings.Add("User::HasCenResWebFormsData", Int32.Parse(datasettings["ETL_HASCENRESWEBFORMS"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_CENRES_WEBFORMS_SERVERNAME") && datasettings["ETL_CENRES_WEBFORMS_SERVERNAME"] != null && datasettings.ContainsKey("ETL_CENRES_WEBFORMS_DATABASENAME") && datasettings["ETL_CENRES_WEBFORMS_DATABASENAME"] != null && datasettings.ContainsKey("ETL_CENRES_WEBFORMS_DBUSER") && datasettings["ETL_CENRES_WEBFORMS_DBUSER"] != null && datasettings.ContainsKey("ETL_CENRES_WEBFORMS_DBPASSWORD") && datasettings["ETL_CENRES_WEBFORMS_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=WebFomrs;Password={3};",
                            datasettings["ETL_CENRES_WEBFORMS_SERVERNAME"],
                            datasettings["ETL_CENRES_WEBFORMS_DBUSER"],
                            datasettings["ETL_CENRES_WEBFORMS_DATABASENAME"],
                            datasettings["ETL_CENRES_WEBFORMS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasCenResWebFormsData", 0); 
                    necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING", string.Empty);
                }

                //check if has cenres omni data
                if (datasettings.ContainsKey("ETL_HASCENRESOMNI"))
                {
                    necessarySettings.Add("User::HasCenResOmniData", Int32.Parse(datasettings["ETL_HASCENRESOMNI"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_CENRES_OMNI_SERVERNAME") && datasettings["ETL_CENRES_OMNI_SERVERNAME"] != null && datasettings.ContainsKey("ETL_CENRES_OMNI_DATABASENAME") && datasettings["ETL_CENRES_OMNI_DATABASENAME"] != null && datasettings.ContainsKey("ETL_CENRES_OMNI_DBUSER") && datasettings["ETL_CENRES_OMNI_DBUSER"] != null && datasettings.ContainsKey("ETL_CENRES_OMNI_DBPASSWORD") && datasettings["ETL_CENRES_OMNI_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=WebFomrs;Password={3};",
                            datasettings["ETL_CENRES_OMNI_SERVERNAME"],
                            datasettings["ETL_CENRES_OMNI_DBUSER"],
                            datasettings["ETL_CENRES_OMNI_DATABASENAME"],
                            datasettings["ETL_CENRES_OMNI_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasCenResOmniData", 0); 
                    necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING", string.Empty);
                }

                //check if property has CONDO data
                if (datasettings.ContainsKey("ETL_HASCONDODATA"))
                {
                    necessarySettings.Add("User::HasCONDOData", Int32.Parse(datasettings["ETL_HASCONDODATA"] ?? "0"));
                    if (datasettings.ContainsKey("ETL_CONDO_SERVERNAME") && datasettings["ETL_CONDO_SERVERNAME"] != null && datasettings.ContainsKey("ETL_CONDO_DATABASENAME") && datasettings["ETL_CONDO_DATABASENAME"] != null && datasettings.ContainsKey("ETL_CONDO_DBUSER") && datasettings["ETL_CONDO_DBUSER"] != null && datasettings.ContainsKey("ETL_CONDO_DBPASSWORD") && datasettings["ETL_CONDO_DBPASSWORD"] != null)
                    {
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING",
                            string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_CONDO_SERVERNAME"],
                            datasettings["ETL_CONDO_DBUSER"],
                            datasettings["ETL_CONDO_DATABASENAME"],
                            datasettings["ETL_CONDO_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING", string.Empty);
                    }
                }
                else
                {
                    necessarySettings.Add("User::HasCONDOData", 0); 
                    necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING", string.Empty);
                }

                //Setting if enalbed then ETL will save the rate type description from CenRes database reservations_image table to eInsight database L_Data_Dictionary table for each property 
                datasettings.TryGetValue("BringRateTypeDescFromCenRes", out tempValue);
                necessarySettings.Add("User::BringRateTypeDescFromCenRes", Int32.Parse(tempValue ?? "0"));
                
                //check if property has sirius ware data
                datasettings.TryGetValue("ETL_HASSIRIUSWAREDATA", out tempValue);
                necessarySettings.Add("User::ETL_HASSiriusWareData", Int32.Parse(tempValue ?? "0"));
                
                //check if property has membership data (This is not same as what we have done for Trump)
                datasettings.TryGetValue("ETL_HASMEMBERSHIPSDATA", out tempValue);
                necessarySettings.Add("User::ETL_HASMEMBERSHIPSDATA", Int32.Parse(tempValue ?? "0"));
                
                //check if property has pms membership data
                datasettings.TryGetValue("HAS_PMSMemberships", out tempValue);
                necessarySettings.Add("User::HAS_PMSMemberships", Int32.Parse(tempValue ?? "0"));
                
                //necessarySettings.Add("User::HasMemberships", string.Format(datasettings["ETL_HASMEMBERSHIPS"]);

                //OLD
                //"Data Source=CENSRV0038;User ID=sadev;Initial Catalog=CenResNew;Provider=SQLNCLI10.1;Persist Security Info=True;Auto Translate=False;Application Name=SSIS-CustomerStay-{EF4C1286-9D99-41B7-AA27-E40838FDD5F6}CENSRV0038.CenResNew.sadev;Password=QWer1234;";
                //necessarySettings.Add("User::strOLEDBeInsight_SSIS_LogsCONNECTSTRING", "Data Source=CENSRV0038;User ID=sadev;Initial Catalog=eInsight_SSIS_Logs;Provider=SQLNCLI10.1;Persist Security Info=True;Auto Translate=False;Application Name=SSIS-CustomerStay-{EF4C1286-9D99-41B7-AA27-E40838FDD5F6}CENSRV0038.CenResNew.sadev;Password=QWer1234;";

                //System.Windows.Forms.MessageBox.Show(necessarySettings.Add("User::strOLEDBeInsightCRMCONNECTSTRING"].Value.ToString());
            }
            else
            {
                throw new KeyNotFoundException("Company is missing settings.");
            }

            return necessarySettings;
        }

    }
}
