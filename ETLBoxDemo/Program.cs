using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBoxDemo.src.Customer;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace ALE.ETLBoxDemo {
    class Program {
        static void Main(string[] args) {
        string cmdString = @"SELECT DISTINCT SettingName,SettingValue
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


            try
            {
                string ConnString = System.Configuration.ConfigurationManager.AppSettings["ConnectionString"];
                //String path = Directory.GetCurrentDirectory().Split("bin")[0];
                //var conf = new ConfigurationBuilder().SetBasePath(path).AddJsonFile("appsettings.json", true, true).Build();
                //String strConnectString = conf["ConnectionString"];
                ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ETLBox.ConnectionString("data source=DHB-ARDB003.CENTRALSERVICES.LOCAL;initial catalog=econtact_cendyn_qa;uid=eInsight_DynamicContentService;pwd=einsight$123#;MultipleActiveResultSets=True"));
                List<string> DatabaseValues = new List<string>();
                List<QueryParameter> parameter = new List<QueryParameter>() { new QueryParameter("CompanyID", "int", 1338) };
                new SqlTask("Select", cmdString, parameter)
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
                Dictionary<string, object> necessarySettings = new Dictionary<string, object>();
                necessarySettings.Add("CompanyID",1338);
                if (datasettings.Count > 0 && datasettings["SP_ServerName"] != null && datasettings["SP_DBUser"] != null && datasettings["SP_DatabaseName"] != null && datasettings["SP_DBPassword"] != null && datasettings["ETL_CENRES_SERVERNAME"] != null && datasettings["ETL_CENRES_DBUSER"] != null && datasettings["ETL_CENRES_DATABASENAME"] != null && datasettings["ETL_CENRES_DBPASSWORD"] != null && datasettings["ETL_SSIS_Logs_SERVERNAME"] != null && datasettings["ETL_SSIS_Logs_DBUSER"] != null && datasettings["ETL_SSIS_Logs_DATABASENAME"] != null && datasettings["ETL_SSIS_Logs_DBPASSWORD"] != null && datasettings["ETL_HASNEWETL"] != null)
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
                    //CENRES
                    necessarySettings.Add("User::strPMSCONNECTSTRING",
                     string.Format("Data Source = {0}; initial catalog = {2}; uid = {1}; pwd = {3}; MultipleActiveResultSets = True;",
                        datasettings["ETL_CENRES_SERVERNAME"],
                        datasettings["ETL_CENRES_DBUSER"],
                        datasettings["ETL_CENRES_DATABASENAME"],
                        datasettings["ETL_CENRES_DBPASSWORD"]));
                    //SSIS Logs
                    necessarySettings.Add("User::streInsight_SSIS_LogsCONNECTSTRING",
                     string.Format("Data Source = {0}; initial catalog = {2}; uid = {1}; pwd = {3}; MultipleActiveResultSets = True;",
                        datasettings["ETL_SSIS_Logs_SERVERNAME"],
                        datasettings["ETL_SSIS_Logs_DBUSER"],
                        datasettings["ETL_SSIS_Logs_DATABASENAME"],
                        datasettings["ETL_SSIS_Logs_DBPASSWORD"]));

                    //Added by Amit to set servername and databasename variables
                    necessarySettings.Add("User::DatabaseName", string.Format(datasettings["SP_DatabaseName"]));
                    necessarySettings.Add("User::ServerName", string.Format(datasettings["SP_ServerName"]));

                    //check if property has new ETL
                    necessarySettings.Add("User::HasNewETL", string.Format(datasettings["ETL_HASNEWETL"]));

                    //check if Company has property unsubscribe
                    //necessarySettings.Add("User::HasPropertyUnsubscribe", string.Format(datasettings

                    //check if property has auto dedupe
                    if (datasettings.ContainsKey("ETL_AUTODEDUPE"))
                    {
                        necessarySettings.Add("User::ETLAutoDedupe", Int32.Parse(string.Format(datasettings["ETL_AUTODEDUPE"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::ETLAutoDedupe", 0); //Int32.Parse(string.Format(datasettings["ETL_AUTODEDUPE"]));

                    }

                    if (datasettings.ContainsKey("ETL_PMSDEDUPE"))
                    {
                        necessarySettings.Add("User::ETLPMSDedupe", Int32.Parse(string.Format(datasettings["ETL_PMSDEDUPE"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::ETLPMSDedupe", 0);

                    }

                    //check if property has new dedupe model enabled
                    if (datasettings.ContainsKey("ETL_DEDUPE"))
                    {
                        necessarySettings.Add("User::HAS_ETL_DEDUPE", Int32.Parse(string.Format(datasettings["ETL_DEDUPE"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::HAS_ETL_DEDUPE", 0); //Int32.Parse(string.Format(datasettings["ETL_DEDUPE"]));
                    }

                    //check if property has loyalty program
                    if (datasettings.ContainsKey("ETL_HASMEMBERSHIPS"))
                    {
                        necessarySettings.Add("User::HasMemberships", Int32.Parse(string.Format(datasettings["ETL_HASMEMBERSHIPS"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasMemberships", 0); // string.Format(datasettings["ETL_HASMEMBERSHIPS"]);

                    }


                    //check if property has econcierge export
                    if (datasettings.ContainsKey("ETL_ECONCIERGEEXPORT"))
                    {
                        necessarySettings.Add("User::ETLExportForeConcierge", Int32.Parse(string.Format(datasettings["ETL_ECONCIERGEEXPORT"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::ETLExportForeConcierge", 0);

                    }

                    //check if property has econcierge type
                    if (datasettings.ContainsKey("ETL_ECONCIERGE_TYPE"))
                    {
                        necessarySettings.Add("User::streConciergeType", datasettings["ETL_ECONCIERGE_TYPE"].ToString());
                    }
                    else
                    {
                        necessarySettings.Add("User::streConciergeType", "1.0");

                    }

                    //check if client has HasPropertyUnsubscribe enabled
                    if (datasettings.ContainsKey("HasPropertyUnsubscribe"))
                    {
                        necessarySettings.Add("User::HasPropertyUnsubscribe", string.Format(datasettings["HasPropertyUnsubscribe"]).ToUpper() == "Y" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::HasPropertyUnsubscribe", 0);

                    }

                    //check if client has HasBrandUnsubscribe enabled
                    if (datasettings.ContainsKey("HasBrandUnsubscribe"))
                    {
                        necessarySettings.Add("User::HasBrandUnsubscribe", string.Format(datasettings["HasBrandUnsubscribe"]).ToUpper() == "Y" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::HasBrandUnsubscribe", 0);

                    }

                    //check if client has ETL_HASVOUCHERSSDATA enabled
                    if (datasettings.ContainsKey("ETL_HASVOUCHERSSDATA"))
                    {
                        necessarySettings.Add("User::intHasVouchersData", string.Format(datasettings["ETL_HASVOUCHERSSDATA"]).ToUpper() == "Y" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::intHasVouchersData", 0);

                    }

                    //check if client has ETL_HAS_EC_SURVEY_RESPONSE_DATA enabled
                    if (datasettings.ContainsKey("ETL_HAS_EC_SURVEY_RESPONSE_DATA"))
                    {
                        necessarySettings.Add("User::intHASECSURVEYRESPONSEDATA", string.Format(datasettings["ETL_HAS_EC_SURVEY_RESPONSE_DATA"]).ToUpper() == "Y" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::intHASECSURVEYRESPONSEDATA", 0);

                    }

                    //check if client has HasStayActivities enabled
                    if (datasettings.ContainsKey("HasStayActivities"))
                    {
                        necessarySettings.Add("User::HasStayActivities", string.Format(datasettings["HasStayActivities"]).ToUpper() == "1" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::HasStayActivities", 0);

                    }

                    //check if client has HasStayOneToManyStayDetailHeader enabled
                    if (datasettings.ContainsKey("HasStayOneToManyStayDetailHeader"))
                    {
                        necessarySettings.Add("User::HasStayOneToManyStayDetailHeader", string.Format(datasettings["HasStayOneToManyStayDetailHeader"]).ToUpper() == "1" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::HasStayOneToManyStayDetailHeader", 0);

                    }

                    //check if client has ETL_Has_RelatedTravelers_Info enabled
                    if (datasettings.ContainsKey("ETL_Has_RelatedTravelers_Info"))
                    {
                        necessarySettings.Add("User::ETL_Has_RelatedTravelers_Info", string.Format(datasettings["ETL_Has_RelatedTravelers_Info"]).ToUpper() == "1" ? 1 : 0);
                    }
                    else
                    {
                        necessarySettings.Add("User::ETL_Has_RelatedTravelers_Info", 0);

                    }

                    //check if property has MDS data
                    if (datasettings.ContainsKey("ETL_HASMDSDATA"))
                    {
                        necessarySettings.Add("User::HasMDSData", Int32.Parse(string.Format(datasettings["ETL_HASMDSDATA"])));
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_MDS_SERVERNAME"],
                            datasettings["ETL_MDS_DBUSER"],
                            datasettings["ETL_MDS_DATABASENAME"],
                            datasettings["ETL_MDS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasMDSData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASMDSDATA"]));
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING", string.Empty);
                    }

                    //check if property has Club Essentials data
                    if (datasettings.ContainsKey("ETL_HASCLUBESSENTIALSDATA"))
                    {
                        necessarySettings.Add("User::HasClubEssentialsData", Int32.Parse(string.Format(datasettings["ETL_HASCLUBESSENTIALSDATA"])));
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_CLUBESSENTIALS_SERVERNAME"],
                            datasettings["ETL_CLUBESSENTIALS_DBUSER"],
                            datasettings["ETL_CLUBESSENTIALS_DATABASENAME"],
                            datasettings["ETL_CLUBESSENTIALS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasClubEssentialsData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASCLUBESSENTIALSDATA"]));
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING", string.Empty);
                    }

                    //check if property has UNIFOCUS data
                    if (datasettings.ContainsKey("ETL_HASUNIFOCUSDATA"))
                    {
                        necessarySettings.Add("User::HasUNIFOCUSData", Int32.Parse(string.Format(datasettings["ETL_HASUNIFOCUSDATA"])));
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=UNIFOCUS;Password={3};",
                            datasettings["ETL_UNIFOCUS_SERVERNAME"],
                            datasettings["ETL_UNIFOCUS_DBUSER"],
                            datasettings["ETL_UNIFOCUS_DATABASENAME"],
                            datasettings["ETL_UNIFOCUS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasUNIFOCUSData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASUNIFOCUSDATA"]));
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING", string.Empty);
                    }

                    //check if property has VENGA data
                    if (datasettings.ContainsKey("ETL_HASVENGADATA"))
                    {
                        necessarySettings.Add("User::HasVengaData", Int32.Parse(string.Format(datasettings["ETL_HASVENGADATA"])));
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_VENGASERVERNAME"],
                            datasettings["ETL_VENGADBUSER"],
                            datasettings["ETL_VENGADATABASENAME"],
                            datasettings["ETL_VENGADBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasVengaData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASVENGADATA"]));
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING", string.Empty);
                    }

                    //check if property has Date of Birth data
                    if (datasettings.ContainsKey("ETL_HASDOB"))
                    {
                        necessarySettings.Add("User::HasDOB", Int32.Parse(string.Format(datasettings["ETL_HASDOB"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasDOB", 0); //Int32.Parse(string.Format(datasettings["ETL_HASDOB"]));
                    }

                    //check if property has A flag ETL_AUTOMATICALLY_MAP_MARKET_SEG set to 1
                    if (datasettings.ContainsKey("ETL_AUTOMATICALLY_MAP_MARKET_SEG"))
                    {
                        necessarySettings.Add("User::AUTOMATICALLY_MAP_MARKET_SEG", Int32.Parse(string.Format(datasettings["ETL_AUTOMATICALLY_MAP_MARKET_SEG"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::AUTOMATICALLY_MAP_MARKET_SEG", 0); //Int32.Parse(string.Format(datasettings["ETL_AUTOMATICALLY_MAP_MARKET_SEG"]));
                    }

                    //check if property has A flag ETL_HASTRANSACTIONSDATA set to 1
                    if (datasettings.ContainsKey("ETL_HASTRANSACTIONSDATA"))
                    {
                        necessarySettings.Add("User::HasTransactionsData", Int32.Parse(string.Format(datasettings["ETL_HASTRANSACTIONSDATA"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasTransactionsData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASTRANSACTIONSDATA"]));
                    }

                    //check if has cenres web forms data
                    if (datasettings.ContainsKey("ETL_HASCENRESWEBFORMS"))
                    {
                        necessarySettings.Add("User::HasCenResWebFormsData", Int32.Parse(string.Format(datasettings["ETL_HASCENRESWEBFORMS"])));
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=WebFomrs;Password={3};",
                            datasettings["ETL_CENRES_WEBFORMS_SERVERNAME"],
                            datasettings["ETL_CENRES_WEBFORMS_DBUSER"],
                            datasettings["ETL_CENRES_WEBFORMS_DATABASENAME"],
                            datasettings["ETL_CENRES_WEBFORMS_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasCenResWebFormsData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASCENRESWEBFORMS"]));
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING", string.Empty);
                    }

                    //check if has cenres omni data
                    if (datasettings.ContainsKey("ETL_HASCENRESOMNI"))
                    {
                        necessarySettings.Add("User::HasCenResOmniData", Int32.Parse(string.Format(datasettings["ETL_HASCENRESOMNI"])));
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=WebFomrs;Password={3};",
                            datasettings["ETL_CENRES_OMNI_SERVERNAME"],
                            datasettings["ETL_CENRES_OMNI_DBUSER"],
                            datasettings["ETL_CENRES_OMNI_DATABASENAME"],
                            datasettings["ETL_CENRES_OMNI_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasCenResOmniData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASCENRESOMNI"]));
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING", string.Empty);
                    }

                    //check if property has CONDO data
                    if (datasettings.ContainsKey("ETL_HASCONDODATA"))
                    {
                        necessarySettings.Add("User::HasCONDOData", Int32.Parse(string.Format(datasettings["ETL_HASCONDODATA"])));
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING",
                         string.Format("Data Source={0};User ID={1};Initial Catalog={2};Provider=SQLNCLI10.1;Auto Translate=False;Application Name=MDS;Password={3};",
                            datasettings["ETL_CONDO_SERVERNAME"],
                            datasettings["ETL_CONDO_DBUSER"],
                            datasettings["ETL_CONDO_DATABASENAME"],
                            datasettings["ETL_CONDO_DBPASSWORD"]));
                    }
                    else
                    {
                        necessarySettings.Add("User::HasCONDOData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASCONDODATA"]));
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING", string.Empty);
                    }

                    //Setting if enalbed then ETL will save the rate type description from CenRes database reservations_image table to eInsight database L_Data_Dictionary table for each property 
                    if (datasettings.ContainsKey("BringRateTypeDescFromCenRes"))
                    {
                        necessarySettings.Add("User::BringRateTypeDescFromCenRes", Int32.Parse(string.Format(datasettings["BringRateTypeDescFromCenRes"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::BringRateTypeDescFromCenRes", 0);
                    }

                    //check if property has sirius ware data
                    if (datasettings.ContainsKey("ETL_HASSIRIUSWAREDATA"))
                    {
                        necessarySettings.Add("User::ETL_HASSiriusWareData", Int32.Parse(string.Format(datasettings["ETL_HASSIRIUSWAREDATA"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::ETL_HASSiriusWareData", 0); //Int32.Parse(string.Format(datasettings["ETL_HASSIRIUSWAREDATA"]));

                    }

                    //check if property has membership data (This is not same as what we have done for Trump)
                    if (datasettings.ContainsKey("ETL_HASMEMBERSHIPSDATA"))
                    {
                        necessarySettings.Add("User::ETL_HASMEMBERSHIPSDATA", Int32.Parse(string.Format(datasettings["ETL_HASMEMBERSHIPSDATA"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::ETL_HASMEMBERSHIPSDATA", 0); //Int32.Parse(string.Format(datasettings["ETL_HASMEMBERSHIPSDATA"]));

                    }

                    //check if property has pms membership data
                    if (datasettings.ContainsKey("HAS_PMSMemberships"))
                    {
                        necessarySettings.Add("User::HAS_PMSMemberships", Int32.Parse(string.Format(datasettings["HAS_PMSMemberships"])));
                    }
                    else
                    {
                        necessarySettings.Add("User::HAS_PMSMemberships", 0); //Int32.Parse(string.Format(datasettings["HAS_PMSMemberships"]));

                    }

                    //necessarySettings.Add("User::HasMemberships", string.Format(datasettings["ETL_HASMEMBERSHIPS"]);

                    //OLD
                    //"Data Source=CENSRV0038;User ID=sadev;Initial Catalog=CenResNew;Provider=SQLNCLI10.1;Persist Security Info=True;Auto Translate=False;Application Name=SSIS-CustomerStay-{EF4C1286-9D99-41B7-AA27-E40838FDD5F6}CENSRV0038.CenResNew.sadev;Password=QWer1234;";
                    //necessarySettings.Add("User::strOLEDBeInsight_SSIS_LogsCONNECTSTRING", "Data Source=CENSRV0038;User ID=sadev;Initial Catalog=eInsight_SSIS_Logs;Provider=SQLNCLI10.1;Persist Security Info=True;Auto Translate=False;Application Name=SSIS-CustomerStay-{EF4C1286-9D99-41B7-AA27-E40838FDD5F6}CENSRV0038.CenResNew.sadev;Password=QWer1234;";

                    //System.Windows.Forms.MessageBox.Show(necessarySettings.Add("User::strOLEDBeInsightCRMCONNECTSTRING"].Value.ToString());
                }
                else
                {
                    if (datasettings.Count == 0)
                    {
                        necessarySettings.Add("User::strOLEDBeInsightCRMCONNECTSTRING",
                        string.Format("Missing settings"));
                        //eInsight ADO.NET CRM
                        necessarySettings.Add("User::strADONETeInsightCRMCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CENRES
                        necessarySettings.Add("User::strOLEDBPMSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //MDS
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //UNIFOCUS
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CONDO
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CLUBESSENTIALS
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //Webforms
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //Omni
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING",
                            string.Format("Missing settings"));
                        //VENGA
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING",
                            string.Format("Missing settings"));


                        //Added by Amit to set servername and databasename variables
                        necessarySettings.Add("User::DatabaseName", string.Format("Missing setting"));
                        necessarySettings.Add("User::ServerName", string.Format("Missing setting"));

                        //check if property has new ETL
                        necessarySettings.Add("User::HasNewETL", string.Format("Missing ETL_HASNEWETL setting"));
                        
                        throw new KeyNotFoundException("Company is missing settings.");
                    }
                    else if ((datasettings["SP_DatabaseName"] == null || datasettings["SP_ServerName"] == null) && datasettings["SP_DBUser"] != null && datasettings["SP_DBPassword"] != null && datasettings["ETL_CENRES_SERVERNAME"] != null && datasettings["ETL_CENRES_DBUSER"] != null && datasettings["ETL_CENRES_DATABASENAME"] != null && datasettings["ETL_CENRES_DBPASSWORD"] != null && datasettings["ETL_SSIS_Logs_SERVERNAME"] != null && datasettings["ETL_SSIS_Logs_DBUSER"] != null && datasettings["ETL_SSIS_Logs_DATABASENAME"] != null && datasettings["ETL_SSIS_Logs_DBPASSWORD"] != null && datasettings["ETL_HASNEWETL"] != null)
                    {
                        necessarySettings.Add("User::DatabaseName", string.Format("Missing SP_DatabaseName setting"));
                        necessarySettings.Add("User::ServerName", string.Format("Missing SP_ServerName setting"));
                        
                        throw new KeyNotFoundException("Company is missing Database Name or Server Name settings.");
                    }
                    else
                    {
                        necessarySettings.Add("User::strOLEDBeInsightCRMCONNECTSTRING",
                        string.Format("Missing settings"));
                        //eInsight ADO.NET CRM
                        necessarySettings.Add("User::strADONETeInsightCRMCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CENRES
                        necessarySettings.Add("User::strOLEDBPMSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //MDS
                        necessarySettings.Add("User::strOLEDBMDSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //UNIFOCUS
                        necessarySettings.Add("User::strOLEDBUNIFOCUSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CONDO
                        necessarySettings.Add("User::strOLEDBCONDOCONNECTSTRING",
                            string.Format("Missing settings"));
                        //CLUBESSENTIALS
                        necessarySettings.Add("User::strOLEDBCLUBESSENTIALSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //Webforms
                        necessarySettings.Add("User::strOLEDBCENRESWEBFORMSCONNECTSTRING",
                            string.Format("Missing settings"));
                        //Omni
                        necessarySettings.Add("User::strOLEDBCENRESOMNICONNECTSTRING",
                            string.Format("Missing settings"));
                        //VENGA
                        necessarySettings.Add("User::strOLEDBVengaCONNECTSTRING",
                            string.Format("Missing settings"));

                        //Added by Amit to set servername and databasename variables
                        necessarySettings.Add("User::DatabaseName", string.Format("Missing setting"));
                        necessarySettings.Add("User::ServerName", string.Format("Missing setting"));

                        //check if property has new ETL
                        necessarySettings.Add("User::HasNewETL", string.Format("Missing ETL_HASNEWETL setting"));
                        
                        throw new KeyNotFoundException("Company is missing settings.");
                    }
                }

                //Customer
                Console.WriteLine("Starting Customer");
                CustomerTask CT = new CustomerTask(necessarySettings);
                CT.Start();
                Console.WriteLine("Customer finished...");
            }
            catch(Exception ex)
            {
                Debug.WriteLine(ex);
                throw ex;
            }



            //Console.WriteLine("Starting ControlFlow example");
            //ControlFlowTasks cft = new ControlFlowTasks();
            //cft.Start();
            //Console.WriteLine("ControlFlow finished...");

            //Console.WriteLine("Start Logging example");
            //Logging log = new Logging();
            //log.Start();
            //Console.WriteLine("Logging finished...");

            //Console.WriteLine("Starting DataFlow example");
            //DataFlowTasks dft = new DataFlowTasks();
            //dft.Preparation();
            //dft.Start();
            //Console.WriteLine("Dafaflow finished...");

            //Console.WriteLine("Press any key to continue...");
            //Console.ReadLine();
        }
    }
}
