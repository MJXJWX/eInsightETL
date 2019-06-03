using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using ETLBoxDemo.src.Manager;
using ETLBoxDemo.src.Modules.Customer;
using ETLBoxDemo.src.Tasks;
using ETLBoxDemo.src.Utility;
using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Handlers;
using Rebus.Retry.Simple;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace ETLBoxDemo.Handler
{
    class eInsightETLHandler : IHandleMessages<string>, IHandleMessages<IFailed<string>>
    {
        private IBus Bus;

        public eInsightETLHandler(IBus bus)
        {
            this.Bus = bus;
        }

        public async Task Handle(IFailed<string> message)
        {
            await this.Bus.Advanced.Routing.Send(
                "ErrorThrownQueue",
                message.ErrorDescription
            );
        }

        public async Task Handle(string request)
        {
            //string sC = "data source=QHB-CRMDB001.centralservices.local;initial catalog=eInsightCRM_OceanProperties_QA;uid=eInsightCRM_eContact_OceanProperties;pwd=Tv3CxdZwA%9;MultipleActiveResultSets=True";
            //string dC = "data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True";

            //string dT = "dbo.D_Customer";
            //string sql = "SELECT TOP 20 CustomerID, FirstName, LastName, Email, PropertyCode, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer with(Nolock);";
            //string sql1 = "SELECT TOP 20 CustomerID, FirstName, LastName, Email, PropertyCode, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer_03092017 with(Nolock);";
            //List<Dictionary<string,string>> SQL_GetDataFromPMS_SpecialRequestsList = SQLHelper.GetDbValues(sC, "SQL_GetDataFromPMS_SpecialRequests", sql, null);
            //new DataFlowTask<D_Customer>().runTask(sC, dC, dT, sql, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });
           // new DataFlowTask<D_Customer>().runTask1<D_Customer, D_Customer, D_Customer>(sC, dC, dT, sql, sql1, true, true, new List<string>() { "FirstName"}, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });
           
            try
            {
                //ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True"));
                //CreateLogTablesTask.CreateLog();
                //StartLoadProcessTask.Start("Process 1", "Start Message 1", "ETL");
                var settings = JsonConvert.DeserializeObject<Dictionary<string, object>>(request);
                ControlFlow.STAGE = "0";
                eContactDBManager.GetCompanySetting(settings);
                ControlFlow.SetLoggingDatabase(new SqlConnectionManager(new ConnectionString("data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True")));

                //Customer 
                CustomerTask CT = new CustomerTask();
                CT.Start();
            }
            catch (Exception ex)
            {
                ControlFlow.STAGE = "0";
                new LogTask() { Message = $"Exception({ex.Message}) Occurred. Task Stopped!", ActionType = "Stop"}.Fatal();
                NLog.LogManager.GetCurrentClassLogger().Fatal(ex);
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
