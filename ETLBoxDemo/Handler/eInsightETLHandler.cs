using ETLBoxDemo.Common;
using ETLBoxDemo.src.Customer;
using ETLBoxDemo.src.Manager;
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
using static ALE.ETLBoxDemo.DataFlowTasks;

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
            var settings = JsonConvert.DeserializeObject<Dictionary<string, object>>(request);

            if(settings != null && settings.Count > 0)
            {
                foreach (var key in settings.Keys)
                {
                    if (typeof(CompanySettings).GetProperty(key) != null)
                    {
                        typeof(CompanySettings).GetProperty(key).SetValue(key, settings[key] + "");
                    }
                }
            }
            
            try
            {
                string sC = "data source=QHB-CRMDB001.centralservices.local;initial catalog=eInsightCRM_OceanProperties_QA;uid=eInsightCRM_eContact_OceanProperties;pwd=Tv3CxdZwA%9;MultipleActiveResultSets=True";
                string dC = "data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True";

                string dT = "dbo.D_Customer";
                string sql = "SELECT TOP 20 CustomerID, FirstName, LastName, Email, PropertyCode, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer with(Nolock);";

                new DataFlowTask<Customer>().runTask(sC, dC, dT, sql, true, true, new List<string>() { "FirstName", "LastName" }, new List<string>() { "CustomerID", "FirstName", "LastName", "Email", "PropertyCode", "InsertDate", "SourceID", "AddressStatus", "DedupeCheck", "AllowEMail", "Report_Flag", "UNIFOCUS_SCORE" });

                //string dT = "dbo.eInsight_L_Languages";
                //string sql = "select ID, Language, Language_en, Globalization from dbo.eInsight_L_Languages with(nolock);";
                //new DataFlowTask<eInsight_L_Languages>().runTask(sC, dC, dT, sql);

                eContactDBManager.GetCompanySetting(1338);

                //Customer
                Console.WriteLine("Starting Customer");
                CustomerTask CT = new CustomerTask();
                CT.Start();
                Console.WriteLine("Customer finished...");
            }
            catch (Exception ex)
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
