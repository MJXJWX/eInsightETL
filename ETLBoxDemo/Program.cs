using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBoxDemo.src.Customer;
using ETLBoxDemo.src.Manager;
using ETLBoxDemo.src.Modules.Customer;
using ETLBoxDemo.src.Utility;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace ALE.ETLBoxDemo {
    class Program {
        static void Main(string[] args) {

            try
            {
                eContactManager.GetCompanySetting(1338);
                string sC = "data source=QHB-CRMDB001.centralservices.local;initial catalog=eInsightCRM_OceanProperties_QA;uid=eInsightCRM_eContact_OceanProperties;pwd=Tv3CxdZwA%9;MultipleActiveResultSets=True";
                string dC = "data source=localhost;initial catalog=eInsightCRM_AMResorts_QA;uid=sa;pwd=123456;MultipleActiveResultSets=True";

                string dT = "dbo.D_Customer";
                string sql = "SELECT TOP 20 CustomerID, FirstName, LastName, Email, PropertyCode, InsertDate, SourceID, AddressStatus, DedupeCheck, AllowEMail, Report_Flag, UNIFOCUS_SCORE FROM dbo.D_Customer with(Nolock);";
                new DataFlowTask<Customer>().runTask(sC, dC, dT, sql);

                //string dT = "dbo.eInsight_L_Languages";
                //string sql = "select ID, Language, Language_en, Globalization from dbo.eInsight_L_Languages with(nolock);";
                //new DataFlowTask<eInsight_L_Languages>().runTask(sC, dC, dT, sql);

                //Customer
                Console.WriteLine("Starting Customer");
                CustomerTask CT = new CustomerTask();
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
