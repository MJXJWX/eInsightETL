using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBoxDemo.src.Customer;
using ETLBoxDemo.src.Manager;
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
