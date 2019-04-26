using ETLBoxDemo.Common;
using ETLBoxDemo.src.Customer;
using ETLBoxDemo.src.Manager;
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
            var settings = JsonConvert.DeserializeObject<Dictionary<string, object>>(request);
            
            try
            {
                eContactDBManager.GetCompanySetting(settings);

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
