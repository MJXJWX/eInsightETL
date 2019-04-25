using Rebus.Bus;
using Rebus.Handlers;
using Rebus.Retry.Simple;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ETLBoxDemo.Handler
{
    class eInsightETLErrorHandler : IHandleMessages<IFailed<Exception>>
    {
        private IBus Bus;

        public eInsightETLErrorHandler(IBus bus)
        {
            this.Bus = bus;
        }

        public async Task Handle(IFailed<Exception> message)
        {
            await this.Bus.Advanced.Routing.Send(
                "ErrorThrownQueue",
                message.ErrorDescription
            );
        }
    }
}
