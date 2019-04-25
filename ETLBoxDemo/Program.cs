using ETLBoxDemo;
using System;
using Topshelf;

namespace ALE.ETLBoxDemo
{
    class Program {
        static void Main(string[] args) {
            var rc = HostFactory.Run(x =>
            {
                x.Service<eInsightETLService>(s =>
                {
                    s.ConstructUsing(name => new eInsightETLService());
                    s.WhenStarted(tefs => tefs.Start());
                    s.WhenStopped(ters => ters.Stop());
                });
                x.RunAsLocalSystem();

                x.SetDescription("eInight ETL Service");
                x.SetDisplayName("eInight ETL Service");
                x.SetServiceName("eInightETLService");
                x.UseNLog();
                // Set the Service Recovery Options
                // From http://appetere.com/post/topshelf-enableservicerecovery-configuration
                //
                x.EnableServiceRecovery(svc =>
                {
                    svc.OnCrashOnly();
                    // First Failure (restart immediately)
                    svc.RestartService(delayInMinutes: 0);
                    // Second Failure (restart immediately)
                    svc.RestartService(delayInMinutes: 0);
                    // Subsequent Failures (restart after 1 min)
                    svc.RestartService(delayInMinutes: 1);
                    // Reset counters each day
                    svc.SetResetPeriod(days: 1);
                });
            });

            var exitCode = (int)Convert.ChangeType(rc, rc.GetTypeCode());
            Environment.ExitCode = exitCode; 
        }
    }
}
