using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.eInsightETL_Driver
{
    public class eInsightETL_DriverTask
    {
        private Dictionary<string, object> dictionarySettings;
        public eInsightETL_DriverTask(Dictionary<string, object> dictionarySettings)
        {
            this.dictionarySettings = dictionarySettings;
        }
        public void start()
        {
            string ConnString = System.Configuration.ConfigurationManager.AppSettings["ConnectionString"];
        }
    }
}