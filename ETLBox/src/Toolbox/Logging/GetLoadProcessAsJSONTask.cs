﻿using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging {
    /// <summary>
    /// Returns the content of the etl.LoadProcess table as JSON.
    /// </summary>
    public class GetLoadProcessAsJSONTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_GETJSON";
        public override string TaskName => $"Get load process list as JSON";

        public override void Execute() {
            //TODO umschreiben in eine Zeile?
            var read = new ReadLoadProcessTableTask() { ReadOption = ReadOptions.ReadAllProcesses};
            read.Execute();
            List<LoadProcess> logEntries = read.AllLoadProcesses;
            JSON = JsonConvert.SerializeObject(logEntries, new JsonSerializerSettings {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                NullValueHandling = NullValueHandling.Ignore
            });

        }
       
        public string JSON { get; private set; }

        public GetLoadProcessAsJSONTask Create() {
            this.Execute();
            return this;
        }

        public GetLoadProcessAsJSONTask() {

        }
      
        public static string GetJSON() => new GetLoadProcessAsJSONTask().Create().JSON;

    }
}
