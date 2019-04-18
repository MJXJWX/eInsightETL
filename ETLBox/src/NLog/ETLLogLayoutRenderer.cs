﻿using NLog;
using NLog.Config;
using NLog.LayoutRenderers;
using System.Text;

namespace ALE.ETLBox.Logging {
    [LayoutRenderer("etllog")]
    public class ETLLogLayoutRenderer : LayoutRenderer {
        [DefaultParameter]
        public string LogType { get; set; } = "message";

        protected override void Append(StringBuilder builder, LogEventInfo logEvent) {
            if (LogType?.ToLower() == "message")
                builder.Append(logEvent.Message);
            else if (LogType?.ToLower() == "type" && logEvent?.Parameters?.Length >= 1)
                builder.Append(logEvent.Parameters[0]);
            else if (LogType?.ToLower() == "action" && logEvent?.Parameters?.Length >= 2)
                builder.Append(logEvent.Parameters[1]);
            else if (LogType?.ToLower() == "hash" && logEvent?.Parameters?.Length >= 3)
                builder.Append(logEvent.Parameters[2]);
            else if (LogType?.ToLower() == "stage" && logEvent?.Parameters?.Length >= 4)
                builder.Append(logEvent.Parameters[3]);
            else if (LogType?.ToLower() == "loadprocesskey" && logEvent?.Parameters?.Length >= 5) 
                builder.Append(logEvent.Parameters[4]);
        }

    }
}
