﻿using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System;
using System.Linq;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks. 
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    public static class ControlFlow
    {
        /// <summary>
        /// For logging purposes only. If the stage is set, you can access the stage value in the logging configuration.
        /// </summary>
        public static string STAGE { get; set; }

        private static IDbConnectionManager _currentDbConnection;
        /// <summary>
        /// You can store your general database connection string here. This connection will then used by all Tasks where no DB connection is excplicitly set. 
        /// </summary>
        public static IDbConnectionManager CurrentDbConnection
        {
            get => _currentDbConnection;
            set
            {
                _currentDbConnection = value;
                if (value != null)
                {
                    SetLoggingDatabase(value);
                }
            }
        }

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the logging tables with CreateLogTablesTask) then this Property will hold the current load process information.
        /// </summary>
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        /// <summary>
        /// If set to true, nothing will be logged by any task. When switched back to false, task will continue to log.
        /// </summary>
        public static bool DisableAllLogging { get; set; }
        static ControlFlow()
        {
            NLog.Config.ConfigurationItemFactory.Default.LayoutRenderers.RegisterDefinition("etllog", typeof(ETLLogLayoutRenderer));
        }

   
        /// <summary>
        /// By default, the logging database is derived from the CurrentDBConnection property. If you need to manually change the logging database, you can change it with this method
        /// </summary>
        /// <param name="connection">The new logging database connection</param>
        public static void SetLoggingDatabase(IConnectionManager connection)
        {
            var dbTarget = LogManager.Configuration?.ConfiguredNamedTargets?.Where(t => t.GetType() == typeof(NLog.Targets.DatabaseTarget)).FirstOrDefault() as NLog.Targets.DatabaseTarget;
            if (dbTarget != null)
            {
                dbTarget.ConnectionString = connection.ConnectionString.Value; //?? CurrentDbConnection.ConnectionString.Value; //""; Parameter.DWHConnection?.Value;
            }
        }

        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            CurrentDbConnection = null;
            CurrentLoadProcess = null;
            DisableAllLogging = false;
        }

    }



}
