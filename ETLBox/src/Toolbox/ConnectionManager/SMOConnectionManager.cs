﻿using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager for Sql Server Managed Objects (SMO) connection to a sql server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;"));
    /// SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####'; go 2");
    /// </code>
    /// </example>
    public class SMOConnectionManager : IDbConnectionManager, IDisposable {
        public IDbConnectionString ConnectionString { get; set; }
        public bool IsConnectionOpen => SqlConnectionManager.DbConnection?.State == ConnectionState.Open;

        public SMOConnectionManager(ConnectionString connectionString) {
            //RuntimePolicyHelper.SetNET20Compatibilty();
            ConnectionString = connectionString;
            SqlConnectionManager = new SqlConnectionManager(connectionString);
        }

        internal Server Server { get; set; }
        internal ServerConnection Context => Server.ConnectionContext;
        internal SqlConnectionManager SqlConnectionManager { get; set; }
        internal ServerConnection OpenedContext {
            get {
                if (!IsConnectionOpen)
                    Open();
                return Context;
            }
        }

        public void Open() {
            SqlConnectionManager = new SqlConnectionManager((ConnectionString)ConnectionString);
            SqlConnectionManager.Open();
            Server = new Server(new ServerConnection(SqlConnectionManager.DbConnection));
            Context.StatementTimeout = 0;
        }

        public int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null) {
            return OpenedContext.ExecuteNonQuery(command);
        }

        public object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null) {
            return OpenedContext.ExecuteScalar(command);
        }

        public IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null) {
            return OpenedContext.ExecuteReader(command);
        }

        public void BulkInsert(ITableData data, string tableName)
            => SqlConnectionManager.BulkInsert(data, tableName);

        public void BeforeBulkInsert() => SqlConnectionManager.BeforeBulkInsert();

        public void AfterBulkInsert() => SqlConnectionManager.AfterBulkInsert();
 

        private bool disposedValue = false; // To detect redundant calls
        protected void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    Server?.ConnectionContext?.Disconnect();
                    if (SqlConnectionManager != null)
                        SqlConnectionManager.Close();
                    SqlConnectionManager = null;
                    Server = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose() => Dispose(true);
        public void Close() => Dispose();

        public IDbConnectionManager Clone() {
            SMOConnectionManager clone = new SMOConnectionManager((ConnectionString)ConnectionString) { };
            return clone;
        }


    }



}
