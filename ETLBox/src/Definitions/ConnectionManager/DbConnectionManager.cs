﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace ALE.ETLBox.ConnectionManager {
    public abstract class DbConnectionManager<Connection, Command> : IDisposable, IDbConnectionManager
        where Connection : class, IDbConnection, new()
        where Command : class, IDbCommand, new() {
        public int MaxLoginAttempts { get; set; } = 3;

        public IDbConnectionString ConnectionString { get; set; }

        internal Connection DbConnection { get; set; }

        //internal bool IsConnectionOpen => DbConnection?.State == ConnectionState.Open;

        public DbConnectionManager() { }

        public DbConnectionManager(IDbConnectionString connectionString) : this() {
            this.ConnectionString = connectionString;
        }

        public void Open() {
            DbConnection?.Close();
            DbConnection = new Connection {
                ConnectionString = ConnectionString.Value
            };
            bool successfullyConnected = false;
            Exception lastException = null;
            for (int i = 1; i <= MaxLoginAttempts; i++) {
                try {
                    DbConnection.Open();
                    successfullyConnected = true;
                } catch (Exception e) {
                    successfullyConnected = false;
                    lastException = e;
                    Task.Delay(1000 * i).Wait();
                }
                if (successfullyConnected) {
                    break;
                }
            }
            if (!successfullyConnected) {
                throw lastException ?? new Exception("Could not connect to database!");
            }
        }

        public Command CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = commandText;
            if (parameterList != null) {
                foreach (QueryParameter par in parameterList) {
                    var newPar = cmd.CreateParameter();
                    newPar.ParameterName = par.Name;
                    newPar.DbType = par.DBType;
                    newPar.Value = par.Value;
                    cmd.Parameters.Add(newPar);
                }
            }
            return cmd as Command;
        }

        public int ExecuteNonQuery(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            Command sqlcmd = CreateCommand(commandText, parameterList);
            return sqlcmd.ExecuteNonQuery();
        }

        public object ExecuteScalar(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            Command cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteScalar();
        }

        public IDataReader ExecuteReader(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            Command cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteReader();

        }

        public abstract void BulkInsert(ITableData data, string tableName);
        public abstract void BeforeBulkInsert();
        public abstract void AfterBulkInsert();

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    if (DbConnection != null) {
                        DbConnection.Close();
                    }

                    DbConnection = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose() {
            Dispose(true);
        }

        public void Close() {
            Dispose();
        }

        public abstract IDbConnectionManager Clone();
        #endregion

    }
}
