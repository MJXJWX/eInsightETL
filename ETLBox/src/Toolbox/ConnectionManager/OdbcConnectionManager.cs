﻿using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager for an ODBC connection based on ADO.NET. ODBC can be used to connect to any ODBC able endpoint.
    /// ODBC by default does not support a Bulk Insert - inserting big amounts of data is translated into a
    /// <code>
    /// insert into (...) values (..),(..),(..) statementes.
    /// </code>
    /// Be careful with the batch size - some databases have limitations regarding the length of sql statements. 
    /// Reduce the batch if encounter issues here.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = 
    ///   new OdbcConnectionManager(new ObdcConnectionString(
    ///     "Driver={SQL Server};Server=.;Database=ETLBox;Trusted_Connection=Yes;"));
    /// </code>
    /// </example>
    public class OdbcConnectionManager : DbConnectionManager<OdbcConnection, OdbcCommand> {
        public OdbcConnectionManager() : base() { }

        public OdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(ITableData data, string tableName) {
            OdbcBulkInsertString bulkInsert = new OdbcBulkInsertString() { };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

        public override IDbConnectionManager Clone() {
            OdbcConnectionManager clone = new OdbcConnectionManager((OdbcConnectionString)ConnectionString) {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }


    }
}
