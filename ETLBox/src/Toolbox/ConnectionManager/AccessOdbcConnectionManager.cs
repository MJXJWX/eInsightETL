﻿using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager for an ODBC connection to Acccess databases.
    /// This connection manager also is based on ADO.NET. 
    /// ODBC by default does not support a Bulk Insert - and Access does not supoport the insert into (...) values (...),(...),(...) 
    /// syntax. So the following syntax is used
    /// <code>
    /// insert into (Col1, Col2,...) 
    /// select * from (
    ///   select 'Val1' as Col1 from dummytable
    ///   union all
    ///   select 'Val2' as Col2 from dummytable
    ///   ...
    /// ) a;
    /// </code>
    /// 
    /// The dummytable is a special helper table containing only one record. 
    /// 
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = 
    ///   new AccessOdbcConnectionManager(new OdbcConnectionString(
    ///      "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\DB\Test.mdb"));
    /// </code>
    /// </example>
    public class AccessOdbcConnectionManager : DbConnectionManager<OdbcConnection, OdbcCommand> {

        public AccessOdbcConnectionManager() : base() { }

        public AccessOdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }

        /// <summary>
        /// Helper table that needs to be created in order to simulate bulk inserts. 
        /// Contains only 1 record and is only temporarily created.
        /// </summary>
        public string DummyTableName { get; set; } = "etlboxdummydeleteme";

        public bool AlwaysUseSameConnection { get; set; }

        public override void BulkInsert(ITableData data, string tableName) {
            OdbcBulkInsertString bulkInsert = new OdbcBulkInsertString() {
                IsAccessDatabase = true,
                AccessDummyTableName = DummyTableName
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override void BeforeBulkInsert() {
            TryDropDummyTable();
            CreateDummyTable();
        }
        public override void AfterBulkInsert() {
            TryDropDummyTable();
        }

        private void TryDropDummyTable() {
            try {
                ExecuteCommandOnSameConnection($@"DROP TABLE {DummyTableName};");
            } catch { }
        }

        private void CreateDummyTable() {
            ExecuteCommandOnSameConnection($@"CREATE TABLE {DummyTableName} (Field1 NUMBER);");
            ExecuteCommandOnSameConnection($@"INSERT INTO { DummyTableName} VALUES(1);");
        }

        private void ExecuteCommandOnSameConnection(string sql) {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override IDbConnectionManager Clone() {
            if (AlwaysUseSameConnection)
                return this;
            else {
                AccessOdbcConnectionManager clone = new AccessOdbcConnectionManager((OdbcConnectionString)ConnectionString) {
                    MaxLoginAttempts = this.MaxLoginAttempts
                };
                return clone;
            }
        }


    }
}
