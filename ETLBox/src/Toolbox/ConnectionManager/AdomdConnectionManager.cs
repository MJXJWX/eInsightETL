using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager for Adomd connection to a sql server analysis server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new AdmoConnectionManager(new ConnectionString("..connection string.."));
    /// </code>
    /// </example>
    public class AdomdConnectionManager : DbConnectionManager<AdomdConnection, AdomdCommand> {

        public AdomdConnectionManager() : base() { }

        public AdomdConnectionManager(ConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(ITableData data, string tableName) {
            throw new NotImplementedException();
        }
    
        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

        public override IDbConnectionManager Clone() {
            AdomdConnectionManager clone = new AdomdConnectionManager((ConnectionString)ConnectionString) {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;

        }

        public override void BeforeBulkUpdate()
        {
            throw new NotImplementedException();
        }

        public override void BulkUpdate(ITableData data, string tableName, List<string> keys, List<string> updateFields)
        {
            throw new NotImplementedException();
        }

        public override void AfterBulkUpdate()
        {
            throw new NotImplementedException();
        }

        public override void BeforeBulkUpsert()
        {
            throw new NotImplementedException();
        }

        public override void BulkUpsert(ITableData data, string tableName, List<string> keys, List<string> updateFields)
        {
            throw new NotImplementedException();
        }

        public override void AfterBulkUpsert()
        {
            throw new NotImplementedException();
        }
    }
}
