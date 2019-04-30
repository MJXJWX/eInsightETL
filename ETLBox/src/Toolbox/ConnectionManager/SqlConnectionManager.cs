using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager of a classic ADO.NET connection to a sql server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;"));
    /// </code>
    /// </example>
    public class SqlConnectionManager : DbConnectionManager<SqlConnection, SqlCommand> {

        public SqlConnectionManager() : base() { }

        public SqlConnectionManager(ConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(ITableData data, string tableName) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, null)) {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in data.ColumnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override void BulkUpdate(ITableData data, string tableName, List<string> keys, List<string> updateFields)
        {
            using (var conn = DbConnection)
            {
                if (conn.State == ConnectionState.Closed)
                    conn.Open();
                StringBuilder columns = new StringBuilder();
                List<string> cols = new List<string>();
                foreach (IColumnMapping colMap in data.ColumnMapping)
                {
                    if (columns.Length > 0)
                        columns.Append(", ");
                    columns.Append($"{colMap.SourceColumn} nvarchar(50)");
                    cols.Add(colMap.SourceColumn);
                }
                var createTempTableCommand = $"Create Table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} ({columns.ToString()})";

                //Execute the command to make a temp table
                SqlCommand cmd = new SqlCommand(createTempTableCommand, conn);
                cmd.ExecuteNonQuery();

                //BulkCopy the data in the DataTable to the temp table
                BulkInsert(data, $"#temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)}");

                //use the merge command to upsert from the temp table to the destination table
                string mergeSql = $"merge into {tableName} as Target  using #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} as Source  on  {string.Join(" AND ", keys.ConvertAll(k => k = $"Target.{k} collate SQL_Latin1_General_CP1_CI_AS = Source.{k} collate SQL_Latin1_General_CP1_CI_AS"))} when matched then update set {string.Join(" , ", updateFields.ConvertAll(c => c = $"Target.{c} = Source.{c}"))};";

                cmd.CommandText = mergeSql;
                cmd.ExecuteNonQuery();

                //Clean up the temp table
                cmd.CommandText = $"drop table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)};";
                cmd.ExecuteNonQuery();

            }
        }

        public override void BulkUpsert(ITableData data, string tableName, List<string> keys, List<string> updateFields)
        {
            using(var conn = DbConnection)
            {
                if (conn.State == ConnectionState.Closed)
                    conn.Open();
                StringBuilder columns = new StringBuilder();
                List<string> cols = new List<string>();
                foreach (IColumnMapping colMap in data.ColumnMapping)
                {
                    if (columns.Length > 0)
                        columns.Append(", ");
                    columns.Append($"{colMap.SourceColumn} nvarchar(50)");
                    cols.Add(colMap.SourceColumn);
                }
                var createTempTableCommand = $"Create Table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} ({columns.ToString()})";

                //Execute the command to make a temp table
                SqlCommand cmd = new SqlCommand(createTempTableCommand, conn);
                cmd.ExecuteNonQuery();

                //BulkCopy the data in the DataTable to the temp table
                BulkInsert(data, $"#temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)}");

                //use the merge command to upsert from the temp table to the destination table
                string mergeSql = $"merge into {tableName} as Target  using #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)} as Source  on  {string.Join(" AND ", keys.ConvertAll(k => k = $"Target.{k} collate SQL_Latin1_General_CP1_CI_AS = Source.{k} collate SQL_Latin1_General_CP1_CI_AS"))} when matched then update set {string.Join(" , ", updateFields.ConvertAll(c => c = $"Target.{c} = Source.{c}"))} when not matched then insert ({string.Join(",", cols)}) values (Source.{string.Join(", Source.", cols)});";

                cmd.CommandText = mergeSql;
                cmd.ExecuteNonQuery();

                //Clean up the temp table
                cmd.CommandText = $"drop table #temp_{tableName.Replace("dbo.", "", StringComparison.CurrentCultureIgnoreCase)};";
                cmd.ExecuteNonQuery();

            }
        }

        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

        public override void BeforeBulkUpdate() { }
        public override void AfterBulkUpdate() { }

        public override void BeforeBulkUpsert() { }
        public override void AfterBulkUpsert() { }

        public override IDbConnectionManager Clone() {
            SqlConnectionManager clone = new SqlConnectionManager((ConnectionString)ConnectionString) {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
