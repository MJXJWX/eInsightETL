using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ETLBoxDemo.src.Utility
{
    public class DataFlowTask<T>
    {
        public void runTask(string sConnString, string dConnString, string dTableName, string sql)
        {
            DBSource<T> dBSource = new DBSource<T>(sql);
            RowTransformation<T, T> rowT = new RowTransformation<T, T>(
                input => input
                );
            DBDestination<T> dBDestination = new DBDestination<T>(dTableName);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(sConnString));
            dBSource.LinkTo(rowT);
            rowT.LinkTo(dBDestination);
            dBSource.Execute();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(dConnString));
            dBDestination.Wait();
        }
    }
}
