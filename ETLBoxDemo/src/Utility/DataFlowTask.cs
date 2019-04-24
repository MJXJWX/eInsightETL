using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using static ALE.ETLBox.ControlFlow.DbTask;

namespace ETLBoxDemo.src.Utility
{
    public class DataFlowTask<T>
    {
        public void runTask(string sConnString, string dConnString, string dTableName, string sql, bool isUpdate = false, bool isInsert = false, List<string> primaryKey = null, List<string> properties = null)
        {
            //Type of Data Flow Task
            ActionType actionType = ActionType.Upsert;
            if(isInsert && isUpdate)
            {
                actionType = ActionType.Upsert;
            }
            else if (isInsert)
            {
                actionType = ActionType.Insert;
            }
            else if (isUpdate)
            {
                actionType = ActionType.Update;
            }
            else
            {
                //return;
            }

            //Properties of transfor
            var columns = new List<TableColumn>();
            if (primaryKey != null && primaryKey.Count > 0)
            {
                primaryKey.ForEach(p => columns.Add(new TableColumn(p, "string", allowNulls: false, isPrimaryKey: true, isIdentity: true)));
            }
            if(properties != null && properties.Count > 0)
            {
                properties.ForEach(p => {
                    if(!primaryKey.Contains(p))
                        columns.Add(new TableColumn(p, "string", allowNulls: false, isPrimaryKey: false, isIdentity: true));
                });
            }
            if(actionType == ActionType.Upsert && (primaryKey == null || primaryKey.Count == 0))
            {
                actionType = ActionType.Insert;
            }

            //Execute Task Flow task
            TableDefinition OrderDataTableDef = new TableDefinition(dTableName, columns);
            DBSource<T> dBSource = new DBSource<T>(OrderDataTableDef) {
                Sql = sql
            };
            RowTransformation<T, T> rowT = new RowTransformation<T, T>(
                input => input
                );
            DBDestination<T> dBDestination = new DBDestination<T>(dTableName, actionType, primaryKey);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(sConnString));
            dBSource.LinkTo(rowT);
            rowT.LinkTo(dBDestination);
            dBSource.Execute();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(dConnString));
            dBDestination.Wait();
        }
    }
}
