using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBox.src.Toolbox.Database
{
    public class SQLHelper
    {
        public static List<Dictionary<string, string>> GetDbValues(String strConnectionString, String taskName, String strSql, List<QueryParameter> parameter)
        {
            List<Dictionary<string, string>> result = new List<Dictionary<string, string>>();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            if (parameter == null)
            {
                new SqlTask(taskName, strSql)
                {
                    Actions = new List<Action<object>>() {
                    n => {
                        var temp = new Dictionary<string,string>();
                        var reader = (IDataReader)n;
                        for (int i = 0; i < reader.FieldCount; i ++ )
                        {
                            temp.Add(reader.GetName(i), reader.GetValue(i).ToString());
                        }
                        result.Add(temp);
                     }
                }
                }.ExecuteReader();
            }
            else
            {
                new SqlTask(taskName, strSql, parameter)
                {
                    Actions = new List<Action<object>>() {
                    n => {
                        var temp = new Dictionary<string,string>();
                        var reader = (IDataReader)n;
                        for (int i = 0; i < reader.FieldCount; i ++ )
                        {
                            temp.Add(reader.GetName(i), reader.GetValue(i).ToString());
                        }
                         result.Add(temp);
                    }
                }
                }.ExecuteReader();
            }

            return result;
        }

        public static void InsertOrUpdateDbValue(String strConnectionString, String taskName, String strSql, List<QueryParameter> parameter)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            new SqlTask(taskName, strSql, parameter).ExecuteNonQuery();
        }
        
        public static void TruncateTable(String strConnectionString, String taskName, params string[] tableNames)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            StringBuilder sql = new StringBuilder();
            foreach(var tableName in tableNames)
            {
                if (!string.IsNullOrEmpty(tableName))
                {
                    sql.Append($"Truncate Table {tableName}; ");
                }
            }
            if (!String.IsNullOrEmpty(sql.ToString()))
            {
                new SqlTask(taskName, sql.ToString()).ExecuteNonQuery();
            }
        }

        public static void DeleteTableValue(String strConnectionString, String taskName, String strSql, List<QueryParameter> parameter)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            if (parameter != null)
            {
                new SqlTask(taskName, strSql, parameter).ExecuteNonQuery();
            }
            else
            {
                new SqlTask(taskName, strSql).ExecuteNonQuery();
            }
        }

    }
}