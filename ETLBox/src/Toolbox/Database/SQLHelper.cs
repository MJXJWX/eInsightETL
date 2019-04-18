using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBox.src.Toolbox.Database
{
    public class SQLHelper
    {
        public static List<string> GetDbValues(String strConnectionString, String taskName, String strSql, List<QueryParameter> parameter)
        {
            List<string> DatabaseValues = new List<string>();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            if (parameter == null)
            {
                new SqlTask(taskName, strSql)
                {
                    Actions = new List<Action<object>>() {
                    n => DatabaseValues.Add((string)n)
                }
                }.ExecuteReader();
            }
            else
            {
                new SqlTask(taskName, strSql, parameter)
                {
                    Actions = new List<Action<object>>() {
                    n => DatabaseValues.Add((string)n)
                }
                }.ExecuteReader();
            }
           
            return DatabaseValues;
        }

        public static void InsertOrUpdateDbValue(String strConnectionString, String taskName, String strSql, List<QueryParameter> parameter)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            new SqlTask(taskName, strSql, parameter).ExecuteNonQuery();
        }

        public static void TruncateTable(String strConnectionString, String taskName, String strSql)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            new SqlTask(taskName, strSql).ExecuteNonQuery();
        }
        public static void TruncateTable(String strConnectionString, String taskName, String strSql1, String strSql2, String strSql3, String strSql4)
        {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(strConnectionString));
            if (!String.IsNullOrEmpty(strSql1))
            {
                new SqlTask(taskName, strSql1).ExecuteNonQuery();
            }
            if (!String.IsNullOrEmpty(strSql2))
            {
                new SqlTask(taskName, strSql2).ExecuteNonQuery();
            }
            if (!String.IsNullOrEmpty(strSql3))
            {
                new SqlTask(taskName, strSql3).ExecuteNonQuery();
            }
            if (!String.IsNullOrEmpty(strSql4))
            {
                new SqlTask(taskName, strSql4).ExecuteNonQuery();
            }
        }
    }
}