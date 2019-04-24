﻿using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ALE.ETLBox.ControlFlow
{
    public abstract class DbTask : GenericTask
    {

        /* Public Properties */
        public string Sql { get; set; }
        public FileConnectionManager FileConnection { get; set; }
        public List<Action<object>> Actions { get; set; }
        public Action BeforeRowReadAction { get; set; }
        public Action AfterRowReadAction { get; set; }
        Action InternalBeforeRowReadAction { get; set; }
        Action InternalAfterRowReadAction { get; set; }
        public long ReadTopX { get; set; } = long.MaxValue;
        public int? RowsAffected { get; private set; }
        public bool IsOdbcConnection => DbConnectionManager.GetType() == typeof(OdbcConnectionManager)
            || DbConnectionManager.GetType() == typeof(AccessOdbcConnectionManager);

        public bool DisableExtension { get; set; }
        public string Command
        {
            get
            {
                if (HasSql)
                    return HasName && !IsOdbcConnection ? NameAsComment + Sql : Sql;
                else if (HasFileConnection)
                {
                    if (FileConnection.FileExists)
                        return HasName ? NameAsComment + FileConnection.ReadContent() : FileConnection.ReadContent();
                    else
                    {
                        NLogger.Warn($"Sql file was not found: {FileConnection.FileName}", TaskType, "RUN", TaskHash, ControlFlow.STAGE);
                        return $"SELECT 'File {FileConnection.FileName} not found'";
                    }
                }
                else
                    throw new Exception("Empty command");
            }
        }

        /* Internal/Private properties */
        internal bool DoSkipSql { get; private set; }
        NLog.Logger NLogger { get; set; }
        bool HasSql => !(String.IsNullOrWhiteSpace(Sql));
        bool HasFileConnection => FileConnection != null;
        internal IEnumerable<QueryParameter> _Parameter { get; set; }

        /* Some constructors */
        public DbTask()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public DbTask(string name) : this()
        {
            this.TaskName = name;
        }

        public DbTask(string name, string sql) : this(name)
        {
            this.Sql = sql;
        }

        public DbTask(ITask callingTask, string sql) : this()
        {
            TaskName = callingTask.TaskName;
            TaskHash = callingTask.TaskHash;
            ConnectionManager = callingTask.ConnectionManager;
            TaskType = callingTask.TaskType;
            DisableLogging = callingTask.DisableLogging;
            this.Sql = sql;
        }

        public DbTask(string name, string sql, params Action<object>[] actions) : this(name, sql)
        {
            Actions = actions.ToList();
        }

        public DbTask(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : this(name, sql)
        {
            BeforeRowReadAction = beforeRowReadAction;
            AfterRowReadAction = afterRowReadAction;
            Actions = actions.ToList();
        }

        public DbTask(string name, FileConnectionManager fileConnection) : this(name)
        {
            this.FileConnection = fileConnection;
        }

        /* Public methods */
        public int ExecuteNonQuery()
        {
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart();
                RowsAffected = DoSkipSql ? 0 : conn.ExecuteNonQuery(Command, _Parameter);//DbConnectionManager.ExecuteNonQuery(Command);
                QueryFinish(LogType.Rows);
            }
            return RowsAffected ?? 0;
        }

        public object ExecuteScalar()
        {
            object result = null;
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart();
                result = conn.ExecuteScalar(Command, _Parameter);
                QueryFinish();
            }
            return result;
        }

        public Nullable<T> ExecuteScalar<T>() where T : struct
        {
            object result = ExecuteScalar();
            if (result == null || result == DBNull.Value)
                return null;
            else
                return ((T)result);
        }


        public bool ExecuteScalarAsBool()
        {
            int? result = ExecuteScalar<int>();
            return IntToBool(result);
        }

        public void ExecuteReader()
        {
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart();
                IDataReader reader = conn.ExecuteReader(Command, _Parameter) as IDataReader;
                //for (int rowNr = 0; rowNr < ReadTopX; rowNr++)
                //{
                    while (reader.Read())
                    {
                        InternalBeforeRowReadAction?.Invoke();
                        BeforeRowReadAction?.Invoke();
                        for (int i = 0; i < Actions?.Count; i++)
                        {
                            if (!reader.IsDBNull(i))
                            {
                                //string value = reader.GetValue(i).ToString();
                                //for (int j = 0; j < reader.FieldCount; j++)
                                //{
                                //    if (String.IsNullOrEmpty(value))
                                //    {
                                //        value = reader.GetValue(j).ToString();
                                //    }
                                //    else
                                //    {
                                //        value += "," + reader.GetValue(j).ToString();
                                //    }

                                //}

                                Actions?[i]?.Invoke(reader);
                            }
                            else
                            {
                                Actions?[i]?.Invoke(null);
                            }
                        }
                        AfterRowReadAction?.Invoke();
                        InternalAfterRowReadAction?.Invoke();
                    }
                    //else
                    //{
                    //    break;
                    //}
                //}
                reader.Close();
                QueryFinish();
            }
        }

        public static object GetValueFromReader(object reader, string name)
        {
            object result = null;
            try
            {
                result = ((IDataReader)reader)[name];
            }
            catch
            {
                result = null;
            }
            return result;
        }

        internal List<T> Query<T>(Action<T> doWithRowAction = null, List<string> columnNames = null)
        {
            List<T> result = null;
            PrepareQuery();
            T row = default(T);
            TypeInfo typeInfo = new TypeInfo(typeof(T));

            if (typeInfo.IsArray)
            {
                row = (T)Activator.CreateInstance(typeof(T), new object[] { columnNames.Count }); // Length 1
                var ar = row as System.Array;
                int index = 0;
                foreach (var colName in columnNames)
                {
                    int currentIndexNeededToAvoidClosureInAction = index;
                    Actions.Add(col =>
                    {
                        var x = Convert.ChangeType(col, typeof(T).GetElementType());
                        ar.SetValue(x, currentIndexNeededToAvoidClosureInAction);
                    });
                    index++;
                }
            }
            else
            {
                if (columnNames == null) columnNames = typeInfo.PropertyNames;
                foreach (var colName in columnNames)
                {
                    if (typeInfo.HasProperty(colName))
                        Actions.Add(colValue => typeInfo.GetProperty(colName).SetValue(row, GetValueFromReader(colValue, colName) + ""));
                    else
                        Actions.Add(col => { });
                }
                InternalBeforeRowReadAction = () => row = (T)Activator.CreateInstance(typeof(T));
            }
            InternalAfterRowReadAction = () => doWithRowAction(row);
            ExecuteReader();
            CleanupQuery();
            return result;
        }

        private void PrepareQuery()
        {
            Actions = new List<Action<object>>();
        }

        private void CleanupQuery()
        {
            Actions = null;
        }

        public void BulkInsert(ITableData data, string tableName)
        {
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart(LogType.Bulk);
                conn.BeforeBulkInsert();
                conn.BulkInsert(data, tableName);
                conn.AfterBulkInsert();
                RowsAffected = data.RecordsAffected;
                QueryFinish(LogType.Bulk);
            }
        }

        public void BulkUpdate(ITableData data, string tableName)
        {
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart(LogType.Bulk);
                conn.BeforeBulkUpdate();
                conn.BulkUpdate(data, tableName);
                conn.AfterBulkUpdate();
                RowsAffected = data.RecordsAffected;
                QueryFinish(LogType.Bulk);
            }
        }

        public void BulkUpsert(ITableData data, string tableName, List<string> keys)
        {
            using (var conn = DbConnectionManager.Clone())
            {
                conn.Open();
                QueryStart(LogType.Bulk);
                conn.BeforeBulkUpsert();
                conn.BulkUpsert(data, tableName, keys);
                conn.AfterBulkUpsert();
                RowsAffected = data.RecordsAffected;
                QueryFinish(LogType.Bulk);
            }
        }


        public enum ActionType
        {
            Update,
            Insert,
            Upsert
        }

        /* Private implementation & stuff */
        enum LogType
        {
            None,
            Rows,
            Bulk
        }

        static bool IntToBool(int? result)
        {
            if (result != null && result > 0)
                return true;
            else
                return false;
        }

        void QueryStart(LogType logType = LogType.None)
        {
            if (!DisableLogging)
                LoggingStart(logType);

            if (!DisableExtension)
                ExecuteExtension();
        }

        void QueryFinish(LogType logType = LogType.None)
        {
            if (!DisableLogging)
                LoggingEnd(logType);
        }

        void LoggingStart(LogType logType)
        {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (logType == LogType.Bulk)
                NLogger.Debug($"SQL Bulk Insert", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            else
                NLogger.Debug($"{Command}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void LoggingEnd(LogType logType)
        {
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (logType == LogType.Rows)
                NLogger.Debug($"Rows affected: {RowsAffected ?? 0}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void ExecuteExtension()
        {
            if (ExtensionFileLoader.ExistsFolder && HasName)
            {
                List<ExtensionFile> extFiles = ExtensionFileLoader.GetExtensionFiles(TaskHash);

                if (extFiles.Count > 0)
                {
                    foreach (var extFile in extFiles)
                    {
                        new SqlTask($"Extensions: {extFile.Name}", new FileConnectionManager(extFile.FileName))
                        {
                            ConnectionManager = this.ConnectionManager,
                            DisableExtension = true
                        }.ExecuteNonQuery();
                    }
                    DoSkipSql = extFiles.Any(ef => ef.HasSkipNextStatement);
                }
            }
        }


    }


}
