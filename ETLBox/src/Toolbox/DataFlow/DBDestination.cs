﻿using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks.Dataflow;
using static ALE.ETLBox.ControlFlow.DbTask;

namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DBDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// DBDestination&lt;MyRow&gt; dest = new DBDestination&lt;MyRow&gt;("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DBDestination<TInput> : GenericTask, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_DBDEST";
        public override string TaskName => $"Dataflow: Write Data batchwise into table {DestinationTableDefinition.Name}";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }

        public ITargetBlock<TInput> TargetBlock => Buffer;

        /* Private stuff */
        int BatchSize { get; set; } = DEFAULT_BATCH_SIZE;
        const int DEFAULT_BATCH_SIZE = 100000;
        internal BatchBlock<TInput> Buffer { get; set; }
        internal ActionBlock<TInput[]> TargetAction { get; set; }
        NLog.Logger NLogger { get; set; }
        TypeInfo TypeInfo { get; set; }

        public string ConnString { get; set; }
        public ActionType ActionType { get; set; }
        public List<string> Keys { get; set; }
        public List<string> UpdateFields { get; set; }

        public DBDestination() {
            InitObjects(DEFAULT_BATCH_SIZE);

        }

        public DBDestination(int batchSize) {
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        public DBDestination(TableDefinition tableDefinition) {
            DestinationTableDefinition = tableDefinition;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        //public DBDestination(string connectionString, string tableName, ActionType actionType = ActionType.Insert, List<string> keys = null) {
        //    ConnString = connectionString;
        //    ActionType = actionType;
        //    TableName = tableName;
        //    Keys = keys;
        //    InitObjects(DEFAULT_BATCH_SIZE);
        //}

        public DBDestination(string tableName)
        {
            TableName = tableName;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        public DBDestination(string tableName, int batchSize) {
            TableName = tableName;
            InitObjects(batchSize);
        }

        public DBDestination(TableDefinition tableDefinition, int batchSize) {
            DestinationTableDefinition = tableDefinition;
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        public DBDestination(string name, TableDefinition tableDefinition, int batchSize) {
            this.TaskName = name;
            DestinationTableDefinition = tableDefinition;
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        private void InitObjects(int batchSize) {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(d));
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
            TypeInfo = new TypeInfo(typeof(TInput));
        }

        private void WriteBatch(TInput[] data) {
            if (!string.IsNullOrEmpty(ConnString))
            {
                this.ConnectionManager = new SqlConnectionManager(new ConnectionString(ConnString));
            }
            if (!HasDestinationTableDefinition) LoadTableDefinitionFromTableName();
            NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            TableData<TInput> td = new TableData<TInput>(DestinationTableDefinition, DEFAULT_BATCH_SIZE);
            td.Rows = ConvertRows(data);
            switch (ActionType)
            {
                case ActionType.Insert:
                    new SqlTask(this, $"Execute Bulk Insert into {DestinationTableDefinition.Name}") {
                        ConnectionManager = string.IsNullOrEmpty(ConnString) ? null : new SqlConnectionManager(new ConnectionString(ConnString))
                    }.BulkInsert(td, DestinationTableDefinition.Name);
                    break;
                case ActionType.Update:
                    new SqlTask(this, $"Execute Bulk Update {DestinationTableDefinition.Name}") {
                        ConnectionManager = string.IsNullOrEmpty(ConnString) ? null : new SqlConnectionManager(new ConnectionString(ConnString))
                    }.BulkUpdate(td, DestinationTableDefinition.Name, Keys, UpdateFields);
                    break;
                case ActionType.Upsert:
                    new SqlTask(this, $"Execute Bulk Upsert {DestinationTableDefinition.Name}") {
                        ConnectionManager = string.IsNullOrEmpty(ConnString) ? null : new SqlConnectionManager(new ConnectionString(ConnString))
                    }.BulkUpsert(td, DestinationTableDefinition.Name, Keys, UpdateFields);
                    break;
                default:
                    new SqlTask(this, $"Execute Bulk Insert into {DestinationTableDefinition.Name}") {
                        ConnectionManager = string.IsNullOrEmpty(ConnString) ? null : new SqlConnectionManager(new ConnectionString(ConnString))
                    }.BulkInsert(td, DestinationTableDefinition.Name);
                    break;
            }
            
            NLogFinish();
        }

        private void LoadTableDefinitionFromTableName() {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(TableName, this.DbConnectionManager);
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }


        private List<object[]> ConvertRows(TInput[] data) {
            List<object[]> result = new List<object[]>();
            foreach (var CurrentRow in data) {
                object[] rowResult;
                if (TypeInfo.IsArray) {
                    rowResult = CurrentRow as object[];
                } else {
                    rowResult = new object[TypeInfo.PropertyLength];
                    int index = 0;
                    foreach (PropertyInfo propInfo in TypeInfo.Properties) {
                        rowResult[index] = propInfo.GetValue(CurrentRow);
                        index++;
                    }
                }
                result.Add(rowResult);
            }
            return result;
        }

        public void Wait() => TargetAction.Completion.Wait();

        void NLogStart() {
            if (!DisableLogging)
                NLogger.Debug(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish() {
            ControlFlow.ControlFlow.STAGE = (int.Parse(ControlFlow.ControlFlow.STAGE) + 1) + "";
            if (!DisableLogging)
                NLogger.Debug(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            ControlFlow.ControlFlow.STAGE = (int.Parse(ControlFlow.ControlFlow.STAGE) - 1) + "";
        }
    }

    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The DBDestination access a string array as input type. If you need other data types, use the generic DBDestination instead.
    /// </summary>
    /// <see cref="DBDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic DBDestination works with string[] as input
    /// //use DBDestination&lt;TInput&gt; for generic usage!
    /// DBDestination dest = new DBDestination("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DBDestination : DBDestination<string[]> {
        public DBDestination() : base() { }

        public DBDestination(int batchSize) : base(batchSize) { }

        public DBDestination(TableDefinition tableDefinition) : base(tableDefinition) { }

        public DBDestination(string tableName) : base(tableName) { }

        public DBDestination(string tableName, int batchSize) : base(tableName, batchSize) { }

        public DBDestination(TableDefinition tableDefinition, int batchSize) : base(tableDefinition, batchSize) { }

        public DBDestination(string name, TableDefinition tableDefinition, int batchSize) : base(name, tableDefinition, batchSize) { }
    }

}
