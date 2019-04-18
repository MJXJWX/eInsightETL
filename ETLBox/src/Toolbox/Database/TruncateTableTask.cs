﻿namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Truncates a table.
    /// </summary>
    /// <example>
    /// <code>
    /// TruncateTableTask.Truncate("demo.table1");
    /// </code>
    /// </example>
    public class TruncateTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "TRUNCATE";
        public override string TaskName => $"Truncate table {TableName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string TableName { get; set; }
        public string Sql => $@"truncate table {TableName}";

        public TruncateTableTask()
        {

        }
        public TruncateTableTask(string tableName) : this()
        {
            this.TableName = tableName;
        }

        public static void Truncate(string tableName) => new TruncateTableTask(tableName).Execute();


    }
}
