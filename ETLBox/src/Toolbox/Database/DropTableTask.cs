namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class DropTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPTABLE";
        public override string TaskName => $"Drop Table {TableName}";       
        public override void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
        }
     
        /* Public properties */
        public string TableName { get; set; }
        public string Sql
        {
            get
            {
                return
    $@"
if object_id('{TableName}', 'U') is not null
  drop table {TableName} 
";
            }
        }

        /* Some constructors */
        public DropTableTask() {
        }

        public DropTableTask(string tableName) : this()
        {
            TableName = tableName;
        }       
       

        /* Static methods for convenience */
        public static void Drop(string tableName) => new DropTableTask(tableName).Execute();
       
       
    }


}
