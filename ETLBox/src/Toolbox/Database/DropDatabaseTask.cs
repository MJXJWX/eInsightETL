namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Tries to drop a database if the database exists.
    /// </summary>
    /// <example>
    /// <code>
    /// DropDatabaseTask.Delete("DemoDB");
    /// </code>
    /// </example>
    public class DropDatabaseTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPDB";
        public override string TaskName => $"Drop DB {DatabaseName}";       
        public override void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
        }
     
        /* Public properties */
        public string DatabaseName { get; set; }
        public string Sql
        {
            get
            {
                return
    $@"
if (db_id('{DatabaseName}') is not null)
begin
    use [master]
  --Delete Database  
  alter database [{DatabaseName}]
  set single_user with rollback immediate
  alter database [{DatabaseName}]
  set multi_user
  drop database [{DatabaseName}]  
end
";
            }
        }

        /* Some constructors */
        public DropDatabaseTask() {
        }

        public DropDatabaseTask(string databaseName) : this()
        {
            DatabaseName = databaseName;
        }       
       

        /* Static methods for convenience */
        public static void Drop(string databaseName) => new DropDatabaseTask(databaseName).Execute();

        /* Implementation & stuff */
       
       
    }


}
