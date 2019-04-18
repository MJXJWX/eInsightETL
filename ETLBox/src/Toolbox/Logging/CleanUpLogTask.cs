using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.Logging {
    /// <summary>
    /// Removes log data older than the specified days to keep. 
    /// </summary>
    public class CleanUpLogTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CLEANUPLOG";
        public override string TaskName => $"Clean up log tables";
        public override void Execute() {
            new SqlTask(this, Sql) { DisableLogging = true, DisableExtension = true }.ExecuteNonQuery();            
        }

        public int DaysToKeep { get; set; }

        /* Public properties */
        public string Sql => $@"
delete from etl.Log
 where LogDate < Dateadd(day,-{DaysToKeep},GETDATE())
";

        public CleanUpLogTask() { }

        public CleanUpLogTask(int daysToKeep) : this() {
            DaysToKeep = daysToKeep;
        }
        public static void CleanUp(int daysToKeep) => new CleanUpLogTask(daysToKeep).Execute();
        



    }
}
