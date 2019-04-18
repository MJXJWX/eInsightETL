using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    public class CustomSource<TOutput> : GenericTask, ITask, IDataFlowSource<TOutput> {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CSVSOURCE";
        public override string TaskName => $"Read data from custom source";
        public override void Execute() => ExecuteAsync();

        /* Public properties */             
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        public Func<TOutput> ReadFunc { get; set; }
        public Func<bool> ReadCompletedFunc { get; set; }

        /* Private stuff */
        BufferBlock<TOutput> Buffer { get; set; }
        NLog.Logger NLogger { get; set; }

        public CustomSource() {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BufferBlock<TOutput>();
        }

        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this() {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        public CustomSource(string name, Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this(readFunc, readCompletedFunc) {
            this.TaskName = name;
        }

        public void ExecuteAsync() {
            NLogStart();
            while (!ReadCompletedFunc.Invoke()) {
                Buffer.Post(ReadFunc.Invoke());
            }            
            Buffer.Complete();
            NLogFinish();
        }
  

        public void LinkTo(IDataFlowLinkTarget<TOutput> target) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogStart() {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish() {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }
}
