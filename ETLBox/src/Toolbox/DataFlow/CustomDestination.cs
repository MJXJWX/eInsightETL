using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class CustomDestination<TInput> : GenericTask, ITask, IDataFlowDestination<TInput> {       

        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CUSTOMDEST";
        public override string TaskName => $"Dataflow: Write Data into custom target (unnamed)";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => TargetActionBlock;
        public Action<TInput> WriteAction {
            get {
                return _writeAction;
            }
            set {
                _writeAction = value;
                TargetActionBlock = new ActionBlock<TInput>(_writeAction);

            }
        }

        /* Private stuff */
        private Action<TInput> _writeAction;
        
        internal ActionBlock<TInput> TargetActionBlock { get; set; }

        NLog.Logger NLogger { get; set; }
        public CustomDestination() {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public CustomDestination(Action<TInput> writeAction) {
            WriteAction = writeAction;
        }
        
        public void Wait() => TargetActionBlock.Completion.Wait();
    }

}
