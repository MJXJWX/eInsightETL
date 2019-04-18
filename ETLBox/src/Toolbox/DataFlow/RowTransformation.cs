using System;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <see cref="RowTransformation"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;string[], MyDataRow&gt; trans = new RowTransformation&lt;string[], MyDataRow&gt;(
    ///     csvdata => {
    ///       return new MyDataRow() { Value1 = csvdata[0], Value2 = int.Parse(csvdata[1]) };
    /// });    
    /// trans.LinkTo(dest);    
    /// </code>
    /// </example>
    public class RowTransformation<TInput, TOutput> : GenericTask, ITask, IDataFlowTransformation<TInput, TOutput> {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_ROWTRANSFORMATION";
        public override string TaskName { get; set; } = "Row Transformation (unnamed)";
        public override void Execute() { throw new Exception("Transformations can't be executed directly"); }

        /* Public Properties */
        public Func<TInput, TOutput> RowTransformationFunc {
            get {
                return _rowTransformationFunc;
            }

            set {
                _rowTransformationFunc = value;
                TransformBlock = new TransformBlock<TInput, TOutput>(row => InvokeRowTransformationFunc(row));
            }
        }
        public Action InitAction { get; set; }
        public bool WasInitialized { get; private set; } = false;

        public ITargetBlock<TInput> TargetBlock => TransformBlock;
        public ISourceBlock<TOutput> SourceBlock => TransformBlock;

        /* Private stuff */
        Func<TInput, TOutput> _rowTransformationFunc;
        internal TransformBlock<TInput, TOutput> TransformBlock { get; set; }

        NLog.Logger NLogger { get; set; }
        public RowTransformation() {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public RowTransformation(Func<TInput, TOutput> rowTransformationFunc) : this() {
            RowTransformationFunc = rowTransformationFunc;
        }

        public RowTransformation(string name, Func<TInput, TOutput> rowTransformationFunc) : this(rowTransformationFunc) {
            this.TaskName = name;
        }

        public RowTransformation(string name, Func<TInput, TOutput> rowTransformationFunc, Action initAction) : this(rowTransformationFunc) {
            this.TaskName = name;
            this.InitAction = initAction;
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target) {
            TransformBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate) {
            TransformBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }


        private TOutput InvokeRowTransformationFunc(TInput row) {
            if (!WasInitialized) {
                InitAction?.Invoke();
                WasInitialized = true;
                NLogger.Debug(TaskName + " was initialized!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            }
            return RowTransformationFunc.Invoke(row);
        }
    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// The non generic RowTransformation accepts a string array as input and returns a string array as output. 
    /// If you need other data types, use the generic RowTransformation instead.
    /// </summary>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// //Non generic RowTransformation works with string[] as input and output
    /// //use RowTransformation&lt;TInput,TOutput&gt; for generic usage!
    /// RowTransformation trans = new RowTransformation(
    ///     csvdata => {
    ///       return new string[] { csvdata[0],  int.Parse(csvdata[1]) };
    /// });    
    /// trans.LinkTo(dest);    
    /// </code>
    /// </example>
    public class RowTransformation : RowTransformation<string[],string[]> {
        public RowTransformation() : base() { }

        public RowTransformation(Func<string[], string[]> rowTransformationFunc) : base(rowTransformationFunc) { }

        public RowTransformation(string name, Func<string[], string[]> rowTransformationFunc) : base(name, rowTransformationFunc) { }

        public RowTransformation(string name, Func<string[], string[]> rowTransformationFunc, Action initAction) : base(name, rowTransformationFunc, initAction) { }
    }
}
