﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// A lookup task - data from the input can be enriched with data retrieved from the lookup source. The result is then posted into the output.
    /// </summary>
    /// <typeparam name="TTransformationInput">Type of data input</typeparam>
    /// <typeparam name="TTransformationOutput">Type of data output</typeparam>
    /// <typeparam name="TSourceOutput">Type of lookup data</typeparam>
    /// <example>
    /// <code>
    /// Lookup&lt;MyInputDataRow, MyOutputDataRow, MyLookupRow&gt; lookup = new Lookup&lt;MyInputDataRow, MyOutputDataRow,MyLookupRow&gt;(                
    ///     testClass.TestTransformationFunc, lookupSource, testClass.LookupData                
    /// );
    /// </code>
    /// </example>
    public class Lookup<TTransformationInput, TTransformationOutput, TSourceOutput>
        : GenericTask, ITask, IDataFlowTransformation<TTransformationInput, TTransformationOutput>
        where TSourceOutput : new() {

        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_LOOKUP";
        public override string TaskName { get; set; } = "Lookup (unnamed)";
        public override void Execute() { throw new Exception("Transformations can't be executed directly"); }

        public List<TSourceOutput> LookupList { get; set; }
        ActionBlock<TSourceOutput> LookupBuffer { get; set; }

        /* Public Properties */
        public ISourceBlock<TTransformationOutput> SourceBlock => RowTransformation.SourceBlock;
        public ITargetBlock<TTransformationInput> TargetBlock => RowTransformation.TargetBlock;
        public IDataFlowSource<TSourceOutput> Source {
            get {
                return _source;
            }
            set {
                _source = value;
                Source.SourceBlock.LinkTo(LookupBuffer, new DataflowLinkOptions() { PropagateCompletion = true });
            }
        }

        /* Private stuff */
        RowTransformation<TTransformationInput, TTransformationOutput> RowTransformation { get; set; }

        private Func<TTransformationInput, TTransformationOutput> _rowTransformationFunc;
        private IDataFlowSource<TSourceOutput> _source;

        Func<TTransformationInput, TTransformationOutput> RowTransformationFunc {
            get {
                return _rowTransformationFunc;
            }
            set {
                _rowTransformationFunc = value;
                RowTransformation = new RowTransformation<TTransformationInput, TTransformationOutput>(_rowTransformationFunc);
                RowTransformation.InitAction = LoadLookupData;
            }
        }

        NLog.Logger NLogger { get; set; }
        public Lookup() {
            NLogger = NLog.LogManager.GetLogger("ETL");            
            LookupBuffer = new ActionBlock<TSourceOutput>(row => FillBuffer(row));            
        }

        public Lookup(Func<TTransformationInput, TTransformationOutput> rowTransformationFunc, IDataFlowSource<TSourceOutput> source) : this() {
            RowTransformationFunc = rowTransformationFunc;
            Source = source;
        }

        public Lookup(Func<TTransformationInput, TTransformationOutput> rowTransformationFunc, IDataFlowSource<TSourceOutput> source, List<TSourceOutput> lookupList) : this() {
            RowTransformationFunc = rowTransformationFunc;
            Source = source;
            LookupList = lookupList;
        }


        private void LoadLookupData() {
            Source.ExecuteAsync();
            LookupBuffer.Completion.Wait();
        }

        private void FillBuffer(TSourceOutput sourceRow) {
            if (LookupList == null) LookupList = new List<TSourceOutput>();
            LookupList.Add(sourceRow);
        }

        public void LinkTo(IDataFlowLinkTarget<TTransformationOutput> target) {
            RowTransformation.LinkTo(target);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TTransformationOutput> target, Predicate<TTransformationOutput> predicate) {
            RowTransformation.LinkTo(target, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }



    }
}
