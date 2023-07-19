using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Implementation for unknown, automatic failure data types.
    /// </summary>
    internal class UnknownCapture : CaptureBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public UnknownCapture(CaptureInitData data) : base(data)
        { }

        /// <inheritdoc />
        public override void Capture(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass,
            string instrumentName,
            ITaskParams taskParams
        )
        {
            msg = "Invalid dataset type found: " + datasetInfo.DatasetType;
            returnData.CloseoutMsg = msg;
            LogError(returnData.CloseoutMsg, true);
            mShareConnection.DisconnectShareIfRequired();
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
        }
    }
}
