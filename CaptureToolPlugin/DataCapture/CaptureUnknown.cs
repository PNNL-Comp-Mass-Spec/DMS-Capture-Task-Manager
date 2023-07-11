using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Implementation for unknown, automatic failure data types.
    /// </summary>
    internal class CaptureUnknown : CaptureBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public CaptureUnknown(CaptureInitData data) : base(data)
        { }

        ///// <inheritdoc />
        //public override void Capture(
        //    out string msg,
        //    ToolReturnData returnData,
        //    DatasetInfo datasetInfo,
        //    string sourceDirectoryPath,
        //    string datasetDirectoryPath,
        //    bool copyWithResume,
        //    InstrumentClassInfo.InstrumentClass instrumentClass,
        //    string instrumentName,
        //    ITaskParams taskParams
        //)
        //{
        //}

        public void CaptureFail(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo
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
