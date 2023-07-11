using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// A container object to simplify constructors in the various classes derived from <see cref="CaptureBase"/>
    /// </summary>
    internal readonly struct CaptureInitData
    {
        public SharedState ToolState { get; }
        public IMgrParams MgrParams { get; }
        public FileTools FileTools { get; }
        public ShareConnection ShareConnection { get; }
        public bool TraceMode { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="toolState">SharedState object for tracking critical errors</param>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <param name="shareConnection">State object for connection to file share</param>
        /// <param name="traceMode">When true, show debug messages at the console</param>
        public CaptureInitData(SharedState toolState, IMgrParams mgrParams, FileTools fileTools, ShareConnection shareConnection, bool traceMode)
        {
            ToolState = toolState;
            MgrParams = mgrParams;
            FileTools = fileTools;
            ShareConnection = shareConnection;
            TraceMode = traceMode;
        }
    }
}
