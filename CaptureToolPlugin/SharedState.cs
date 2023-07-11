using System;
using CaptureTaskManager;

namespace CaptureToolPlugin
{
    internal class SharedState : LoggerBase
    {
        /// <summary>
        /// Set to true if an error occurs connecting to the source computer
        /// </summary>
        public bool NeedToAbortProcessing { get; private set; }

        public string ErrorMessage { get; set; } = string.Empty;

        public void SetAbortProcessing()
        {
            NeedToAbortProcessing = true;
        }

        public void HandleCopyException(ToolReturnData returnData, Exception ex)
        {
            if (ex.Message.Contains("An unexpected network error occurred") ||
                ex.Message.Contains("Multiple connections") ||
                ex.Message.Contains("specified network name is no longer available"))
            {
                // Need to completely exit the capture task manager
                NeedToAbortProcessing = true;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else if (ex.Message.Contains("unknown user name or bad password") || ex.Message.Contains("user name or password"))
            {
                // This error randomly occurs; no need to log a full stack trace
                returnData.CloseoutMsg = "Authentication failure: " + ex.Message.Trim('\r', '\n');
                LogError(returnData.CloseoutMsg);

                // Set the EvalCode to 3 so that capture can be retried
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
