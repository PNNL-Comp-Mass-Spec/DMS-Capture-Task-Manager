using PRISM;
using System;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    #region "Enums"

    /// <summary>
    /// Manager status
    /// </summary>
    public enum EnumMgrStatus : short
    {
        Stopped,
        Stopped_Error,
        Running,
        Disabled_Local,
        Disabled_MC
    }

    public enum EnumTaskStatus : short
    {
        Stopped,
        Requesting,
        Running,
        Closing,
        Failed,
        No_Task
    }

    public enum EnumTaskStatusDetail : short
    {
        Retrieving_Resources,
        Running_Tool,
        Packaging_Results,
        Delivering_Results,
        No_Task
    }

    public enum EnumCloseOutType : short
    {
        CLOSEOUT_SUCCESS = 0,
        CLOSEOUT_FAILED = 1,
        CLOSEOUT_NOT_READY = 2,
        CLOSEOUT_NEED_TO_ABORT_PROCESSING = 3
    }

    public enum EnumEvalCode : short
    {
        EVAL_CODE_SUCCESS = 0,
        EVAL_CODE_FAILED = 1,
        EVAL_CODE_NOT_EVALUATED = 2,
        EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE = 3,
        EVAL_CODE_SUBMITTED_TO_MYEMSL = 4,
        EVAL_CODE_VERIFIED_IN_MYEMSL = 5,
        // This enum is obsolete; it was used previously to indicate that we copied data to Aurora via FTP but did not upload to MyEMSL
        // EVAL_CODE_SKIPPED_MYEMSL_UPLOAD = 6,
        EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE = 7,
        EVAL_CODE_FAILURE_DO_NOT_RETRY = 8,
        EVAL_CODE_SKIPPED = 9
    }

    public enum EnumRequestTaskResult : short
    {
        TaskFound = 0,
        NoTaskFound = 1,
        ResultError = 2,
        TooManyRetries = 3,
        Deadlock = 4
    }

    #endregion

    #region "Delegates"

    public delegate void StatusMonitorUpdateReceived(string msg);

    #endregion

    public static class clsErrors
    {
        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex)
        {
            return GetExceptionStackTrace(ex, false);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="useMultiLine">>When true, format the stack trace using newline characters instead of -:-</param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex, bool useMultiLine)
        {
            if (useMultiLine)
            {
                return StackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return StackTraceFormatter.GetExceptionStackTrace(ex);
        }
    }
}