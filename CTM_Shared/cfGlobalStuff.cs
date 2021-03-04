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

    public static class clsConversion
    {
        /// <summary>
        /// Convert string to bool; default false if an error
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string value)
        {
            return CBoolSafe(value, false);
        }

        /// <summary>
        /// Convert a string value to a boolean
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string value, bool defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            if (bool.TryParse(value, out var parsedValue))
            {
                return parsedValue;
            }

            return defaultValue;
        }

        /// <summary>
        /// Convert a string value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static int CIntSafe(string value, int defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            if (int.TryParse(value, out var parsedValue))
            {
                return parsedValue;
            }

            return defaultValue;
        }

        /// <summary>
        /// Convert a string value to a float
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static float CSngSafe(string value, float defaultValue)
        {
            var fValue = defaultValue;

            if (string.IsNullOrEmpty(value))
            {
                return fValue;
            }

            if (float.TryParse(value, out fValue))
            {
                return fValue;
            }

            return fValue;
        }

        /// <summary>
        /// Convert returnCode to an integer
        /// </summary>
        /// <param name="returnCode"></param>
        /// <returns>
        /// If returnCode is blank or '0', returns 0
        /// If returnCode is an integer, returns the integer
        /// Otherwise, returns -1
        /// </returns>
        public static int GetReturnCodeValue(string returnCode)
        {
            if (string.IsNullOrWhiteSpace(returnCode))
            {
                return 0;
            }

            if (int.TryParse(returnCode, out var returnCodeValue))
            {
                return returnCodeValue;
            }

            return -1;
        }

        /// <summary>
        /// Surround a file (or directory) path with double quotes if it contains spaces
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public static string PossiblyQuotePath(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return string.Empty;
            }

            if (filePath.Contains(" "))
            {
                if (!filePath.StartsWith("\""))
                {
                    filePath = "\"" + filePath;
                }

                if (!filePath.EndsWith("\""))
                {
                    filePath += "\"";
                }
            }

            return filePath;
        }
    }

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