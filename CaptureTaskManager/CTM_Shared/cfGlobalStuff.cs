using System;
using System.Data.SqlClient;
using System.IO;

namespace CaptureTaskManager
{

    #region "Enums"

    //Status constants
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
        EVAL_CODE_SKIPPED_MYEMSL_UPLOAD = 6,
        EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE = 7,
        EVAL_CODE_FAILURE_DO_NOT_RETRY = 8
    }

    public enum EnumRequestTaskResult : short
    {
        TaskFound = 0,
        NoTaskFound = 1,
        ResultError = 2
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

        public static bool CBoolSafe(string value, bool defaultValue)
        {
            if (string.IsNullOrEmpty(value))
                return defaultValue;

            bool blnValue;
            if (bool.TryParse(value, out blnValue))
                return blnValue;

            return defaultValue;
        }

        public static int CIntSafe(string value, int defaultValue)
        {
            if (string.IsNullOrEmpty(value))
                return defaultValue;

            int intValue;
            if (int.TryParse(value, out intValue))
                return intValue;

            return defaultValue;
        }

        public static float CSngSafe(string value, float defaultValue)
        {
            var fValue = defaultValue;

            if (string.IsNullOrEmpty(value))
                return fValue;

            if (float.TryParse(value, out fValue))
                return fValue;

            return fValue;
        }

        public static int GetDbValue(SqlDataReader reader, int fieldIndex, int valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            return (int)reader.GetValue(fieldIndex);
        }

        public static string GetDbValue(SqlDataReader reader, int fieldIndex, string valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            return (string)reader.GetValue(fieldIndex);
        }

        public static string PossiblyQuotePath(string strPath)
        {
            if (string.IsNullOrEmpty(strPath))
            {
                return string.Empty;
            }

            if (strPath.Contains(" "))
            {
                if (!strPath.StartsWith("\""))
                {
                    strPath = "\"" + strPath;
                }

                if (!strPath.EndsWith("\""))
                {
                    strPath += "\"";
                }
            }

            return strPath;
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
                return PRISM.clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex);

            return PRISM.clsStackTraceFormatter.GetExceptionStackTrace(ex);
        }
    }

    public static class clsUtilities
    {
        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides 
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppFolderPath()
        {
            var entryAssembly = System.Reflection.Assembly.GetEntryAssembly();

            var fiAssemblyFile = new FileInfo(entryAssembly.Location);

            if (string.IsNullOrEmpty(fiAssemblyFile.DirectoryName))
                return string.Empty;

            return fiAssemblyFile.DirectoryName;
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        public static void LogError(string errorMessage, bool logToDb = false)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ResetColor();

            var loggerType = logToDb ? clsLogTools.LoggerTypes.LogDb : clsLogTools.LoggerTypes.LogFile;
            clsLogTools.WriteLog(loggerType, clsLogTools.LogLevels.ERROR, errorMessage);
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception to log</param>
        public static void LogError(string errorMessage, Exception ex)
        {
            ReportStatus(errorMessage, ex);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        public static void LogWarning(string warningMessage, bool logToDb = false)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(warningMessage);
            Console.ResetColor();

            var loggerType = logToDb ? clsLogTools.LoggerTypes.LogDb : clsLogTools.LoggerTypes.LogFile;
            clsLogTools.WriteLog(loggerType, clsLogTools.LogLevels.WARN, warningMessage);
        }

        /// <summary>
        /// Shows information about an exception at the console and in the log file
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception</param>
        public static void ReportStatus(string errorMessage, Exception ex)
        {
            string formattedError;
            if (errorMessage.EndsWith(ex.Message))
            {
                formattedError = errorMessage;
            }
            else
            {
                formattedError = errorMessage + ": " + ex.Message;
            }
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(formattedError);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(PRISM.Logging.Utilities.GetExceptionStackTraceMultiLine(ex));
            Console.ResetColor();
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, formattedError, ex);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isDebug">True if a debug level message</param>
        public static void ReportStatus(string statusMessage, bool isDebug = false)
        {
            Console.WriteLine(statusMessage);
            var logLevel = isDebug ? clsLogTools.LogLevels.DEBUG : clsLogTools.LogLevels.INFO;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, logLevel, statusMessage);
        }

        /// <summary>
        /// This function was added to debug remote share access issues
        /// The folder was accessible from some classes but not accessible from others
        /// </summary>
        /// <param name="callingFunction"></param>
        public static void VerifyFolder(string callingFunction)
        {
            VerifyFolder(callingFunction, @"\\Proto-2.emsl.pnl.gov\External_Orbitrap_Xfer\");
        }

        /// <summary>
        /// This function was added to debug remote share access issues
        /// The folder was accessible from some classes but not accessible from others
        /// </summary>
        /// <param name="callingFunction"></param>
        /// <param name="pathToCheck"></param>
        public static void VerifyFolder(string callingFunction, string pathToCheck)
        {
            try
            {
                var diSourceFolder = new DirectoryInfo(pathToCheck);
                string msg;

                if (diSourceFolder.Exists)
                    msg = "Folder exists [" + pathToCheck + "]; called from " + callingFunction;
                else
                    msg = "Folder not found [" + pathToCheck + "]; called from " + callingFunction;

                ReportStatus(msg);                
            }
            catch (Exception ex)
            {
                LogError("Exception in VerifyFolder", ex);
            }
        }
    }
}