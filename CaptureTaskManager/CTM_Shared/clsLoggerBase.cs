
using PRISM.Logging;
using System;

namespace CaptureTaskManager
{

    public abstract class clsLoggerBase
    {

        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        /// <remarks>The message is shown in dark grey in the console.</remarks>
        protected static void LogDebug(string statusMessage, bool writeToLog = true)
        {
            LogTools.LogDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogError(string errorMessage, bool logToDb = false)
        {
            LogTools.LogError(errorMessage, null, logToDb);
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log</param>
        protected static void LogError(string errorMessage, Exception ex)
        {
            LogTools.LogError(errorMessage, ex);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isError">True if this is an error</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        public static void LogMessage(string statusMessage, bool isError = false, bool writeToLog = true)
        {
            LogTools.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogWarning(string warningMessage, bool logToDb = false)
        {
            LogTools.LogWarning(warningMessage, logToDb);
        }

    }

}
