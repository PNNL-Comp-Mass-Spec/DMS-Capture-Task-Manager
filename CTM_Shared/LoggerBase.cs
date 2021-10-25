
using PRISM.Logging;
using System;
using JetBrains.Annotations;
using PRISM;

namespace CaptureTaskManager
{
    public abstract class LoggerBase
    {
        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <remarks>The message is shown in dark gray in the console.</remarks>
        /// <param name="statusMessage">Status message</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        protected static void LogDebug(string statusMessage, bool writeToLog = true)
        {
            LogTools.LogDebug(statusMessage, writeToLog);
        }
        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <remarks>The message is shown in dark gray in the console.</remarks>
        /// <param name="format">Status message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected static void LogDebug(string format, params object[] args)
        {
            LogDebug(string.Format(format, args));
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
        /// Log an error message
        /// </summary>
        /// <param name="format">Error message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected static void LogError(string format, params object[] args)
        {
            LogError(string.Format(format, args));
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
        /// Log an error message and exception
        /// </summary>
        /// <param name="ex">Exception to log</param>
        /// <param name="format">Error message format string (do not include ex.message)</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected static void LogError(Exception ex, string format, params object[] args)
        {
            LogError(string.Format(format, args), ex);
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
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="format">Status message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        public static void LogMessage(string format, params object[] args)
        {
            LogMessage(string.Format(format, args));
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

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="format">Warning message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected static void LogWarning(string format, params object[] args)
        {
            LogWarning(string.Format(format, args));
        }

        /// <summary>
        /// Register event handlers
        /// However, does not subscribe to .ProgressUpdate
        /// Note: the DatasetInfoPlugin does subscribe to .ProgressUpdate
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="writeDebugEventsToLog"></param>
        protected void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                processingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                processingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;
            // Ignore: processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="messageType"></param>
        protected void UnregisterEventHandler(EventNotifier processingClass, BaseLogger.LogLevels messageType)
        {
            switch (messageType)
            {
                case BaseLogger.LogLevels.DEBUG:
                    processingClass.DebugEvent -= DebugEventHandler;
                    processingClass.DebugEvent -= DebugEventHandlerConsoleOnly;
                    break;
                case BaseLogger.LogLevels.ERROR:
                    processingClass.ErrorEvent -= ErrorEventHandler;
                    break;
                case BaseLogger.LogLevels.WARN:
                    processingClass.WarningEvent -= WarningEventHandler;
                    break;
                case BaseLogger.LogLevels.INFO:
                    processingClass.StatusEvent -= StatusEventHandler;
                    break;
                default:
                    throw new Exception("Log level not supported for unregistering");
            }
        }

        protected void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        protected void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        protected void StatusEventHandler(string statusMessage)
        {
            if (statusMessage.StartsWith(RunDosProgram.RUN_PROGRAM_STATUS_LINE) &&
                statusMessage.Contains("DLLVersionInspector"))
            {
                LogDebug(statusMessage, writeToLog: false);
            }
            else
            {
                LogMessage(statusMessage);
            }
        }

        protected void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        protected void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }
    }
}
