using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using PRISM;
using PRISM.Logging;

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
                return defaultValue;

            if (bool.TryParse(value, out var blnValue))
                return blnValue;

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
                return defaultValue;

            if (int.TryParse(value, out var intValue))
                return intValue;

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
                return fValue;

            if (float.TryParse(value, out fValue))
                return fValue;

            return fValue;
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldIndex">Field index (0-based)</param>
        /// <param name="valueIfNull">Integer to return if null</param>
        /// <returns>Integer</returns>
        public static int GetDbValue(SqlDataReader reader, int fieldIndex, int valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            return (int)reader.GetValue(fieldIndex);
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldIndex">Field index (0-based)</param>
        /// <param name="valueIfNull">String to return if null</param>
        /// <returns>String</returns>
        public static string GetDbValue(SqlDataReader reader, int fieldIndex, string valueIfNull)
        {
            if (Convert.IsDBNull(reader.GetValue(fieldIndex)))
                return valueIfNull;

            // Use .ToString() and not a string cast to allow for DateTime fields to convert to strings
            return reader.GetValue(fieldIndex).ToString();
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">Integer to return if null</param>
        /// <returns>Integer</returns>
        public static int GetDbValue(SqlDataReader reader, string fieldName, int valueIfNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
                return valueIfNull;

            return (int)reader[fieldName];
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">String to return if null</param>
        /// <returns>String</returns>
        public static string GetDbValue(SqlDataReader reader, string fieldName, string valueIfNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
                return valueIfNull;

            // Use .ToString() and not a string cast to allow for DateTime fields to convert to strings
            return reader[fieldName].ToString();
        }

        /// <summary>
        /// Surround a path with double quotes if it contains spaces
        /// </summary>
        /// <param name="strPath"></param>
        /// <returns></returns>
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
                return clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex);

            return clsStackTraceFormatter.GetExceptionStackTrace(ex);
        }
    }

    public static class clsUtilities
    {

        #region Properties

        /// <summary>
        /// When true, we are running on Linux and thus should not access any Windows features
        /// </summary>
        /// <remarks>Call EnableOfflineMode to set this to true</remarks>
        public static bool LinuxOS { get; private set; }

        /// <summary>
        /// When true, does not contact any databases or remote shares
        /// </summary>
        public static bool OfflineMode { get; private set; }

        #endregion

        #region "Module variables"

        private static string mAppFolderPath;

        #endregion

        /// <summary>
        /// Convert a file size in bytes to gigabytes
        /// </summary>
        /// <param name="sizeBytes"></param>
        /// <returns></returns>
        public static double BytesToGB(long sizeBytes)
        {
            return sizeBytes / 1024.0 / 1024 / 1024;
        }

        /// <summary>
        /// Decode a password
        /// </summary>
        /// <param name="encodedPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        public static string DecodePassword(string encodedPwd)
        {
            return Pacifica.Core.Utilities.DecodePassword(encodedPwd);
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <param name="runningLinux">Set to True if running Linux</param>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        public static void EnableOfflineMode(bool runningLinux = true)
        {
            OfflineMode = true;
            LinuxOS = runningLinux;

            LogTools.OfflineMode = true;

            if (runningLinux)
                Console.WriteLine("Offline mode enabled globally (running Linux)");
            else
                Console.WriteLine("Offline mode enabled globally");
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppFolderPath()
        {
            if (mAppFolderPath != null)
                return mAppFolderPath;

            mAppFolderPath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppFolderPath();

            return mAppFolderPath;
        }

        /// <summary>
        /// Runs the specified Sql query
        /// </summary>
        /// <param name="sqlStr">Sql query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Uses a timeout of 30 seconds</remarks>
        public static bool GetDataTableByQuery(string sqlStr, string connectionString, string callingFunction, short retryCount, out DataTable dtResults)
        {

            const int timeoutSeconds = 30;

            return GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the specified Sql query
        /// </summary>
        /// <param name="sqlStr">Sql query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByQuery(
            string sqlStr, string connectionString, string callingFunction,
            short retryCount, out DataTable dtResults, int timeoutSeconds)
        {

            var cmd = new SqlCommand(sqlStr)
            {
                CommandType = CommandType.Text
            };

            return GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the stored procedure or database query defined by "cmd"
        /// </summary>
        /// <param name="cmd">SqlCommand var (query or stored procedure)</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByCmd(
            SqlCommand cmd,
            string connectionString,
            string callingFunction,
            short retryCount,
            out DataTable dtResults,
            int timeoutSeconds)
        {

            if (cmd == null)
                throw new ArgumentException("command is undefined", nameof(cmd));

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("ConnectionString cannot be empty", nameof(connectionString));
            }

            if (string.IsNullOrEmpty(callingFunction))
                callingFunction = "UnknownCaller";
            if (retryCount < 1)
                retryCount = 1;
            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            // When data retrieval fails, delay for 5 seconds on the first try
            // Double the delay time for each subsequent attempt, up to a maximum of 90 seconds between attempts
            var retryDelaySeconds = 5;

            while (retryCount > 0)
            {
                try
                {
                    using (var cn = new SqlConnection(connectionString))
                    {

                        cmd.Connection = cn;
                        cmd.CommandTimeout = timeoutSeconds;

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            using (var ds = new DataSet())
                            {
                                da.Fill(ds);
                                dtResults = ds.Tables[0];
                            }
                        }
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    string msg;

                    retryCount -= 1;
                    if (cmd.CommandType == CommandType.StoredProcedure)
                    {
                        msg = callingFunction + "; Exception running stored procedure " + cmd.CommandText;
                    }
                    else if (cmd.CommandType == CommandType.TableDirect)
                    {
                        msg = callingFunction + "; Exception querying table " + cmd.CommandText;
                    }
                    else
                    {
                        msg = callingFunction + "; Exception querying database";
                    }

                    msg += ": " + ex.Message + "; ConnectionString: " + connectionString;
                    msg += ", RetryCount = " + retryCount;

                    if (cmd.CommandType == CommandType.Text)
                    {
                        msg += ", Query = " + cmd.CommandText;
                    }

                    LogTools.LogError(msg);

                    if (retryCount <= 0)
                        break;

                    clsProgRunner.SleepMilliseconds(retryDelaySeconds * 1000);

                    retryDelaySeconds *= 2;
                    if (retryDelaySeconds > 90)
                    {
                        retryDelaySeconds = 90;
                    }
                }
            }

            dtResults = null;
            return false;

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

                LogTools.LogMessage(msg);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception in VerifyFolder", ex);
            }
        }
    }
}