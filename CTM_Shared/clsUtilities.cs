using System;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
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

        private static string mAppDirectoryPath;

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
            {
                Console.WriteLine("Offline mode enabled globally (running Linux)");
            }
            else
            {
                Console.WriteLine("Offline mode enabled globally");
            }
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppDirectoryPath()
        {
            if (mAppDirectoryPath != null)
            {
                return mAppDirectoryPath;
            }

            mAppDirectoryPath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppDirectoryPath();

            return mAppDirectoryPath;
        }

        /// <summary>
        /// Runs the specified SQL query
        /// </summary>
        /// <param name="sqlStr">SQL query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="resultsTable">DataTable (Output Parameter)</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Uses a timeout of 30 seconds</remarks>
        [Obsolete("Use PRISMDatabaseUtils.DbToolsFactory.GetDBTools(...).GetQueryResultsDataTable(...)", true)]
        public static bool GetDataTableByQuery(string sqlStr, string connectionString, short retryCount, out DataTable resultsTable, [CallerMemberName] string callingFunction = "")
        {
            const int timeoutSeconds = 30;
            return GetDataTableByQuery(sqlStr, connectionString, retryCount, out resultsTable, timeoutSeconds, callingFunction);
        }

        /// <summary>
        /// Runs the specified SQL query
        /// </summary>
        /// <param name="sqlStr">SQL query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="resultsTable">DataTable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISMDatabaseUtils.DbToolsFactory.GetDBTools(...).GetQueryResultsDataTable(...)", true)]
        public static bool GetDataTableByQuery(
            string sqlStr, string connectionString, short retryCount, out DataTable resultsTable, int timeoutSeconds, [CallerMemberName] string callingFunction = "")
        {
            var dbTools = DbToolsFactory.GetDBTools(connectionString, timeoutSeconds);
            return dbTools.GetQueryResultsDataTable(sqlStr, out resultsTable, retryCount, timeoutSeconds, callingFunction: callingFunction);
        }

        /// <summary>
        /// Runs the stored procedure or database query defined by "cmd"
        /// </summary>
        /// <param name="cmd">SqlCommand var (query or stored procedure)</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="resultsTable">DataTable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISMDatabaseUtils.DbToolsFactory.GetDBTools(...).GetQueryDataTable(...)", true)]
        public static bool GetDataTableByCmd(
            System.Data.SqlClient.SqlCommand cmd,
            string connectionString,
            short retryCount,
            out DataTable resultsTable,
            int timeoutSeconds,
            [CallerMemberName] string callingFunction = "")
        {
            if (cmd == null)
            {
                throw new ArgumentException("command is undefined", nameof(cmd));
            }

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("ConnectionString cannot be empty", nameof(connectionString));
            }

            if (string.IsNullOrEmpty(callingFunction))
            {
                callingFunction = "UnknownCaller";
            }

            if (retryCount < 1)
            {
                retryCount = 1;
            }

            if (timeoutSeconds < 5)
            {
                timeoutSeconds = 5;
            }

            // When data retrieval fails, delay for 5 seconds on the first try
            // Double the delay time for each subsequent attempt, up to a maximum of 90 seconds between attempts
            var retryDelaySeconds = 5;

            while (retryCount > 0)
            {
                try
                {
                    using var cn = new System.Data.SqlClient.SqlConnection(connectionString);

                    cmd.Connection = cn;
                    cmd.CommandTimeout = timeoutSeconds;

                    using var da = new System.Data.SqlClient.SqlDataAdapter(cmd);
                    using var ds = new DataSet();

                    da.Fill(ds);
                    resultsTable = ds.Tables[0];

                    return true;
                }
                catch (Exception ex)
                {
                    string msg;

                    retryCount--;
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
                    {
                        break;
                    }

                    ProgRunner.SleepMilliseconds(retryDelaySeconds * 1000);

                    retryDelaySeconds *= 2;
                    if (retryDelaySeconds > 90)
                    {
                        retryDelaySeconds = 90;
                    }
                }
            }

            resultsTable = null;
            return false;
        }

        /// <summary>
        /// This function was added to debug remote share access issues
        /// The folder was accessible from some classes but not accessible from others
        /// </summary>
        /// <param name="pathToCheck"></param>
        /// <param name="callingFunction"></param>
        public static void VerifyFolder(string pathToCheck = @"\\Proto-2.emsl.pnl.gov\External_Orbitrap_Xfer\", [CallerMemberName] string callingFunction = "")
        {
            try
            {
                var directoryInfo = new DirectoryInfo(pathToCheck);
                string msg;

                if (directoryInfo.Exists)
                {
                    msg = "Directory exists [" + pathToCheck + "]; called from " + callingFunction;
                }
                else
                {
                    msg = "Directory not found [" + pathToCheck + "]; called from " + callingFunction;
                }

                LogTools.LogMessage(msg);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception in VerifyFolder", ex);
            }
        }
    }
}