using System;
using System.Data;
using System.Data.SqlClient;

namespace CaptureTaskManager
{
    public class clsCleanupMgrErrors : clsLoggerBase
    {
        #region "Constants"

        private const string SP_NAME_REPORT_MGR_CLEANUP = "ReportManagerErrorCleanup";

        /// <summary>
        /// Options for auto-removing files from the working directory when the manager starts
        /// </summary>
        public enum eCleanupModeConstants
        {
            /// <summary>
            /// Never auto-remove files from the working directory
            /// </summary>
            Disabled = 0,

            /// <summary>
            /// Auto-remove files from the working directory once
            /// </summary>
            [Obsolete("Not used by this manager")]
            CleanupOnce = 1,

            /// <summary>
            /// Always auto-remove files from the working directory
            /// </summary>
            CleanupAlways = 2
        }

        /// <summary>
        /// Cleanup status codes for stored procedure ReportManagerErrorCleanup
        /// </summary>
        public enum eCleanupActionCodeConstants
        {
            /// <summary>
            /// Starting
            /// </summary>
            Start = 1,

            /// <summary>
            /// Success
            /// </summary>
            Success = 2,

            /// <summary>
            /// Failed
            /// </summary>
            Fail = 3
        }

        #endregion

        #region "Class wide Variables"

        private readonly bool mInitialized;
        private readonly string mMgrConfigDBConnectionString;

        private readonly string mManagerName;

        private readonly IStatusFile mStatusFile;

        private readonly string mWorkingDirPath;
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrConfigDBConnectionString">Connection string to the manager_control database; if empty, database access is disabled</param>
        /// <param name="managerName"></param>
        /// <param name="workingDirPath"></param>
        /// <param name="statusFile"></param>
        public clsCleanupMgrErrors(string mgrConfigDBConnectionString,
                                   string managerName,
                                   string workingDirPath,
                                   IStatusFile statusFile)
        {
            if (string.IsNullOrEmpty(mgrConfigDBConnectionString))
                throw new Exception("Manager config DB connection string is not defined");

            if (string.IsNullOrEmpty(managerName))
                throw new Exception("Manager name is not defined");

            mMgrConfigDBConnectionString = string.Copy(mgrConfigDBConnectionString);
            mManagerName = string.Copy(managerName);

            mWorkingDirPath = workingDirPath;

            mStatusFile = statusFile;

            mInitialized = true;
        }

        /// <summary>
        /// Possibly auto-cleanup manager errors
        /// </summary>
        /// <param name="managerErrorCleanupMode">
        /// 0 = Disabled
        /// 1 = Cleanup once
        /// 2 = Cleanup always
        /// </param>
        /// <returns></returns>
        // ReSharper disable once UnusedMember.Global
        public bool AutoCleanupManagerErrors(int managerErrorCleanupMode)
        {
            eCleanupModeConstants cleanupMode;

            if (Enum.IsDefined(typeof(eCleanupModeConstants), managerErrorCleanupMode))
            {
                cleanupMode = (eCleanupModeConstants)managerErrorCleanupMode;
            }
            else
            {
                cleanupMode = eCleanupModeConstants.Disabled;
            }

            return AutoCleanupManagerErrors(cleanupMode);
        }

        /// <summary>
        /// Remove all files in the working directory
        /// Also calls stored procedure ReportManagerErrorCleanup at the start and finish of the cleanup
        /// </summary>
        /// <param name="eManagerErrorCleanupMode"></param>
        /// <returns>True if success, false if an error</returns>
        public bool AutoCleanupManagerErrors(eCleanupModeConstants eManagerErrorCleanupMode)
        {
            if (!mInitialized)
                return false;

            if (eManagerErrorCleanupMode == eCleanupModeConstants.Disabled)
                return false;

            LogMessage("Attempting to automatically clean the work directory");

            // Call SP ReportManagerErrorCleanup @ActionCode=1
            ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start);

            // Delete all folders and subfolders in work folder
            var success = clsToolRunnerBase.CleanWorkDir(mWorkingDirPath, 1, out var failureMessage);

            if (!success)
            {
                if (string.IsNullOrEmpty(failureMessage))
                    failureMessage = "unable to clear work directory";
            }
            else
            {
                // If successful, delete FlagFile.txt
                success = mStatusFile.DeleteStatusFlagFile();
                if (!success)
                {
                    failureMessage = "error deleting " + clsStatusFile.FLAG_FILE_NAME;
                }
            }

            // If successful, call SP with ReportManagerErrorCleanup @ActionCode=2
            // Otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

            if (success)
            {
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success);
            }
            else
            {
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, failureMessage);
            }

            return success;
        }

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode)
        {
            ReportManagerErrorCleanup(eMgrCleanupActionCode, string.Empty);
        }

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode, string failureMessage)
        {
            if (string.IsNullOrWhiteSpace(mMgrConfigDBConnectionString))
            {
                if (clsUtilities.OfflineMode)
                    LogDebug("Skipping call to " + SP_NAME_REPORT_MGR_CLEANUP + " since offline");
                else
                    LogError("Skipping call to " + SP_NAME_REPORT_MGR_CLEANUP + " since the Manager Control connection string is empty");

                return;
            }

            try
            {
                if (failureMessage == null)
                    failureMessage = string.Empty;

                var myConnection = new SqlConnection(mMgrConfigDBConnectionString);
                myConnection.Open();

                var spCmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REPORT_MGR_CLEANUP,
                    Connection = myConnection
                };

                spCmd.Parameters.Add(new SqlParameter("@ManagerName", SqlDbType.VarChar, 128)).Value = mManagerName;
                spCmd.Parameters.Add(new SqlParameter("@State", SqlDbType.Int)).Value = eMgrCleanupActionCode;
                spCmd.Parameters.Add(new SqlParameter("@FailureMsg", SqlDbType.VarChar, 512)).Value = failureMessage;
                spCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                // Execute the SP
                spCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string errorMessage;
                if (mMgrConfigDBConnectionString == null)
                {
                    errorMessage = "Exception calling " + SP_NAME_REPORT_MGR_CLEANUP + " in ReportManagerErrorCleanup; empty connection string";
                }
                else
                {
                    errorMessage = "Exception calling " + SP_NAME_REPORT_MGR_CLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                }

                LogError(errorMessage, ex);
            }
        }
    }
}