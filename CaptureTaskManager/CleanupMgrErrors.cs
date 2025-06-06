﻿using System;
using System.Data;
using PRISMDatabaseUtils;

namespace CaptureTaskManager
{
    public class CleanupMgrErrors : LoggerBase
    {
        private const string PROCEDURE_NAME_REPORT_MGR_CLEANUP = "report_manager_error_cleanup";

        /// <summary>
        /// Options for auto-removing files from the working directory when the manager starts
        /// </summary>
        public enum CleanupModeConstants
        {
            /// <summary>
            /// Never auto-remove files from the working directory
            /// </summary>
            Disabled = 0,

            /// <summary>
            /// Auto-remove files from the working directory once
            /// </summary>
            [Obsolete("Not used by this manager")]
            // ReSharper disable once UnusedMember.Global
            CleanupOnce = 1,

            /// <summary>
            /// Always auto-remove files from the working directory
            /// </summary>
            CleanupAlways = 2
        }

        /// <summary>
        /// Cleanup status codes for procedure mc.report_manager_error_cleanup
        /// </summary>
        public enum CleanupActionCodeConstants
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

        private readonly bool mInitialized;

        private readonly string mMgrConfigDBConnectionString;

        private readonly string mManagerName;

        private readonly StatusFile mStatusFile;

        private readonly bool mTraceMode;

        private readonly string mWorkingDirPath;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrConfigDBConnectionString">Connection string to the manager_control database; if empty, database access is disabled</param>
        /// <param name="managerName"></param>
        /// <param name="workingDirPath"></param>
        /// <param name="statusFile"></param>
        /// <param name="traceMode"></param>
        public CleanupMgrErrors(
            string mgrConfigDBConnectionString,
            string managerName,
            string workingDirPath,
            StatusFile statusFile,
            bool traceMode)
        {
            if (string.IsNullOrEmpty(mgrConfigDBConnectionString))
            {
                throw new Exception("Manager config DB connection string is not defined");
            }

            if (string.IsNullOrEmpty(managerName))
            {
                throw new Exception("Manager name is not defined");
            }

            mMgrConfigDBConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(mgrConfigDBConnectionString, managerName);
            mManagerName = managerName;

            mStatusFile = statusFile;
            mTraceMode = traceMode;
            mWorkingDirPath = workingDirPath;

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
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public bool AutoCleanupManagerErrors(int managerErrorCleanupMode)
        {
            CleanupModeConstants cleanupMode;

            if (Enum.IsDefined(typeof(CleanupModeConstants), managerErrorCleanupMode))
            {
                cleanupMode = (CleanupModeConstants)managerErrorCleanupMode;
            }
            else
            {
                cleanupMode = CleanupModeConstants.Disabled;
            }

            return AutoCleanupManagerErrors(cleanupMode);
        }

        /// <summary>
        /// Remove all files in the working directory
        /// Also calls procedure mc.report_manager_error_cleanup at the start and finish of the cleanup
        /// </summary>
        /// <param name="managerErrorCleanupMode">Manager error cleanup mode</param>
        /// <returns>True if success, false if an error</returns>
        public bool AutoCleanupManagerErrors(CleanupModeConstants managerErrorCleanupMode)
        {
            if (!mInitialized)
            {
                return false;
            }

            if (managerErrorCleanupMode == CleanupModeConstants.Disabled)
            {
                return false;
            }

            LogMessage("Attempting to automatically clean the work directory");

            // Call procedure report_manager_error_cleanup with @ActionCode=1
            ReportManagerErrorCleanup(CleanupActionCodeConstants.Start);

            // Delete all folders and subdirectories in the working directory
            var success = ToolRunnerBase.CleanWorkDir(mWorkingDirPath, 1, out var failureMessage);

            if (!success)
            {
                if (string.IsNullOrEmpty(failureMessage))
                {
                    failureMessage = "unable to clear work directory";
                }
            }
            else
            {
                // If successful, delete FlagFile.txt
                success = mStatusFile.DeleteStatusFlagFile();

                if (!success)
                {
                    failureMessage = "error deleting " + StatusFile.FLAG_FILE_NAME;
                }
            }

            // If successful, call procedure mc.report_manager_error_cleanup with @ActionCode=2
            // Otherwise call procedure mc.report_manager_error_cleanup with @ActionCode=3

            if (success)
            {
                ReportManagerErrorCleanup(CleanupActionCodeConstants.Success);
            }
            else
            {
                ReportManagerErrorCleanup(CleanupActionCodeConstants.Fail, failureMessage);
            }

            return success;
        }

        private void ReportManagerErrorCleanup(CleanupActionCodeConstants managerCleanupActionCode)
        {
            ReportManagerErrorCleanup(managerCleanupActionCode, string.Empty);
        }

        private void ReportManagerErrorCleanup(CleanupActionCodeConstants managerCleanupActionCode, string failureMessage)
        {
            if (string.IsNullOrWhiteSpace(mMgrConfigDBConnectionString))
            {
                if (CTMUtilities.OfflineMode)
                {
                    LogDebug("Skipping call to " + PROCEDURE_NAME_REPORT_MGR_CLEANUP + " since offline");
                }
                else
                {
                    LogError("Skipping call to " + PROCEDURE_NAME_REPORT_MGR_CLEANUP + " since the Manager Control connection string is empty");
                }

                return;
            }

            try
            {
                failureMessage ??= string.Empty;

                var dbTools = DbToolsFactory.GetDBTools(mMgrConfigDBConnectionString, debugMode: mTraceMode);
                RegisterEvents(dbTools);

                var cmd = dbTools.CreateCommand(PROCEDURE_NAME_REPORT_MGR_CLEANUP, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, mManagerName);
                dbTools.AddParameter(cmd, "@state", SqlType.Int).Value = managerCleanupActionCode;
                dbTools.AddParameter(cmd, "@failureMsg", SqlType.VarChar, 512, failureMessage);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                // Call the procedure
                dbTools.ExecuteSP(cmd);
            }
            catch (Exception ex)
            {
                string errorMessage;

                if (mMgrConfigDBConnectionString == null)
                {
                    errorMessage = "Exception calling " + PROCEDURE_NAME_REPORT_MGR_CLEANUP + " in ReportManagerErrorCleanup; empty connection string";
                }
                else
                {
                    errorMessage = "Exception calling " + PROCEDURE_NAME_REPORT_MGR_CLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                }

                LogError(errorMessage, ex);
            }
        }
    }
}