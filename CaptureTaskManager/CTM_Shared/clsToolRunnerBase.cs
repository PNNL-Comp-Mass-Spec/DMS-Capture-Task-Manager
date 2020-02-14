//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using Pacifica.Core;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISMDatabaseUtils;

namespace CaptureTaskManager
{
    /// <summary>
    /// Base class for capture step tool plugins
    /// </summary>
    /// <remarks>Used in CaptureTaskManager.clsMainProgram</remarks>
    public class clsToolRunnerBase : clsLoggerBase, IToolRunner
    {

        #region "Constants"

        public const string EXCEPTION_CREATING_OUTPUT_DIRECTORY = "Exception creating output directory";

        private const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        #endregion

        #region "Class variables"

        protected IMgrParams mMgrParams;
        protected ITaskParams mTaskParams;

        // Used by CTM plugins
        protected IStatusFile mStatusTools;

        // Used by CTM plugins
        protected FileTools mFileTools;

        protected IDBTools mCaptureDbProcedureExecutor;

        protected DateTime mLastConfigDbUpdate = DateTime.UtcNow;
        protected int mMinutesBetweenConfigDbUpdates = 10;

        // Used by CTM plugins
        // ReSharper disable once UnusedMember.Global
        protected bool mNeedToAbortProcessing = false;

        protected string mWorkDir;

        protected string mDataset;

        protected int mJob;

        protected int mDatasetID;

        protected string mMgrName;

        /// <summary>
        /// LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
        /// </summary>
        protected short mDebugLevel;

        protected bool mTraceMode;

        private DateTime mLastLockQueueWaitTimeLog = DateTime.UtcNow;

        private DateTime mLockQueueWaitTimeStart = DateTime.UtcNow;

        #endregion

        #region "Delegates"

        #endregion

        #region "Events"

        #endregion

        #region "Properties"

        #endregion

        #region "Constructor"

        /// <summary>
        /// Constructor
        /// </summary>
        protected clsToolRunnerBase()
        {
            // Does nothing; see the Setup method for constructor-like behavior
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the plugin tool. Implements IToolRunner.RunTool method
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public virtual clsToolReturnData RunTool()
        {
            // Does nothing at present, so return success
            var retData = new clsToolReturnData
            {
                CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS
            };
            return retData;
        }

        /// <summary>
        /// Initializes plugin. Implements IToolRunner.Setup method
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public virtual void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            mMgrParams = mgrParams;
            mTaskParams = taskParams;
            mStatusTools = statusTools;

            mMgrName = mMgrParams.GetParam("MgrName", "CaptureTaskManager");

            // This connection string points to the DMS_Capture database
            var connectionString = mMgrParams.GetParam("ConnectionString");

            mCaptureDbProcedureExecutor = DbToolsFactory.GetDBTools(connectionString, debugMode: mTraceMode);
            RegisterEvents(mCaptureDbProcedureExecutor);

            mWorkDir = mMgrParams.GetParam("WorkDir");

            mDataset = mTaskParams.GetParam("Dataset");

            mDatasetID = mTaskParams.GetParam("Dataset_ID", 0);

            mJob = mTaskParams.GetParam("Job", 0);

            // Debug level 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            // Log level 4 will also log error messages
            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 4);
            LogTools.SetFileLogLevel(mDebugLevel);

            mTraceMode = mMgrParams.GetParam("TraceMode", false);

            InitFileTools(mMgrName, mDebugLevel);
        }

        // Used by CTM plugins
        // ReSharper disable once UnusedMember.Global
        protected bool UpdateMgrSettings()
        {
            var bSuccess = true;

            if (mMinutesBetweenConfigDbUpdates < 1)
                mMinutesBetweenConfigDbUpdates = 1;

            if (DateTime.UtcNow.Subtract(mLastConfigDbUpdate).TotalMinutes >= mMinutesBetweenConfigDbUpdates)
            {
                mLastConfigDbUpdate = DateTime.UtcNow;

                LogDebug("Updating manager settings using Manager Control database");

                if (!mMgrParams.LoadMgrSettingsFromDB(logConnectionErrors: false))
                {
                    // Error retrieving settings from the manager control DB
                    LogWarning("Error calling mMgrSettings.LoadMgrSettingsFromDB to update manager settings");

                    bSuccess = false;
                }
                else
                {
                    // Update the log level
                    mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 4);
                    LogTools.SetFileLogLevel(mDebugLevel);
                }
            }

            return bSuccess;
        }


        /// <summary>
        /// Appends a string to a job comment string
        /// </summary>
        /// <param name="baseComment">Initial comment</param>
        /// <param name="addnlComment">Comment to be appended</param>
        /// <returns>String containing both comments</returns>
        /// <remarks></remarks>
        protected static string AppendToComment(string baseComment, string addnlComment)
        {

            if (string.IsNullOrWhiteSpace(baseComment))
            {
                return addnlComment.Trim();
            }

            if (string.IsNullOrWhiteSpace(addnlComment) || baseComment.Contains(addnlComment))
            {
                // Either addnlComment is empty (unlikely) or addnlComment is a duplicate comment
                // Return the base comment
                return baseComment.Trim();
            }

            // Append a semicolon to baseComment, but only if it doesn't already end in a semicolon
            if (baseComment.TrimEnd().EndsWith(";"))
            {
                return baseComment.TrimEnd() + addnlComment.Trim();
            }

            return baseComment.Trim() + "; " + addnlComment.Trim();
        }

        /// <summary>
        /// Delete files in the working directory
        /// </summary>
        /// <param name="workDir">Working directory path</param>
        /// <returns></returns>
        // ReSharper disable once UnusedMember.Global
        public static bool CleanWorkDir(string workDir)
        {
            const float HoldoffSeconds = 0.1f;

            return CleanWorkDir(workDir, HoldoffSeconds, out _);
        }

        /// <summary>
        /// Delete files in the working directory
        /// </summary>
        /// <param name="workDir">Working directory path</param>
        /// <param name="holdoffSeconds">
        /// Time to wait after garbage collection before deleting files.
        /// Set to 0 (or a negative number) to skip garbage collection</param>
        /// <param name="failureMessage">Output: failure message</param>
        /// <returns></returns>
        public static bool CleanWorkDir(string workDir, float holdoffSeconds, out string failureMessage)
        {

            failureMessage = string.Empty;

            if (holdoffSeconds > 0)
            {
                int holdoffMilliseconds;
                try
                {
                    holdoffMilliseconds = Convert.ToInt32(holdoffSeconds * 1000);
                    if (holdoffMilliseconds < 100)
                        holdoffMilliseconds = 100;
                    if (holdoffMilliseconds > 300000)
                        holdoffMilliseconds = 300000;
                }
                catch (Exception)
                {
                    holdoffMilliseconds = 3000;
                }

                // Try to ensure there are no open objects with file handles
                ProgRunner.GarbageCollectNow();
                Thread.Sleep(holdoffMilliseconds);
            }

            var workingDirectory = new DirectoryInfo(workDir);

            // Delete the files
            try
            {
                if (!workingDirectory.Exists)
                {
                    failureMessage = "Working directory does not exist";
                    return false;
                }

                foreach (var fileToDelete in workingDirectory.GetFiles("*", SearchOption.AllDirectories))
                {
                    try
                    {
                        fileToDelete.Delete();
                    }
                    catch (Exception ex)
                    {
                        ShowTraceMessage(string.Format("Error deleting file {0}: {1}", fileToDelete.FullName, ex.Message));

                        // Make sure the readonly and system attributes are not set
                        // The manager will try to delete the file the next time is starts
                        fileToDelete.Attributes = fileToDelete.Attributes & ~FileAttributes.ReadOnly & ~FileAttributes.System;
                    }
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting files in working directory";
                LogError("clsGlobal.ClearWorkDir(), " + failureMessage + " " + workDir, ex);
                return false;
            }

            // Delete the subdirectories
            try
            {
                foreach (var subDirectory in workingDirectory.GetDirectories())
                {
                    try
                    {
                        subDirectory.Delete(true);
                    }
                    catch (Exception ex)
                    {
                        ShowTraceMessage(string.Format("Error deleting subdirectory {0}: {1}", subDirectory.FullName, ex.Message));

                        // Make sure the readonly and system attributes are not set
                        // The manager will try to delete the file the next time is starts
                        subDirectory.Attributes = subDirectory.Attributes & ~FileAttributes.ReadOnly & ~FileAttributes.System;
                    }
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting subdirectories in the working directory";
                LogError("clsGlobal.ClearWorkDir(), " + failureMessage, ex);
                return false;
            }

            return true;
        }

        // Used by CTM plugins
        // ReSharper disable once UnusedMember.Global
        protected void DeleteFileIgnoreErrors(string filePath)
        {
            try
            {
                File.Delete(filePath);
            }
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Lookup the MyEMSL ingest status for the current job
        /// </summary>
        /// <param name="job"></param>
        /// <param name="statusChecker"></param>
        /// <param name="statusURI"></param>
        /// <param name="retData"></param>
        /// <param name="serverResponse">Server response (dictionary representation of JSON)</param>
        /// <param name="currentTask">Output: current task</param>
        /// <param name="percentComplete">Output: ingest process percent complete (value between 0 and 100)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool GetMyEMSLIngestStatus(
            int job,
            MyEMSLStatusCheck statusChecker,
            string statusURI,
            clsToolReturnData retData,
            out Dictionary<string, object> serverResponse,
            out string currentTask,
            out int percentComplete)
        {
            serverResponse = statusChecker.GetIngestStatus(
                statusURI,
                out currentTask,
                out percentComplete,
                out var lookupError,
                out var errorMessage);

            if (lookupError)
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.CloseoutMsg = errorMessage;
                LogError(errorMessage + ", job " + job);

                return false;
            }

            if (serverResponse.Keys.Count == 0)
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.CloseoutMsg = "Empty JSON server response";
                LogError(retData.CloseoutMsg + ", job " + job);
                return false;
            }

            if (serverResponse.TryGetValue("state", out var ingestState))
            {
                if (string.Equals((string)ingestState, "failed", StringComparison.OrdinalIgnoreCase) ||
                    !string.IsNullOrWhiteSpace(errorMessage))
                {
                    // Error should have already been logged during the call to GetIngestStatus
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    if (string.IsNullOrWhiteSpace(errorMessage))
                        retData.CloseoutMsg = "Ingest failed; unknown reason";
                    else
                        retData.CloseoutMsg = errorMessage;

                    retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                    return false;
                }

                return true;
            }

            // State parameter was not present

            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            retData.CloseoutMsg = "State parameter not found in ingest status; see " + statusURI;

            return false;
        }

        /// <summary>
        /// Initialize mFileTools
        /// </summary>
        /// <param name="mgrName"></param>
        /// <param name="debugLevel"></param>
        protected void InitFileTools(string mgrName, short debugLevel)
        {
            ResetTimestampForQueueWaitTimeLogging();
            mFileTools = new FileTools(mgrName, debugLevel);
            RegisterEvents(mFileTools, false);

            // Use a custom event handler for status messages
            UnregisterEventHandler(mFileTools, BaseLogger.LogLevels.INFO);
            mFileTools.StatusEvent += FileTools_StatusEvent;

            mFileTools.LockQueueTimedOut += FileTools_LockQueueTimedOut;
            mFileTools.LockQueueWaitComplete += FileTools_LockQueueWaitComplete;
            mFileTools.WaitingForLockQueue += FileTools_WaitingForLockQueue;
            mFileTools.WaitingForLockQueueNotifyLockFilePaths += FileTools_WaitingForLockQueueNotifyLockFilePaths;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime lockQueueWaitTimeStart, ref DateTime lastLockQueueWaitTimeLog)
        {

            int waitTimeLogIntervalSeconds;

            if (lockQueueWaitTimeStart == DateTime.MinValue)
                lockQueueWaitTimeStart = DateTime.UtcNow;

            var waitTimeMinutes = DateTime.UtcNow.Subtract(lockQueueWaitTimeStart).TotalMinutes;

            if (waitTimeMinutes >= 30)
            {
                waitTimeLogIntervalSeconds = 240;
            }
            else if (waitTimeMinutes >= 15)
            {
                waitTimeLogIntervalSeconds = 120;
            }
            else if (waitTimeMinutes >= 5)
            {
                waitTimeLogIntervalSeconds = 60;
            }
            else
            {
                waitTimeLogIntervalSeconds = 30;
            }

            if (DateTime.UtcNow.Subtract(lastLockQueueWaitTimeLog).TotalSeconds >= waitTimeLogIntervalSeconds)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInfoFilePath"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ReadVersionInfoFile(string dllFilePath, string versionInfoFilePath, out string version)
        {
            // Open versionInfoFilePath and read the Version= line

            version = string.Empty;

            try
            {
                if (!File.Exists(versionInfoFilePath))
                {
                    LogError("Version Info File not found: " + versionInfoFilePath);
                    return false;
                }

                var success = false;

                using (var reader = new StreamReader(new FileStream(versionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var equalsIndex = dataLine.IndexOf('=');

                        if (equalsIndex <= 0)
                        {
                            continue;
                        }

                        var keyName = dataLine.Substring(0, equalsIndex);
                        var value = string.Empty;

                        if (equalsIndex < dataLine.Length)
                        {
                            value = dataLine.Substring(equalsIndex + 1);
                        }

                        switch (keyName.ToLower())
                        {
                            case "filename":
                                break;
                            case "path":
                                break;
                            case "version":
                                version = string.Copy(value);
                                if (string.IsNullOrWhiteSpace(version))
                                {
                                    LogError("Empty version line in Version Info file for " +
                                             Path.GetFileName(dllFilePath));
                                    success = false;
                                }
                                else
                                {
                                    success = true;
                                }
                                break;
                            case "error":
                                LogError("Error reported by DLLVersionInspector for " +
                                         Path.GetFileName(dllFilePath) + ": " + value);
                                success = false;
                                break;
                        }
                    }
                }

                return success;
            }
            catch (Exception ex)
            {
                LogError("Error reading Version Info File for " + Path.GetFileName(dllFilePath), ex);
                return false;
            }
        }


        /// <summary>
        /// Reset the timestamp for logging that we are waiting for a lock file queue to decrease
        /// </summary>
        protected void ResetTimestampForQueueWaitTimeLogging()
        {
            mLastLockQueueWaitTimeLog = DateTime.UtcNow;
            mLockQueueWaitTimeStart = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="toolVersionInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private void SaveToolVersionInfoFile(string directoryPath, string toolVersionInfo)
        {
            try
            {
                var toolVersionFilePath = Path.Combine(directoryPath, "Tool_Version_Info_" + mTaskParams.GetParam("StepTool") + ".txt");

                using (var toolVersionWriter = new StreamWriter(new FileStream(toolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    toolVersionWriter.WriteLine("Date: " + DateTime.Now.ToString(DATE_TIME_FORMAT));
                    toolVersionWriter.WriteLine("Dataset: " + mDataset);
                    toolVersionWriter.WriteLine("Job: " + mJob);
                    toolVersionWriter.WriteLine("Step: " + mTaskParams.GetParam("Step"));
                    toolVersionWriter.WriteLine("Tool: " + mTaskParams.GetParam("StepTool"));
                    toolVersionWriter.WriteLine("ToolVersionInfo:");

                    toolVersionWriter.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));
                    toolVersionWriter.Close();
                }
            }
            catch (Exception ex)
            {
                LogError("Exception saving tool version info: " + ex.Message);
            }

        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool SetStepTaskToolVersion(
            string toolVersionInfo,
            IReadOnlyList<FileInfo> toolFiles,
            bool saveToolVersionTextFile)
        {
            var exeInfo = string.Empty;
            string toolVersionInfoCombined;

            if (string.IsNullOrWhiteSpace(mWorkDir))
            {
                return false;
            }

            if (toolFiles != null)
            {
                foreach (var toolFile in toolFiles)
                {
                    try
                    {
                        if (toolFile.Exists)
                        {
                            exeInfo = AppendToComment(exeInfo,
                                                         toolFile.Name + ": " +
                                                         toolFile.LastWriteTime.ToString(DATE_TIME_FORMAT));

                            var writeToLog = mDebugLevel >= 5;
                            LogDebug("EXE Info: " + exeInfo, writeToLog);
                        }
                        else
                        {
                            LogWarning("Tool file not found: " + toolFile.FullName);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception looking up tool version file info: " + ex.Message);
                    }
                }
            }

            // Append the .Exe info to toolVersionInfo
            if (string.IsNullOrEmpty(exeInfo))
            {
                toolVersionInfoCombined = string.Copy(toolVersionInfo);
            }
            else
            {
                toolVersionInfoCombined = AppendToComment(toolVersionInfo, exeInfo);
            }

            if (saveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(mWorkDir, toolVersionInfoCombined);
            }

            // Setup for execution of the stored procedure
            var dbTools = mCaptureDbProcedureExecutor;
            var cmd = dbTools.CreateCommand(SP_NAME_SET_TASK_TOOL_VERSION, CommandType.StoredProcedure);

            dbTools.AddTypedParameter(cmd, "@job", SqlType.Int, value: mTaskParams.GetParam("Job", 0));
            dbTools.AddTypedParameter(cmd, "@step", SqlType.Int, value: mTaskParams.GetParam("Step", 0));
            dbTools.AddParameter(cmd, "@toolVersionInfo", SqlType.VarChar, 900, toolVersionInfoCombined);
            var returnParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.Output);

            // Execute the SP (retry the call up to 4 times)
            var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 4);

            var returnCode = returnParam.Value.ToString();
            var returnCodeValue = clsConversion.GetReturnCodeValue(returnCode);

            if (resCode == 0 && returnCodeValue == 0)
            {
                return true;
            }

            if (resCode != 0)
            {
                LogError("Error " + resCode + " storing tool version for current processing step");
                return false;
            }

            LogError("Stored procedure " + SP_NAME_SET_TASK_TOOL_VERSION + " reported return code " + returnCode + ", job " + mJob);
            return false;
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        /// <param name="includeDate"></param>
        /// <param name="emptyLinesBeforeMessage"></param>
        public static void ShowTraceMessage(string message, bool includeDate = false, int emptyLinesBeforeMessage = 1)
        {
            BaseLogger.ShowTraceMessage(message, includeDate, "  ", emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Determines the version info for a DLL using reflection
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Used by CTM plugins</remarks>
        // ReSharper disable once UnusedMember.Global
        protected virtual bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string dllFilePath)
        {
            bool success;

            try
            {
                var dllFile = new FileInfo(dllFilePath);

                if (!dllFile.Exists)
                {
                    LogWarning("File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
                    return false;
                }

                var oAssemblyName = Assembly.LoadFrom(dllFile.FullName).GetName();

                var nameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
                toolVersionInfo = AppendToComment(toolVersionInfo, nameAndVersion);

                return true;
            }
            catch (BadImageFormatException)
            {
                // Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
                // Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

                // Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

                // Use this when compiled as AnyCPU
                success = StoreToolVersionInfoOneFile32Bit(ref toolVersionInfo, dllFilePath);

                // Use this when compiled as 32-bit
                // success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, dllFilePath)
            }
            catch (Exception ex)
            {
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, add these lines to the end of file AnalysisManagerProg.exe.config
                //  <startup useLegacyV2RuntimeActivationPolicy="true">
                //    <supportedRuntime version="v4.0" />
                //  </startup>
                LogError("Exception determining Assembly info for " + Path.GetFileName(dllFilePath) + ": " + ex.Message);
                success = false;
            }

            if (!success)
            {
                success = StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dllFilePath);
            }

            return success;
        }

        /// <summary>
        /// Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfoViaSystemDiagnostics(ref string toolVersionInfo, string dllFilePath)
        {
            try
            {
                var dllFile = new FileInfo(dllFilePath);

                if (!dllFile.Exists)
                {
                    LogWarning("File not found by StoreToolVersionInfoViaSystemDiagnostics: " + dllFilePath);
                    return false;
                }

                var oFileVersionInfo = FileVersionInfo.GetVersionInfo(dllFilePath);

                var name = oFileVersionInfo.FileDescription;
                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.InternalName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.FileName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = dllFile.Name;
                }

                var version = oFileVersionInfo.FileVersion;
                if (string.IsNullOrEmpty(version))
                {
                    version = oFileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrEmpty(version))
                {
                    version = "??";
                }

                var nameAndVersion = name + ", Version=" + version;
                toolVersionInfo = AppendToComment(toolVersionInfo, nameAndVersion);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception determining File Version for " + Path.GetFileName(dllFilePath), ex);
                return false;
            }
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFile32Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Used by CTM plugins</remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool StoreToolVersionInfoOneFile64Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x64.exe");
        }

        /// <summary>
        /// Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string dllFilePath,
                                                         string versionInspectorExeName)
        {
            try
            {
                var versionInspectorAppPath = Path.Combine(clsUtilities.GetAppDirectoryPath(), versionInspectorExeName);

                var dllFile = new FileInfo(dllFilePath);

                if (!dllFile.Exists)
                {
                    LogError("File not found by StoreToolVersionInfoOneFileUseExe: " + dllFilePath);
                    return false;
                }

                if (!File.Exists(versionInspectorAppPath))
                {
                    LogError("DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe: " + versionInspectorAppPath);
                    return false;
                }

                // Call DLLVersionInspector.exe to determine the tool version

                var versionInfoFilePath = Path.Combine(mWorkDir,
                                                       Path.GetFileNameWithoutExtension(dllFile.Name) +
                                                       "_VersionInfo.txt");

                var args = clsConversion.PossiblyQuotePath(dllFile.FullName) + " /O:" +
                              clsConversion.PossiblyQuotePath(versionInfoFilePath);

                var progRunner = new clsRunDosProgram(clsUtilities.GetAppDirectoryPath(), mDebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250
                };

                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(versionInspectorAppPath, args, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                ProgRunner.SleepMilliseconds(100);

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out var version);

                // Delete the version info file
                try
                {
                    if (File.Exists(versionInfoFilePath))
                    {
                        ProgRunner.SleepMilliseconds(100);
                        File.Delete(versionInfoFilePath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (!success || string.IsNullOrWhiteSpace(version))
                {
                    return false;
                }

                toolVersionInfo = AppendToComment(toolVersionInfo, version);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception determining Version info for " + Path.GetFileName(dllFilePath) + ": " + ex.Message);
                toolVersionInfo = AppendToComment(toolVersionInfo, Path.GetFileNameWithoutExtension(dllFilePath));
            }

            return false;
        }

        /// <summary>
        /// Updates the value for Ingest_Steps_Completed in table T_MyEMSL_Uploads for the given upload task
        /// </summary>
        /// <param name="statusNum">MyEMSL Status number</param>
        /// <param name="ingestStepsCompleted">Number of completed ingest steps</param>
        /// <param name="transactionId">
        /// TransactionID for the given status item; if 0, the Transaction ID is not updated
        /// Prior to July 2017, TransactionID was the transactionID used by the majority of the verified files
        /// Starting in July 2017, StatusNum and TransactionID are identical
        /// </param>
        /// <param name="fatalError">True if ingest failed with a fatal error and thus the ErrorCode should be updated in T_MyEMSL_Uploads</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool UpdateIngestStepsCompletedOneTask(
            int statusNum,
            byte ingestStepsCompleted,
            long transactionId = 0,
            bool fatalError = false)
        {
            const string SP_NAME = "UpdateMyEMSLUploadIngestStats";

            if (transactionId > 0 && transactionId != statusNum)
            {
                // Starting in July 2017, StatusNum and TransactionID are identical
                // Assure that they match
                transactionId = statusNum;
            }

            var dbTools = mCaptureDbProcedureExecutor;
            var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

            // Note that if transactionId is 0, the stored procedure will leave TransactionID unchanged in table T_MyEMSL_Uploads

            dbTools.AddTypedParameter(cmd, "@datasetID", SqlType.Int, value: mDatasetID);
            dbTools.AddTypedParameter(cmd, "@statusNum", SqlType.Int, value: statusNum);
            dbTools.AddTypedParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt, value: ingestStepsCompleted);
            dbTools.AddTypedParameter(cmd, "@fatalError", SqlType.TinyInt, value: fatalError ? 1 : 0);
            dbTools.AddTypedParameter(cmd, "@transactionId", SqlType.Int, value: transactionId);
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);
            var returnParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.Output);

            mCaptureDbProcedureExecutor.TimeoutSeconds = 20;
            var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

            var returnCode = returnParam.Value.ToString();
            var returnCodeValue = clsConversion.GetReturnCodeValue(returnCode);

            if (resCode == 0 && returnCodeValue == 0)
            {
                return true;
            }

            if (resCode != 0)
            {
                LogError("Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + mJob);
                return false;
            }

            LogError("Stored procedure " + SP_NAME + " reported return code " + returnCode + ", job " + mJob);
            return false;

        }

        #endregion

        #region "Event Handlers"

        private void FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            var msg = "Lockfile queue timed out after " + waitTimeMinutes.ToString("0") + " minutes; Source=" + sourceFilePath + ", Target=" + targetFilePath;
            LogWarning(msg);
        }

        private void FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (waitTimeMinutes >= 1)
            {
                var msg = "Exited lockfile queue after " + waitTimeMinutes.ToString("0") + " minutes; will now copy file";
                LogMessage(msg);
            }
        }

        private void FileTools_StatusEvent(string message)
        {
            // Do not log certain common messages
            if (message.StartsWith("Created lock file") ||
                message.StartsWith("Copying file with CopyFileEx") ||
                message.StartsWith("File to copy is") && message.Contains("will use CopyFileEx for"))
            {
                if (mTraceMode)
                    ConsoleMsgUtils.ShowDebug(message);

                return;
            }

            LogMessage(message);
        }

        private void FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (!IsLockQueueLogMessageNeeded(ref mLockQueueWaitTimeStart, ref mLastLockQueueWaitTimeLog))
                return;

            mLastLockQueueWaitTimeLog = DateTime.UtcNow;
            LogMessage(string.Format(
                         "Waiting for lockfile queue to fall below threshold; " +
                         "SourceBacklog={0:N0} MB, TargetBacklog={1:N0} MB, " +
                         "Source={2}, Target={3}",
                         backlogSourceMB, backlogTargetMB, sourceFilePath, targetFilePath));
        }

        private void FileTools_WaitingForLockQueueNotifyLockFilePaths(string sourceLockFilePath, string targetLockFilePath, string adminBypassMessage)
        {
            if (string.IsNullOrWhiteSpace(adminBypassMessage))
            {
                LogMessage(string.Format("Waiting for lockfile queue to fall below threshold; see lock file(s) at {0} and {1}",
                                         sourceLockFilePath ?? "(n/a)", targetLockFilePath ?? "(n/a)"));
                return;
            }

            LogMessage(adminBypassMessage);
        }

        #endregion

    }
}