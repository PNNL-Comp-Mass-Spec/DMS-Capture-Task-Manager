//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using JetBrains.Annotations;
using Pacifica.Core;
using Pacifica.Json;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace CaptureTaskManager
{
    /// <summary>
    /// Base class for capture step tool plugins
    /// </summary>
    /// <remarks>Used in CaptureTaskManager.MainProgram</remarks>
    public class ToolRunnerBase : LoggerBase, IToolRunner
    {
        // Ignore Spelling: addnl, holdoff, yyyy-MM-dd hh:mm:ss tt, Lockfile

        protected const string DEFAULT_DMS_CONNECTION_STRING = "prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";

        public const string EXCEPTION_CREATING_OUTPUT_DIRECTORY = "Exception creating output directory";

        private const string SP_NAME_SET_TASK_TOOL_VERSION = "set_ctm_step_task_tool_version";

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

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
        protected bool mNeedToAbortProcessing;

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

        /// <summary>
        /// Constructor
        /// </summary>
        protected ToolRunnerBase()
        {
            // Does nothing; see the Setup method for constructor-like behavior
        }

        /// <summary>
        /// Runs the plugin tool. Implements IToolRunner.RunTool method
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public virtual ToolReturnData RunTool()
        {
            // Does nothing at present, so return success
            return new ToolReturnData
            {
                CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS
            };
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

            // This connection string points to the DMS database on prismdb2 (previously, DMS_Capture on Gigasax)
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            mCaptureDbProcedureExecutor = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mTraceMode);
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
            var success = true;

            if (mMinutesBetweenConfigDbUpdates < 1)
            {
                mMinutesBetweenConfigDbUpdates = 1;
            }

            if (DateTime.UtcNow.Subtract(mLastConfigDbUpdate).TotalMinutes >= mMinutesBetweenConfigDbUpdates)
            {
                mLastConfigDbUpdate = DateTime.UtcNow;

                LogDebug("Updating manager settings using Manager Control database");

                if (!mMgrParams.LoadMgrSettingsFromDB(logConnectionErrors: false))
                {
                    // Error retrieving settings from the manager control DB
                    LogWarning("Error calling mMgrSettings.LoadMgrSettingsFromDB to update manager settings");

                    success = false;
                }
                else
                {
                    // Update the log level
                    mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 4);
                    LogTools.SetFileLogLevel(mDebugLevel);
                }
            }

            return success;
        }

        /// <summary>
        /// Appends a string to a job comment string
        /// </summary>
        /// <param name="baseComment">Initial comment</param>
        /// <param name="addnlComment">Comment to be appended</param>
        /// <returns>String containing both comments</returns>
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
        /// <returns>True if successful, false if an error</returns>
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
        /// <returns>True if successful, false if an error</returns>
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
                    {
                        holdoffMilliseconds = 100;
                    }

                    if (holdoffMilliseconds > 300000)
                    {
                        holdoffMilliseconds = 300000;
                    }
                }
                catch (Exception)
                {
                    holdoffMilliseconds = 3000;
                }

                // Try to ensure there are no open objects with file handles
                AppUtils.GarbageCollectNow();
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
                        ShowTraceMessage("Error deleting file {0}: {1}", fileToDelete.FullName, ex.Message);

                        // Make sure the ReadOnly and System attributes are not set
                        // The manager will try to delete the file the next time is starts
                        fileToDelete.Attributes = fileToDelete.Attributes & ~FileAttributes.ReadOnly & ~FileAttributes.System;
                    }
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting files in working directory";
                LogError("ToolRunnerBase.CleanWorkDir(), " + failureMessage + " " + workDir, ex);
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
                        ShowTraceMessage("Error deleting subdirectory {0}: {1}", subDirectory.FullName, ex.Message);

                        // Make sure the ReadOnly and System attributes are not set
                        // The manager will try to delete the file the next time is starts
                        subDirectory.Attributes = subDirectory.Attributes & ~FileAttributes.ReadOnly & ~FileAttributes.System;
                    }
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting subdirectories in the working directory";
                LogError("ToolRunnerBase.CleanWorkDir(), " + failureMessage, ex);
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
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        /// <param name="job"></param>
        /// <param name="statusChecker"></param>
        /// <param name="statusURI"></param>
        /// <param name="returnData"></param>
        /// <param name="serverResponse">Server response (dictionary representation of JSON)</param>
        /// <param name="currentTask">Output: current task</param>
        /// <param name="percentComplete">Output: ingest process percent complete (value between 0 and 100)</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        protected bool GetMyEMSLIngestStatus(
            int job,
            MyEMSLStatusCheck statusChecker,
            string statusURI,
            ToolReturnData returnData,
            out MyEMSLTaskStatus serverResponse,
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
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = errorMessage;
                LogError(errorMessage + ", job " + job);

                return false;
            }

            if (!serverResponse.Valid)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = "Empty JSON server response, or invalid data; see " + statusURI;
                LogError(returnData.CloseoutMsg + ", job " + job);
                return false;
            }

            var ingestState = serverResponse.State;

            if (!string.Equals(ingestState, "failed", StringComparison.OrdinalIgnoreCase) &&
                string.IsNullOrWhiteSpace(errorMessage))
            {
                return true;
            }

            // Error should have already been logged during the call to GetIngestStatus
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

            if (string.IsNullOrWhiteSpace(errorMessage))
            {
                returnData.CloseoutMsg = "Ingest failed; unknown reason";
            }
            else
            {
                returnData.CloseoutMsg = errorMessage;
            }

            returnData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
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

        /// <summary>
        /// This method looks for files in the .d directory that indicate that IMS data was acquired
        /// </summary>
        /// <remarks>
        /// Searches for the expected files in both the dataset directory and in all of its subdirectories
        /// </remarks>
        /// <param name="datasetDirectory">Dataset directory (typically the .d directory)</param>
        /// <returns>True if the IMS files are found, otherwise false</returns>
        protected bool IsAgilentIMSDataset(DirectoryInfo datasetDirectory)
        {
            if (!datasetDirectory.Exists)
                return false;

            var filesToFind = new List<string>
            {
                // ReSharper disable StringLiteralTypo
                "IMSFrame.bin",
                "IMSFrame.xsd",
                "IMSFrameMeth.xml"
                // ReSharper restore StringLiteralTypo
            };

            var foundFilePaths = new List<string>();

            foreach (var fileToFind in filesToFind)
            {
                var foundFiles = datasetDirectory.GetFiles(fileToFind, SearchOption.AllDirectories).ToList();

                if (foundFiles.Count == 0)
                    continue;

                foundFilePaths.Add(foundFiles[0].FullName);
            }

            return foundFilePaths.Count == filesToFind.Count;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime lockQueueWaitTimeStart, ref DateTime lastLockQueueWaitTimeLog)
        {
            int waitTimeLogIntervalSeconds;

            if (lockQueueWaitTimeStart == DateTime.MinValue)
            {
                lockQueueWaitTimeStart = DateTime.UtcNow;
            }

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

            return DateTime.UtcNow.Subtract(lastLockQueueWaitTimeLog).TotalSeconds >= waitTimeLogIntervalSeconds;
        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInfoFilePath"></param>
        /// <param name="version"></param>
        /// <returns>True if successful, false if an error</returns>
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

                using var reader = new StreamReader(new FileStream(versionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

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
                            version = value;

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
        private void SaveToolVersionInfoFile(string directoryPath, string toolVersionInfo)
        {
            try
            {
                var toolVersionFilePath = Path.Combine(directoryPath, "Tool_Version_Info_" + mTaskParams.GetParam("StepTool") + ".txt");

                using var toolVersionWriter = new StreamWriter(new FileStream(toolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                toolVersionWriter.WriteLine("Date: " + DateTime.Now.ToString(DATE_TIME_FORMAT));
                toolVersionWriter.WriteLine("Dataset: " + mDataset);
                toolVersionWriter.WriteLine("Job: " + mJob);
                toolVersionWriter.WriteLine("Step: " + mTaskParams.GetParam("Step"));
                toolVersionWriter.WriteLine("Tool: " + mTaskParams.GetParam("StepTool"));
                toolVersionWriter.WriteLine("ToolVersionInfo:");

                toolVersionWriter.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));
                toolVersionWriter.Close();
            }
            catch (Exception ex)
            {
                LogError("Exception saving tool version info: " + ex.Message);
            }
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
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
                toolVersionInfoCombined = toolVersionInfo;
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
            var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

            // Call the procedure (retry the call, up to 4 times)
            var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 4);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
            {
                return true;
            }

            if (resCode != 0 && returnCode == 0)
            {
                LogError("ExecuteSP() reported result code {0} storing tool version for current processing step, job {1}", resCode, mJob);
                return false;
            }

            LogError("Stored procedure {0} reported return code {1}, job {2}",
                SP_NAME_SET_TASK_TOOL_VERSION, returnCodeParam.Value.CastDBVal<string>(), mJob);

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
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        [StringFormatMethod("format")]
        public static void ShowTraceMessage(string format, params object[] args)
        {
            ShowTraceMessage(string.Format(format, args));
        }

        /// <summary>
        /// Determines the version info for a DLL using reflection
        /// </summary>
        /// <remarks>Used by CTM plugins</remarks>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        protected virtual bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string dllFilePath)
        {
            var success = false;

            try
            {
                var dllFile = new FileInfo(dllFilePath);

                if (!dllFile.Exists)
                {
                    LogWarning("File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
                    return false;
                }

                //var assemblyName = Assembly.LoadFrom(dllFile.FullName).GetName(); // Throws BadImageFormatException if x86/x64 don't match, or not a valid assembly; loads the assembly into the current domain
                var assemblyName = AssemblyName.GetAssemblyName(dllFile.FullName); // Throws BadImageFormatException if not a valid assembly; does not load the assembly into the current domain

                if (!string.IsNullOrWhiteSpace(assemblyName.Name) && assemblyName.Version != null)
                {
                    var nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                    toolVersionInfo = AppendToComment(toolVersionInfo, nameAndVersion);
                    return true;
                }
            }
            catch (BadImageFormatException)
            {
                // If the exe is a .NET Core app host, there is not a valid manifest (it's a native binary); get the version from the respective dll
                if (dllFilePath.EndsWith(".exe", StringComparison.OrdinalIgnoreCase))
                {
                    var dllPath = Path.ChangeExtension(dllFilePath, ".dll");
                    if (File.Exists(dllPath))
                    {
                        return StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath);
                    }
                }

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
        protected bool StoreToolVersionInfoOneFile32Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <remarks>Used by CTM plugins</remarks>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
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
        protected bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string dllFilePath,
                                                         string versionInspectorExeName)
        {
            try
            {
                var versionInspectorAppPath = Path.Combine(CTMUtilities.GetAppDirectoryPath(), versionInspectorExeName);

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

                var versionInfoFilePath = Path.Combine(
                    mWorkDir,
                    Path.GetFileNameWithoutExtension(dllFile.Name) + "_VersionInfo.txt");

                var args = Conversion.PossiblyQuotePath(dllFile.FullName) + " /O:" +
                           Conversion.PossiblyQuotePath(versionInfoFilePath);

                var progRunner = new RunDosProgram(CTMUtilities.GetAppDirectoryPath(), mDebugLevel)
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

                AppUtils.SleepMilliseconds(100);

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out var version);

                // Delete the version info file
                try
                {
                    if (File.Exists(versionInfoFilePath))
                    {
                        AppUtils.SleepMilliseconds(100);
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
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        /// <param name="statusNum">MyEMSL Status number</param>
        /// <param name="ingestStepsCompleted">Number of completed ingest steps</param>
        /// <param name="transactionId">
        /// TransactionID for the given status item; if 0, the Transaction ID is not updated
        /// Prior to July 2017, TransactionID was the transactionID used by the majority of the verified files
        /// Starting in July 2017, StatusNum and TransactionID are identical
        /// </param>
        /// <param name="fatalError">True if ingest failed with a fatal error and thus the error code should be updated in T_MyEMSL_Uploads</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        protected bool UpdateIngestStepsCompletedOneTask(
            int statusNum,
            byte ingestStepsCompleted,
            long transactionId = 0,
            bool fatalError = false)
        {
            const string SP_NAME = "update_myemsl_upload_ingest_stats";

            if (transactionId > 0 && transactionId != statusNum)
            {
                // Starting in July 2017, StatusNum and TransactionID are identical
                // Assure that they match
                transactionId = statusNum;
            }

            var dbTools = mCaptureDbProcedureExecutor;

            var dbServerType = DbToolsFactory.GetServerTypeFromConnectionString(dbTools.ConnectStr);

            var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

            // Note that if transactionId is 0, the stored procedure will leave TransactionID unchanged in table T_MyEMSL_Uploads

            dbTools.AddTypedParameter(cmd, "@datasetID", SqlType.Int, value: mDatasetID);
            dbTools.AddTypedParameter(cmd, "@statusNum", SqlType.Int, value: statusNum);
            dbTools.AddTypedParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt, value: ingestStepsCompleted);

            if (dbServerType == DbServerTypes.PostgreSQL)
            {
                dbTools.AddTypedParameter(cmd, "@fatalError", SqlType.Boolean, value: fatalError);
            }
            else
            {
                dbTools.AddTypedParameter(cmd, "@fatalError", SqlType.TinyInt, value: fatalError ? 1 : 0);
            }

            dbTools.AddTypedParameter(cmd, "@transactionId", SqlType.Int, value: transactionId);
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);
            var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

            mCaptureDbProcedureExecutor.TimeoutSeconds = 20;
            var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
            {
                return true;
            }

            if (resCode != 0 && returnCode == 0)
            {
                LogError("ExecuteSP() reported result code {0} calling stored procedure {1}, job {2}", resCode, SP_NAME, mJob);
                return false;
            }

            LogError("Stored procedure {0} reported return code {1}, job {2}",
                SP_NAME, returnCodeParam.Value.CastDBVal<string>(), mJob);

            return false;
        }

        private void FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            LogWarning("Lockfile queue timed out after {0:F0} minutes; Source={1}, Target={2}",
                waitTimeMinutes, sourceFilePath, targetFilePath);
        }

        private void FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (waitTimeMinutes >= 1)
            {
                LogMessage("Exited lockfile queue after {0:F0} minutes; will now copy file", waitTimeMinutes);
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
                {
                    ConsoleMsgUtils.ShowDebug(message);
                }

                return;
            }

            LogMessage(message);
        }

        private void FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (!IsLockQueueLogMessageNeeded(ref mLockQueueWaitTimeStart, ref mLastLockQueueWaitTimeLog))
            {
                return;
            }

            mLastLockQueueWaitTimeLog = DateTime.UtcNow;
            LogMessage("Waiting for lockfile queue to fall below threshold; " +
                       "SourceBacklog={0:N0} MB, TargetBacklog={1:N0} MB, " +
                       "Source={2}, Target={3}",
                backlogSourceMB, backlogTargetMB, sourceFilePath, targetFilePath);
        }

        private void FileTools_WaitingForLockQueueNotifyLockFilePaths(string sourceLockFilePath, string targetLockFilePath, string adminBypassMessage)
        {
            if (string.IsNullOrWhiteSpace(adminBypassMessage))
            {
                LogMessage("Waiting for lockfile queue to fall below threshold; see lock file(s) at {0} and {1}",
                    sourceLockFilePath ?? "(n/a)", targetLockFilePath ?? "(n/a)");
                return;
            }

            LogMessage(adminBypassMessage);
        }
    }
}
