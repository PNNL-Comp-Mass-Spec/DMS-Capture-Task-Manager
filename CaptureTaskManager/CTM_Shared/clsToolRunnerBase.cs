//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Pacifica.Core;
using PRISM;
using PRISM.Logging;

namespace CaptureTaskManager
{
    /// <summary>
    /// Base class for capture step tool plugins
    /// </summary>
    /// <remarks>Used in CaptureTaskManager.clsMainProgram</remarks>
    public class clsToolRunnerBase : clsLoggerBase, IToolRunner
    {

        #region "Constants"

        private const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";
        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        #endregion

        #region "Class variables"

        protected IMgrParams m_MgrParams;
        protected ITaskParams m_TaskParams;

        // Used by CTM plugins
        protected IStatusFile m_StatusTools;

        // Used by CTM plugins
        protected clsFileTools m_FileTools;

        protected clsExecuteDatabaseSP m_CaptureDBProcedureExecutor;

        protected DateTime m_LastConfigDBUpdate = DateTime.UtcNow;
        protected int m_MinutesBetweenConfigDBUpdates = 10;

        // Used by CTM plugins
        // ReSharper disable once UnusedMember.Global
        protected bool m_NeedToAbortProcessing = false;

        protected string m_WorkDir;

        protected string m_Dataset;

        protected int m_Job;

        protected int m_DatasetID;

        protected string m_MgrName;

        /// <summary>
        /// LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
        /// </summary>
        protected short m_DebugLevel;

        protected bool m_TraceMode;

        private DateTime m_LastLockQueueWaitTimeLog = DateTime.UtcNow;

        private DateTime m_LockQueueWaitTimeStart = DateTime.UtcNow;

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
            m_MgrParams = mgrParams;
            m_TaskParams = taskParams;
            m_StatusTools = statusTools;

            m_MgrName = m_MgrParams.GetParam("MgrName", "CaptureTaskManager");

            // This connection string points to the DMS_Capture database
            var connectionString = m_MgrParams.GetParam("ConnectionString");
            m_CaptureDBProcedureExecutor = new clsExecuteDatabaseSP(connectionString);

            RegisterEvents(m_CaptureDBProcedureExecutor);

            m_WorkDir = m_MgrParams.GetParam("WorkDir");

            m_Dataset = m_TaskParams.GetParam("Dataset");

            m_DatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

            m_Job = m_TaskParams.GetParam("Job", 0);

            // Debug level 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            // Log level 4 will also log error messages
            m_DebugLevel = (short)m_MgrParams.GetParam("DebugLevel", 4);
            LogTools.SetFileLogLevel(m_DebugLevel);

            m_TraceMode = m_MgrParams.GetParam("TraceMode", false);

            InitFileTools(m_MgrName, m_DebugLevel);
        }

        // Used by CTM plugins
        // ReSharper disable once UnusedMember.Global
        protected bool UpdateMgrSettings()
        {
            var bSuccess = true;

            if (m_MinutesBetweenConfigDBUpdates < 1)
                m_MinutesBetweenConfigDBUpdates = 1;

            if (DateTime.UtcNow.Subtract(m_LastConfigDBUpdate).TotalMinutes >= m_MinutesBetweenConfigDBUpdates)
            {
                m_LastConfigDBUpdate = DateTime.UtcNow;

                LogDebug("Updating manager settings using Manager Control database");

                if (!m_MgrParams.LoadMgrSettingsFromDB(logConnectionErrors: false))
                {
                    // Error retrieving settings from the manager control DB
                    LogWarning("Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings");

                    bSuccess = false;
                }
                else
                {
                    // Update the log level
                    m_DebugLevel = (short)m_MgrParams.GetParam("DebugLevel", 4);
                    LogTools.SetFileLogLevel(m_DebugLevel);
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
            var strCurrentSubfolder = string.Empty;

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
                clsProgRunner.GarbageCollectNow();
                Thread.Sleep(holdoffMilliseconds);
            }

            var diWorkFolder = new DirectoryInfo(workDir);

            // Delete the files
            try
            {
                if (!diWorkFolder.Exists)
                {
                    failureMessage = "Working directory does not exist";
                    return false;
                }

                foreach (var fiFile in diWorkFolder.GetFiles())
                {
                    try
                    {
                        fiFile.Delete();
                    }
                    catch (Exception)
                    {
                        // Make sure the readonly bit is not set
                        // The manager will try to delete the file the next time is starts
                        fiFile.Attributes = fiFile.Attributes & (~FileAttributes.ReadOnly);
                    }
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting files in working directory";
                LogError("clsGlobal.ClearWorkDir(), " + failureMessage + " " + workDir, ex);
                return false;
            }

            // Delete the sub directories
            try
            {
                foreach (var diSubDirectory in diWorkFolder.GetDirectories())
                {
                    diSubDirectory.Delete(true);
                }
            }
            catch (Exception ex)
            {
                failureMessage = "Error deleting subfolder " + strCurrentSubfolder;
                LogError(failureMessage + " in working directory", ex);
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
        /// <param name="eusInstrumentID"></param>
        /// <param name="eusProposalID"></param>
        /// <param name="eusUploaderID"></param>
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
            int eusInstrumentID,
            string eusProposalID,
            int eusUploaderID,
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
                retData.CloseoutMsg = errorMessage;
                LogError(errorMessage + ", job " + job);

                return false;
            }

            if (serverResponse.Keys.Count == 0)
            {
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
        /// Initialize m_FileTools
        /// </summary>
        /// <param name="mgrName"></param>
        /// <param name="debugLevel"></param>
        protected void InitFileTools(string mgrName, short debugLevel)
        {
            ResetTimestampForQueueWaitTimeLogging();
            m_FileTools = new clsFileTools(mgrName, debugLevel);
            RegisterEvents(m_FileTools, false);

            // Use a custom event handler for status messages
            UnregisterEventHandler(m_FileTools, BaseLogger.LogLevels.INFO);
            m_FileTools.StatusEvent += m_FileTools_StatusEvent;

            m_FileTools.LockQueueTimedOut += m_FileTools_LockQueueTimedOut;
            m_FileTools.LockQueueWaitComplete += m_FileTools_LockQueueWaitComplete;
            m_FileTools.WaitingForLockQueue += m_FileTools_WaitingForLockQueue;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime dtLockQueueWaitTimeStart, ref DateTime dtLastLockQueueWaitTimeLog)
        {

            int waitTimeLogIntervalSeconds;

            if (dtLockQueueWaitTimeStart == DateTime.MinValue)
                dtLockQueueWaitTimeStart = DateTime.UtcNow;

            var waitTimeMinutes = DateTime.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes;

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

            if (DateTime.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= waitTimeLogIntervalSeconds)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="dllFilePath"></param>
        /// <param name="strVersionInfoFilePath"></param>
        /// <param name="strVersion"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ReadVersionInfoFile(string dllFilePath, string strVersionInfoFilePath, out string strVersion)
        {
            // Open strVersionInfoFilePath and read the Version= line

            strVersion = string.Empty;

            try
            {
                if (!File.Exists(strVersionInfoFilePath))
                {
                    LogError("Version Info File not found: " + strVersionInfoFilePath);
                    return false;
                }

                var success = false;

                using (var reader = new StreamReader(new FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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
                                strVersion = string.Copy(value);
                                if (string.IsNullOrWhiteSpace(strVersion))
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
            m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
            m_LockQueueWaitTimeStart = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="strFolderPath"></param>
        /// <param name="toolVersionInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private void SaveToolVersionInfoFile(string strFolderPath, string toolVersionInfo)
        {
            try
            {
                var strToolVersionFilePath = Path.Combine(strFolderPath, "Tool_Version_Info_" + m_TaskParams.GetParam("StepTool") + ".txt");

                using (var swToolVersionFile = new StreamWriter(new FileStream(strToolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    swToolVersionFile.WriteLine("Date: " + DateTime.Now.ToString(DATE_TIME_FORMAT));
                    swToolVersionFile.WriteLine("Dataset: " + m_Dataset);
                    swToolVersionFile.WriteLine("Job: " + m_Job);
                    swToolVersionFile.WriteLine("Step: " + m_TaskParams.GetParam("Step"));
                    swToolVersionFile.WriteLine("Tool: " + m_TaskParams.GetParam("StepTool"));
                    swToolVersionFile.WriteLine("ToolVersionInfo:");

                    swToolVersionFile.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));
                    swToolVersionFile.Close();
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
            var strExeInfo = string.Empty;
            string toolVersionInfoCombined;

            if (string.IsNullOrWhiteSpace(m_WorkDir))
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
                            strExeInfo = AppendToComment(strExeInfo,
                                                         toolFile.Name + ": " +
                                                         toolFile.LastWriteTime.ToString(DATE_TIME_FORMAT));

                            var writeToLog = m_DebugLevel >= 5;
                            LogDebug("EXE Info: " + strExeInfo, writeToLog);
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
            if (string.IsNullOrEmpty(strExeInfo))
            {
                toolVersionInfoCombined = string.Copy(toolVersionInfo);
            }
            else
            {
                toolVersionInfoCombined = AppendToComment(toolVersionInfo, strExeInfo);
            }

            if (saveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(m_WorkDir, toolVersionInfoCombined);
            }

            // Setup for execution of the stored procedure
            var spCmd = new SqlCommand(SP_NAME_SET_TASK_TOOL_VERSION)
            {
                CommandType = CommandType.StoredProcedure
            };

            spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

            spCmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = m_TaskParams.GetParam("Job", 0);

            spCmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = m_TaskParams.GetParam("Step", 0);

            spCmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900)).Value = toolVersionInfoCombined;

            // Execute the SP (retry the call up to 4 times)
            var resCode = m_CaptureDBProcedureExecutor.ExecuteSP(spCmd, 4);

            if (resCode == 0)
            {
                return true;
            }

            LogError("Error " + resCode + " storing tool version for current processing step");
            return false;
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTraceMessage(string message)
        {
            ConsoleMsgUtils.ShowDebug(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + message);
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
                var fiFile = new FileInfo(dllFilePath);

                if (!fiFile.Exists)
                {
                    LogWarning("File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
                    return false;
                }

                var oAssemblyName = Assembly.LoadFrom(fiFile.FullName).GetName();

                var strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
                toolVersionInfo = AppendToComment(toolVersionInfo, strNameAndVersion);

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
                var ioFileInfo = new FileInfo(dllFilePath);

                if (!ioFileInfo.Exists)
                {
                    LogWarning("File not found by StoreToolVersionInfoViaSystemDiagnostics: " + dllFilePath);
                    return false;
                }

                var oFileVersionInfo = FileVersionInfo.GetVersionInfo(dllFilePath);

                var strName = oFileVersionInfo.FileDescription;
                if (string.IsNullOrEmpty(strName))
                {
                    strName = oFileVersionInfo.InternalName;
                }

                if (string.IsNullOrEmpty(strName))
                {
                    strName = oFileVersionInfo.FileName;
                }

                if (string.IsNullOrEmpty(strName))
                {
                    strName = ioFileInfo.Name;
                }

                var strVersion = oFileVersionInfo.FileVersion;
                if (string.IsNullOrEmpty(strVersion))
                {
                    strVersion = oFileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrEmpty(strVersion))
                {
                    strVersion = "??";
                }

                var strNameAndVersion = strName + ", Version=" + strVersion;
                toolVersionInfo = AppendToComment(toolVersionInfo, strNameAndVersion);

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
                var strAppPath = Path.Combine(clsUtilities.GetAppDirectoryPath(), versionInspectorExeName);

                var fiDLLFile = new FileInfo(dllFilePath);

                if (!fiDLLFile.Exists)
                {
                    LogError("File not found by StoreToolVersionInfoOneFileUseExe: " + dllFilePath);
                    return false;
                }

                if (!File.Exists(strAppPath))
                {
                    LogError("DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe: " + strAppPath);
                    return false;
                }

                // Call DLLVersionInspector.exe to determine the tool version

                var versionInfoFilePath = Path.Combine(m_WorkDir,
                                                       Path.GetFileNameWithoutExtension(fiDLLFile.Name) +
                                                       "_VersionInfo.txt");

                var strArgs = clsConversion.PossiblyQuotePath(fiDLLFile.FullName) + " /O:" +
                              clsConversion.PossiblyQuotePath(versionInfoFilePath);

                var progRunner = new clsRunDosProgram(clsUtilities.GetAppDirectoryPath(), m_DebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250
                };

                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                Thread.Sleep(100);

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out var strVersion);

                // Delete the version info file
                try
                {
                    if (File.Exists(versionInfoFilePath))
                    {
                        Thread.Sleep(100);
                        File.Delete(versionInfoFilePath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (!success || string.IsNullOrWhiteSpace(strVersion))
                {
                    return false;
                }

                toolVersionInfo = AppendToComment(toolVersionInfo, strVersion);

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

            var spCmd = new SqlCommand(SP_NAME)
            {
                CommandType = CommandType.StoredProcedure
            };

            spCmd.Parameters.Add("@Return", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;

            spCmd.Parameters.Add("@datasetID", SqlDbType.Int).Value = m_DatasetID;

            spCmd.Parameters.Add("@statusNum", SqlDbType.Int).Value = statusNum;

            spCmd.Parameters.Add("@ingestStepsCompleted", SqlDbType.TinyInt).Value = ingestStepsCompleted;

            spCmd.Parameters.Add("@fatalError", SqlDbType.TinyInt).Value = fatalError ? 1 : 0;

            // Note that if transactionId is 0, the stored procedure will leave TransactionID unchanged in table T_MyEMSL_Uploads
            spCmd.Parameters.Add("@transactionId", SqlDbType.Int).Value = transactionId;

            spCmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

            m_CaptureDBProcedureExecutor.TimeoutSeconds = 20;
            var resCode = m_CaptureDBProcedureExecutor.ExecuteSP(spCmd, 2);

            if (resCode != 0)
            {
                LogError("Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job);
                return false;
            }

            return true;
        }

        #endregion


        #region "Event Handlers"

        private void m_FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            var msg = "Lockfile queue timed out after " + waitTimeMinutes.ToString("0") + " minutes; Source=" + sourceFilePath + ", Target=" + targetFilePath;
            LogWarning(msg);
        }

        private void m_FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (waitTimeMinutes >= 1)
            {
                var msg = "Exited lockfile queue after " + waitTimeMinutes.ToString("0") + " minutes; will now copy file";
                LogMessage(msg);
            }
        }

        private void m_FileTools_StatusEvent(string message)
        {
            // Do not log certain common messages
            if (message.StartsWith("Created lock file") ||
                message.StartsWith("Copying file with CopyFileEx") ||
                message.StartsWith("File to copy is") && message.Contains("will use CopyFileEx for"))
            {
                if (m_TraceMode)
                    ConsoleMsgUtils.ShowDebug(message);

                return;
            }

            LogMessage(message);
        }

        private void m_FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (IsLockQueueLogMessageNeeded(ref m_LockQueueWaitTimeStart, ref m_LastLockQueueWaitTimeLog))
            {
                m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
                var msg = "Waiting for lockfile queue to fall below threshold; " +
                          "SourceBacklog=" + backlogSourceMB + " MB, " +
                          "TargetBacklog=" + backlogTargetMB + " MB, " +
                          "Source=" + sourceFilePath + ", " +
                          "Target=" + targetFilePath;
                LogMessage(msg);
            }

        }

        #endregion

        #region "clsEventNotifier events"

        /// <summary>
        /// Register event handlers
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="writeDebugEventsToLog"></param>
        protected void RegisterEvents(clsEventNotifier processingClass, bool writeDebugEventsToLog = true)
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
        protected void UnregisterEventHandler(clsEventNotifier processingClass, BaseLogger.LogLevels messageType)
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

        private void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            if (statusMessage.StartsWith("RunProgram") && statusMessage.Contains("DLLVersionInspector"))
                LogDebug(statusMessage, writeToLog: false);
            else
                LogMessage(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        #endregion
    }
}