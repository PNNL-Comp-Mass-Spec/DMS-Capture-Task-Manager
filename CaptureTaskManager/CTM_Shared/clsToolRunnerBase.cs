//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using Pacifica.Core;
using PRISM;

namespace CaptureTaskManager
{
    // ReSharper disable once ClassNeverInstantiated.Global
    // Used in CaptureTaskManager.clsMainProgram
    public class clsToolRunnerBase : clsLoggerBase, IToolRunner
    {
        //*********************************************************************************************************
        // Base class for capture step tool plugins
        //**********************************************************************************************************

        #region "Constants"

        private const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";
        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        #endregion

        #region "Class variables"

        protected IMgrParams m_MgrParams;
        protected ITaskParams m_TaskParams;

        // ReSharper disable once NotAccessedField.Global
        // Used by CTM plugins
        protected IStatusFile m_StatusTools;

        // ReSharper disable once NotAccessedField.Global
        // Used by CTM plugins
        protected clsFileTools m_FileTools;

        protected clsExecuteDatabaseSP CaptureDBProcedureExecutor;

        protected DateTime m_LastConfigDBUpdate = DateTime.UtcNow;
        protected int m_MinutesBetweenConfigDBUpdates = 10;

        // ReSharper disable once UnusedMember.Global
        // Used by CTM plugins
        protected bool m_NeedToAbortProcessing = false;

        protected string m_WorkDir;
        protected string m_Dataset;
        protected int m_Job;

        protected int m_DatasetID;

        protected string m_MgrName;

        /// <summary>
        /// LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
        /// </summary>
        protected int m_DebugLevel;

        protected bool m_TraceMode;

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
            m_FileTools = new clsFileTools(m_MgrName, 1);

            // This Connection String points to the DMS_Capture database
            var sConnectionString = m_MgrParams.GetParam("connectionstring");
            CaptureDBProcedureExecutor = new clsExecuteDatabaseSP(sConnectionString);

            RegisterEvents(CaptureDBProcedureExecutor);

            m_WorkDir = m_MgrParams.GetParam("workdir");

            m_Dataset = m_TaskParams.GetParam("Dataset");

            m_DatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

            m_Job = m_TaskParams.GetParam("Job", 0);

            // Debug level 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            // Log level 4 will also log error messages
            m_DebugLevel = m_MgrParams.GetParam("debuglevel", 4);

            m_TraceMode = m_MgrParams.GetParam("TraceMode", false);
        }

        // ReSharper disable once UnusedMember.Global
        // Used by CTM plugins
        protected bool UpdateMgrSettings()
        {
            var bSuccess = true;

            if (m_MinutesBetweenConfigDBUpdates < 1)
                m_MinutesBetweenConfigDBUpdates = 1;

            if (DateTime.UtcNow.Subtract(m_LastConfigDBUpdate).TotalMinutes >= m_MinutesBetweenConfigDBUpdates)
            {
                m_LastConfigDBUpdate = DateTime.UtcNow;

                LogDebug("Updating manager settings using Manager Control database");

                const bool logConnectionErrors = false;
                if (!m_MgrParams.LoadMgrSettingsFromDB(logConnectionErrors))
                {
                    // Error retrieving settings from the manager control DB
                    LogWarning("Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings");

                    bSuccess = false;
                }
                else
                {
                    // Update the log level
                    m_DebugLevel = m_MgrParams.GetParam("debuglevel", 4);
                    clsLogTools.SetFileLogLevel(m_DebugLevel);
                }
            }

            return bSuccess;
        }

        protected string AppendToComment(string InpComment, string NewComment)
        {
            // Appends a comment string to an existing comment string

            if (string.IsNullOrWhiteSpace(InpComment))
            {
                return NewComment;
            }

            // Append a semicolon to InpComment, but only if it doesn't already end in a semicolon
            if (!InpComment.TrimEnd(' ').EndsWith(";"))
            {
                InpComment += "; ";
            }

            return InpComment + NewComment;
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

            return CleanWorkDir(workDir, HoldoffSeconds, out string strFailureMessage);
        }

        /// <summary>
        /// Delete files in the working directory
        /// </summary>
        /// <param name="workDir">Working directory path</param>
        /// <param name="holdoffSeconds"></param>
        /// <param name="failureMessage">Output: failure message</param>
        /// <returns></returns>
        public static bool CleanWorkDir(string workDir, float holdoffSeconds, out string failureMessage)
        {
            int HoldoffMilliseconds;

            var strCurrentSubfolder = string.Empty;

            failureMessage = string.Empty;

            try
            {
                HoldoffMilliseconds = Convert.ToInt32(holdoffSeconds * 1000);
                if (HoldoffMilliseconds < 100)
                    HoldoffMilliseconds = 100;
                if (HoldoffMilliseconds > 300000)
                    HoldoffMilliseconds = 300000;
            }
            catch (Exception)
            {
                HoldoffMilliseconds = 3000;
            }

            // Try to ensure there are no open objects with file handles
            clsProgRunner.GarbageCollectNow();
            Thread.Sleep(HoldoffMilliseconds);

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

        // ReSharper disable once UnusedMember.Global
        // Used by CTM plugins
        protected void DeleteFileIgnoreErrors(string sFilePath)
        {
            try
            {
                File.Delete(sFilePath);
            }
                // ReSharper disable once EmptyGeneralCatchClause
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
            out int percentComplete)
        {
            serverResponse = statusChecker.GetIngestStatus(
                statusURI,
                out percentComplete,
                out bool lookupError,
                out string errorMessage);

            if (lookupError)
            {
                retData.CloseoutMsg = errorMessage;
                LogError(errorMessage + ", job " + job);

                // These are obsolete messages from the old status service
                if (errorMessage.Contains("[Errno 5] Input/output error") ||
                    errorMessage.Contains("[Errno 28] No space left on device") ||
                    errorMessage.Contains("object has no attribute") ||
                    errorMessage.Contains("invalid literal for int"))
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                }

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
                if (string.Equals((string)ingestState, "failed", StringComparison.InvariantCultureIgnoreCase) ||
                    !string.IsNullOrWhiteSpace(errorMessage))
                {
                    // Error should have already been logged
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

                using (var srInFile = new StreamReader(new FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }
                        var intEqualsLoc = strLineIn.IndexOf('=');

                        if (intEqualsLoc <= 0)
                        {
                            continue;
                        }

                        var strKey = strLineIn.Substring(0, intEqualsLoc);
                        var strValue = string.Empty;

                        if (intEqualsLoc < strLineIn.Length)
                        {
                            strValue = strLineIn.Substring(intEqualsLoc + 1);
                        }

                        switch (strKey.ToLower())
                        {
                            case "filename":
                                break;
                            case "path":
                                break;
                            case "version":
                                strVersion = string.Copy(strValue);
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
                                         Path.GetFileName(dllFilePath) + ": " + strValue);
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
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="strFolderPath"></param>
        /// <param name="toolVersionInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool SaveToolVersionInfoFile(string strFolderPath, string toolVersionInfo)
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
                return false;
            }

            return true;
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool SetStepTaskToolVersion(string toolVersionInfo)
        {
            return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo, IReadOnlyList<FileInfo> ioToolFiles)
        {
            return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles, false);
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="blnSaveToolVersionTextFile">If true, then creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo, IReadOnlyList<FileInfo> ioToolFiles,
                                              bool blnSaveToolVersionTextFile)
        {
            var strExeInfo = string.Empty;
            string toolVersionInfoCombined;

            bool Outcome;

            if (string.IsNullOrWhiteSpace(m_WorkDir))
            {
                return false;
            }

            if (ioToolFiles != null)
            {
                foreach (var fiFile in ioToolFiles)
                {
                    try
                    {
                        if (fiFile.Exists)
                        {
                            strExeInfo = AppendToComment(strExeInfo,
                                                         fiFile.Name + ": " +
                                                         fiFile.LastWriteTime.ToString(DATE_TIME_FORMAT));

                            var writeToLog = m_DebugLevel >= 5;
                            LogDebug("EXE Info: " + strExeInfo, writeToLog);
                        }
                        else
                        {
                            LogWarning("Tool file not found: " + fiFile.FullName);
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

            if (blnSaveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(m_WorkDir, toolVersionInfoCombined);
            }

            // Setup for execution of the stored procedure
            var spCmd = new SqlCommand(SP_NAME_SET_TASK_TOOL_VERSION)
            {
                CommandType = System.Data.CommandType.StoredProcedure
            };

            spCmd.Parameters.Add(new SqlParameter("@Return", System.Data.SqlDbType.Int)).Direction = System.Data.ParameterDirection.ReturnValue;

            spCmd.Parameters.Add(new SqlParameter("@job", System.Data.SqlDbType.Int)).Value = m_TaskParams.GetParam("Job", 0);

            spCmd.Parameters.Add(new SqlParameter("@step", System.Data.SqlDbType.Int)).Value = m_TaskParams.GetParam("Step", 0);

            spCmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", System.Data.SqlDbType.VarChar, 900)).Value = toolVersionInfoCombined;

            // Execute the SP (retry the call up to 4 times)
            var resCode = CaptureDBProcedureExecutor.ExecuteSP(spCmd, 4);

            if (resCode == 0)
            {
                Outcome = true;
            }
            else
            {
                LogError("Error " + resCode + " storing tool version for current processing step");
                Outcome = false;
            }

            return Outcome;
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTraceMessage(string message)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") + @": " + message);
        }

        /// <summary>
        /// Determines the version info for a DLL using reflection
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the veresion info to</param>
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
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
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
                var strAppPath = Path.Combine(clsUtilities.GetAppFolderPath(), versionInspectorExeName);

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


                var objProgRunner = new clsRunDosProgram(clsUtilities.GetAppFolderPath())
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250
                };

                RegisterEvents(objProgRunner);

                var success = objProgRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                Thread.Sleep(100);

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out string strVersion);

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
        /// <param name="transactionId">TransactionID for the given status item; if 0, the Transaction ID is not updated</param>
        /// <param name="fatalError">True if ingest failed with a fatal error and thus the ErrorCode should be updated in T_MyEMSL_Uploads</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        // ReSharper disable once UnusedMember.Global
        protected bool UpdateIngestStepsCompletedOneTask(
            int statusNum,
            byte ingestStepsCompleted,
            long transactionId,
            bool fatalError = false)
        {
            const string SP_NAME = "UpdateMyEMSLUploadIngestStats";

            var spCmd = new SqlCommand(SP_NAME)
            {
                CommandType = System.Data.CommandType.StoredProcedure
            };

            spCmd.Parameters.Add("@Return", System.Data.SqlDbType.Int).Direction = System.Data.ParameterDirection.ReturnValue;

            spCmd.Parameters.Add("@datasetID", System.Data.SqlDbType.Int).Value = m_DatasetID;

            spCmd.Parameters.Add("@statusNum", System.Data.SqlDbType.Int).Value = statusNum;

            spCmd.Parameters.Add("@ingestStepsCompleted", System.Data.SqlDbType.TinyInt).Value = ingestStepsCompleted;

            spCmd.Parameters.Add("@fatalError", System.Data.SqlDbType.TinyInt).Value = fatalError ? 1 : 0;

            // Note that if transactionId is 0, the stored procedure will leave TransactionID unchanged in table T_MyEMSL_Uploads
            spCmd.Parameters.Add("@transactionId", System.Data.SqlDbType.Int).Value = transactionId;

            spCmd.Parameters.Add("@message", System.Data.SqlDbType.VarChar, 512).Direction = System.Data.ParameterDirection.Output;

            CaptureDBProcedureExecutor.TimeoutSeconds = 20;
            var resCode = CaptureDBProcedureExecutor.ExecuteSP(spCmd, 2);

            if (resCode != 0)
            {
                LogError("Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job);
                return false;
            }

            return true;
        }

        #endregion

        #region "clsEventNotifier events"

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

        protected void UnregisterEventHandler(clsEventNotifier processingClass, clsLogTools.LogLevels messageType)
        {
            switch (messageType)
            {
                case clsLogTools.LogLevels.DEBUG:
                    processingClass.DebugEvent -= DebugEventHandler;
                    processingClass.DebugEvent -= DebugEventHandlerConsoleOnly;
                    break;
                case clsLogTools.LogLevels.ERROR:
                    processingClass.ErrorEvent -= ErrorEventHandler;
                    break;
                case clsLogTools.LogLevels.WARN:
                    processingClass.WarningEvent -= WarningEventHandler;
                    break;
                case clsLogTools.LogLevels.INFO:
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