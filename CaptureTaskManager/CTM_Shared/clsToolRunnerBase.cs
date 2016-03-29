
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

namespace CaptureTaskManager
{
    public class clsToolRunnerBase : IToolRunner
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
        protected IStatusFile m_StatusTools;

        protected PRISM.Files.clsFileTools m_FileTools;

        public PRISM.DataBase.clsExecuteDatabaseSP CaptureDBProcedureExecutor;

        protected DateTime m_LastConfigDBUpdate = DateTime.UtcNow;
        protected int m_MinutesBetweenConfigDBUpdates = 10;
        protected bool m_NeedToAbortProcessing = false;

        protected string m_WorkDir;
        protected string m_Dataset;
        protected int m_Job;

        protected int m_DatasetID;
        protected int m_DebugLevel;

        #endregion

        #region "Delegates"
        #endregion

        #region "Events"
        #endregion

        #region "Properties"
        #endregion

        #region "Constructors"
        /// <summary>
        /// Constructor
        /// </summary>
        protected clsToolRunnerBase()
        {
            // Does nothing; see the Setup method for constructor-like behavior
        }	// End sub

        /// <summary>
        /// Destructor
        /// </summary>
        ~clsToolRunnerBase()
        {
            DetachExecuteSpEvents();
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
        }	// End sub

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

            m_FileTools = new PRISM.Files.clsFileTools(m_MgrParams.GetParam("MgrName", "CaptureTaskManager"), 1);

            // This Connection String points to the DMS_Capture database
            var sConnectionString = m_MgrParams.GetParam("connectionstring");
            CaptureDBProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(sConnectionString);

            AttachExecuteSpEvents();

            m_WorkDir = m_MgrParams.GetParam("workdir");

            m_Dataset = m_TaskParams.GetParam("Dataset");

            if (!int.TryParse(m_TaskParams.GetParam("Dataset_ID"), out m_DatasetID))
            {
                m_DatasetID = 0;
            }

            if (!int.TryParse(m_TaskParams.GetParam("Job"), out m_Job))
            {
                m_Job = 0;
            }

            m_DebugLevel = clsConversion.CIntSafe(m_MgrParams.GetParam("debuglevel"), 4);

        }

        protected bool UpdateMgrSettings()
        {
            var bSuccess = true;

            if (m_MinutesBetweenConfigDBUpdates < 1)
                m_MinutesBetweenConfigDBUpdates = 1;

            if (DateTime.UtcNow.Subtract(m_LastConfigDBUpdate).TotalMinutes >= m_MinutesBetweenConfigDBUpdates)
            {
                m_LastConfigDBUpdate = DateTime.UtcNow;

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings using Manager Control database");

                const bool logConnectionErrors = false;
                if (!m_MgrParams.LoadMgrSettingsFromDB(logConnectionErrors))
                {
                    // Error retrieving settings from the manager control DB
                    const string msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

                    bSuccess = false;
                }
                else
                {
                    // Update the log level
                    m_DebugLevel = clsConversion.CIntSafe(m_MgrParams.GetParam("debuglevel"), 4);
                    clsLogTools.SetFileLogLevel(m_DebugLevel);
                }
            }

            return bSuccess;
        }


        protected string AppendToComment(string InpComment, string NewComment)
        {

            //Appends a comment string to an existing comment string

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

        public static bool CleanWorkDir(string WorkDir)
        {
            const float HoldoffSeconds = 0.1f;
            string strFailureMessage;

            return CleanWorkDir(WorkDir, HoldoffSeconds, out strFailureMessage);
        }

        public static bool CleanWorkDir(string WorkDir, float HoldoffSeconds, out string strFailureMessage)
        {

            int HoldoffMilliseconds;

            var strCurrentSubfolder = string.Empty;

            strFailureMessage = string.Empty;

            try
            {
                HoldoffMilliseconds = Convert.ToInt32(HoldoffSeconds * 1000);
                if (HoldoffMilliseconds < 100)
                    HoldoffMilliseconds = 100;
                if (HoldoffMilliseconds > 300000)
                    HoldoffMilliseconds = 300000;
            }
            catch (Exception)
            {
                HoldoffMilliseconds = 3000;
            }

            //Try to ensure there are no open objects with file handles
            PRISM.Processes.clsProgRunner.GarbageCollectNow();
            Thread.Sleep(HoldoffMilliseconds);

            var diWorkFolder = new DirectoryInfo(WorkDir);

            // Delete the files
            try
            {
                if (!diWorkFolder.Exists)
                {
                    strFailureMessage = "Working directory does not exist";
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
                strFailureMessage = "Error deleting files in working directory";
                LogError("clsGlobal.ClearWorkDir(), " + strFailureMessage + " " + WorkDir + ": " + ex.Message);
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
                strFailureMessage = "Error deleting subfolder " + strCurrentSubfolder;
                LogError(strFailureMessage + " in working directory: " + ex.Message);
                return false;
            }

            return true;

        }

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
        /// <param name="cookieJar"></param>
        /// <param name="retData"></param>
        /// <param name="xmlServerResponse"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        public bool GetMyEMSLIngestStatus(
            int job,
            MyEMSLStatusCheck statusChecker,
            string statusURI,
            int eusInstrumentID, 
            string eusProposalID, 
            int eusUploaderID, 
            CookieContainer cookieJar,
            clsToolReturnData retData,
            out string xmlServerResponse)
        {
            bool lookupError;
            string errorMessage;

            xmlServerResponse = statusChecker.GetIngestStatus(statusURI, cookieJar, out lookupError, out errorMessage);

            if (lookupError)
            {
                retData.CloseoutMsg = errorMessage;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage + ", job " + job);

                if (errorMessage.Contains("[Errno 5] Input/output error") ||
                    errorMessage.Contains("[Errno 28] No space left on device"))
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                }

                return false;
            }

            if (string.IsNullOrEmpty(xmlServerResponse))
            {
                retData.CloseoutMsg = "Empty XML server response";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage + ", job " + job);
                return false;
            }

            // Look for any steps in error
            if (!statusChecker.HasStepError(xmlServerResponse, out errorMessage))
            {
                return true;
            }

            if (errorMessage.ToLower().StartsWith("invalid permissions"))
            {
                // Append the EUS proposal ID and EUS instrument ID that was used
                retData.CloseoutMsg = errorMessage + 
                                      "; EUSInstID=" + eusInstrumentID +
                                      ", EUSProposal=" + eusProposalID +
                                      ", EUSUploader=" + eusUploaderID;
            }
            else
            {
                retData.CloseoutMsg = errorMessage;
            }
                
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage + ", job " + job);
            return false;
        }

        protected static void LogError(string errorMessage, bool logToDatabase = false)
        {
            if (logToDatabase)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, errorMessage);
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
            }

        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="dllFilePath"></param>
        /// <param name="strVersionInfoFilePath"></param>
        /// <param name="strVersion"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ReadVersionInfoFile(string dllFilePath, string strVersionInfoFilePath, out string strVersion)
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading Version Info File for " + Path.GetFileName(dllFilePath), ex);
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
        protected bool SaveToolVersionInfoFile(string strFolderPath, string toolVersionInfo)
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
        protected bool SetStepTaskToolVersion(string toolVersionInfo, List<FileInfo> ioToolFiles)
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
        protected bool SetStepTaskToolVersion(string toolVersionInfo, List<FileInfo> ioToolFiles, bool blnSaveToolVersionTextFile)
        {

            var strExeInfo = string.Empty;
            string toolVersionInfoCombined;

            bool Outcome;

            if (string.IsNullOrWhiteSpace(m_WorkDir))
            {
                return false;
            }

            if ((ioToolFiles != null))
            {
                foreach (var fiFile in ioToolFiles)
                {
                    try
                    {
                        if (fiFile.Exists)
                        {
                            strExeInfo = AppendToComment(strExeInfo, fiFile.Name + ": " + fiFile.LastWriteTime.ToString(DATE_TIME_FORMAT));

                            if (m_DebugLevel >= 5)
                            {
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "EXE Info: " + strExeInfo);
                            }

                        }
                        else
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Tool file not found: " + fiFile.FullName);
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

            //Setup for execution of the stored procedure
            var MyCmd = new SqlCommand();
            {
                MyCmd.CommandType = System.Data.CommandType.StoredProcedure;
                MyCmd.CommandText = SP_NAME_SET_TASK_TOOL_VERSION;

                MyCmd.Parameters.Add(new SqlParameter("@Return", System.Data.SqlDbType.Int));
                MyCmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

                MyCmd.Parameters.Add(new SqlParameter("@job", System.Data.SqlDbType.Int));
                MyCmd.Parameters["@job"].Direction = System.Data.ParameterDirection.Input;
                MyCmd.Parameters["@job"].Value = Convert.ToInt32(m_TaskParams.GetParam("Job"));

                MyCmd.Parameters.Add(new SqlParameter("@step", System.Data.SqlDbType.Int));
                MyCmd.Parameters["@step"].Direction = System.Data.ParameterDirection.Input;
                MyCmd.Parameters["@step"].Value = Convert.ToInt32(m_TaskParams.GetParam("Step"));

                MyCmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", System.Data.SqlDbType.VarChar, 900));
                MyCmd.Parameters["@ToolVersionInfo"].Direction = System.Data.ParameterDirection.Input;
                MyCmd.Parameters["@ToolVersionInfo"].Value = toolVersionInfoCombined;
            }

            //Execute the SP (retry the call up to 4 times)
            var resCode = CaptureDBProcedureExecutor.ExecuteSP(MyCmd, 4);

            if (resCode == 0)
            {
                Outcome = true;
            }
            else
            {
                var Msg = "Error " + resCode + " storing tool version for current processing step";
                LogError(Msg);
                Outcome = false;
            }

            return Outcome;

        }

        /// <summary>
        /// Determines the version info for a DLL using reflection
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the veresion info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// 	  ''' <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected virtual bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string dllFilePath)
        {

            bool success;

            try
            {
                var fiFile = new FileInfo(dllFilePath);

                if (!fiFile.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                         "File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
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
                //success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, dllFilePath)
             
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
        protected bool StoreToolVersionInfoViaSystemDiagnostics(ref string toolVersionInfo, string dllFilePath)
        {
          
            try
            {
                var ioFileInfo = new FileInfo(dllFilePath);

                if (!ioFileInfo.Exists)
                {
                    var errMsg = "File not found by StoreToolVersionInfoViaSystemDiagnostics";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, errMsg + ": " + dllFilePath);
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
                var errMsg = "Exception determining File Version for " + Path.GetFileName(dllFilePath);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg + ": " + ex.Message);
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
        /// <remarks></remarks>
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
        protected bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string dllFilePath, string versionInspectorExeName)
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

                var versionInfoFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(fiDLLFile.Name) + "_VersionInfo.txt");

                var strArgs = clsConversion.PossiblyQuotePath(fiDLLFile.FullName) + " /O:" + clsConversion.PossiblyQuotePath(versionInfoFilePath);

                if (m_DebugLevel >= 3)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strAppPath + " " + strArgs);
                }

                var objProgRunner = new clsRunDosProgram(clsUtilities.GetAppFolderPath())
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    DebugLevel = 1,
                    MonitorInterval = 250
                };

                var success = objProgRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                Thread.Sleep(100);

                string strVersion;
                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out strVersion);

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
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This function is used by the ArchiveStatusCheck plugin and the ArchiveVerify plugin </remarks>
        protected bool UpdateIngestStepsCompletedOneTask(
            int statusNum,
            byte ingestStepsCompleted)
        {
            const string SP_NAME = "UpdateMyEMSLUploadIngestStats";

            var cmd = new SqlCommand(SP_NAME)
            {
                CommandType = System.Data.CommandType.StoredProcedure
            };

            cmd.Parameters.Add("@Return", System.Data.SqlDbType.Int);
            cmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

            cmd.Parameters.Add("@DatasetID", System.Data.SqlDbType.Int);
            cmd.Parameters["@DatasetID"].Direction = System.Data.ParameterDirection.Input;
            cmd.Parameters["@DatasetID"].Value = m_DatasetID;

            cmd.Parameters.Add("@StatusNum", System.Data.SqlDbType.Int);
            cmd.Parameters["@StatusNum"].Direction = System.Data.ParameterDirection.Input;
            cmd.Parameters["@StatusNum"].Value = statusNum;

            cmd.Parameters.Add("@IngestStepsCompleted", System.Data.SqlDbType.TinyInt);
            cmd.Parameters["@IngestStepsCompleted"].Direction = System.Data.ParameterDirection.Input;
            cmd.Parameters["@IngestStepsCompleted"].Value = ingestStepsCompleted;

            cmd.Parameters.Add("@message", System.Data.SqlDbType.VarChar, 512);
            cmd.Parameters["@message"].Direction = System.Data.ParameterDirection.Output;

            CaptureDBProcedureExecutor.TimeoutSeconds = 20;
            var resCode = CaptureDBProcedureExecutor.ExecuteSP(cmd, 2);

            if (resCode != 0)
            {
                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            return true;
        }

        #endregion

        #region "Events"

        private void AttachExecuteSpEvents()
        {
            try
            {
                CaptureDBProcedureExecutor.DBErrorEvent += m_ExecuteSP_DBErrorEvent;
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        private void DetachExecuteSpEvents()
        {
            try
            {
                if (CaptureDBProcedureExecutor != null)
                {
                    CaptureDBProcedureExecutor.DBErrorEvent -= m_ExecuteSP_DBErrorEvent;
                }
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        private void m_ExecuteSP_DBErrorEvent(string Message)
        {
            var logToDatabase = Message.Contains("permission was denied");            
            LogError("Stored procedure execution error: " + Message, logToDatabase);
        }


        #endregion

    }
}
