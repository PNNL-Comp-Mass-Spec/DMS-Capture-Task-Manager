
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
// Last modified 09/25/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;

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

		protected System.DateTime m_LastConfigDBUpdate = System.DateTime.UtcNow;
		protected int m_MinutesBetweenConfigDBUpdates = 10;
		protected bool m_NeedToAbortProcessing = false;
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
			// Does nothing at present
		}	// End sub
		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the plugin tool. Implements IToolRunner.RunTool method
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public virtual clsToolReturnData RunTool()
		{
			// Does nothing at present, so return success
			clsToolReturnData retData = new clsToolReturnData();
			retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
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
		}

		protected bool UpdateMgrSettings()
		{
			bool bSuccess = true;

			if (m_MinutesBetweenConfigDBUpdates < 1)
				m_MinutesBetweenConfigDBUpdates = 1;

			if (System.DateTime.UtcNow.Subtract(m_LastConfigDBUpdate).TotalMinutes >= m_MinutesBetweenConfigDBUpdates)
			{
				m_LastConfigDBUpdate = System.DateTime.UtcNow;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings using Manager Control database");

				bool logConnectionErrors = false;
				if (!m_MgrParams.LoadMgrSettingsFromDB(logConnectionErrors))
				{
					// Error retrieving settings from the manager control DB
					string msg;
					msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

					bSuccess = false;
				}
				else
				{
					// Update the log level
					int debugLevel = clsConversion.CIntSafe(m_MgrParams.GetParam("debuglevel"), 4);
					clsLogTools.SetFileLogLevel(debugLevel);
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
			else
			{
				// Append a semicolon to InpComment, but only if it doesn't already end in a semicolon
				if (!InpComment.TrimEnd(' ').EndsWith(";"))
				{
					InpComment += "; ";
				}

				return InpComment + NewComment;
			}

		}

		protected void DeleteFileIgnoreErrors(string sFilePath)
		{
			try
			{
				System.IO.File.Delete(sFilePath);
			}
			catch
			{
				// Ignore errors here
			}
		}

		protected int ExecuteSP(System.Data.SqlClient.SqlCommand spCmd, string connStr, int MaxRetryCount)
		{
			int resCode = -9999;
			string msg = null;
			int retryCount = MaxRetryCount;
			int intTimeoutSeconds = 0;

			if (retryCount > 1)
				retryCount = 1;

			int.TryParse(m_MgrParams.GetParam("cmdtimeout"), out intTimeoutSeconds);
			if (intTimeoutSeconds <= 1)
				intTimeoutSeconds = 30;

			while (retryCount > 0)
			{
				//Multiple retry loop for handling SP execution failures
				try
				{
					using (System.Data.SqlClient.SqlConnection cn = new System.Data.SqlClient.SqlConnection(connStr))
					{
						cn.Open();
						spCmd.Connection = cn;
						//Change command timeout from 30 second default in attempt to reduce SP execution timeout errors
						spCmd.CommandTimeout = intTimeoutSeconds;
						spCmd.ExecuteNonQuery();

						resCode = (int)spCmd.Parameters["@Return"].Value;
					}
					break;
				}
				catch (System.Exception ex)
				{
					retryCount -= 1;
					msg = "clsToolRunnerbase.ExecuteSP(), exception calling SP " + spCmd.CommandText + ", " + ex.Message;
					msg += ". ResCode = " + resCode.ToString() + ". Retry count = " + retryCount.ToString();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				//Wait 10 seconds before retrying
				System.Threading.Thread.Sleep(10000);
			}

			if (retryCount < 1)
			{
				//Too many retries, log and return error
				msg = "Excessive retries executing SP " + spCmd.CommandText;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return -1;
			}

			return resCode;
		}	// End sub

		/// <summary>
		/// Creates a Tool Version Info file
		/// </summary>
		/// <param name="strFolderPath"></param>
		/// <param name="strToolVersionInfo"></param>
		/// <returns></returns>
		/// <remarks></remarks>
		protected bool SaveToolVersionInfoFile(string strFolderPath, string strToolVersionInfo)
		{
			string strToolVersionFilePath = null;

			try
			{
				strToolVersionFilePath = System.IO.Path.Combine(strFolderPath, "Tool_Version_Info_" + m_TaskParams.GetParam("StepTool") + ".txt");

				using (System.IO.StreamWriter swToolVersionFile = new System.IO.StreamWriter(new System.IO.FileStream(strToolVersionFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.ReadWrite))) {

					swToolVersionFile.WriteLine("Date: " + System.DateTime.Now.ToString(DATE_TIME_FORMAT));
					swToolVersionFile.WriteLine("Dataset: " + m_TaskParams.GetParam("Dataset"));
					swToolVersionFile.WriteLine("Job: " + m_TaskParams.GetParam("Job"));
					swToolVersionFile.WriteLine("Step: " + m_TaskParams.GetParam("Step"));
					swToolVersionFile.WriteLine("Tool: " + m_TaskParams.GetParam("StepTool"));
					swToolVersionFile.WriteLine("ToolVersionInfo:");

					swToolVersionFile.WriteLine(strToolVersionInfo.Replace("; ", Environment.NewLine));
					swToolVersionFile.Close();

				}
				
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception saving tool version info: " + ex.Message);
				return false;
			}

			return true;
		}

		/// <summary>
		/// Communicates with database to record the tool version(s) for the current step task
		/// </summary>
		/// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
		/// <returns>True for success, False for failure</returns>
		/// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
		protected bool SetStepTaskToolVersion(string strToolVersionInfo)
		{
			return SetStepTaskToolVersion(strToolVersionInfo, new System.Collections.Generic.List<System.IO.FileInfo>());
		}

		/// <summary>
		/// Communicates with database to record the tool version(s) for the current step task
		/// </summary>
		/// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
		/// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
		/// <returns>True for success, False for failure</returns>
		/// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
		protected bool SetStepTaskToolVersion(string strToolVersionInfo, System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles)
		{

			return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
		}

		/// <summary>
		/// Communicates with database to record the tool version(s) for the current step task
		/// </summary>
		/// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
		/// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
		/// <param name="blnSaveToolVersionTextFile">If true, then creates a text file with the tool version information</param>
		/// <returns>True for success, False for failure</returns>
		/// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
		protected bool SetStepTaskToolVersion(string strToolVersionInfo, System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles, bool blnSaveToolVersionTextFile)
		{

			string strExeInfo = string.Empty;
			string strToolVersionInfoCombined = null;

			bool Outcome = false;
			int ResCode = 0;

			int debugLevel = clsConversion.CIntSafe(m_MgrParams.GetParam("debuglevel"), 4);
			string workingDir = m_MgrParams.GetParam("WorkDir");
			if (string.IsNullOrWhiteSpace(workingDir))
			{
				return false;
			}

			if ((ioToolFiles != null))
			{
				foreach (System.IO.FileInfo ioFileInfo in ioToolFiles)
				{
					try
					{
						if (ioFileInfo.Exists)
						{
							strExeInfo = AppendToComment(strExeInfo, ioFileInfo.Name + ": " + ioFileInfo.LastWriteTime.ToString(DATE_TIME_FORMAT));

							if (debugLevel >= 5)
							{
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "EXE Info: " + strExeInfo);
							}

						}
						else
						{
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Tool file not found: " + ioFileInfo.FullName);
						}

					}
					catch (Exception ex)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception looking up tool version file info: " + ex.Message);
					}
				}
			}

			// Append the .Exe info to strToolVersionInfo
			if (string.IsNullOrEmpty(strExeInfo))
			{
				strToolVersionInfoCombined = string.Copy(strToolVersionInfo);
			}
			else
			{
				strToolVersionInfoCombined = AppendToComment(strToolVersionInfo, strExeInfo);
			}

			if (blnSaveToolVersionTextFile)
			{
				SaveToolVersionInfoFile(workingDir, strToolVersionInfoCombined);
			}

			//Setup for execution of the stored procedure
			System.Data.SqlClient.SqlCommand MyCmd = new System.Data.SqlClient.SqlCommand();
			{
				MyCmd.CommandType = System.Data.CommandType.StoredProcedure;
				MyCmd.CommandText = SP_NAME_SET_TASK_TOOL_VERSION;

				MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return",  System.Data.SqlDbType.Int));
				MyCmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

				MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@job",  System.Data.SqlDbType.Int));
				MyCmd.Parameters["@job"].Direction = System.Data.ParameterDirection.Input;
				MyCmd.Parameters["@job"].Value = Convert.ToInt32(m_TaskParams.GetParam("Job"));

				MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@step",  System.Data.SqlDbType.Int));
				MyCmd.Parameters["@step"].Direction = System.Data.ParameterDirection.Input;
				MyCmd.Parameters["@step"].Value = Convert.ToInt32(m_TaskParams.GetParam("Step"));

				MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ToolVersionInfo",  System.Data.SqlDbType.VarChar, 900));
				MyCmd.Parameters["@ToolVersionInfo"].Direction = System.Data.ParameterDirection.Input;
				MyCmd.Parameters["@ToolVersionInfo"].Value = strToolVersionInfoCombined;
			}

			string strConnStr = m_MgrParams.GetParam("connectionstring");

			//Execute the SP (retry the call up to 4 times)
			ResCode = this.ExecuteSP(MyCmd, strConnStr, 4);

			if (ResCode == 0)
			{
				Outcome = true;
			}
			else
			{
				string Msg = "Error " + ResCode.ToString() + " storing tool version for current processing step";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
				Outcome = false;
			}

			return Outcome;

		}

		/// <summary>
		/// Determines the version info for a DLL using reflection
		/// </summary>
		/// <param name="strToolVersionInfo">Version info string to append the veresion info to</param>
		/// <param name="strDLLFilePath">Path to the DLL</param>
		/// 	  ''' <returns>True if success; false if an error</returns>
		/// <remarks></remarks>
		protected virtual bool StoreToolVersionInfoOneFile(ref string strToolVersionInfo, string strDLLFilePath)
		{

			try
			{
				System.IO.FileInfo ioFileInfo = new System.IO.FileInfo(strDLLFilePath);

				if (!ioFileInfo.Exists)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "File not found by StoreToolVersionInfoOneFile: " + strDLLFilePath);
					return false;

				}
				else
				{
					System.Reflection.AssemblyName oAssemblyName;
					oAssemblyName = System.Reflection.Assembly.LoadFrom(ioFileInfo.FullName).GetName();

					string strNameAndVersion = null;
					strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
					strToolVersionInfo = AppendToComment(strToolVersionInfo, strNameAndVersion);

					return true;
				}

			}
			catch (Exception ex)
			{
				// If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
				//  <startup useLegacyV2RuntimeActivationPolicy="true">
				//    <supportedRuntime version="v4.0" />
				//  </startup>
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for " + System.IO.Path.GetFileName(strDLLFilePath) + ": " + ex.Message);
			}

			return false;

		}

		#endregion
	}	// End class
}	// End namespace
