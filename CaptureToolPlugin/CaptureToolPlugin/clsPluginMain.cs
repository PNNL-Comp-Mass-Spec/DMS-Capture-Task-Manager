//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.IO;
using CaptureTaskManager;

namespace CaptureToolPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constructors"
        // The base-class constructor is automatically called
		#endregion

		#region "Methods"
			/// <summary>
			/// Runs the capture step tool
			/// </summary>
			/// <returns>clsToolReturnData object containing tool operation results</returns>
			public override clsToolReturnData RunTool()
			{
				var msg = "Starting CaptureToolPlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Note that retData.CloseoutMsg will be stored in the Completion_Message field of the database
				// Similarly, retData.EvalMsg will be stored in the Evaluation_Message field of the database
				
				// Perform base class operations, if any
				var retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				// Store the version info in the database
				if (!StoreToolVersionInfo())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					retData.CloseoutMsg = "Error determining tool version info";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				msg = "Capturing dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

				// Determine if instrument is on Bionet
				var capMethod = m_TaskParams.GetParam("Method");
				bool useBionet;
				if (capMethod.ToLower() == "secfso")
				{
					useBionet = true;
				}
				else
				{
					useBionet = false;
				}

				// Create the object that will perform capture operation
				var capOpTool = new clsCaptureOps(m_MgrParams, useBionet);
				try
				{
					msg = "clsPluginMain.RunTool(): Starting capture operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

					capOpTool.DoOperation(m_TaskParams, ref retData);

					if (capOpTool.NeedToAbortProcessing)
					{
						m_NeedToAbortProcessing = true;
						if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
							retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
					}

					msg = "clsPluginMain.RunTool(): Completed capture operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
				catch (Exception ex)
				{
                    msg = "clsPluginMain.RunTool(): Exception during capture operation (useBionet=" + useBionet + ")";
					if (ex.Message.Contains("unknown user name or bad password")) 
					{
						// This error randomly occurs; no need to log a full stack trace
						msg += ", Logon failure: unknown user name or bad password";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						// Set the EvalCode to 3 so that capture can be retried
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
						retData.CloseoutMsg = msg;
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						retData.CloseoutMsg = msg;
					}
					
				}

				capOpTool.DetachEvents();

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;

			}	// End sub

			/// <summary>
			/// Initializes the capture tool
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <param name="statusTools">Tools for status reporting</param>
			public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
			{
				var msg = "Starting clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				
				base.Setup(mgrParams, taskParams, statusTools);

				msg = "Completed clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}	// End sub

			/// <summary>
			/// Stores the tool version info in the database
			/// </summary>
			/// <remarks></remarks>
			protected bool StoreToolVersionInfo()
			{

				var strToolVersionInfo = string.Empty;
				var ioAppFileInfo = new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

				// Lookup the version of the Capture tool plugin
			    if (ioAppFileInfo.DirectoryName == null)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to determine the directory path for the Exe using Reflection");
			        return false;
			    }

			    var strPluginPath = Path.Combine(ioAppFileInfo.DirectoryName, "CaptureToolPlugin.dll");
			    var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			    if (!bSuccess)
			    {
			        return false;
			    }

			    // Lookup the version of the Capture task manager
			    var strCTMPath = Path.Combine(ioAppFileInfo.DirectoryName, "CaptureTaskManager.exe");
			    bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strCTMPath);
			    if (!bSuccess)
			    {
			        return false;
			    }

			    // Store path to CaptureToolPlugin.dll in ioToolFiles
			    var ioToolFiles = new System.Collections.Generic.List<FileInfo>
			    {
			        new FileInfo(strPluginPath)
			    };

			    try
			    {
			        return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
			    }
			    catch (Exception ex)
			    {
			        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
			                             "Exception calling SetStepTaskToolVersion: " + ex.Message);
			        return false;
			    }
			}

		#endregion
	}
}
