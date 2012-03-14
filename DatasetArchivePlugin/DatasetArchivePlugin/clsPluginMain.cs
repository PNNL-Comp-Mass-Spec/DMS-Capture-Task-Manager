
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//
// Last modified 10/08/2009
//						02/03/2010 (DAC) - Modified logging to include job number
//*********************************************************************************************************
using System;
using CaptureTaskManager;

namespace DatasetArchivePlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constructors"
			public clsPluginMain()
				: base()
			{
				// Does nothing at present
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Runs the archive and archive update step tools
			/// </summary>
			/// <returns>Enum indicating success or failure</returns>
			public override clsToolReturnData RunTool()
			{
				string msg;
				IArchiveOps archOpTool = null;

				msg = "Starting DatasetArchivePlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Perform base class operations, if any
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				// Store the version info in the database
				if (!StoreToolVersionInfo())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					retData.CloseoutMsg = "Error determining tool version info";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Select appropriate operation tool based on StepTool specification
				if (m_TaskParams.GetParam("StepTool").ToLower() == "datasetarchive")
				{
					// This is an archive operation
					archOpTool = new clsArchiveDataset(m_MgrParams, m_TaskParams);
					msg = "Starting archive, job " + m_TaskParams.GetParam("Job") + ", dataset " + m_TaskParams.GetParam("Dataset");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					if (archOpTool.PerformTask())
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					}
					else
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					}
					msg = "Completed archive, job " + m_TaskParams.GetParam("Job");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					// This is an archive update operation
					archOpTool = new clsArchiveUpdate(m_MgrParams, m_TaskParams);
					msg = "Starting archive update, job " + m_TaskParams.GetParam("Job") + ", dataset " + m_TaskParams.GetParam("Dataset");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					if (archOpTool.PerformTask())
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					}
					else
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					}
					msg = "Completed archive update, job " + m_TaskParams.GetParam("Job");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				
				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;
			}	// End sub

			/// <summary>
			/// Initializes the dataset archive tool
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <param name="statusTools">Tools for status reporting</param>
			public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
			{
				string msg = "Starting clsPluginMain.Setup()";
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

				string strToolVersionInfo = string.Empty;
				System.IO.FileInfo ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
				bool bSuccess;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

				// Lookup the version of the Dataset Archive plugin
				string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "DatasetArchivePlugin.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
				if (!bSuccess)
					return false;

				// Lookup the version of the MD5StageFileCreator
				string strMD5StageFileCreatorPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "MD5StageFileCreator.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMD5StageFileCreatorPath);
				if (!bSuccess)
					return false;

				// Store path to CaptureToolPlugin.dll in ioToolFiles
				System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
				ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));

				try
				{
					return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
				}
				catch (System.Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
					return false;
				}
			}

		#endregion
	}	// End class
}	// End namespace
