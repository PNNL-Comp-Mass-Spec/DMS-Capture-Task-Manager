
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//
// Last modified 11/17/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;

namespace SrcFileRenamePlugin
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
		}
		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the source file rename tool
		/// </summary>
		/// <returns>clsToolReturnData object containing tool operation results</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;

			msg = "Starting SrcFileRenamePlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			clsToolReturnData retData = base.RunTool();
			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining tool version info";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			string dataset = m_TaskParams.GetParam("Dataset");

			msg = "Renaming dataset '" + dataset + "'";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

			// Determine if instrument is on Bionet
			string capMethod = m_TaskParams.GetParam("Method");
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
			clsRenameOps renameOpTool = new clsRenameOps(m_MgrParams, useBionet);
			try
			{
				msg = "clsPluginMain.RunTool(): Starting rename operation";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				retData.CloseoutType = renameOpTool.DoOperation(m_TaskParams);

				msg = "clsPluginMain.RunTool(): Completed rename operation";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}
			catch (Exception ex)
			{
				msg = "clsPluginMain.RunTool(): Exception during rename operation";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
			}

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return retData;
		}	// End sub

		/// <summary>
		/// Initializes the rename tool
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
		}

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

			// Lookup the version of the Source File Rename plugin
			string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "SrcFileRenamePlugin.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the Capture task manager
			string strCTMPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "CaptureTaskManager.exe");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strCTMPath);
			if (!bSuccess)
				return false;

			// Store path to SrcFileRenamePlugin.dll in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));

			try
			{
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
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
