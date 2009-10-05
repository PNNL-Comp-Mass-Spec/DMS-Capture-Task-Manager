
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
// Last modified 09/25/2009
//*********************************************************************************************************
using System;
using CaptureTaskManager;

namespace CaptureToolPlugin
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
			/// Runs the capture step tool
			/// </summary>
			/// <returns>clsToolReturnData object containing tool operation results</returns>
			public override clsToolReturnData RunTool()
			{
				string msg;

				msg = "Starting CaptureToolPlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Perform base class operations, if any
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				msg = "Capturing dataset '" + dataset + "'";
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
				clsCaptureOps capOpTool = new clsCaptureOps(m_MgrParams, useBionet);
				try
				{
					msg = "clsPluginMain.RunTool(): Starting capture operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

					retData.CloseoutType = capOpTool.DoOperation(m_TaskParams);

					msg = "clsPluginMain.RunTool(): Completed capture operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
				catch (Exception ex)
				{
					msg = "clsPluginMain.RunTool(): Exception during capture operation";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}

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
				string msg = "Starting clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				
				base.Setup(mgrParams, taskParams, statusTools);

				msg = "Completed clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
