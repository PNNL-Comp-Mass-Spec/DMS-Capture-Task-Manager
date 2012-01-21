
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
		#endregion
	}	// End class
}	// End namespace
