
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//
// Last modified 10/02/2009
//*********************************************************************************************************
using System;
using CaptureTaskManager;

namespace DatasetInfoPlugin
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
			/// Runs the dataset info step tool
			/// </summary>
			/// <returns>Enum indicating success or failure</returns>
			public override clsToolReturnData RunTool()
			{
				string msg;

				msg = "Starting DatasetInfoPlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Perform base class operations, if any
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				msg = "Creating dataset info for dataset '" + dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

				if (clsMetaDataFile.CreateMetadataFile(m_MgrParams, m_TaskParams))
				{
					// Everything was good
					msg = "Metadata file created for dataset " + dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				}
				else
				{
					// There was a problem
					msg = "Problem creating metadata file for dataset " + dataset + ". See local log for details";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
					retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
					retData.EvalMsg = msg;
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				}

				//msg = "DatasetInfo tool is not presently active";
				//clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
				//retData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
				//retData.EvalMsg = msg;
				//retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;
				
			}	// End sub

			/// <summary>
			/// Initializes the dataset info tool
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
