//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using System.IO;

namespace ImsDemuxPlugin
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
			/// Runs the IMS demux step tool
			/// </summary>
			/// <returns>Enum indicating success or failure</returns>
			public override clsToolReturnData RunTool()
			{
				string msg;

				msg = "Starting ImsDemuxPlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Perform base class operations, if any
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				// Locate data file on storage server
				string svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
				string dsPath = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));
				// Use this name first to test if demux has already been performed once
				string uimfFileName = dataset + "_encoded.uimf";
				if (!File.Exists(Path.Combine(dsPath, uimfFileName)))
				{
					uimfFileName = dataset + ".uimf";
					if (!File.Exists(Path.Combine(dsPath, uimfFileName)))
					{
						msg = "UIMF file not found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						retData.CloseoutMsg = msg;
						msg = "Completed clsPluginMain.RunTool()";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						return retData;
					}
				}

				// Query to determine if demux is needed. Closeout if not required (adding "NonMultiplexed" to eval comment)
				string uimfFileNamePath = Path.Combine(dsPath, uimfFileName);
				clsSQLiteTools.UimfQueryResults queryResult = clsSQLiteTools.GetUimfMuxStatus(uimfFileNamePath);
				if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
				{
					// De-mulitiplexing not required, so just report and exit
					msg = "No de-multiplexing required for dataset " + dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					retData.EvalMsg = "Non-Multiiplexed";
					msg = "Completed clsPluginMain.RunTool()";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					return retData;
				}
				else if (queryResult == clsSQLiteTools.UimfQueryResults.Error)
				{
					// There was a problem determining the UIMF file status. Set state and exit
					msg = "Problem determining UIMF file status for dataset " + dataset;
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					retData.CloseoutMsg = msg;
					msg = "Completed clsPluginMain.RunTool()";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					return retData;
				}

				// De-multiplexing is needed
				retData = clsDemuxTools.PerformDemux(m_MgrParams, m_TaskParams, uimfFileName);

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;
			}	// End sub

			/// <summary>
			/// Initializes the demux tool
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
