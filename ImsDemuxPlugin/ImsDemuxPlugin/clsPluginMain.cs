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
using System.Text;
using CaptureTaskManager;
using System.IO;

namespace ImsDemuxPlugin
{
	#region "Delegates"
		public delegate void DelDemuxProgressHandler(float newProgress);
	#endregion

    public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

        #region "Constants"
            public const int MANAGER_UPDATE_INTERVAL_MINUTES = 10;
        #endregion

        #region "Module variables"
            protected clsDemuxTools mDemuxTools;
        #endregion

        #region "Constructors"
            public clsPluginMain()
				: base()
			{
                mDemuxTools = new clsDemuxTools();

                // Add a handler to catch progress events
                mDemuxTools.DemuxProgress += new DelDemuxProgressHandler(clsDemuxTools_DemuxProgress);
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

                // Initialize the config DB update interval
                base.m_LastConfigDBUpdate = System.DateTime.Now;
                base.m_MinutesBetweenConfigDBUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

				string dataset = m_TaskParams.GetParam("Dataset");

				// Locate data file on storage server
				string svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
				string dsPath = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

                // Use this name first to test if demux has already been performed once
				string uimfFileName = dataset + "_encoded.uimf";
				FileInfo fi = new FileInfo(Path.Combine(dsPath, uimfFileName));
				if (fi.Exists && (fi.Length != 0))
				{
					// Do nothing - this will be the file used for demultiplexing
				}
				else 
				{
					// Was the file zero bytes? If so, then delete it
					if (fi.Exists && (fi.Length == 0))
					{
						try
						{
							fi.Delete();
						}
						catch (Exception ex)
						{
							msg = "Exception deleting 0-byte uimf_encoded file";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
							retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
							retData.CloseoutMsg = msg;
							msg = "Completed clsPluginMain.RunTool()";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
							return retData;
						}
					}

					// If we got to here, _encoded uimf file doesn't exist. So, use the other uimf file
					uimfFileName = dataset + ".uimf";
					if (!File.Exists(Path.Combine(dsPath, uimfFileName)))
					{
                        msg = "UIMF file not found: " + uimfFileName;
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
                clsSQLiteTools oSQLiteTools = new clsSQLiteTools();

                clsSQLiteTools.UimfQueryResults queryResult = oSQLiteTools.GetUimfMuxStatus(uimfFileNamePath);
				if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
				{
					// De-mulitiplexing not required, so just report and exit
					msg = "No de-multiplexing required for dataset " + dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    retData.EvalMsg = "Non-Multiplexed";
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
                retData = mDemuxTools.PerformDemux(m_MgrParams, m_TaskParams, uimfFileName);

                if (mDemuxTools.OutOfMemoryException)
                    this.m_NeedToAbortProcessing = true;

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

		#region "Event handlers"
			/// <summary>
			/// Reports progress from demux dll
			/// </summary>
			/// <param name="newProgress">Current progress</param>
			void clsDemuxTools_DemuxProgress(float newProgress)
			{
				m_StatusTools.UpdateAndWrite(newProgress);

                // Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTESS
                base.UpdateMgrSettings();
			}
		#endregion
	}	// End class
}	// End namespace
