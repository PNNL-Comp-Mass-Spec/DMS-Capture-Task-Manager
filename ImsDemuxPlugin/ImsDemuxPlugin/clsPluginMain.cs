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
using System.Data.SQLite;

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
		protected const string COULD_NOT_OBTAIN_GOOD_CALIBRATION = "Could not obtain a good calibration";

		#endregion

		#region "Enums"
		protected enum CalibrationMode
		{
			NoCalibration,
			ManualCalibration,
			AutoCalibration
		}
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
			mDemuxTools.BinCentricTableProgress += new DelDemuxProgressHandler(clsDemuxTools_BinCentricTableProgress);
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
			base.m_LastConfigDBUpdate = System.DateTime.UtcNow;
			base.m_MinutesBetweenConfigDBUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining version of IMSDemultiplexer";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			// Determine whether or not calibration should be performed
			// Note that stored procedure GetJobParamTable in the DMS_Capture database
			// sets this value based on the value in column Perform_Calibration of table T_Instrument_Name in the DMS5 database

			CalibrationMode calibrationMode;
			double calibrationSlope = 0;
			double calibrationIntercept = 0;

			if (m_TaskParams.GetParam("PerformCalibration", true))
				calibrationMode = CalibrationMode.AutoCalibration;
			else
				calibrationMode = CalibrationMode.NoCalibration;

			// Locate data file on storage server
			string svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
			string dsPath = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

			// Use this name first to test if demux has already been performed once
			string uimfFileName = m_Dataset + "_encoded.uimf";
			var fiUIMFFile = new FileInfo(Path.Combine(dsPath, uimfFileName));
			if (fiUIMFFile.Exists && (fiUIMFFile.Length != 0))
			{
				// The _encoded.uimf file will be used for demultiplexing

				// Look for a CalibrationLog.txt file
				// If it exists, and if the last line contains "Could not obtain a good calibration"
				//   then we need to examine table T_Log_Entries for messages regarding manual calibration
				// If manual calibration values are found, then we should cache the calibration slope and intercept values
				//   and apply them to the new demultiplexed file and skip auto-calibration
				// If manual calibration vales are not found, then we want to fail out the job immediately, 
				//   since demultiplexing succeeded, but calibration failed, and manual calibration was not performed

				var calibrationError = CheckForCalibrationError(dsPath);

				if (calibrationError)
				{
					var fiDecodedUIMFFile = new FileInfo(Path.Combine(dsPath, m_Dataset + ".uimf"));					
				
					var manuallyCalibrated = CheckForManualCalibration(fiDecodedUIMFFile.FullName, out calibrationSlope, out calibrationIntercept);

					if (manuallyCalibrated)
					{
						// Update the calibration mode
						calibrationMode = CalibrationMode.ManualCalibration;
					}
					else
					{
						msg = "CalibrationLog.txt file ends with '" + COULD_NOT_OBTAIN_GOOD_CALIBRATION + "'; will not attempt to re-demultiplex the _encoded.uimf file.  If you want to re-demultiplex the _encoded.uimf file, then you should rename the CalibrationLog.txt file";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						retData.CloseoutMsg = "Error calibrating UIMF file; see " + clsDemuxTools.CALIBRATION_LOG_FILE;
						retData.EvalMsg = "De-multiplexed but Calibration failed";

						msg = "Completed clsPluginMain.RunTool()";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						return retData;
					}
				}
			}
			else
			{
				// Was the file zero bytes? If so, then delete it
				if (fiUIMFFile.Exists && (fiUIMFFile.Length == 0))
				{
					try
					{
						fiUIMFFile.Delete();
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
				uimfFileName = m_Dataset + ".uimf";
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

			// Query to determine if demux is needed. 
			string uimfFileNamePath = Path.Combine(dsPath, uimfFileName);
			bool bMultiplexed = true;
			clsSQLiteTools oSQLiteTools = new clsSQLiteTools();

			clsSQLiteTools.UimfQueryResults queryResult = oSQLiteTools.GetUimfMuxStatus(uimfFileNamePath);
			if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
			{
				// De-mulitiplexing not required, but we should still attempt calibration (if enabled)
				msg = "No de-multiplexing required for dataset " + m_Dataset;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				retData.EvalMsg = "Non-Multiplexed";
				bMultiplexed = false;
			}
			else if (queryResult == clsSQLiteTools.UimfQueryResults.Error)
			{
				// There was a problem determining the UIMF file status. Set state and exit
				msg = "Problem determining UIMF file status for dataset " + m_Dataset;

				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				retData.CloseoutMsg = msg;

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				return retData;
			}

			if (bMultiplexed)
			{
				// De-multiplexing is needed
				retData = mDemuxTools.PerformDemux(m_MgrParams, m_TaskParams, uimfFileName);

				if (mDemuxTools.OutOfMemoryException)
				{
					if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						if (string.IsNullOrEmpty(retData.CloseoutMsg))
							retData.CloseoutMsg = "Out of memory";
					}

					this.m_NeedToAbortProcessing = true;
				}

			}


			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
			{
				// Demultiplexing succeeded (or skipped)

				// Add the bin-centric tables if not yet present
				retData = mDemuxTools.AddBinCentricTablesIfMissing(m_MgrParams, m_TaskParams, retData);
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
				{

					if (calibrationMode == CalibrationMode.AutoCalibration)
						retData = mDemuxTools.PerformCalibration(m_MgrParams, m_TaskParams, retData);
					else if (calibrationMode == CalibrationMode.ManualCalibration)
					{
						retData = mDemuxTools.PerformManualCalibration(m_MgrParams, m_TaskParams, retData, calibrationSlope, calibrationIntercept);
					}
				}
			}

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

		protected bool CheckForCalibrationError(string dsPath)
		{
			string sCalibrationLogPath = Path.Combine(dsPath, clsDemuxTools.CALIBRATION_LOG_FILE);
			bool bCalibrationError = false;

			if (System.IO.File.Exists(sCalibrationLogPath))
			{
				System.IO.StreamReader srInFile;
				string sLine;
				srInFile = new System.IO.StreamReader(new System.IO.FileStream(sCalibrationLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

				while (srInFile.Peek() >= 0)
				{
					sLine = srInFile.ReadLine();

					if (!string.IsNullOrEmpty(sLine) && sLine.Trim().Length > 0)
					{
						if (sLine.Contains(COULD_NOT_OBTAIN_GOOD_CALIBRATION))
							bCalibrationError = true;
						else
							// Only count this as a calibration error if the last non-blank line of the file contains the error message
							bCalibrationError = false;
					}
				}

				srInFile.Close();
			}

			return bCalibrationError;
		}

		protected bool CheckForManualCalibration(string decodedUimfFilePath, out double calibrationSlope, out double calibrationIntercept)
		{
			calibrationSlope = 0;
			calibrationIntercept = 0;

			if (!File.Exists(decodedUimfFilePath))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Decoded UIMF file does not exist (" + decodedUimfFilePath + "); cannot determine manual calibration coefficients");
				return false;
			}

			var oReader = new UIMFLibrary.DataReader(decodedUimfFilePath);
			bool manuallyCalibrated = false;
		

			if (oReader.TableExists("Log_Entries"))
			{
				string connectionString = "Data Source = " + decodedUimfFilePath + "; Version=3; DateTimeFormat=Ticks;";
				using (SQLiteConnection cnUIMF = new SQLiteConnection(connectionString))
				{
					cnUIMF.Open();
					SQLiteCommand cmdLogEntries = cnUIMF.CreateCommand();

					cmdLogEntries.CommandText = "SELECT Message FROM Log_Entries where Posted_By = '" + clsDemuxTools.UIMF_CALIBRATION_UPDATER_NAME + "' order by Entry_ID desc";
					using (var logEntriesReader = cmdLogEntries.ExecuteReader())
					{
						while (logEntriesReader.Read())
						{
							string message = logEntriesReader.GetString(0);
							if (message.StartsWith("New calibration coefficients"))
							{
								// Extract out the coefficients
								var reCoefficients = new System.Text.RegularExpressions.Regex("slope = ([0-9.+-]+), intercept = ([0-9.+-]+)");
								var reMatch = reCoefficients.Match(message);
								if (reMatch.Success)
								{
									double.TryParse(reMatch.Groups[1].Value, out calibrationSlope);
									double.TryParse(reMatch.Groups[2].Value, out calibrationIntercept);
								}
							}
							else if (message.StartsWith("Manually applied calibration coefficients"))
							{
								manuallyCalibrated = true;
							}
						}
					}
				}
			}

			if (manuallyCalibrated && calibrationSlope == 0)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Found message 'Manually applied calibration coefficients' but could not determine slope or intercept manually applied");
				manuallyCalibrated = false;
			}

			return manuallyCalibrated;
		}

		protected bool CopyFileToStorageServer(string sourceDirPath, string fileName)
		{
			
            string msg;
            bool bSuccess = true;

			string svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
			string sDatasetFolderPathRemote = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

			// Copy file fileName from sourceDirPath to the dataset folder
			string sSourceFilePath = Path.Combine(sourceDirPath, fileName);
			string sTargetFilePath = Path.Combine(sDatasetFolderPathRemote, fileName);

            if (!System.IO.File.Exists(sSourceFilePath))
            {
				msg = "File not found: " + sSourceFilePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);                    
            }
            else
            {
                int retryCount = 3;
                if (!clsDemuxTools.CopyFileWithRetry(sSourceFilePath, sTargetFilePath, true, retryCount))
                {
					msg = "Error copying " + fileName + " to storage server";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    bSuccess = false;
                }
            }

            return bSuccess;

		}

		/// <summary>
		/// Stores the tool version info in the database
		/// </summary>
		/// <remarks></remarks>
		protected bool StoreToolVersionInfo()
		{

			string strToolVersionInfo = string.Empty;
			string strDemultiplexerPath = string.Empty;

			System.IO.FileInfo ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
			bool bSuccess;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			// Lookup the version of the UIMFDemultiplexer (in the Capture Task Manager folder)
			string strUIMFDemultiplexerPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "UIMFDemultiplexer.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFDemultiplexerPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the IMSDemultiplexer
			strDemultiplexerPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "IMSDemultiplexer.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDemultiplexerPath);
			if (!bSuccess)
				return false;

			string strAutoCalibrateUIMFPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "AutoCalibrateUIMF.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strAutoCalibrateUIMFPath);
			if (!bSuccess)
				return false;

			string strUIMFLibrary = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "UIMFLibrary.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibrary);
			if (!bSuccess)
				return false;

			// Store path to the demultiplexer DLL in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo>ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strDemultiplexerPath));

			try
			{
				bool bSaveToolVersionTextFile = false;
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, bSaveToolVersionTextFile);
			}
			catch (System.Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}

		}
		
		#endregion

		#region "Event handlers"
		
		/// <summary>
		/// Reports progress from demux dll
		/// </summary>
		/// <param name="newProgress">Current progress (value between 0 and 100)</param>
		void clsDemuxTools_DemuxProgress(float newProgress)
		{
			// Multipling by 0.5 since we're assuming that demultiplexing will take 50% of the time while addition of bin-centric tables will take 50% of the time
			m_StatusTools.UpdateAndWrite(0 + newProgress * 0.50f);

			// Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTESS
			base.UpdateMgrSettings();
		}


		/// <summary>
		/// Reports progress for the addition of bin-centric tables
		/// </summary>
		/// <param name="newProgress">Current progress (value between 0 and 100)</param>
		void clsDemuxTools_BinCentricTableProgress(float newProgress)
		{
			// Multipling by 0.5 since we're assuming that demultiplexing will take 50% of the time while addition of bin-centric tables will take 50% of the time
			m_StatusTools.UpdateAndWrite(50 + newProgress * 0.50f);

			// Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTESS
			base.UpdateMgrSettings();
		}


		#endregion
	}	// End class
}	// End namespace
