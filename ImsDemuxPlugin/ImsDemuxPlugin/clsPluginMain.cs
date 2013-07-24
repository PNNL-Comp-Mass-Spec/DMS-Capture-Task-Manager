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
			const string COULD_NOT_OBTAIN_GOOD_CALIBRATION = "Could not obtain a good calibration";
			
			bool bUseBelovTransform = false;				// Hard-coding to no longer use BelovTransform.dll
			string msg;

			msg = "Starting ImsDemuxPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			clsToolReturnData retData = base.RunTool();
			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

			// Initialize the config DB update interval
			base.m_LastConfigDBUpdate = System.DateTime.UtcNow;
			base.m_MinutesBetweenConfigDBUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

			string dataset = m_TaskParams.GetParam("Dataset");

			// Store the version info in the database
			if (!StoreToolVersionInfo(bUseBelovTransform))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining version of IMSDemultiplexer";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			// Locate data file on storage server
			string svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
			string dsPath = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

			// Use this name first to test if demux has already been performed once
			string uimfFileName = dataset + "_encoded.uimf";
			FileInfo fi = new FileInfo(Path.Combine(dsPath, uimfFileName));
			if (fi.Exists && (fi.Length != 0))
			{
				// The _encoded.uimf file will be used for demultiplexing

				// Look for a CalibrationLog.txt file
				// If it exists, and if the last line contains "Could not obtain a good calibration"
				//   then we do not want to re-demultiplex
				// In this case, we want to fail out the job immediately, since demultiplexing succeded, but calibration failed

				string sCalibrationLogPath = Path.Combine(dsPath, clsDemuxTools.CALIBRATION_LOG_FILE);
				if (System.IO.File.Exists(sCalibrationLogPath))
				{
					System.IO.StreamReader srInFile;
					string sLine;
					bool bCalibrationError = false;
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

					if (bCalibrationError)
					{
						msg = "CalibrationLog.txt file ends with '" + COULD_NOT_OBTAIN_GOOD_CALIBRATION + "'; will not attempt to re-demultiplex the _encoded.uimf file.  If you want to re-demultiplex the _encoded.uimf file, then you should delete the CalibrationLog.txt file";
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

			// Query to determine if demux is needed. 
			string uimfFileNamePath = Path.Combine(dsPath, uimfFileName);
			bool bMultiplexed = true;
			clsSQLiteTools oSQLiteTools = new clsSQLiteTools();

			clsSQLiteTools.UimfQueryResults queryResult = oSQLiteTools.GetUimfMuxStatus(uimfFileNamePath);
			if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
			{
				// De-mulitiplexing not required, but we should still attempt calibration
				msg = "No de-multiplexing required for dataset " + dataset;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				retData.EvalMsg = "Non-Multiplexed";
				bMultiplexed = false;
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

			if (bMultiplexed)
			{
				// De-multiplexing is needed
				retData = mDemuxTools.PerformDemux(m_MgrParams, m_TaskParams, uimfFileName, bUseBelovTransform);

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

				// Determine whether or not calibration should be performed
				// Note that stored procedure GetJobParamTable in the DMS_Capture database
				// sets this value based on the value in column Perform_Calibration of table T_Instrument_Name in the DMS5 database
				bool bCalibrate = m_TaskParams.GetParam("PerformCalibration", true);

				if (bCalibrate)
					retData = mDemuxTools.PerformCalibration(m_MgrParams, m_TaskParams, retData);
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
                if (!clsDemuxTools.CopyFile(sSourceFilePath, sTargetFilePath, true, retryCount))
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
		protected bool StoreToolVersionInfo(bool bUseBelovTransform)
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

			if (bUseBelovTransform)
			{
				// This is a C++ app, so we can't use reflection to determine the version
				// Thus, we don't call base.StoreToolVersionInfoOneFile()
				strDemultiplexerPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "BelovTransform.dll");
			}
			else
			{
				// Lookup the version of the IMSDemultiplexer
				strDemultiplexerPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "IMSDemultiplexer.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDemultiplexerPath);
				if (!bSuccess)
					return false;
			}

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
