//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//               04/22/2011 dac - Modified to use "real" demultiplexing dll's
//				 03/12/2012 mem - Replaced BelovTransform.dll with IMSDemultiplexer.dll
//				 08/28/2012 mem - Removed option to use BelovTransform
//*********************************************************************************************************
using System;
using System.IO;
using CaptureTaskManager;
using FileProcessor;

namespace ImsDemuxPlugin
{
	public class clsDemuxTools
	{
		//*********************************************************************************************************
		// This class demultiplexes a .UIMF file using
		//**********************************************************************************************************

		#region "Constants"
		public const string CALIBRATION_LOG_FILE = "CalibrationLog.txt";

		protected const string DECODED_UIMF_SUFFIX = "_decoded.uimf";

		protected const int MAX_CHECKPOINT_FRAME_INTERVAL = 200;
		protected const int MAX_CHECKPOINT_WRITE_FREQUENCY_MINUTES = 20;

		public const string UIMF_CALIBRATION_UPDATER_NAME = "UIMF Calibration Updater";

		#endregion

		#region "Module variables"
		UIMFDemultiplexer.UIMFDemultiplexer m_DeMuxTool;

		bool m_OutOfMemoryException;

		string m_Dataset;
		string m_DatasetFolderPathRemote = string.Empty;
		string m_WorkDir;

		DateTime m_LastProgressUpdateTime;
		DateTime m_LastProgressMessageTime;

		#endregion

		#region "Events"
		// Events used for communication back to clsPluginMain, where the logging and status updates are handled
		//public event DelDemuxErrorHandler DemuxError;
		//public event DelDemuxMessageHandler DumuxMsg;
		//public event DelDumuxExceptionHandler DemuxException;
		public event DelDemuxProgressHandler DemuxProgress;
		public event DelDemuxProgressHandler BinCentricTableProgress;

		#endregion

		#region "Properties"
		public bool OutOfMemoryException
		{
			get { return m_OutOfMemoryException; }
		}
		#endregion

		#region "Constructor"
		public clsDemuxTools()
		{
			m_DeMuxTool = new UIMFDemultiplexer.UIMFDemultiplexer();
			m_DeMuxTool.ErrorEvent += deMuxTool_ErrorEventHandler;
			m_DeMuxTool.WarningEvent += deMuxTool_WarningEventHandler;
			m_DeMuxTool.MessageEvent += deMuxTool_MessageEventHandler;
		}
		#endregion

		#region "Methods"

		public clsToolReturnData AddBinCentricTablesIfMissing(IMgrParams mgrParams, ITaskParams taskParams, clsToolReturnData retData)
		{
			try
			{
				UpdateDatasetInfo(mgrParams, taskParams);

				// Locate data file on storage server
				// Don't copy it locally yet
				string sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
				if (string.IsNullOrEmpty(sUimfPath))
				{
					if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				using (var objReader = new UIMFLibrary.DataReader(sUimfPath))
				{
					bool hasBinCentricData = objReader.DoesContainBinCentricData();

					if (hasBinCentricData)
					{
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
						return retData;
					}
				}

				// Make sure the working directory is empty
				clsToolRunnerBase.CleanWorkDir(m_WorkDir);

				// Copy the UIMF file from the storage server to the working directory
				string uimfRemoteFileNamePath;
				string uimfLocalFileNamePath;
				string uimfFileName = m_Dataset + ".uimf";

				retData = CopyUIMFToWorkDir(taskParams, uimfFileName, retData, out uimfRemoteFileNamePath, out uimfLocalFileNamePath);
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
					return retData;

				// Add the bin-centric tables
				using (var uimfReader = new UIMFLibrary.DataReader(uimfLocalFileNamePath))
				{
					string connectionString = "Data Source = " + uimfLocalFileNamePath;
					using (var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString))
					{
						dbConnection.Open();

						using (var dbCommand = dbConnection.CreateCommand())
						{
							dbCommand.CommandText = "PRAGMA synchronous=0";
							dbCommand.ExecuteNonQuery();
						}


						var binCentricTableCreator = new UIMFLibrary.BinCentricTableCreation();

						// Attach the events
						binCentricTableCreator.ProgressEvent += binCentricTableCreator_ProgressEvent;
						binCentricTableCreator.MessageEvent += binCentricTableCreator_MessageEvent;

						m_LastProgressUpdateTime = DateTime.UtcNow;
						m_LastProgressMessageTime = DateTime.UtcNow;

						binCentricTableCreator.CreateBinCentricTable(dbConnection, uimfReader, m_WorkDir);

						dbConnection.Close();
					}
				}

				// Confirm that the bin-centric tables were truly added
				using (var objReader = new UIMFLibrary.DataReader(uimfLocalFileNamePath))
				{
					bool hasBinCentricData = objReader.DoesContainBinCentricData();

					if (!hasBinCentricData)
					{
						retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Bin-centric tables were not added to the UIMF file");
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}

				// Copy the result files to the storage server
				if (!CopyUIMFFileToStorageServer(retData, uimfLocalFileNamePath, "bin-centric UIMF"))
				{
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}

				try
				{
					// Delete the local file
					File.Delete(uimfLocalFileNamePath);
				}
				// ReSharper disable once EmptyGeneralCatchClause
				catch
				{
					// Ignore errors here
				}

				return retData;

			}
			catch (Exception ex)
			{
				const string msg = "Exception adding the bin-centric tables to the UIMF file";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				if (retData == null)
					retData = new clsToolReturnData();
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, msg);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

		}

		protected clsToolReturnData CopyUIMFToWorkDir(
			ITaskParams taskParams,
			string uimfFileName,
			clsToolReturnData retData,
			out string uimfRemoteFileNamePath,
			out string uimfLocalFileNamePath)
		{
			// Locate data file on storage server
			uimfRemoteFileNamePath = Path.Combine(m_DatasetFolderPathRemote, uimfFileName);
			uimfLocalFileNamePath = Path.Combine(m_WorkDir, m_Dataset + ".uimf");

			// Copy uimf file to working directory
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file from storage server");
			const int retryCount = 0;
			if (!CopyFileWithRetry(uimfRemoteFileNamePath, uimfLocalFileNamePath, false, retryCount))
			{
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Error copying UIMF file to working directory");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			return retData;

		}

		protected string GetRemoteUIMFFilePath(ITaskParams taskParams, ref clsToolReturnData retData)
		{

			try
			{
				// Locate data file on storage server
				// Don't copy it locally; just work with it over the network
				string sUimfPath = Path.Combine(m_DatasetFolderPathRemote, m_Dataset + ".uimf");

				if (File.Exists(sUimfPath))
					return sUimfPath;
				
				string msg = "UIMF file not found on storage server, unable to calibrate: " + sUimfPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "UIMF file not found on storage server, unable to calibrate");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return string.Empty;
			}
			catch (Exception ex)
			{
				const string msg = "Exception finding UIMF file to calibrate";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return string.Empty;
			}

		}

		/// <summary>
		/// Calibrate a UIMF file
		/// </summary>
		/// <param name="mgrParams"></param>
		/// <param name="taskParams"></param>
		/// <param name="retData"></param>
		/// <returns></returns>
		public clsToolReturnData PerformCalibration(IMgrParams mgrParams, ITaskParams taskParams, clsToolReturnData retData)
		{

			UpdateDatasetInfo(mgrParams, taskParams);

			string msg = "Calibrating dataset " + m_Dataset;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			bool bAutoCalibrate;

			// Make sure the working directory is empty
			clsToolRunnerBase.CleanWorkDir(m_WorkDir);

			// Locate data file on storage server
			// Don't copy it locally; just work with it over the network
			string sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
			if (string.IsNullOrEmpty(sUimfPath))
			{
				if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			try
			{

				// Lookup the instrument name
				string instrumentName = taskParams.GetParam("Instrument_Name");

				switch (instrumentName.ToLower())
				{
					case "ims_tof_1":
					case "ims_tof_2":
					case "ims_tof_3":
						msg = "Skipping calibration since instrument is " + instrumentName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						bAutoCalibrate = false;
						break;
					default:
						bAutoCalibrate = true;
						break;
				}

			}
			catch (Exception ex)
			{
				msg = "Exception determining whether instrument should be calibrated";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			if (!bAutoCalibrate)
				return retData;

			try
			{

				// Count the number of frames
				// If fewer than 5 frames, then don't calibrate
				var objReader = new UIMFLibrary.DataReader(sUimfPath);

				var oFrameList = objReader.GetMasterFrameList();

				if (oFrameList.Count < 5)
				{
					if (oFrameList.Count == 0)
						msg = "Skipping calibration since .UIMF file has no frames";
					else
					{
						msg = "Skipping calibration since .UIMF file only has " + oFrameList.Count + " frame";
						if (oFrameList.Count != 1)
							msg += "s";
					}

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					bAutoCalibrate = false;
				}
				else
				{
					// Look for the presence of calibration frames or calibration tables
					// If neither exists, then we cannot perform calibration
					bool bCalibrationDataExists = false;


					var objFrameEnumerator = oFrameList.GetEnumerator();
					while (objFrameEnumerator.MoveNext())
					{
						if (objFrameEnumerator.Current.Value == UIMFLibrary.DataReader.FrameType.Calibration)
						{
							bCalibrationDataExists = true;
							break;
						}
					}

					if (!bCalibrationDataExists)
					{
						// No calibration frames were found
						System.Collections.Generic.List<string> sCalibrationTables = objReader.GetCalibrationTableNames();
						if (sCalibrationTables.Count > 0)
						{
							bCalibrationDataExists = true;
						}
						else
						{
							msg = "Skipping calibration since .UIMF file does not contain any calibration frames or calibration tables";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
							bAutoCalibrate = false;
						}

					}
				}
			}
			catch (Exception ex)
			{
				msg = "Exception checking for calibration frames";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			if (!bAutoCalibrate)
				return retData;


			// Perform calibration operation
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling demux dll to calibrate");

			bool bCalibrationFailed = false;

			try
			{
				if (!CalibrateFile(sUimfPath, m_Dataset))
				{
					retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Error calibrating UIMF file");
					bCalibrationFailed = true;
				}
			}
			catch (Exception ex)
			{
				msg = "Exception calling CalibrateFile for dataset " + m_Dataset;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
				bCalibrationFailed = true;
			}

			if (!bCalibrationFailed)
			{
				try
				{
					if (!ValidateUIMFCalibrated(sUimfPath, retData))
					{
						// Calibration failed
						bCalibrationFailed = true;
					}
				}
				catch (Exception ex)
				{
					msg = "Exception validating calibrated .UIMF ifle";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

			}

			// Copy the CalibrationLog.txt file to the storage server (even if calibration failed)
			CopyCalibrationLogToStorageServer(retData);


			// Update the return data
			if (bCalibrationFailed)
			{
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				retData.EvalMsg = AppendToString(retData.EvalMsg, " but Calibration failed", "");
			}
			else
			{
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

				retData.EvalMsg = AppendToString(retData.EvalMsg, " and calibrated", "");
			}

			return retData;

		}

		/// <summary>
		/// Manually applies calibration coefficients to a UIMF file
		/// </summary>
		/// <param name="mgrParams"></param>
		/// <param name="taskParams"></param>
		/// <param name="retData"></param>
		/// <param name="calibrationSlope"></param>
		/// <param name="calibrationIntercept"></param>
		/// <returns></returns>
		public clsToolReturnData PerformManualCalibration(IMgrParams mgrParams, ITaskParams taskParams, clsToolReturnData retData, double calibrationSlope, double calibrationIntercept)
		{
			UpdateDatasetInfo(mgrParams, taskParams);

			try
			{
				// Locate data file on storage server
				// Don't copy it locally; just work with it over the network
				string sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
				if (string.IsNullOrEmpty(sUimfPath))
				{
					if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				string connectionString = "Data Source = " + sUimfPath;
				using (var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString))
				{
					dbConnection.Open();

					double currentSlope = 0;
					double currentIntercept = 0;

					using (var dbCommand = dbConnection.CreateCommand())
					{
						dbCommand.CommandText = "SELECT CalibrationSlope, CalibrationIntercept FROM Frame_Parameters LIMIT 1";
						using (var reader = dbCommand.ExecuteReader())
						{
							if (reader.Read())
							{
								currentSlope = reader.GetDouble(0);
								currentIntercept = reader.GetDouble(1);
							}
						}
					}

					if (Math.Abs(currentSlope) < Double.Epsilon)
					{
						const string msg = "Existing CalibrationSlope is 0 in PerformManualCalibration; this is unexpected";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					}

					using (var dbCommand = dbConnection.CreateCommand())
					{
						dbCommand.CommandText = "UPDATE Frame_Parameters SET CalibrationSlope = " + calibrationSlope.ToString("0.0000000") + ", CalibrationIntercept = " + calibrationIntercept.ToString("0.0000000") + ", CALIBRATIONDONE = -1";
						dbCommand.ExecuteNonQuery();
					}

					string logMessage = "Manually applied calibration coefficients to all frames using user-specified calibration coefficients";
					UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

					logMessage = "Old calibration coefficients: slope = " + currentSlope + ", intercept = " + currentIntercept;
					UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

					logMessage = "New calibration coefficients: slope = " + calibrationSlope.ToString("0.0000000") + ", intercept = " + calibrationIntercept.ToString("0.0000000");
					UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

				}

			}
			catch (Exception ex)
			{
				string msg = "Exception in PerformManualCalibration for dataset " + m_Dataset;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				if (retData == null)
					retData = new clsToolReturnData();
				retData.CloseoutMsg = "Error manually calibrating UIMF file";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			return retData;
		}

		/// <summary>
		/// Performs demultiplexing of IMS data files
		/// </summary>
		/// <param name="mgrParams">Parameters for manager operation</param>
		/// <param name="taskParams">Parameters for the assigned task</param>
		/// <param name="uimfFileName"></param>
		/// <returns>Enum indicating task success or failure</returns>
		public clsToolReturnData PerformDemux(IMgrParams mgrParams, ITaskParams taskParams, string uimfFileName)
		{
			UpdateDatasetInfo(mgrParams, taskParams);

			string jobNum = taskParams.GetParam("Job");
			string msg = "Performing demultiplexing, job " + jobNum + ", dataset " + m_Dataset;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			var retData = new clsToolReturnData();

			bool bPostProcessingError = false;

			int framesToSum = taskParams.GetParam("DemuxFramesToSum", 1);
			if (framesToSum > 0)
				m_DeMuxTool.FramesToSum = framesToSum;

			if (framesToSum > 1)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Will sum " + framesToSum + " LC Frames when demultiplexing");

			// Make sure the working directory is empty
			clsToolRunnerBase.CleanWorkDir(m_WorkDir);

			// Copy the UIMF file from the storage server to the working directory
			string uimfRemoteEncodedFileNamePath;
			string uimfLocalEncodedFileNamePath;

			retData = CopyUIMFToWorkDir(taskParams, uimfFileName, retData, out uimfRemoteEncodedFileNamePath, out uimfLocalEncodedFileNamePath);
			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return retData;

			// Look for a _decoded.uimf.tmp file on the storage server
			// Copy it local if present
			string sTmpUIMFFileName = m_Dataset + DECODED_UIMF_SUFFIX + ".tmp";
			string sTmpUIMFRemoteFileNamePath = Path.Combine(m_DatasetFolderPathRemote, sTmpUIMFFileName);
			string sTmpUIMFLocalFileNamePath = Path.Combine(m_WorkDir, sTmpUIMFFileName);

			bool bResumeDemultiplexing = false;
			int iResumeStartFrame;

			if (File.Exists(sTmpUIMFRemoteFileNamePath))
			{
				// Copy _decoded.uimf.tmp file to working directory so that we can resume demultiplexing where we left off
				const int retryCount = 0;
				if (CopyFileWithRetry(sTmpUIMFRemoteFileNamePath, sTmpUIMFLocalFileNamePath, false, retryCount))
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, ".tmp decoded file found at " + sTmpUIMFRemoteFileNamePath + "; will resume demultiplexing");
					bResumeDemultiplexing = true;
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Error copying .tmp decoded file from " + sTmpUIMFRemoteFileNamePath + " to work folder; unable to resume demultiplexing");
				}
			}


			// Perform demux operation
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling demux dll");

			try
			{
				const bool bAutoCalibrate = false;

				if (!DemultiplexFile(uimfLocalEncodedFileNamePath, m_Dataset, bResumeDemultiplexing, out iResumeStartFrame, bAutoCalibrate))
				{
					retData.CloseoutMsg = "Error demultiplexing UIMF file";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}
			}
			catch (Exception ex)
			{
				msg = "Exception calling DemultiplexFile for dataset " + m_Dataset;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				retData.CloseoutMsg = "Error demultiplexing UIMF file";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}


			// Look for the demultiplexed .UIMF file
			string localUimfDecodedFilePath = Path.Combine(m_WorkDir, m_Dataset + DECODED_UIMF_SUFFIX);

			if (!File.Exists(localUimfDecodedFilePath))
			{
				retData.CloseoutMsg = "Decoded UIMF file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, retData.CloseoutMsg + ": " + localUimfDecodedFilePath);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			if (!ValidateUIMFDemultiplexed(localUimfDecodedFilePath, retData))
			{
				if (string.IsNullOrEmpty(retData.CloseoutMsg))
					retData.CloseoutMsg = "ValidateUIMFDemultiplexed returned false";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				bPostProcessingError = true;
			}

			if (!bPostProcessingError)
			{
				// Rename uimf file on storage server
				msg = "Renaming uimf file on storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				// If this is a re-run, then encoded file has already been renamed
				// This is determined by looking for "encoded" in uimf file name
				if (!uimfFileName.Contains("encoded"))
				{
					if (!RenameFile(uimfRemoteEncodedFileNamePath, Path.Combine(m_DatasetFolderPathRemote, m_Dataset + "_encoded.uimf")))
					{
						retData.CloseoutMsg = "Error renaming encoded UIMF file on storage server";
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						bPostProcessingError = true;
					}
				}
			}

			if (!bPostProcessingError)
			{
				// Delete CheckPoint file from storage server (if it exists)
				if (!string.IsNullOrEmpty(m_DatasetFolderPathRemote))
				{
					msg = "Deleting .uimf.tmp CheckPoint file from storage server";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

					try
					{
						string sCheckpointTargetPath = Path.Combine(m_DatasetFolderPathRemote, sTmpUIMFFileName);

						if (File.Exists(sCheckpointTargetPath))
							File.Delete(sCheckpointTargetPath);
					}
					catch (Exception ex)
					{
						msg = "Error deleting .uimf.tmp CheckPoint file: " + ex.Message;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					}

				}
			}

			if (!bPostProcessingError)
			{
				// Copy the result files to the storage server
				if (!CopyUIMFFileToStorageServer(retData, localUimfDecodedFilePath, "de-multiplexed UIMF"))
					bPostProcessingError = true;

			}

			if (bPostProcessingError)
			{
				try
				{
					// Delete the multiplexed .UIMF file (no point in saving it)
					File.Delete(uimfLocalEncodedFileNamePath);
				}
				// ReSharper disable once EmptyGeneralCatchClause
				catch
				{
					// Ignore errors deleting the multiplexed .UIMF file
				}

				// Try to save the demultiplexed .UIMF file (and any other files in the work directory)
				var oFailedResultsCopier = new clsFailedResultsCopier(mgrParams, taskParams);
				oFailedResultsCopier.CopyFailedResultsToArchiveFolder(m_WorkDir);

				return retData;
			}

			// Delete local uimf file(s)
			msg = "Cleaning up working directory";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			try
			{
				File.Delete(localUimfDecodedFilePath);
				File.Delete(uimfLocalEncodedFileNamePath);
			}
			catch (Exception ex)
			{
				// Error deleting files; don't treat this as a fatal error
				msg = "Exception deleting working directory file(s): " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
			}

			// Update the return data
			retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			retData.EvalMsg = "De-multiplexed";

			if (bResumeDemultiplexing)
				retData.EvalMsg += " (resumed at frame " + iResumeStartFrame + ")";

			return retData;

		}	// End sub

		/// <summary>
		/// Copies a file, allowing for retries
		/// </summary>
		/// <param name="sourceFilePath">Source file</param>
		/// <param name="targetFilePath">Destination file</param>
		/// <param name="overWrite"></param>
		/// <param name="retryCount"></param>
		/// <returns>True if success, false if an error</returns>
		public static bool CopyFileWithRetry(string sourceFilePath, string targetFilePath, bool overWrite, int retryCount)
		{
			return CopyFileWithRetry(sourceFilePath, targetFilePath, overWrite, retryCount, backupDestFileBeforeCopy: false);
		}

		/// <summary>
		/// Copies a file, allowing for retries
		/// </summary>
		/// <param name="sourceFilePath">Source file</param>
		/// <param name="targetFilePath">Destination file</param>
		/// <param name="overWrite"></param>
		/// <param name="retryCount"></param>
		/// <param name="backupDestFileBeforeCopy">If True and if the target file exists, then renames the target file to have _Old1 before the extension</param>
		/// <returns>True if success, false if an error</returns>
		public static bool CopyFileWithRetry(string sourceFilePath, string targetFilePath, bool overWrite, int retryCount, bool backupDestFileBeforeCopy)
		{
			bool bRetryingCopy = false;

			if (retryCount < 0)
				retryCount = 0;

			var oFileTools = new PRISM.Files.clsFileTools();

			while (retryCount >= 0)
			{
				string msg;
				try
				{
					if (bRetryingCopy)
					{
						msg = "Retrying copy; retryCount = " + retryCount;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					}

					oFileTools.CopyFile(sourceFilePath, targetFilePath, overWrite, backupDestFileBeforeCopy);
					return true;
				}
				catch (Exception ex)
				{
					msg = "Exception copying file " + sourceFilePath + " to " + targetFilePath + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

					System.Threading.Thread.Sleep(2000);
					retryCount -= 1;
					bRetryingCopy = true;
				}
			}

			// If we get here, then we were not able to successfully copy the file
			return false;

		}	// End sub


		/// <summary>
		/// Copies the result files to the storage server
		/// </summary>
		/// <param name="retData"></param>
		/// <param name="localUimfDecodedFilePath"></param>
		/// <param name="fileDescription"></param>
		/// <returns>True if success; otherwise false</returns>
		private bool CopyUIMFFileToStorageServer(clsToolReturnData retData, string localUimfDecodedFilePath, string fileDescription)
		{
			bool bSuccess = true;

			// Copy demuxed file to storage server, renaming as datasetname.uimf in the process
			string msg = "Copying " + fileDescription + " file to storage server";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			const int retryCount = 3;
			if (!CopyFileWithRetry(localUimfDecodedFilePath, Path.Combine(m_DatasetFolderPathRemote, m_Dataset + ".uimf"), true, retryCount))
			{
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Error copying " + fileDescription + " file to storage server");
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				bSuccess = false;
			}

			return bSuccess;
		}

		/// <summary>
		/// Copies the result files to the storage server
		/// </summary>
		/// <param name="retData"></param>
		/// <returns>True if success; otherwise false</returns>
		private void CopyCalibrationLogToStorageServer(clsToolReturnData retData)
		{
			string msg;

			// Copy file CalibrationLog.txt to the storage server (if it exists)
			string sCalibrationLogFilePath = Path.Combine(m_WorkDir, CALIBRATION_LOG_FILE);
			string sTargetFilePath = Path.Combine(m_DatasetFolderPathRemote, CALIBRATION_LOG_FILE);

			if (!File.Exists(sCalibrationLogFilePath))
			{
				msg = "CalibrationLog.txt not found at " + m_WorkDir;
				if (File.Exists(sTargetFilePath))
				{
					msg += "; this is OK since " + CALIBRATION_LOG_FILE + " exists at " + m_DatasetFolderPathRemote;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
				else
				{
					msg += "; in addition, could not find " + CALIBRATION_LOG_FILE + " at " + m_DatasetFolderPathRemote;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}

			}
			else
			{
				msg = "Copying CalibrationLog.txt file to storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				const int retryCount = 3;
				const bool backupDestFileBeforeCopy = true;
				if (!CopyFileWithRetry(sCalibrationLogFilePath, sTargetFilePath, true, retryCount, backupDestFileBeforeCopy))
				{
					retData.CloseoutMsg = "Error copying CalibrationLog.txt file to storage server";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

		}

		public static string AppendToString(string sCurrentText, string sNewText)
		{
			return AppendToString(sCurrentText, sNewText, "; ");
		}

		public static string AppendToString(string sCurrentText, string sNewText, string sSeparator)
		{
			if (string.IsNullOrEmpty(sCurrentText))
				return sNewText;
			else
				return sCurrentText + sSeparator + sNewText;
		}


		/// <summary>
		/// Performs actual calbration operation
		/// </summary>
		/// <param name="inputFilePath">Input file name</param>
		/// <param name="datasetName">Dataset name</param>
		/// <returns>Enum indicating success or failure</returns>
		private bool CalibrateFile(string inputFilePath, string datasetName)
		{
			string msg;
			bool success;

			try
			{

				msg = "Starting caibration, dataset " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

				// Set the options
				m_DeMuxTool.ResumeDemultiplexing = false;
				m_DeMuxTool.CreateCheckpointFiles = false;

				// Set additional options
				m_DeMuxTool.MissingCalTableSearchExternal = true;       // Instruct tool to look for calibration table names in other similarly named .UIMF files if not found in the primary .UIMF file

				// Disable calibration if processing a .UIMF from the older IMS TOFs
				m_DeMuxTool.CalibrateAfterDemultiplexing = true;

				// Use all of the cores
				m_DeMuxTool.CPUCoresToUse = -1;

				success = m_DeMuxTool.CalibrateUIMFFile(inputFilePath);

				// Confirm that things have succeeded
				if (success && m_DeMuxTool != null &&
					m_DeMuxTool.ProcessingStatus == UIMFDemultiplexer.UIMFDemultiplexer.eProcessingStatus.Complete)
				{
					msg = "Calibration complete, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					string errorMsg = "Unknown error";

					// Log the processing status
					if (m_DeMuxTool != null)
					{
						msg = "Calibration processing status: " + m_DeMuxTool.ProcessingStatus.ToString();

						// Get the error msg
						errorMsg = m_DeMuxTool.GetErrorMessage();
						if (string.IsNullOrEmpty(errorMsg))
							errorMsg = "Unknown error";

					}
					else
					{
						msg = "Calibration processing status: ??? (m_DeMuxTool is null)";
					}

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
					success = false;
				}
			}
			catch (Exception ex)
			{
				msg = "Exception calibrating dataset " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				success = false;
			}

			return success;
		}

		/// <summary>
		/// Performs actual demultiplexing operation
		/// </summary>
		/// <param name="inputFilePath">Input file name</param>
		/// <param name="datasetName">Dataset name</param>
		/// <param name="bResumeDemultiplexing">True to Resume demultiplexing using an existing _decoded.uimf.tmp file</param>
		/// <param name="iResumeStartFrame">Frame that we resumed at (output parameter)</param>
		/// <param name="bAutoCalibrate">Set to True to run calibration after demultiplexing; false to skip calibration</param>
		/// <returns>Enum indicating success or failure</returns>
		private bool DemultiplexFile(
			string inputFilePath,
			string datasetName,
			bool bResumeDemultiplexing,
			out int iResumeStartFrame,
			bool bAutoCalibrate)
		{
			const int STATUS_DELAY_MSEC = 5000;

			string msg;
			bool success;
			iResumeStartFrame = 0;

			var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

			var fi = new FileInfo(inputFilePath);
			string folderName = fi.DirectoryName;

			if (string.IsNullOrEmpty(folderName))
			{
				msg = "Could not determine the parent folder for " + inputFilePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
			string outputFilePath = Path.Combine(folderName, datasetName + DECODED_UIMF_SUFFIX);

			System.Threading.Timer tmrUpdateProgress = null;

			try
			{
				m_OutOfMemoryException = false;

				if (bResumeDemultiplexing)
				{
					string sTempUIMFFilePath = outputFilePath + ".tmp";
					if (!File.Exists(sTempUIMFFilePath))
					{
						msg = "Resuming demultiplexing, but .tmp UIMF file not found at " + sTempUIMFFilePath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						m_DeMuxTool.ResumeDemultiplexing = false;
					}
					else
					{
						string sLogEntryAccessorMsg;
						int iMaxDemultiplexedFrameNum = oUIMFLogEntryAccessor.GetMaxDemultiplexedFrame(sTempUIMFFilePath, out sLogEntryAccessorMsg);
						if (iMaxDemultiplexedFrameNum > 0)
						{
							iResumeStartFrame = iMaxDemultiplexedFrameNum + 1;
							m_DeMuxTool.ResumeDemultiplexing = true;
							msg = "Resuming demultiplexing, dataset " + datasetName + " frame " + iResumeStartFrame;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						}
						else
						{
							msg = "Error looking up max demultiplexed frame number from the Log_Entries table in " + sTempUIMFFilePath;
							if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
								msg += "; " + sLogEntryAccessorMsg;

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

							m_DeMuxTool.ResumeDemultiplexing = false;
						}
					}
				}
				else
				{
					msg = "Starting demultiplexing, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_DeMuxTool.ResumeDemultiplexing = false;
				}

				// Enable checkpoint file creation
				m_DeMuxTool.CreateCheckpointFiles = true;

				m_DeMuxTool.CheckpointFrameIntervalMax = MAX_CHECKPOINT_FRAME_INTERVAL;

				m_DeMuxTool.CheckpointWriteFrequencyMinutesMax = MAX_CHECKPOINT_WRITE_FREQUENCY_MINUTES;
				m_DeMuxTool.CheckpointTargetFolder = m_DatasetFolderPathRemote;

				// Set additional options
				m_DeMuxTool.MissingCalTableSearchExternal = true;       // Instruct tool to look for calibration table names in other similarly named .UIMF files if not found in the primary .UIMF file

				// Disable calibration if processing a .UIMF from the older IMS TOFs
				m_DeMuxTool.CalibrateAfterDemultiplexing = bAutoCalibrate;

				// Create a timer that will be used to log progress
				tmrUpdateProgress = new System.Threading.Timer(Demux_Timer_ElapsedEvent);
				tmrUpdateProgress.Change(STATUS_DELAY_MSEC, STATUS_DELAY_MSEC);

				success = m_DeMuxTool.Demultiplex(inputFilePath, outputFilePath);

				// Confirm that things have succeeded
				if (success && m_DeMuxTool != null &&
					m_DeMuxTool.ProcessingStatus == UIMFDemultiplexer.UIMFDemultiplexer.eProcessingStatus.Complete &&
					!m_OutOfMemoryException)
				{
					msg = "Demultiplexing complete, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					string errorMsg = "Unknown error";
					if (m_OutOfMemoryException)
						errorMsg = "OutOfMemory exception was thrown";

					// Log the processing status
					if (m_DeMuxTool != null)
					{
						msg = "Demux processing status: " + m_DeMuxTool.ProcessingStatus.ToString();

						// Get the error msg
						errorMsg = m_DeMuxTool.GetErrorMessage();
						if (string.IsNullOrEmpty(errorMsg))
						{
							errorMsg = "Unknown error";
							if (m_OutOfMemoryException)
								errorMsg = "OutOfMemory exception was thrown";
						}

					}
					else
					{
						msg = "Demux processing status: ??? (m_DeMuxTool is null)";
					}

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
					success = false;
				}
			}
			catch (Exception ex)
			{
				msg = "Exception demultiplexing dataset " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ex.StackTrace);
				success = false;
			}
			finally
			{
				if (tmrUpdateProgress != null)
					tmrUpdateProgress.Dispose();
			}

			return success;
		}	// End sub

		/// <summary>
		/// Renames a file
		/// </summary>
		/// <param name="currFileNamePath">Original file name and path</param>
		/// <param name="newFileNamePath">New file name and path</param>
		/// <returns></returns>
		private bool RenameFile(string currFileNamePath, string newFileNamePath)
		{
			try
			{
				var fi = new FileInfo(currFileNamePath);
				fi.MoveTo(newFileNamePath);
				return true;
			}
			catch (Exception ex)
			{
				string msg = "Exception renaming file " + currFileNamePath + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
		}	// End sub

		private void UpdateDatasetInfo(IMgrParams mgrParams, ITaskParams taskParams)
		{
			m_Dataset = taskParams.GetParam("Dataset");
			m_WorkDir = mgrParams.GetParam("workdir");

			string svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
			m_DatasetFolderPathRemote = Path.Combine(svrPath, taskParams.GetParam("Folder"));

		}

		/// <summary>
		/// Examines the Log_Entries table to make sure the .UIMF file was demultiplexed
		/// </summary>
		/// <param name="localUimfDecodedFilePath"></param>
		/// <param name="retData"></param>
		/// <returns>True if it was demultiplexed, otherwise false</returns>
		private bool ValidateUIMFDemultiplexed(string localUimfDecodedFilePath, clsToolReturnData retData)
		{
			bool bUIMFDemultiplexed = true;
			string msg;

			// Make sure the Log_Entries table contains entry "Finished demultiplexing" (with today's date)
			var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();
			string sLogEntryAccessorMsg;

			DateTime dtDemultiplexingFinished = oUIMFLogEntryAccessor.GetDemultiplexingFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

			if (dtDemultiplexingFinished == DateTime.MinValue)
			{
				retData.CloseoutMsg = "Demultiplexing finished message not found in Log_Entries table";
				msg = retData.CloseoutMsg + " in " + localUimfDecodedFilePath;
				if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
					msg += "; " + sLogEntryAccessorMsg;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				bUIMFDemultiplexed = false;
			}
			else
			{
				if (DateTime.Now.Subtract(dtDemultiplexingFinished).TotalMinutes < 5)
				{
					msg = "Demultiplexing finished message in Log_Entries table has date " + dtDemultiplexingFinished;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					bUIMFDemultiplexed = true;
				}
				else
				{
					retData.CloseoutMsg = "Demultiplexing finished message in Log_Entries table is more than 5 minutes old";
					msg = retData.CloseoutMsg + ": " + dtDemultiplexingFinished + "; assuming this is a demultiplexing failure";
					if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
						msg += "; " + sLogEntryAccessorMsg;

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					bUIMFDemultiplexed = false;
				}
			}

			return bUIMFDemultiplexed;
		}

		/// <summary>
		/// Examines the Log_Entries table to make sure the .UIMF file was calibrated
		/// </summary>
		/// <param name="localUimfDecodedFilePath"></param>
		/// <param name="retData"></param>
		/// <returns>True if it was calibrated, otherwise false</returns>
		private bool ValidateUIMFCalibrated(string localUimfDecodedFilePath, clsToolReturnData retData)
		{
			bool bUIMFCalibrated = true;
			string msg;

			// Make sure the Log_Entries table contains entry "Applied calibration coefficients to all frames" (with today's date)
			var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();
			string sLogEntryAccessorMsg;

			DateTime dtCalibrationApplied = oUIMFLogEntryAccessor.GetCalibrationFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

			if (dtCalibrationApplied == DateTime.MinValue)
			{
				const string sLogMessage = "Applied calibration message not found in Log_Entries table";
				msg = sLogMessage + " in " + localUimfDecodedFilePath;
				if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
					msg += "; " + sLogEntryAccessorMsg;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, sLogMessage);
				bUIMFCalibrated = false;
			}
			else
			{
				if (DateTime.Now.Subtract(dtCalibrationApplied).TotalMinutes < 5)
				{
					msg = "Applied calibration message in Log_Entries table has date " + dtCalibrationApplied;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					bUIMFCalibrated = true;
				}
				else
				{
					const string sLogMessage = "Applied calibration message in Log_Entries table is more than 5 minutes old";
					msg = sLogMessage + ": " + dtCalibrationApplied + "; assuming this is a calibration failure";
					if (!String.IsNullOrEmpty(sLogEntryAccessorMsg))
						msg += "; " + sLogEntryAccessorMsg;

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, sLogMessage);
					bUIMFCalibrated = false;
				}
			}

			return bUIMFCalibrated;
		}


		#endregion

		#region "Event handlers"
		/// <summary>
		/// Logs a message from the demux dll
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		void deMuxTool_MessageEventHandler(object sender, MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Demux message: " + e.Message);
		}

		/// <summary>
		/// Logs a warning from the demux dll
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		void deMuxTool_WarningEventHandler(object sender, MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Demux warning: " + e.Message);
		}

		/// <summary>
		/// Logs an error from the debug dll
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		void deMuxTool_ErrorEventHandler(object sender, MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Demux error: " + e.Message);
			if (e.Message.Contains("OutOfMemoryException"))
				m_OutOfMemoryException = true;
		}

		void Demux_Timer_ElapsedEvent(object stateInfo)
		{
			// Update the status if it has changed since the last call
			if (DemuxProgress != null)
				DemuxProgress(m_DeMuxTool.ProgressPercentComplete);

		}

		#endregion

		#region "Event Handlers"

		void binCentricTableCreator_ProgressEvent(object sender, UIMFLibrary.ProgressEventArgs e)
		{
			if (DateTime.UtcNow.Subtract(m_LastProgressUpdateTime).TotalSeconds >= 5)
			{
				if (BinCentricTableProgress != null)
					BinCentricTableProgress((float)e.PercentComplete);

				m_LastProgressUpdateTime = DateTime.UtcNow;
			}
		}

		void binCentricTableCreator_MessageEvent(object sender, UIMFLibrary.MessageEventArgs e)
		{
			if (DateTime.UtcNow.Subtract(m_LastProgressMessageTime).TotalSeconds >= 30)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message);

				m_LastProgressMessageTime = DateTime.UtcNow;
			}

		}

		#endregion

	}	// End class
}	// End namespace
