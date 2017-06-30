//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//               04/22/2011 dac - Modified to use "real" demultiplexing dll's
//               03/12/2012 mem - Replaced BelovTransform.dll with IMSDemultiplexer.dll
//               08/28/2012 mem - Removed option to use BelovTransform
//               09/30/2014 mem - Switched to using 64-bit UIMFDemultiplexer_Console.exe
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;
using PRISM;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// This class demultiplexes a .UIMF file using the UIMFDemultiplexer
    /// </summary>
    public class clsDemuxTools : clsEventNotifier
    {

        #region "Constants"
        public const string CALIBRATION_LOG_FILE = "CalibrationLog.txt";

        private const string DECODED_UIMF_SUFFIX = "_decoded.uimf";

        // Set the max runtime at 5 days
        private const int MAX_DEMUX_RUNTIME_MINUTES = 1440 * 5;

        // Calibration should be fast (typically just a second a two)
        private const int MAX_CALIBRATION_RUNTIME_MINUTES = 5;

        public const string UIMF_CALIBRATION_UPDATER_NAME = "UIMF Calibration Updater";

        #endregion

        #region "Module variables"

        // Deprecated: UIMFDemultiplexer.UIMFDemultiplexer m_DeMuxTool;

        private string mDataset;
        private string mDatasetFolderPathRemote = string.Empty;
        private string mWorkDir;

        private readonly string mUimfDemultiplexerPath;
        private string mUimfDemultiplexerConsoleOutputFilePath;

        private DateTime mLastProgressUpdateTime;
        private DateTime mLastProgressMessageTime;

        private DateTime mBinCentricStartTime;
        private int mProgressUpdateIntervalSeconds;

        private DateTime mDemuxStartTime;
        private float mDemuxProgressPercentComplete;

        private bool mCalibrating;

        private readonly List<string> mLoggedConsoleOutputErrors;

        private struct udtDemuxOptionsType
        {
            public int FramesToSum;
            public bool CalibrateOnly;
            
            // public int StartFrame;
            // public int EndFrame;

            /// <summary>
            /// Number of bits used to encode the data when multiplexing (historically 4-bit)
            /// </summary>
            public byte NumBitsForEncoding;

            /// <summary>
            /// True to Resume demultiplexing using an existing _decoded.uimf.tmp file
            /// </summary>
            public bool ResumeDemultiplexing;

            // public int NumCores;
            public bool AutoCalibrate;
            public string CheckpointTargetFolder;
        }

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
        public bool OutOfMemoryException { get; private set; }
        #endregion

        #region "Constructor"
        public clsDemuxTools(string uimDemultiplexerPath)
        {
            mProgressUpdateIntervalSeconds = 5;

            mUimfDemultiplexerPath = uimDemultiplexerPath;

            mLoggedConsoleOutputErrors = new List<string>();
        }
        #endregion

        #region "Methods"

        public clsToolReturnData AddBinCentricTablesIfMissing(IMgrParams mgrParams, ITaskParams taskParams, clsToolReturnData retData)
        {
            try
            {
                mLoggedConsoleOutputErrors.Clear();
                UpdateDatasetInfo(mgrParams, taskParams);

                // Locate data file on storage server
                // Don't copy it locally yet
                var sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
                if (string.IsNullOrEmpty(sUimfPath))
                {
                    if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

                using (var objReader = new UIMFLibrary.DataReader(sUimfPath))
                {
                    var hasBinCentricData = objReader.DoesContainBinCentricData();

                    if (hasBinCentricData)
                    {
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                        return retData;
                    }
                }

                // Make sure the working directory is empty
                clsToolRunnerBase.CleanWorkDir(mWorkDir);

                // Copy the UIMF file from the storage server to the working directory
                string uimfRemoteFileNamePath;
                string uimfLocalFileNamePath;
                var uimfFileName = mDataset + ".uimf";

                retData = CopyUIMFToWorkDir(taskParams, uimfFileName, retData, out uimfRemoteFileNamePath, out uimfLocalFileNamePath);
                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                    return retData;

                // Add the bin-centric tables
                using (var uimfReader = new UIMFLibrary.DataReader(uimfLocalFileNamePath))
                {
                    // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in readonly folders
                    var connectionString = "Data Source = " + uimfLocalFileNamePath;
                    using (var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString, true))
                    {
                        dbConnection.Open();

                        // Start a transaction
                        using (var dbCommand = dbConnection.CreateCommand())
                        {
                            dbCommand.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
                            dbCommand.ExecuteNonQuery();
                        }

                        var binCentricTableCreator = new UIMFLibrary.BinCentricTableCreation();

                        // Attach the events
                        binCentricTableCreator.OnProgress += binCentricTableCreator_ProgressEvent;
                        binCentricTableCreator.Message += binCentricTableCreator_MessageEvent;

                        mBinCentricStartTime = DateTime.UtcNow;
                        mProgressUpdateIntervalSeconds = 5;

                        mLastProgressUpdateTime = DateTime.UtcNow;
                        mLastProgressMessageTime = DateTime.UtcNow;

                        binCentricTableCreator.CreateBinCentricTable(dbConnection, uimfReader, mWorkDir);

                        // Finalize the transaction
                        using (var dbCommand = dbConnection.CreateCommand())
                        {
                            dbCommand.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
                            dbCommand.ExecuteNonQuery();
                        }

                        dbConnection.Close();
                    }
                }

                // Confirm that the bin-centric tables were truly added
                using (var objReader = new UIMFLibrary.DataReader(uimfLocalFileNamePath))
                {
                    var hasBinCentricData = objReader.DoesContainBinCentricData();

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
                OnErrorEvent(msg, ex);
                if (retData == null)
                    retData = new clsToolReturnData();
                retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, msg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

        }

        private clsToolReturnData CopyUIMFToWorkDir(
            ITaskParams taskParams,
            string uimfFileName,
            clsToolReturnData retData,
            out string uimfRemoteFileNamePath,
            out string uimfLocalFileNamePath)
        {
            // Locate data file on storage server
            uimfRemoteFileNamePath = Path.Combine(mDatasetFolderPathRemote, uimfFileName);
            uimfLocalFileNamePath = Path.Combine(mWorkDir, mDataset + ".uimf");

            // Copy uimf file to working directory
            OnDebugEvent("Copying file from storage server");
            const int retryCount = 0;
            if (!CopyFileWithRetry(uimfRemoteFileNamePath, uimfLocalFileNamePath, false, retryCount))
            {
                retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Error copying UIMF file to working directory");
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            return retData;

        }

        private string GetRemoteUIMFFilePath(ITaskParams taskParams, ref clsToolReturnData retData)
        {

            try
            {
                // Locate data file on storage server
                // Don't copy it locally; just work with it over the network
                var sUimfPath = Path.Combine(mDatasetFolderPathRemote, mDataset + ".uimf");

                if (File.Exists(sUimfPath))
                    return sUimfPath;

                var msg = "UIMF file not found on storage server, unable to calibrate: " + sUimfPath;
                OnErrorEvent(msg);
                retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "UIMF file not found on storage server, unable to calibrate");
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return string.Empty;
            }
            catch (Exception ex)
            {
                const string msg = "Exception finding UIMF file to calibrate";
                OnErrorEvent(msg, ex);
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
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            var msg = "Calibrating dataset " + mDataset;
            OnDebugEvent(msg);

            bool bAutoCalibrate;

            // Make sure the working directory is empty
            clsToolRunnerBase.CleanWorkDir(mWorkDir);

            // Locate data file on storage server
            // Don't copy it locally; just work with it over the network
            var sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
            if (string.IsNullOrEmpty(sUimfPath))
            {
                if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            try
            {

                // Lookup the instrument name
                var instrumentName = taskParams.GetParam("Instrument_Name");

                switch (instrumentName.ToLower())
                {
                    case "ims_tof_1":
                    case "ims_tof_2":
                    case "ims_tof_3":
                        msg = "Skipping calibration since instrument is " + instrumentName;
                        OnStatusEvent(msg);
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
                OnErrorEvent(msg, ex);
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

                    OnStatusEvent(msg);
                    bAutoCalibrate = false;
                }
                else
                {
                    // Look for the presence of calibration frames or calibration tables
                    // If neither exists, then we cannot perform calibration
                    var bCalibrationDataExists = false;

                    using (var objFrameEnumerator = oFrameList.GetEnumerator())
                    {
                        while (objFrameEnumerator.MoveNext())
                        {
                            if (objFrameEnumerator.Current.Value == UIMFLibrary.DataReader.FrameType.Calibration)
                            {
                                bCalibrationDataExists = true;
                                break;
                            }
                        }
                    }

                    if (!bCalibrationDataExists)
                    {
                        // No calibration frames were found
                        var sCalibrationTables = objReader.GetCalibrationTableNames();
                        if (sCalibrationTables.Count > 0)
                        {
                            bCalibrationDataExists = true;
                        }
                        else
                        {
                            msg = "Skipping calibration since .UIMF file does not contain any calibration frames or calibration tables";
                            OnWarningEvent(msg);
                            bAutoCalibrate = false;
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Exception checking for calibration frames";
                OnErrorEvent(msg, ex);
                retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            if (!bAutoCalibrate)
                return retData;


            // Perform calibration operation
            OnDebugEvent("Calling UIMFDemultiplexer to calibrate");

            var bCalibrationFailed = false;

            try
            {
                string errorMessage;
                if (!CalibrateFile(sUimfPath, mDataset, out errorMessage))
                {
                    if (string.IsNullOrEmpty(errorMessage))
                        errorMessage = "Error calibrating UIMF file";

                    retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, errorMessage);
                    bCalibrationFailed = true;
                }
            }
            catch (Exception ex)
            {
                msg = "Exception calling CalibrateFile for dataset " + mDataset;
                OnErrorEvent(msg, ex);
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
                    OnErrorEvent(msg, ex);
                    retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, "Exception while calibrating UIMF file");
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

            }

            if (!sUimfPath.StartsWith(@"\\"))
            {
                // Copy the CalibrationLog.txt file to the storage server (even if calibration failed)
                CopyCalibrationLogToStorageServer(retData);
            }

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
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            try
            {
                // Locate data file on storage server
                // Don't copy it locally; just work with it over the network
                var sUimfPath = GetRemoteUIMFFilePath(taskParams, ref retData);
                if (string.IsNullOrEmpty(sUimfPath))
                {
                    if (retData != null && retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

                // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in readonly folders
                var connectionString = "Data Source = " + sUimfPath;
                using (var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString, true))
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

                    if (Math.Abs(currentSlope) < double.Epsilon)
                    {
                        const string msg = "Existing CalibrationSlope is 0 in PerformManualCalibration; this is unexpected";
                        OnWarningEvent(msg);
                    }

                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandText = "UPDATE Frame_Parameters SET CalibrationSlope = " + calibrationSlope.ToString("0.0000000") + ", CalibrationIntercept = " + calibrationIntercept.ToString("0.0000000") + ", CALIBRATIONDONE = -1";
                        dbCommand.ExecuteNonQuery();
                    }

                    var logMessage = "Manually applied calibration coefficients to all frames using user-specified calibration coefficients";
                    UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

                    logMessage = "Old calibration coefficients: slope = " + currentSlope + ", intercept = " + currentIntercept;
                    UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

                    logMessage = "New calibration coefficients: slope = " + calibrationSlope.ToString("0.0000000") + ", intercept = " + calibrationIntercept.ToString("0.0000000");
                    UIMFLibrary.DataWriter.PostLogEntry(dbConnection, "Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

                }

            }
            catch (Exception ex)
            {
                var msg = "Exception in PerformManualCalibration for dataset " + mDataset;
                OnErrorEvent(msg, ex);
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
        /// <param name="uimfFileName">Name of the .uimf file</param>
        /// <param name="numBitsForEncoding">Number of bits used to encode the file (traditionally 4 bit)</param>
        /// <returns>Enum indicating task success or failure</returns>
        public clsToolReturnData PerformDemux(
            IMgrParams mgrParams,
            ITaskParams taskParams,
            string uimfFileName,
            byte numBitsForEncoding)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            var jobNum = taskParams.GetParam("Job");
            var msg = "Performing demultiplexing, job " + jobNum + ", dataset " + mDataset;
            OnStatusEvent(msg);

            var retData = new clsToolReturnData();

            var bPostProcessingError = false;

            // Default to summing 5 LC frames if this parameter is not defined
            var framesToSum = taskParams.GetParam("DemuxFramesToSum", 5);

            if (framesToSum > 1)
                OnStatusEvent("Will sum " + framesToSum + " LC Frames when demultiplexing");

            // Make sure the working directory is empty
            clsToolRunnerBase.CleanWorkDir(mWorkDir);

            // Copy the UIMF file from the storage server to the working directory
            string uimfRemoteEncodedFileNamePath;
            string uimfLocalEncodedFileNamePath;

            retData = CopyUIMFToWorkDir(taskParams, uimfFileName, retData, out uimfRemoteEncodedFileNamePath, out uimfLocalEncodedFileNamePath);
            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return retData;

            // Look for a _decoded.uimf.tmp file on the storage server
            // Copy it local if present
            var sTmpUIMFFileName = mDataset + DECODED_UIMF_SUFFIX + ".tmp";
            var sTmpUIMFRemoteFileNamePath = Path.Combine(mDatasetFolderPathRemote, sTmpUIMFFileName);
            var sTmpUIMFLocalFileNamePath = Path.Combine(mWorkDir, sTmpUIMFFileName);

            var demuxOptions = new udtDemuxOptionsType
            {
                AutoCalibrate = false,
                FramesToSum = framesToSum,
                ResumeDemultiplexing = false,
                NumBitsForEncoding = numBitsForEncoding
            };

            int resumeStartFrame;

            if (File.Exists(sTmpUIMFRemoteFileNamePath))
            {
                // Copy _decoded.uimf.tmp file to working directory so that we can resume demultiplexing where we left off
                const int retryCount = 0;
                if (CopyFileWithRetry(sTmpUIMFRemoteFileNamePath, sTmpUIMFLocalFileNamePath, false, retryCount))
                {
                    OnStatusEvent(".tmp decoded file found at " + sTmpUIMFRemoteFileNamePath + "; will resume demultiplexing");
                    demuxOptions.ResumeDemultiplexing = true;
                }
                else
                {
                    OnStatusEvent("Error copying .tmp decoded file from " + sTmpUIMFRemoteFileNamePath + " to work folder; unable to resume demultiplexing");
                }
            }


            // Perform demux operation
            OnDebugEvent("Calling UIMFDemultiplexer_Console.exe");

            try
            {
                string errorMessage;
                if (!DemultiplexFile(uimfLocalEncodedFileNamePath, mDataset, demuxOptions, out resumeStartFrame, out errorMessage))
                {
                    if (string.IsNullOrEmpty(errorMessage))
                        errorMessage = "Error demultiplexing UIMF file";

                    retData.CloseoutMsg = errorMessage;
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }
            }
            catch (Exception ex)
            {
                msg = "Exception calling DemultiplexFile for dataset " + mDataset;
                OnErrorEvent(msg, ex);
                retData.CloseoutMsg = "Error demultiplexing UIMF file";
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }


            // Look for the demultiplexed .UIMF file
            var localUimfDecodedFilePath = Path.Combine(mWorkDir, mDataset + DECODED_UIMF_SUFFIX);

            if (!File.Exists(localUimfDecodedFilePath))
            {
                retData.CloseoutMsg = "Decoded UIMF file not found";
                OnErrorEvent(retData.CloseoutMsg + ": " + localUimfDecodedFilePath);
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
                OnDebugEvent(msg);

                // If this is a re-run, then encoded file has already been renamed
                // This is determined by looking for "_encoded" in uimf file name
                if (!uimfFileName.Contains("_encoded"))
                {
                    if (!RenameFile(uimfRemoteEncodedFileNamePath, Path.Combine(mDatasetFolderPathRemote, mDataset + "_encoded.uimf")))
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
                if (!string.IsNullOrEmpty(mDatasetFolderPathRemote))
                {
                    msg = "Deleting .uimf.tmp CheckPoint file from storage server";
                    OnDebugEvent(msg);

                    try
                    {
                        var sCheckpointTargetPath = Path.Combine(mDatasetFolderPathRemote, sTmpUIMFFileName);

                        if (File.Exists(sCheckpointTargetPath))
                            File.Delete(sCheckpointTargetPath);
                    }
                    catch (Exception ex)
                    {
                        msg = "Error deleting .uimf.tmp CheckPoint file: " + ex.Message;
                        OnErrorEvent(msg);
                    }

                }
            }

            if (!bPostProcessingError)
            {
                // Copy the result files to the storage server
                if (!CopyUIMFFileToStorageServer(retData, localUimfDecodedFilePath, "de-multiplexed UIMF"))
                {
                    bPostProcessingError = true;
                }

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
                oFailedResultsCopier.CopyFailedResultsToArchiveFolder(mWorkDir);

                return retData;
            }

            // Delete local uimf file(s)
            msg = "Cleaning up working directory";
            OnDebugEvent(msg);
            try
            {
                File.Delete(localUimfDecodedFilePath);
                File.Delete(uimfLocalEncodedFileNamePath);
            }
            catch (Exception ex)
            {
                // Error deleting files; don't treat this as a fatal error
                msg = "Exception deleting working directory file(s): " + ex.Message;
                OnErrorEvent(msg);
            }

            // Update the return data
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            retData.EvalMsg = "De-multiplexed";

            if (demuxOptions.ResumeDemultiplexing)
                retData.EvalMsg += " (resumed at frame " + resumeStartFrame + ")";

            return retData;

        }

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
        public static bool CopyFileWithRetry(string sourceFilePath, string targetFilePath, bool overWrite, int retryCount,
                                             bool backupDestFileBeforeCopy)
        {
            var bRetryingCopy = false;

            if (retryCount < 0)
                retryCount = 0;

            var oFileTools = new clsFileTools();

            while (retryCount >= 0)
            {
                string msg;
                try
                {
                    if (bRetryingCopy)
                    {
                        msg = "Retrying copy; retryCount = " + retryCount;
                        clsUtilities.LogMessage(msg);
                    }

                    oFileTools.CopyFile(sourceFilePath, targetFilePath, overWrite, backupDestFileBeforeCopy);
                    return true;
                }
                catch (Exception ex)
                {
                    msg = "Exception copying file " + sourceFilePath + " to " + targetFilePath + ": " + ex.Message;
                    clsUtilities.LogError(msg, ex);

                    System.Threading.Thread.Sleep(2000);
                    retryCount -= 1;
                    bRetryingCopy = true;
                }
            }

            // If we get here, then we were not able to successfully copy the file
            return false;

        }


        /// <summary>
        /// Copies the result files to the storage server
        /// </summary>
        /// <param name="retData"></param>
        /// <param name="localUimfDecodedFilePath"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if success; otherwise false</returns>
        private bool CopyUIMFFileToStorageServer(clsToolReturnData retData, string localUimfDecodedFilePath, string fileDescription)
        {
            var bSuccess = true;

            // Copy demuxed file to storage server, renaming as datasetname.uimf in the process
            var msg = "Copying " + fileDescription + " file to storage server";
            OnDebugEvent(msg);
            const int retryCount = 3;
            if (!CopyFileWithRetry(localUimfDecodedFilePath, Path.Combine(mDatasetFolderPathRemote, mDataset + ".uimf"), true, retryCount))
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
            var sCalibrationLogFilePath = Path.Combine(mWorkDir, CALIBRATION_LOG_FILE);
            var sTargetFilePath = Path.Combine(mDatasetFolderPathRemote, CALIBRATION_LOG_FILE);

            if (!File.Exists(sCalibrationLogFilePath))
            {
                msg = "CalibrationLog.txt not found at " + mWorkDir;
                if (File.Exists(sTargetFilePath))
                {
                    msg += "; this is OK since " + CALIBRATION_LOG_FILE + " exists at " + mDatasetFolderPathRemote;
                    OnDebugEvent(msg);
                }
                else
                {
                    msg += "; in addition, could not find " + CALIBRATION_LOG_FILE + " at " + mDatasetFolderPathRemote;
                    OnErrorEvent(msg);
                }

            }
            else
            {
                msg = "Copying CalibrationLog.txt file to storage server";
                OnDebugEvent(msg);
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

            return sCurrentText + sSeparator + sNewText;
        }


        /// <summary>
        /// Performs actual calbration operation
        /// </summary>
        /// <param name="inputFilePath">Input file name</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool CalibrateFile(string inputFilePath, string datasetName, out string errorMessage)
        {
            try
            {
                var msg = "Starting calibration, dataset " + datasetName;
                OnStatusEvent(msg);

                // Set the options
                var demuxOptions = new udtDemuxOptionsType
                {
                    ResumeDemultiplexing = false,
                    CheckpointTargetFolder = string.Empty,
                    CalibrateOnly = true
                };

                var success = RunUIMFDemultiplexer(inputFilePath, inputFilePath, demuxOptions, MAX_CALIBRATION_RUNTIME_MINUTES, out errorMessage);

                // Confirm that things have succeeded
                if (success && mLoggedConsoleOutputErrors.Count == 0)
                {
                    msg = "Calibration complete, dataset " + datasetName;
                    OnStatusEvent(msg);
                    return true;
                }

                if (string.IsNullOrEmpty(errorMessage))
                    errorMessage = "Unknown error";

                if (string.IsNullOrEmpty(errorMessage) && mLoggedConsoleOutputErrors.Count > 0)
                {
                    errorMessage = mLoggedConsoleOutputErrors.First();
                }

                OnErrorEvent(errorMessage);
                return false;

            }
            catch (Exception ex)
            {
                errorMessage = "Exception calibrating dataset";
                OnErrorEvent(errorMessage + " " + datasetName, ex);
                return false;
            }

        }

        /// <summary>
        /// Performs actual demultiplexing operation
        /// </summary>
        /// <param name="inputFilePath">Input file name</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="demuxOptions">Demultiplexing options</param>
        /// <param name="resumeStartFrame">Frame that we resumed at (output parameter)</param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool DemultiplexFile(
            string inputFilePath,
            string datasetName,
            udtDemuxOptionsType demuxOptions,
            out int resumeStartFrame,
            out string errorMessage)
        {
            string msg;
            resumeStartFrame = 0;
            errorMessage = string.Empty;

            var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

            var fi = new FileInfo(inputFilePath);
            var folderName = fi.DirectoryName;

            if (string.IsNullOrEmpty(folderName))
            {
                errorMessage = "Could not determine the parent folder for " + inputFilePath;
                OnErrorEvent(errorMessage);
                return false;
            }
            var outputFilePath = Path.Combine(folderName, datasetName + DECODED_UIMF_SUFFIX);

            try
            {
                OutOfMemoryException = false;

                if (demuxOptions.ResumeDemultiplexing)
                {
                    var sTempUIMFFilePath = outputFilePath + ".tmp";
                    if (!File.Exists(sTempUIMFFilePath))
                    {
                        errorMessage = "Resuming demultiplexing, but .tmp UIMF file not found at " + sTempUIMFFilePath;
                        OnErrorEvent(errorMessage);
                        demuxOptions.ResumeDemultiplexing = false;
                    }
                    else
                    {
                        string sLogEntryAccessorMsg;
                        var iMaxDemultiplexedFrameNum = oUIMFLogEntryAccessor.GetMaxDemultiplexedFrame(sTempUIMFFilePath, out sLogEntryAccessorMsg);
                        if (iMaxDemultiplexedFrameNum > 0)
                        {
                            resumeStartFrame = iMaxDemultiplexedFrameNum + 1;
                            demuxOptions.ResumeDemultiplexing = true;
                            msg = "Resuming demultiplexing, dataset " + datasetName + " frame " + resumeStartFrame;
                            OnStatusEvent(msg);
                        }
                        else
                        {
                            errorMessage = "Error looking up max demultiplexed frame number from the Log_Entries table";
                            msg = errorMessage + " in " + sTempUIMFFilePath;
                            if (!string.IsNullOrEmpty(sLogEntryAccessorMsg))
                                msg += "; " + sLogEntryAccessorMsg;

                            OnErrorEvent(msg);

                            demuxOptions.ResumeDemultiplexing = false;
                        }
                    }
                }
                else
                {
                    msg = "Starting demultiplexing, dataset " + datasetName;
                    OnStatusEvent(msg);
                    demuxOptions.ResumeDemultiplexing = false;
                }

                // Enable checkpoint file creation
                demuxOptions.CheckpointTargetFolder = mDatasetFolderPathRemote;

                var success = RunUIMFDemultiplexer(inputFilePath, outputFilePath, demuxOptions, MAX_DEMUX_RUNTIME_MINUTES, out errorMessage);

                // Confirm that things have succeeded
                if (success && mLoggedConsoleOutputErrors.Count == 0 && !OutOfMemoryException)
                {
                    msg = "Demultiplexing complete, dataset " + datasetName;
                    OnStatusEvent(msg);
                    return true;
                }

                if (string.IsNullOrEmpty(errorMessage))
                    errorMessage = "Unknown error";

                if (OutOfMemoryException)
                    errorMessage = "OutOfMemory exception was thrown";

                if (string.IsNullOrEmpty(errorMessage) && mLoggedConsoleOutputErrors.Count > 0)
                {
                    errorMessage = mLoggedConsoleOutputErrors.First();
                }

                OnErrorEvent(errorMessage);
                return false;
            }
            catch (Exception ex)
            {
                msg = "Exception demultiplexing dataset " + datasetName;
                OnErrorEvent(msg, ex);
                return false;
            }

        }


        private void ParseConsoleOutputFileDemux()
        {
            // Example Console output:
            //
            // Demultiplexing PlasmaND_2pt5ng_0pt005fmol_Frac05_9Sep14_Methow_14-06-13_encoded.uimf
            //  in folder F:\My Documents\Projects\DataMining\UIMFDemultiplexer\UIMFDemultiplexer_Console\bin
            // Auto-switching instrument from IMS4 to QTOF
            //
            // Cloning .UIMF file
            // Initializing data arrays
            //
            // Total number of frames to demultiplex: 39
            //  (processing frames 1 to 40)
            //
            // Demultiplexing frame 1
            // Demultiplexing frame 2
            // Demultiplexing frame 3
            // Processing: 5%
            // Demultiplexing frame 4
            // Demultiplexing frame 5
            // Demultiplexing frame 19
            // ...
            // Processing: 100%
            //
            // PlasmaND_2pt5ng_0pt005fmol_Frac05_9Sep14_Methow_14-06-13_encoded_inverse.uimf already exists; renaming existing file
            // Finished demultiplexing all frames. Now performing calibration
            // Calibration frame 26 matched 7 / 7 calibrants within 10 ppm; Slope = 0.347632, Intercept = 0.034093;
            //  Average AbsoluteValue(mass error) = 1.736 ppm; average mass error = 0.007 ppm


            var rePercentComplete = new Regex(@"Processing: (\d+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var reTotalFrames = new Regex(@"frames to demultiplex: (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var reCurrentFrame = new Regex(@"Demultiplexing frame (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var totalframeCount = 0;
            var framesProcessed = 0;

            try
            {
                if (string.IsNullOrEmpty(mUimfDemultiplexerConsoleOutputFilePath))
                    return;

                using (var srInFile = new StreamReader(new FileStream(mUimfDemultiplexerConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (srInFile.Peek() >= 0)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        if (strLineIn.StartsWith("Error in") | strLineIn.StartsWith("Error:"))
                        {
                            if (!mLoggedConsoleOutputErrors.Contains(strLineIn))
                            {
                                OnErrorEvent(strLineIn);
                                mLoggedConsoleOutputErrors.Add(strLineIn);
                            }

                            if (strLineIn.Contains("OutOfMemoryException"))
                                OutOfMemoryException = true;

                        }
                        else if (strLineIn.StartsWith("Warning:"))
                        {
                            if (!mLoggedConsoleOutputErrors.Contains(strLineIn))
                            {
                                OnWarningEvent(strLineIn);
                                mLoggedConsoleOutputErrors.Add(strLineIn);
                            }
                        }
                        else
                        {
                            // Compare the line against the various Regex specs

                            // % complete (integer values only)
                            var oMatch = rePercentComplete.Match(strLineIn);

                            if (oMatch.Success)
                            {
                                short percentComplete;
                                if (short.TryParse(oMatch.Groups[1].Value, out percentComplete))
                                {
                                    mDemuxProgressPercentComplete = percentComplete;
                                }
                            }

                            // Total frames
                            oMatch = reTotalFrames.Match(strLineIn);

                            if (oMatch.Success)
                            {
                                int.TryParse(oMatch.Groups[1].Value, out totalframeCount);
                            }

                            // Current frame processed
                            oMatch = reCurrentFrame.Match(strLineIn);

                            if (oMatch.Success)
                            {
                                int.TryParse(oMatch.Groups[1].Value, out framesProcessed);
                            }
                        }

                    }
                }

                if (totalframeCount > 0)
                {
                    var percentCompleteFractional = framesProcessed / (float)totalframeCount * 100;

                    if (percentCompleteFractional > mDemuxProgressPercentComplete)
                        mDemuxProgressPercentComplete = percentCompleteFractional;
                }


            }
            catch (Exception ex)
            {
                if (!mLoggedConsoleOutputErrors.Contains(ex.Message))
                {
                    OnErrorEvent("Exception in ParseConsoleOutputFileDemux", ex);
                    mLoggedConsoleOutputErrors.Add(ex.Message);
                }

            }

        }

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
                var msg = "Exception renaming file " + currFileNamePath + " to " + Path.GetFileName(newFileNamePath) + ": " + ex.Message;
                OnErrorEvent(msg);

                // Garbage collect, then try again to rename the file
                System.Threading.Thread.Sleep(250);
                clsProgRunner.GarbageCollectNow();
                System.Threading.Thread.Sleep(250);

                try
                {
                    var fi = new FileInfo(currFileNamePath);
                    fi.MoveTo(newFileNamePath);
                    return true;
                }
                catch (Exception ex2)
                {
                    msg = "Rename failed despite garbage collection call: " + ex2.Message;
                    OnErrorEvent(msg);
                }

                return false;
            }
        }

        /// <summary>
        /// This function is called both by CalibrateFile and DemultiplexFile
        /// </summary>
        /// <param name="inputFilePath"></param>
        /// <param name="outputFilePath"></param>
        /// <param name="demuxOptions"></param>
        /// <param name="maxRuntimeMinutes"></param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns></returns>
        private bool RunUIMFDemultiplexer(
            string inputFilePath,
            string outputFilePath,
            udtDemuxOptionsType demuxOptions,
            int maxRuntimeMinutes,
            out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                var fiInputFile = new FileInfo(inputFilePath);
                var fiOutputFile = new FileInfo(outputFilePath);

                // Construct the command line arguments

                // Input file
                var cmdStr = clsConversion.PossiblyQuotePath(inputFilePath);

                if (String.Compare(fiInputFile.DirectoryName, fiOutputFile.DirectoryName, StringComparison.OrdinalIgnoreCase) != 0)
                {
                    // Output folder
                    cmdStr += " /O:" + clsConversion.PossiblyQuotePath(fiOutputFile.DirectoryName);
                }

                if (demuxOptions.CalibrateOnly)
                {
                    // Calibrating
                    mCalibrating = true;

                    cmdStr += " /CalibrateOnly";

                    // Instruct tool to look for calibration table names in other similarly named .UIMF files if not found in the primary .UIMF file
                    cmdStr += " /CX";
                }
                else
                {
                    // Demultiplexing
                    mCalibrating = false;

                    // Output file name
                    cmdStr += " /N:" + clsConversion.PossiblyQuotePath(fiOutputFile.Name);

                    if (demuxOptions.NumBitsForEncoding > 1)
                        cmdStr += " /Bits:" + demuxOptions.NumBitsForEncoding;

                    /*
                    if (demuxOptions.StartFrame > 0)
                        cmdStr += " /First:" + demuxOptions.StartFrame;

                    if (demuxOptions.EndFrame > 0)
                        cmdStr += " /Last:" + demuxOptions.EndFrame;
                    */

                    cmdStr += " /FramesToSum:" + demuxOptions.FramesToSum;

                    if (demuxOptions.ResumeDemultiplexing)
                    {
                        cmdStr += " /Resume";
                    }

                    /*
                    if (demuxOptions.NumCores > 0)
                    {
                        cmdStr += " /Cores:" + demuxOptions.NumCores;
                    }
                    */

                    if (!demuxOptions.AutoCalibrate)
                    {
                        cmdStr += " /SkipCalibration";
                    }

                    if (!string.IsNullOrEmpty(demuxOptions.CheckpointTargetFolder))
                    {
                        cmdStr += " /CheckPointFolder:" + clsConversion.PossiblyQuotePath(demuxOptions.CheckpointTargetFolder);
                    }

                }

                mUimfDemultiplexerConsoleOutputFilePath = Path.Combine(mWorkDir, "UIMFDemultiplexer_ConsoleOutput.txt");

                OnStatusEvent(mUimfDemultiplexerPath + " " + cmdStr);
                var cmdRunner = new clsRunDosProgram(mWorkDir);
                mDemuxStartTime = DateTime.UtcNow;
                mLastProgressUpdateTime = DateTime.UtcNow;
                mLastProgressMessageTime = DateTime.UtcNow;

                AttachCmdrunnerEvents(cmdRunner);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;

                if (demuxOptions.CalibrateOnly)
                {
                    // Note that file CalibrationLog.txt will be auto-created by UIMFDemultiplexer_Console.exe during the calibration
                    cmdRunner.WriteConsoleOutputToFile = false;
                }
                else
                {
                    // Create a console output file
                    cmdRunner.WriteConsoleOutputToFile = true;
                    cmdRunner.ConsoleOutputFilePath = mUimfDemultiplexerConsoleOutputFilePath;
                }

                var bSuccess = cmdRunner.RunProgram(mUimfDemultiplexerPath, cmdStr, "UIMFDemultiplexer", true, maxRuntimeMinutes * 60);

                if (!mCalibrating)
                    ParseConsoleOutputFileDemux();

                if (bSuccess)
                    return true;

                errorMessage = "Error running UIMF Demultiplexer";
                OnErrorEvent(errorMessage);

                if (cmdRunner.ExitCode != 0)
                {
                    OnWarningEvent("UIMFDemultiplexer returned a non-zero exit code: " + cmdRunner.ExitCode.ToString());
                }
                else
                {
                    OnWarningEvent("Call to UIMFDemultiplexer failed (but exit code is 0)");
                }

                return false;

            }
            catch (Exception ex)
            {
                errorMessage = "Exception in RunUIMFDemultiplexer";
                OnErrorEvent(errorMessage, ex);
                return false;
            }


        }

        private void UpdateDatasetInfo(IMgrParams mgrParams, ITaskParams taskParams)
        {
            mDataset = taskParams.GetParam("Dataset");
            mWorkDir = mgrParams.GetParam("workdir");

            var svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
            mDatasetFolderPathRemote = Path.Combine(svrPath, taskParams.GetParam("Folder"));

        }

        /// <summary>
        /// Examines the Log_Entries table to make sure the .UIMF file was demultiplexed
        /// </summary>
        /// <param name="localUimfDecodedFilePath"></param>
        /// <param name="retData"></param>
        /// <returns>True if it was demultiplexed, otherwise false</returns>
        private bool ValidateUIMFDemultiplexed(string localUimfDecodedFilePath, clsToolReturnData retData)
        {
            bool bUIMFDemultiplexed;
            string msg;

            // Make sure the Log_Entries table contains entry "Finished demultiplexing" (with today's date)
            var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();
            string sLogEntryAccessorMsg;

            var dtDemultiplexingFinished = oUIMFLogEntryAccessor.GetDemultiplexingFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

            if (dtDemultiplexingFinished == DateTime.MinValue)
            {
                retData.CloseoutMsg = "Demultiplexing finished message not found in Log_Entries table";
                msg = retData.CloseoutMsg + " in " + localUimfDecodedFilePath;
                if (!string.IsNullOrEmpty(sLogEntryAccessorMsg))
                    msg += "; " + sLogEntryAccessorMsg;

                OnErrorEvent(msg);
                bUIMFDemultiplexed = false;
            }
            else
            {
                if (DateTime.Now.Subtract(dtDemultiplexingFinished).TotalMinutes < 5)
                {
                    msg = "Demultiplexing finished message in Log_Entries table has date " + dtDemultiplexingFinished;
                    OnDebugEvent(msg);
                    bUIMFDemultiplexed = true;
                }
                else
                {
                    retData.CloseoutMsg = "Demultiplexing finished message in Log_Entries table is more than 5 minutes old";
                    msg = retData.CloseoutMsg + ": " + dtDemultiplexingFinished + "; assuming this is a demultiplexing failure";
                    if (!string.IsNullOrEmpty(sLogEntryAccessorMsg))
                        msg += "; " + sLogEntryAccessorMsg;

                    OnErrorEvent(msg);
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
            bool bUIMFCalibrated;
            string msg;

            // Make sure the Log_Entries table contains entry "Applied calibration coefficients to all frames" (with today's date)
            var oUIMFLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();
            string sLogEntryAccessorMsg;

            var dtCalibrationApplied = oUIMFLogEntryAccessor.GetCalibrationFinishDate(localUimfDecodedFilePath, out sLogEntryAccessorMsg);

            if (dtCalibrationApplied == DateTime.MinValue)
            {
                const string sLogMessage = "Applied calibration message not found in Log_Entries table";
                msg = sLogMessage + " in " + localUimfDecodedFilePath;
                if (!string.IsNullOrEmpty(sLogEntryAccessorMsg))
                    msg += "; " + sLogEntryAccessorMsg;

                OnErrorEvent(msg);
                retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, sLogMessage);
                bUIMFCalibrated = false;
            }
            else
            {
                if (DateTime.Now.Subtract(dtCalibrationApplied).TotalMinutes < 5)
                {
                    msg = "Applied calibration message in Log_Entries table has date " + dtCalibrationApplied;
                    OnDebugEvent(msg);
                    bUIMFCalibrated = true;
                }
                else
                {
                    const string sLogMessage = "Applied calibration message in Log_Entries table is more than 5 minutes old";
                    msg = sLogMessage + ": " + dtCalibrationApplied + "; assuming this is a calibration failure";
                    if (!string.IsNullOrEmpty(sLogEntryAccessorMsg))
                        msg += "; " + sLogEntryAccessorMsg;

                    OnErrorEvent(msg);
                    retData.CloseoutMsg = AppendToString(retData.CloseoutMsg, sLogMessage);
                    bUIMFCalibrated = false;
                }
            }

            return bUIMFCalibrated;
        }


        #endregion

        #region "Event handlers"

        private void AttachCmdrunnerEvents(clsRunDosProgram cmdRunner)
        {
            try
            {
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
                cmdRunner.Timeout += CmdRunner_Timeout;
            }
            catch
            {
                // Ignore errors here
            }
        }

        void CmdRunner_Timeout()
        {
            OnErrorEvent("CmdRunner timeout reported");
        }

        void CmdRunner_LoopWaiting()
        {

            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
            {
                mLastProgressUpdateTime = DateTime.UtcNow;

                string toolName;

                if (mCalibrating)
                {
                    toolName = "Calibration";
                }
                else
                {
                    toolName = "UIMFDemultiplexer";
                    ParseConsoleOutputFileDemux();

                    DemuxProgress?.Invoke(mDemuxProgressPercentComplete);
                }

                if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds >= 300)
                {
                    mLastProgressMessageTime = DateTime.UtcNow;
                    OnDebugEvent(toolName + " running; " + DateTime.UtcNow.Subtract(mDemuxStartTime).TotalMinutes.ToString("0.0") + " minutes elapsed, " + mDemuxProgressPercentComplete.ToString("0.0") + "% complete");
                }
            }
        }

        void binCentricTableCreator_ProgressEvent(object sender, UIMFLibrary.ProgressEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < mProgressUpdateIntervalSeconds)
                return;

            if (DateTime.UtcNow.Subtract(mBinCentricStartTime).TotalMinutes > 5 && mProgressUpdateIntervalSeconds < 15)
            {
                mProgressUpdateIntervalSeconds = 15;
            }
            else
            if (DateTime.UtcNow.Subtract(mBinCentricStartTime).TotalMinutes > 10 && mProgressUpdateIntervalSeconds < 30)
            {
                mProgressUpdateIntervalSeconds = 30;
            }
            else
            if (DateTime.UtcNow.Subtract(mBinCentricStartTime).TotalMinutes > 30 && mProgressUpdateIntervalSeconds < 60)
            {
                mProgressUpdateIntervalSeconds = 60;
            }

            BinCentricTableProgress?.Invoke((float)e.PercentComplete);

            mLastProgressUpdateTime = DateTime.UtcNow;
        }

        void binCentricTableCreator_MessageEvent(object sender, UIMFLibrary.MessageEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds < 30)
                return;

            OnStatusEvent(e.Message);

            mLastProgressMessageTime = DateTime.UtcNow;
        }

        #endregion

    }
}
