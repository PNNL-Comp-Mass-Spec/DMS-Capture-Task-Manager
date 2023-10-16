//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;
using PRISM;
using PRISM.Logging;
using UIMFLibrary;

namespace ImsDemuxPlugin
{
    // TODO: Update for newer version UIMF files, with the mux sequence stored directly in MultiplexingEncodingSequence.

    /// <summary>
    /// This class demultiplexes a .UIMF file using the UIMFDemultiplexer
    /// </summary>
    public class DemuxTools : EventNotifier
    {
        // Ignore Spelling: calibrants, cmd, demultiplexed, demultiplexer, demultiplexes, demultiplexing, demux, dest
        // Ignore Spelling: ims, ims_tof, Methow, mux, UIMF, workdir

        public const string CALIBRATION_LOG_FILE = "CalibrationLog.txt";

        private const string DECODED_UIMF_SUFFIX = "_decoded.uimf";

        // Set the max runtime at 5 days
        private const int MAX_DEMUX_RUNTIME_DAYS = 5;
        private const int MAX_DEMUX_RUNTIME_MINUTES = 1440 * MAX_DEMUX_RUNTIME_DAYS;

        // Calibration should be fast (typically just a second a two)
        private const int MAX_CALIBRATION_RUNTIME_MINUTES = 5;

        public const string UIMF_CALIBRATION_UPDATER_NAME = "UIMF Calibration Updater";

        private string mDataset;
        private string mDatasetDirectoryPathRemote = string.Empty;
        private readonly FileTools mFileTools;
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

        private struct DemuxOptionsType
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

            public string CheckpointTargetDirectory;
        }

        // Events used for communication back to PluginMain, where the logging and status updates are handled

        public event DelDemuxProgressHandler DemuxProgress;
        public event DelDemuxProgressHandler BinCentricTableProgress;

        public event StatusEventEventHandler CopyFileWithRetryEvent;

        public bool OutOfMemoryException { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="uimfDemultiplexerPath"></param>
        /// <param name="fileTools"></param>
        public DemuxTools(string uimfDemultiplexerPath, FileTools fileTools)
        {
            mProgressUpdateIntervalSeconds = 5;

            mUimfDemultiplexerPath = uimfDemultiplexerPath;
            mFileTools = fileTools;

            mLoggedConsoleOutputErrors = new List<string>();
        }

        /// <summary>
        /// Add bin-centric tables if missing
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="returnData"></param>
        /// <returns>Updated version of returnData</returns>
        public ToolReturnData AddBinCentricTablesIfMissing(IMgrParams mgrParams, ITaskParams taskParams, ToolReturnData returnData)
        {
            try
            {
                mLoggedConsoleOutputErrors.Clear();
                UpdateDatasetInfo(mgrParams, taskParams);

                // Locate data file on storage server
                // Don't copy it locally yet
                var uimfFilePath = GetRemoteUIMFFilePath(returnData);

                if (string.IsNullOrEmpty(uimfFilePath))
                {
                    if (returnData != null && returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    return returnData;
                }

                using (var uimfReader = new DataReader(uimfFilePath))
                {
                    var hasBinCentricData = uimfReader.DoesContainBinCentricData();

                    if (hasBinCentricData)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                        return returnData;
                    }
                }

                // Make sure the working directory is empty
                ToolRunnerBase.CleanWorkDir(mWorkDir);

                // Copy the UIMF file from the storage server to the working directory
                var uimfFileName = mDataset + ".uimf";

                returnData = CopyUIMFToWorkDir(uimfFileName, returnData, out _, out var uimfLocalFileNamePath);

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    return returnData;
                }

                // Add the bin-centric tables
                using (var uimfReader = new DataReader(uimfLocalFileNamePath))
                {
                    // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in ReadOnly directories
                    var connectionString = "Data Source = " + uimfLocalFileNamePath;

                    using var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString, true);

                    dbConnection.Open();

                    // Start a transaction
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
                        dbCommand.ExecuteNonQuery();
                    }

                    var binCentricTableCreator = new BinCentricTableCreation();

                    // Attach the events
                    binCentricTableCreator.OnProgress += BinCentricTableCreator_ProgressEvent;
                    binCentricTableCreator.Message += BinCentricTableCreator_MessageEvent;

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

                // Confirm that the bin-centric tables were truly added
                using (var uimfReader = new DataReader(uimfLocalFileNamePath))
                {
                    var hasBinCentricData = uimfReader.DoesContainBinCentricData();

                    if (!hasBinCentricData)
                    {
                        returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Bin-centric tables were not added to the UIMF file");
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Copy the result files to the storage server
                if (!CopyUIMFFileToStorageServer(returnData, uimfLocalFileNamePath, "bin-centric UIMF"))
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
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

                return returnData;
            }
            catch (Exception ex)
            {
                const string msg = "Exception adding the bin-centric tables to the UIMF file";
                OnErrorEvent(msg, ex);
                returnData ??= new ToolReturnData();

                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }
        }

        private ToolReturnData CopyUIMFToWorkDir(
            string uimfFileName,
            ToolReturnData returnData,
            out string uimfRemoteFileNamePath,
            out string uimfLocalFileNamePath)
        {
            // Locate data file on storage server
            uimfRemoteFileNamePath = Path.Combine(mDatasetDirectoryPathRemote, uimfFileName);
            uimfLocalFileNamePath = Path.Combine(mWorkDir, mDataset + ".uimf");

            // Copy the UIMF file to working directory
            OnDebugEvent("Copying file from storage server");
            const int retryCount = 0;

            if (!CopyFileWithRetry(uimfRemoteFileNamePath, uimfLocalFileNamePath, false, retryCount))
            {
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Error copying UIMF file to working directory");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            return returnData;
        }

        private string GetRemoteUIMFFilePath(ToolReturnData returnData)
        {
            try
            {
                // Locate data file on storage server
                // Don't copy it locally; just work with it over the network
                var uimfFilePath = Path.Combine(mDatasetDirectoryPathRemote, mDataset + ".uimf");

                if (File.Exists(uimfFilePath))
                {
                    return uimfFilePath;
                }

                OnErrorEvent("UIMF file not found on storage server, unable to calibrate: " + uimfFilePath);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "UIMF file not found on storage server, unable to calibrate");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return string.Empty;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception finding UIMF file to calibrate", ex);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Exception while calibrating UIMF file");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return string.Empty;
            }
        }

        /// <summary>
        /// Calibrate a UIMF file
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="returnData"></param>
        public ToolReturnData PerformCalibration(IMgrParams mgrParams, ITaskParams taskParams, ToolReturnData returnData)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            OnDebugEvent("Calibrating dataset " + mDataset);

            bool autoCalibrate;

            // Make sure the working directory is empty
            ToolRunnerBase.CleanWorkDir(mWorkDir);

            // Locate data file on storage server
            // Don't copy it locally; just work with it over the network
            var uimfFilePath = GetRemoteUIMFFilePath(returnData);

            if (string.IsNullOrEmpty(uimfFilePath))
            {
                if (returnData != null && returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }

                return returnData;
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
                        OnStatusEvent("Skipping calibration since instrument is " + instrumentName);
                        autoCalibrate = false;
                        break;
                    default:
                        autoCalibrate = true;
                        break;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining whether instrument should be calibrated", ex);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Exception while calibrating UIMF file");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            if (!autoCalibrate)
            {
                return returnData;
            }

            try
            {
                // Count the number of frames
                // If fewer than 5 frames, don't calibrate

                using var uimfReader = new DataReader(uimfFilePath);

                var frameList = uimfReader.GetMasterFrameList();

                if (frameList.Count < 5)
                {
                    string msg;

                    if (frameList.Count == 0)
                    {
                        msg = "Skipping calibration since .UIMF file has no frames";
                    }
                    else
                    {
                        msg = string.Format(
                            "Skipping calibration since .UIMF file only has {0} frame{1}",
                            frameList.Count, frameList.Count == 1 ? string.Empty : "s");
                    }

                    OnStatusEvent(msg);
                    autoCalibrate = false;
                }
                else
                {
                    // Look for the presence of calibration frames or calibration tables
                    // If neither exists, we cannot perform calibration
                    var calibrationDataExists = frameList.Any(item => item.Value == UIMFData.FrameType.Calibration);

                    if (!calibrationDataExists)
                    {
                        // No calibration frames were found
                        var calibrationTables = uimfReader.GetCalibrationTableNames();

                        if (calibrationTables.Count == 0)
                        {
                            OnWarningEvent("Skipping calibration since .UIMF file does not contain any calibration frames or calibration tables");
                            autoCalibrate = false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception checking for calibration frames", ex);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Exception while calibrating UIMF file");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            if (!autoCalibrate)
            {
                return returnData;
            }

            // Perform calibration operation
            OnDebugEvent("Calling UIMFDemultiplexer to calibrate");

            var calibrationFailed = false;

            try
            {
                if (!CalibrateFile(uimfFilePath, mDataset, out var errorMessage))
                {
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "Error calibrating UIMF file";
                    }

                    returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, errorMessage);
                    calibrationFailed = true;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling CalibrateFile for dataset " + mDataset, ex);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Exception while calibrating UIMF file");
                calibrationFailed = true;
            }

            if (!calibrationFailed)
            {
                try
                {
                    if (!ValidateUIMFCalibrated(uimfFilePath, returnData))
                    {
                        // Calibration failed
                        calibrationFailed = true;
                    }
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Exception validating calibrated .UIMF file", ex);
                    returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Exception while calibrating UIMF file");
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return returnData;
                }
            }

            if (!uimfFilePath.StartsWith(@"\\"))
            {
                // Copy the CalibrationLog.txt file to the storage server (even if calibration failed)
                CopyCalibrationLogToStorageServer(returnData);
            }

            // Update the return data
            if (calibrationFailed)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.EvalMsg = CTMUtilities.AppendToString(returnData.EvalMsg, " but Calibration failed", string.Empty);
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                returnData.EvalMsg = CTMUtilities.AppendToString(returnData.EvalMsg, " and calibrated", string.Empty);
            }

            return returnData;
        }

        /// <summary>
        /// Manually applies calibration coefficients to a UIMF file
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="returnData"></param>
        /// <param name="calibrationSlope"></param>
        /// <param name="calibrationIntercept"></param>
        public ToolReturnData PerformManualCalibration(IMgrParams mgrParams, ITaskParams taskParams, ToolReturnData returnData, double calibrationSlope, double calibrationIntercept)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            try
            {
                // Locate data file on storage server
                // Don't copy it locally; just work with it over the network
                var uimfFilePath = GetRemoteUIMFFilePath(returnData);

                if (string.IsNullOrEmpty(uimfFilePath))
                {
                    if (returnData != null && returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    return returnData;
                }

                double currentSlope;
                double currentIntercept;

                using (var reader = new DataReader(uimfFilePath))
                {
                    var frameList = reader.GetMasterFrameList();

                    if (frameList.Count > 0)
                    {
                        var firstFrame = frameList.Keys.Min();

                        var frameParams = reader.GetFrameParams(firstFrame);

                        currentSlope = frameParams.CalibrationSlope;
                        currentIntercept = frameParams.CalibrationIntercept;

                        if (Math.Abs(currentSlope) < double.Epsilon)
                        {
                            OnWarningEvent("Existing CalibrationSlope is 0 in PerformManualCalibration for frame {0}; this is unexpected",
                                firstFrame);
                        }
                    }
                    else
                    {
                        currentSlope = 0;
                        currentIntercept = 0;
                    }
                }

                using var writer = new DataWriter(uimfFilePath, false);

                writer.UpdateAllCalibrationCoefficients(calibrationSlope, calibrationIntercept, false, true);

                var logMessage = "Manually applied calibration coefficients to all frames using user-specified calibration coefficients";
                writer.PostLogEntry("Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

                logMessage = "Old calibration coefficients: slope = " + currentSlope + ", intercept = " + currentIntercept;
                writer.PostLogEntry("Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);

                logMessage = string.Format(
                    "New calibration coefficients: slope = {0:0.0000000}, intercept = {1:0.0000000}",
                    calibrationSlope, calibrationIntercept);

                writer.PostLogEntry("Normal", logMessage, UIMF_CALIBRATION_UPDATER_NAME);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in PerformManualCalibration for dataset " + mDataset, ex);
                returnData ??= new ToolReturnData();

                returnData.CloseoutMsg = "Error manually calibrating UIMF file";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            return returnData;
        }

        /// <summary>
        /// Performs demultiplexing of IMS data files
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="returnData">Instance of ToolReturnData</param>
        /// <param name="uimfFileName">Name of the .uimf file</param>
        /// <param name="numBitsForEncoding">Number of bits used to encode the file (traditionally 4 bit)</param>
        public void PerformDemux(
            IMgrParams mgrParams,
            ITaskParams taskParams,
            ToolReturnData returnData,
        string uimfFileName,
            byte numBitsForEncoding)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            var jobNum = taskParams.GetParam("Job");
            OnStatusEvent("Performing demultiplexing, job {0}, dataset {1}", jobNum, mDataset);

            var postProcessingError = false;

            // Default to summing 5 LC frames if this parameter is not defined
            var framesToSum = taskParams.GetParam("DemuxFramesToSum", 5);

            if (framesToSum > 1)
            {
                OnStatusEvent("Will sum " + framesToSum + " LC Frames when demultiplexing");
            }

            // Make sure the working directory is empty
            ToolRunnerBase.CleanWorkDir(mWorkDir);

            // Copy the UIMF file from the storage server to the working directory

            CopyUIMFToWorkDir(uimfFileName, returnData, out var uimfRemoteEncodedFileNamePath, out var uimfLocalEncodedFileNamePath);

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return;
            }

            // Look for a _decoded.uimf.tmp file on the storage server
            // Copy it local if present
            var tmpUIMFFileName = mDataset + DECODED_UIMF_SUFFIX + ".tmp";
            var tmpUIMFRemoteFileNamePath = Path.Combine(mDatasetDirectoryPathRemote, tmpUIMFFileName);
            var tmpUIMFLocalFileNamePath = Path.Combine(mWorkDir, tmpUIMFFileName);

            var demuxOptions = new DemuxOptionsType
            {
                AutoCalibrate = false,
                FramesToSum = framesToSum,
                ResumeDemultiplexing = false,
                NumBitsForEncoding = numBitsForEncoding
            };

            int resumeStartFrame;

            if (File.Exists(tmpUIMFRemoteFileNamePath))
            {
                // Copy _decoded.uimf.tmp file to working directory so that we can resume demultiplexing where we left off
                const int retryCount = 0;

                if (CopyFileWithRetry(tmpUIMFRemoteFileNamePath, tmpUIMFLocalFileNamePath, false, retryCount))
                {
                    OnStatusEvent(".tmp decoded file found at " + tmpUIMFRemoteFileNamePath + "; will resume demultiplexing");
                    demuxOptions.ResumeDemultiplexing = true;
                }
                else
                {
                    OnStatusEvent("Error copying .tmp decoded file from " + tmpUIMFRemoteFileNamePath + " to work directory; unable to resume demultiplexing");
                }
            }

            // Perform demux operation
            OnDebugEvent("Calling UIMFDemultiplexer_Console.exe");

            try
            {
                if (!DemultiplexFile(uimfLocalEncodedFileNamePath, mDataset, demuxOptions, out resumeStartFrame, out var errorMessage))
                {
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "Error demultiplexing UIMF file";
                    }

                    returnData.CloseoutMsg = errorMessage;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling DemultiplexFile for dataset " + mDataset, ex);
                returnData.CloseoutMsg = "Error demultiplexing UIMF file";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            // Look for the demultiplexed .UIMF file
            var localUimfDecodedFilePath = Path.Combine(mWorkDir, mDataset + DECODED_UIMF_SUFFIX);

            if (!File.Exists(localUimfDecodedFilePath))
            {
                returnData.CloseoutMsg = "Decoded UIMF file not found";
                OnErrorEvent(returnData.CloseoutMsg + ": " + localUimfDecodedFilePath);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (!ValidateUIMFDemultiplexed(localUimfDecodedFilePath, returnData))
            {
                if (string.IsNullOrEmpty(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "ValidateUIMFDemultiplexed returned false";
                }

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                postProcessingError = true;
            }

            if (!postProcessingError)
            {
                // Rename UIMF file on storage server
                OnDebugEvent("Renaming UIMF file on storage server");

                // If this is a re-run, the encoded file has already been renamed
                // This is determined by looking for "_encoded" in the UIMF file name
                if (!uimfFileName.Contains("_encoded"))
                {
                    if (!RenameFile(uimfRemoteEncodedFileNamePath, Path.Combine(mDatasetDirectoryPathRemote, mDataset + "_encoded.uimf")))
                    {
                        returnData.CloseoutMsg = "Error renaming encoded UIMF file on storage server";
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        postProcessingError = true;
                    }
                }
            }

            if (!postProcessingError)
            {
                // Delete CheckPoint file from storage server (if it exists)
                if (!string.IsNullOrEmpty(mDatasetDirectoryPathRemote))
                {
                    OnDebugEvent("Deleting .uimf.tmp CheckPoint file from storage server");

                    try
                    {
                        var checkpointTargetPath = Path.Combine(mDatasetDirectoryPathRemote, tmpUIMFFileName);

                        if (File.Exists(checkpointTargetPath))
                        {
                            File.Delete(checkpointTargetPath);
                        }
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Error deleting .uimf.tmp CheckPoint file", ex);
                    }
                }
            }

            if (!postProcessingError)
            {
                // Copy the result files to the storage server
                if (!CopyUIMFFileToStorageServer(returnData, localUimfDecodedFilePath, "demultiplexed UIMF"))
                {
                    postProcessingError = true;
                }
            }

            if (postProcessingError)
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
                var failedResultsCopier = new FailedResultsCopier(mgrParams, taskParams);
                failedResultsCopier.CopyFailedResultsToArchiveDirectory(mWorkDir);

                return;
            }

            // Delete local UIMF file(s)
            OnDebugEvent("Cleaning up working directory");

            try
            {
                File.Delete(localUimfDecodedFilePath);
                File.Delete(uimfLocalEncodedFileNamePath);
            }
            catch (Exception ex)
            {
                // Error deleting files; don't treat this as a fatal error
                OnErrorEvent("Exception deleting working directory file(s)", ex);
            }

            // Update the return data
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            returnData.EvalMsg = "De-multiplexed";

            if (demuxOptions.ResumeDemultiplexing)
            {
                returnData.EvalMsg += " (resumed at frame " + resumeStartFrame + ")";
            }
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceFilePath">Source file</param>
        /// <param name="targetFilePath">Destination file</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestFileBeforeCopy">If True and if the target file exists, renames the target file to have _Old1 before the extension</param>
        /// <returns>True if success, false if an error</returns>
        private bool CopyFileWithRetry(string sourceFilePath, string targetFilePath, bool overWrite, int retryCount, bool backupDestFileBeforeCopy = false)
        {
            OnCopyFileWithRetry(sourceFilePath, targetFilePath);
            return CopyFileWithRetry(sourceFilePath, targetFilePath, overWrite, retryCount, backupDestFileBeforeCopy, mFileTools);
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceFilePath">Source file</param>
        /// <param name="targetFilePath">Destination file</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestFileBeforeCopy">If True and if the target file exists, renames the target file to have _Old1 before the extension</param>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <returns>True if success, false if an error</returns>
        public static bool CopyFileWithRetry(
            string sourceFilePath,
            string targetFilePath,
            bool overWrite,
            int retryCount,
            bool backupDestFileBeforeCopy,
            FileTools fileTools)
        {
            var retryingCopy = false;

            if (retryCount < 0)
            {
                retryCount = 0;
            }

            if (backupDestFileBeforeCopy)
            {
                FileTools.BackupFileBeforeCopy(targetFilePath);
            }

            while (retryCount >= 0)
            {
                string msg;
                try
                {
                    if (retryingCopy)
                    {
                        msg = "Retrying copy; retryCount = " + retryCount;
                        LogTools.LogMessage(msg);
                    }

                    // The parent method should call OnCopyFileWithRetry() or ResetTimestampForQueueWaitTimeLogging() prior to calling this method

                    fileTools.CopyFileUsingLocks(sourceFilePath, targetFilePath, overWrite);
                    return true;
                }
                catch (Exception ex)
                {
                    msg = "Exception copying file " + sourceFilePath + " to " + targetFilePath + ": " + ex.Message;
                    LogTools.LogError(msg, ex);

                    System.Threading.Thread.Sleep(2000);
                    retryCount--;
                    retryingCopy = true;
                }
            }

            // If we get here, we were not able to successfully copy the file
            return false;
        }

        /// <summary>
        /// Copy the demultiplexed file to the storage server, renaming as DatasetName.uimf in the process
        /// </summary>
        /// <param name="returnData"></param>
        /// <param name="localUimfDecodedFilePath"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if success; otherwise false</returns>
        private bool CopyUIMFFileToStorageServer(ToolReturnData returnData, string localUimfDecodedFilePath, string fileDescription)
        {
            var success = true;

            OnDebugEvent("Copying {0} file to storage server", fileDescription);

            const int retryCount = 3;

            if (!CopyFileWithRetry(localUimfDecodedFilePath, Path.Combine(mDatasetDirectoryPathRemote, mDataset + ".uimf"), true, retryCount))
            {
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Error copying " + fileDescription + " file to storage server");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Copies the result files to the storage server
        /// </summary>
        /// <param name="returnData"></param>
        /// <returns>True if success; otherwise false</returns>
        private void CopyCalibrationLogToStorageServer(ToolReturnData returnData)
        {
            string msg;

            // Copy file CalibrationLog.txt to the storage server (if it exists)
            var calibrationLogFilePath = Path.Combine(mWorkDir, CALIBRATION_LOG_FILE);
            var targetFilePath = Path.Combine(mDatasetDirectoryPathRemote, CALIBRATION_LOG_FILE);

            if (!File.Exists(calibrationLogFilePath))
            {
                msg = "CalibrationLog.txt not found at " + mWorkDir;

                if (File.Exists(targetFilePath))
                {
                    msg += "; this is OK since " + CALIBRATION_LOG_FILE + " exists at " + mDatasetDirectoryPathRemote;
                    OnDebugEvent(msg);
                }
                else
                {
                    msg += "; in addition, could not find " + CALIBRATION_LOG_FILE + " at " + mDatasetDirectoryPathRemote;
                    OnErrorEvent(msg);
                }
            }
            else
            {
                msg = "Copying CalibrationLog.txt file to storage server";
                OnDebugEvent(msg);
                const int retryCount = 3;
                const bool backupDestFileBeforeCopy = true;

                if (!CopyFileWithRetry(calibrationLogFilePath, targetFilePath, true, retryCount, backupDestFileBeforeCopy))
                {
                    returnData.CloseoutMsg = "Error copying CalibrationLog.txt file to storage server";
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
        }

        /// <summary>
        /// Performs actual calibration operation
        /// </summary>
        /// <param name="inputFilePath">Input file name</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool CalibrateFile(string inputFilePath, string datasetName, out string errorMessage)
        {
            try
            {
                OnStatusEvent("Starting calibration, dataset " + datasetName);

                // Set the options
                var demuxOptions = new DemuxOptionsType
                {
                    ResumeDemultiplexing = false,
                    CheckpointTargetDirectory = string.Empty,
                    CalibrateOnly = true
                };

                var success = RunUIMFDemultiplexer(inputFilePath, inputFilePath, demuxOptions, MAX_CALIBRATION_RUNTIME_MINUTES, out errorMessage);

                // Confirm that things have succeeded
                if (success && mLoggedConsoleOutputErrors.Count == 0)
                {
                    OnStatusEvent("Calibration complete, dataset " + datasetName);
                    return true;
                }

                if (string.IsNullOrEmpty(errorMessage) && mLoggedConsoleOutputErrors.Count > 0)
                {
                    errorMessage = mLoggedConsoleOutputErrors.First();
                }
                else if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Unknown error";
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
            DemuxOptionsType demuxOptions,
            out int resumeStartFrame,
            out string errorMessage)
        {
            string msg;
            resumeStartFrame = 0;
            errorMessage = string.Empty;

            var uimfLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

            var inputFile = new FileInfo(inputFilePath);
            var directoryName = inputFile.DirectoryName;

            if (string.IsNullOrEmpty(directoryName))
            {
                errorMessage = "Could not determine the parent directory for " + inputFilePath;
                OnErrorEvent(errorMessage);
                return false;
            }
            var outputFilePath = Path.Combine(directoryName, datasetName + DECODED_UIMF_SUFFIX);

            try
            {
                OutOfMemoryException = false;

                if (demuxOptions.ResumeDemultiplexing)
                {
                    var tempUIMFFilePath = outputFilePath + ".tmp";

                    if (!File.Exists(tempUIMFFilePath))
                    {
                        errorMessage = "Resuming demultiplexing, but .tmp UIMF file not found at " + tempUIMFFilePath;
                        OnErrorEvent(errorMessage);
                        demuxOptions.ResumeDemultiplexing = false;
                    }
                    else
                    {
                        var maxDemultiplexedFrameNum = uimfLogEntryAccessor.GetMaxDemultiplexedFrame(tempUIMFFilePath, out var logEntryAccessorMsg);

                        if (maxDemultiplexedFrameNum > 0)
                        {
                            resumeStartFrame = maxDemultiplexedFrameNum + 1;
                            msg = "Resuming demultiplexing, dataset " + datasetName + " frame " + resumeStartFrame;
                            OnStatusEvent(msg);
                        }
                        else
                        {
                            errorMessage = "Error looking up max demultiplexed frame number from the Log_Entries table";
                            msg = errorMessage + " in " + tempUIMFFilePath;

                            if (!string.IsNullOrEmpty(logEntryAccessorMsg))
                            {
                                msg += "; " + logEntryAccessorMsg;
                            }

                            OnErrorEvent(msg);

                            demuxOptions.ResumeDemultiplexing = false;
                        }
                    }
                }
                else
                {
                    msg = "Starting demultiplexing, dataset " + datasetName;
                    OnStatusEvent(msg);
                }

                // Enable checkpoint file creation
                demuxOptions.CheckpointTargetDirectory = mDatasetDirectoryPathRemote;

                var success = RunUIMFDemultiplexer(inputFilePath, outputFilePath, demuxOptions, MAX_DEMUX_RUNTIME_MINUTES, out errorMessage);

                // Confirm that things have succeeded
                if (success && mLoggedConsoleOutputErrors.Count == 0 && !OutOfMemoryException)
                {
                    msg = "Demultiplexing complete, dataset " + datasetName;
                    OnStatusEvent(msg);
                    return true;
                }

                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Unknown error";
                }

                if (OutOfMemoryException)
                {
                    errorMessage = "OutOfMemory exception was thrown";
                }

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

        private void OnCopyFileWithRetry(string sourceFilePath, string targetFilePath)
        {
            CopyFileWithRetryEvent?.Invoke(sourceFilePath + " -> " + targetFilePath);
        }

        private void ParseConsoleOutputFileDemux()
        {
            // ReSharper disable CommentTypo

            // Example Console output:
            //
            // Demultiplexing PlasmaND_2pt5ng_0pt005fmol_Frac05_9Sep14_Methow_14-06-13_encoded.uimf
            //  in directory F:\My Documents\Projects\DataMining\UIMFDemultiplexer\UIMFDemultiplexer_Console\bin
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

            // ReSharper restore CommentTypo

            var percentCompleteMatcher = new Regex(@"Processing: (\d+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var totalFramesMatcher = new Regex(@"frames to demultiplex: (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var currentFrameMatcher = new Regex(@"Demultiplexing frame (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var totalFrameCount = 0;
            var framesProcessed = 0;

            try
            {
                if (string.IsNullOrEmpty(mUimfDemultiplexerConsoleOutputFilePath))
                {
                    return;
                }

                using var reader = new StreamReader(new FileStream(mUimfDemultiplexerConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.StartsWith("Error in") ||
                        dataLine.StartsWith("Error:") ||
                        dataLine.StartsWith("Exception"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnErrorEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }

                        if (dataLine.Contains("OutOfMemoryException"))
                        {
                            OutOfMemoryException = true;
                        }
                    }
                    else if (dataLine.StartsWith("Warning:"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnWarningEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }
                    }
                    else
                    {
                        // Compare the line against the various RegEx specs

                        // % complete (integer values only)
                        var percentCompleteMatch = percentCompleteMatcher.Match(dataLine);

                        if (percentCompleteMatch.Success)
                        {
                            if (short.TryParse(percentCompleteMatch.Groups[1].Value, out var percentComplete))
                            {
                                mDemuxProgressPercentComplete = percentComplete;
                            }
                        }

                        // Total frames
                        var totalFramesMatch = totalFramesMatcher.Match(dataLine);

                        if (totalFramesMatch.Success)
                        {
                            int.TryParse(totalFramesMatch.Groups[1].Value, out totalFrameCount);
                        }

                        // Current frame processed
                        var currentFrameMatch = currentFrameMatcher.Match(dataLine);

                        if (currentFrameMatch.Success)
                        {
                            int.TryParse(currentFrameMatch.Groups[1].Value, out framesProcessed);
                        }
                    }
                }

                if (totalFrameCount <= 0)
                    return;

                var percentCompleteFractional = framesProcessed / (float)totalFrameCount * 100;

                if (percentCompleteFractional > mDemuxProgressPercentComplete)
                {
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
        /// <param name="sourceFilePath">Original file path</param>
        /// <param name="newFilePath">New file path</param>
        /// <returns>True if successful, false if an error</returns>
        private bool RenameFile(string sourceFilePath, string newFilePath)
        {
            try
            {
                var fi = new FileInfo(sourceFilePath);
                fi.MoveTo(newFilePath);
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception renaming file {0} to {1}: {2}",
                    sourceFilePath, Path.GetFileName(newFilePath), ex.Message);

                // Garbage collect, then try again to rename the file
                System.Threading.Thread.Sleep(250);
                AppUtils.GarbageCollectNow();
                System.Threading.Thread.Sleep(250);

                try
                {
                    var fi = new FileInfo(sourceFilePath);
                    fi.MoveTo(newFilePath);
                    return true;
                }
                catch (Exception ex2)
                {
                    OnErrorEvent("Rename failed despite garbage collection call: " + ex2.Message);
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
        /// <returns>True if successful, false if an error</returns>
        private bool RunUIMFDemultiplexer(
            string inputFilePath,
            string outputFilePath,
            DemuxOptionsType demuxOptions,
            int maxRuntimeMinutes,
            out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                var inputFile = new FileInfo(inputFilePath);
                var outputFile = new FileInfo(outputFilePath);

                // Construct the command line arguments

                // Input file
                var arguments = Conversion.PossiblyQuotePath(inputFilePath);

                if (!string.Equals(inputFile.DirectoryName, outputFile.DirectoryName, StringComparison.OrdinalIgnoreCase))
                {
                    // Output directory
                    arguments += " /O:" + Conversion.PossiblyQuotePath(outputFile.DirectoryName);
                }

                if (demuxOptions.CalibrateOnly)
                {
                    // Calibrating
                    mCalibrating = true;

                    arguments += " /CalibrateOnly";

                    // Instruct tool to look for calibration table names in other similarly named .UIMF files if not found in the primary .UIMF file
                    arguments += " /CX";
                }
                else
                {
                    // Demultiplexing
                    mCalibrating = false;

                    // Output file name
                    arguments += " /N:" + Conversion.PossiblyQuotePath(outputFile.Name);

                    if (demuxOptions.NumBitsForEncoding > 1)
                    {
                        arguments += " /Bits:" + demuxOptions.NumBitsForEncoding;
                    }

                    /*
                    if (demuxOptions.StartFrame > 0)
                        arguments += " /First:" + demuxOptions.StartFrame;

                    if (demuxOptions.EndFrame > 0)
                        arguments += " /Last:" + demuxOptions.EndFrame;
                    */

                    arguments += " /FramesToSum:" + demuxOptions.FramesToSum;

                    if (demuxOptions.ResumeDemultiplexing)
                    {
                        arguments += " /Resume";
                    }

                    /*
                    if (demuxOptions.NumCores > 0)
                    {
                        arguments += " /Cores:" + demuxOptions.NumCores;
                    }
                    */

                    if (!demuxOptions.AutoCalibrate)
                    {
                        arguments += " /SkipCalibration";
                    }

                    if (!string.IsNullOrEmpty(demuxOptions.CheckpointTargetDirectory))
                    {
                        arguments += " /CheckPointDirectory:" + Conversion.PossiblyQuotePath(demuxOptions.CheckpointTargetDirectory);
                    }
                }

                mUimfDemultiplexerConsoleOutputFilePath = Path.Combine(mWorkDir, "UIMFDemultiplexer_ConsoleOutput.txt");

                OnStatusEvent(mUimfDemultiplexerPath + " " + arguments);
                var cmdRunner = new RunDosProgram(mWorkDir);
                mDemuxStartTime = DateTime.UtcNow;
                mLastProgressUpdateTime = DateTime.UtcNow;
                mLastProgressMessageTime = DateTime.UtcNow;

                AttachCmdRunnerEvents(cmdRunner);

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

                var success = cmdRunner.RunProgram(mUimfDemultiplexerPath, arguments, "UIMFDemultiplexer", true, maxRuntimeMinutes * 60);

                if (!mCalibrating)
                {
                    ParseConsoleOutputFileDemux();
                }

                if (success)
                {
                    return true;
                }

                errorMessage = "Error running UIMF Demultiplexer";
                OnErrorEvent(errorMessage);

                if (cmdRunner.ExitCode != 0)
                {
                    OnWarningEvent("UIMFDemultiplexer returned a non-zero exit code: " + cmdRunner.ExitCode);
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
            var datasetDirectory = taskParams.GetParam(taskParams.HasParam("Directory") ? "Directory" : "Folder");

            mDatasetDirectoryPathRemote = Path.Combine(svrPath, datasetDirectory);
        }

        /// <summary>
        /// Examines the Log_Entries table to make sure the .UIMF file was demultiplexed
        /// </summary>
        /// <param name="localUimfDecodedFilePath"></param>
        /// <param name="returnData"></param>
        /// <returns>True if it was demultiplexed, otherwise false</returns>
        private bool ValidateUIMFDemultiplexed(string localUimfDecodedFilePath, ToolReturnData returnData)
        {
            bool uimfDemultiplexed;
            string msg;

            // Make sure the Log_Entries table contains entry "Finished demultiplexing" (with today's date)
            var uimfLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

            var demultiplexingFinished = uimfLogEntryAccessor.GetDemultiplexingFinishDate(localUimfDecodedFilePath, out var logEntryAccessorMsg);

            if (demultiplexingFinished == DateTime.MinValue)
            {
                returnData.CloseoutMsg = "Demultiplexing finished message not found in Log_Entries table";
                msg = returnData.CloseoutMsg + " in " + localUimfDecodedFilePath;

                if (!string.IsNullOrEmpty(logEntryAccessorMsg))
                {
                    msg += "; " + logEntryAccessorMsg;
                }

                OnErrorEvent(msg);
                uimfDemultiplexed = false;
            }
            else
            {
                if (DateTime.Now.Subtract(demultiplexingFinished).TotalMinutes < 5)
                {
                    msg = "Demultiplexing finished message in Log_Entries table has date " + demultiplexingFinished;
                    OnDebugEvent(msg);
                    uimfDemultiplexed = true;
                }
                else
                {
                    returnData.CloseoutMsg = "Demultiplexing finished message in Log_Entries table is more than 5 minutes old";
                    msg = returnData.CloseoutMsg + ": " + demultiplexingFinished + "; assuming this is a demultiplexing failure";

                    if (!string.IsNullOrEmpty(logEntryAccessorMsg))
                    {
                        msg += "; " + logEntryAccessorMsg;
                    }

                    OnErrorEvent(msg);
                    uimfDemultiplexed = false;
                }
            }

            return uimfDemultiplexed;
        }

        /// <summary>
        /// Examines the Log_Entries table to make sure the .UIMF file was calibrated
        /// </summary>
        /// <param name="localUimfDecodedFilePath"></param>
        /// <param name="returnData"></param>
        /// <returns>True if it was calibrated, otherwise false</returns>
        private bool ValidateUIMFCalibrated(string localUimfDecodedFilePath, ToolReturnData returnData)
        {
            bool uimfCalibrated;
            string msg;

            // Make sure the Log_Entries table contains entry "Applied calibration coefficients to all frames" (with today's date)
            var uimfLogEntryAccessor = new UIMFDemultiplexer.clsUIMFLogEntryAccessor();

            var calibrationApplied = uimfLogEntryAccessor.GetCalibrationFinishDate(localUimfDecodedFilePath, out var logEntryAccessorMsg);

            if (calibrationApplied == DateTime.MinValue)
            {
                const string logMessage = "Applied calibration message not found in Log_Entries table";
                msg = logMessage + " in " + localUimfDecodedFilePath;

                if (!string.IsNullOrEmpty(logEntryAccessorMsg))
                {
                    msg += "; " + logEntryAccessorMsg;
                }

                OnErrorEvent(msg);
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, logMessage);
                uimfCalibrated = false;
            }
            else
            {
                if (DateTime.Now.Subtract(calibrationApplied).TotalMinutes < 5)
                {
                    msg = "Applied calibration message in Log_Entries table has date " + calibrationApplied;
                    OnDebugEvent(msg);
                    uimfCalibrated = true;
                }
                else
                {
                    const string logMessage = "Applied calibration message in Log_Entries table is more than 5 minutes old";
                    msg = logMessage + ": " + calibrationApplied + "; assuming this is a calibration failure";

                    if (!string.IsNullOrEmpty(logEntryAccessorMsg))
                    {
                        msg += "; " + logEntryAccessorMsg;
                    }

                    OnErrorEvent(msg);
                    returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, logMessage);
                    uimfCalibrated = false;
                }
            }

            return uimfCalibrated;
        }

        private void AttachCmdRunnerEvents(RunDosProgram cmdRunner)
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

        private void CmdRunner_Timeout()
        {
            OnErrorEvent("CmdRunner timeout reported (the UIMF Demultiplexer has been running for over {0} days)", MAX_DEMUX_RUNTIME_DAYS);
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < 30)
            {
                return;
            }

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

            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds < 300)
                return;

            mLastProgressMessageTime = DateTime.UtcNow;
            OnDebugEvent("{0} running; {1:F1} minutes elapsed, {2:F1}% complete",
                toolName,
                DateTime.UtcNow.Subtract(mDemuxStartTime).TotalMinutes,
                mDemuxProgressPercentComplete);
        }

        private void BinCentricTableCreator_ProgressEvent(object sender, ProgressEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < mProgressUpdateIntervalSeconds)
            {
                return;
            }

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

        private void BinCentricTableCreator_MessageEvent(object sender, MessageEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds < 30)
            {
                return;
            }

            OnStatusEvent(e.Message);

            mLastProgressMessageTime = DateTime.UtcNow;
        }
    }
}
