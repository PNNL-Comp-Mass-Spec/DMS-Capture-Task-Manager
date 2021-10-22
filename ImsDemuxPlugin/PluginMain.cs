//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//*********************************************************************************************************

using CaptureTaskManager;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using UIMFLibrary;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// Progress event delegate
    /// </summary>
    /// <param name="newProgress">Value between 0 and 100</param>
    public delegate void DelDemuxProgressHandler(float newProgress);

    /// <summary>
    /// IMS Demultiplexer plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: Demultiplexer, demux, demultiplexing, demultiplexed, uimf, desc

        public const int MANAGER_UPDATE_INTERVAL_MINUTES = 10;
        private const string COULD_NOT_OBTAIN_GOOD_CALIBRATION = "Could not obtain a good calibration";

        private const bool ADD_BIN_CENTRIC_TABLES = false;

        private enum CalibrationMode
        {
            NoCalibration,
            ManualCalibration,
            AutoCalibration
        }

        private DemuxTools mDemuxTools;
        private AgilentDemuxTools mAgDemuxTools;
        private bool mDemultiplexingPerformed;

        private ToolReturnData mRetData = new();
        private string mUimfFilePath;
        private bool mNeedToDemultiplex;

        /// <summary>
        /// Runs the IMS demux step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override ToolReturnData RunTool()
        {
            var msg = "Starting ImsDemuxPlugin.PluginMain.RunTool()";
            LogDebug(msg);
            mDemultiplexingPerformed = false;

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return mRetData;
            }

            // Initialize the config DB update interval
            mLastConfigDbUpdate = DateTime.UtcNow;
            mMinutesBetweenConfigDbUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.IMS_Agilent_TOF_DotD)
            {
                RunToolAgilentDotD();
            }
            else
            {
                RunToolUIMF();
            }

            if (mRetData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS || mRetData.EvalCode == EnumEvalCode.EVAL_CODE_SKIPPED)
            {
                return mRetData;
            }

            // Demultiplexing succeeded (or skipped)

            // October 2013: Disabled the addition of bin-centric tables since datasets currently being acquired on the IMS platform will not have IQ run on them
            // March 2015: Re-enabled automatic addition of bin-centric tables
            // May 22, 2015: Now adding bin-centric tables only if the original .UIMF file is less than 2 GB in size
            // October 12, 2017: Again disabled the addition of bin-centric tables since they can greatly increase .UIMF file size and because usage of the bin-centric tables is low

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (ADD_BIN_CENTRIC_TABLES)
#pragma warning disable 162
            {
                // Lookup the current .uimf file size
                var uimfFile = new FileInfo(mUimfFilePath);
                var fileSizeGBStart = uimfFile.Length / 1024.0 / 1024.0 / 1024.0;
                var fileSizeText = " (" + Math.Round(fileSizeGBStart, 0).ToString("0") + " GB)";

                if (fileSizeGBStart > 2)
                {
                    LogMessage("Not adding bin-centric tables to " + uimfFile.Name + " since over 2 GB in size" + fileSizeText);
                }
                else
                {
                    // Add the bin-centric tables if not yet present
                    LogMessage("Adding bin-centric tables to " + uimfFile.Name + fileSizeText);
                    mRetData = mDemuxTools.AddBinCentricTablesIfMissing(mMgrParams, mTaskParams, mRetData);

                    uimfFile.Refresh();
                    var fileSizeGBEnd = uimfFile.Length / 1024.0 / 1024.0 / 1024.0;
                    double foldIncrease = 0;
                    if (fileSizeGBStart > 0)
                    {
                        foldIncrease = fileSizeGBEnd / fileSizeGBStart;
                    }

                    LogMessage("UIMF file size increased from " + fileSizeGBStart.ToString("0.00") + " GB to " + fileSizeGBEnd.ToString("0.00") +
                               " GB, a " + foldIncrease.ToString("0.0") + " fold increase");
                }
#pragma warning restore 162
            }

            msg = "Completed PluginMain.RunTool()";
            LogDebug(msg);

            return mRetData;
        }

        private ToolReturnData RunToolUIMF()
        {
            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mRetData.CloseoutMsg = "Error determining version of IMSDemultiplexer";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            // Determine whether or not calibration should be performed
            //
            // Note that stored procedure GetJobParamTable in the DMS_Capture database
            // sets this value based on the value in column Perform_Calibration of table T_Instrument_Name in the DMS5 database
            //
            // Furthermore, if the value for Perform_Calibration is changed for a given instrument, you must update the job parameters
            // using UpdateParametersForJob for any jobs that need to be re-run
            //   exec dbo.UpdateParametersForJob 356778
            //   exec dbo.GetJobStepParamsAsTable 356778, 3

            CalibrationMode calibrationMode;
            double calibrationSlope = 0;
            double calibrationIntercept = 0;

            if (mTaskParams.GetParam("PerformCalibration", true))
            {
                calibrationMode = CalibrationMode.AutoCalibration;
            }
            else
            {
                calibrationMode = CalibrationMode.NoCalibration;
            }

            // Locate data file on storage server
            var svrPath = Path.Combine(mTaskParams.GetParam("Storage_Vol_External"), mTaskParams.GetParam("Storage_Path"));
            var datasetDirectory = mTaskParams.GetParam(mTaskParams.HasParam("Directory") ? "Directory" : "Folder");

            var datasetDirectoryPath = Path.Combine(svrPath, datasetDirectory);

            // Use this name first to test if demux has already been performed once
            var uimfFileName = mDataset + "_encoded.uimf";
            var existingUimfFile = new FileInfo(Path.Combine(datasetDirectoryPath, uimfFileName));
            if (existingUimfFile.Exists && (existingUimfFile.Length != 0))
            {
                // The _encoded.uimf file will be used for demultiplexing

                // Look for a CalibrationLog.txt file
                // If it exists, and if the last line contains "Could not obtain a good calibration"
                //   then we need to examine table T_Log_Entries for messages regarding manual calibration
                // If manual calibration values are found, we should cache the calibration slope and intercept values
                //   and apply them to the new demultiplexed file and skip auto-calibration
                // If manual calibration values are not found, we want to fail out the job immediately,
                //   since demultiplexing succeeded, but calibration failed, and manual calibration was not performed

                var calibrationError = CheckForCalibrationError(datasetDirectoryPath);

                if (calibrationError)
                {
                    var decodedUIMFFile = new FileInfo(Path.Combine(datasetDirectoryPath, mDataset + ".uimf"));

                    var manuallyCalibrated = CheckForManualCalibration(decodedUIMFFile.FullName, out calibrationSlope, out calibrationIntercept);

                    if (manuallyCalibrated)
                    {
                        // Update the calibration mode
                        calibrationMode = CalibrationMode.ManualCalibration;
                    }
                    else
                    {
                        var msg = "CalibrationLog.txt file ends with '" + COULD_NOT_OBTAIN_GOOD_CALIBRATION +
                              "'; will not attempt to re-demultiplex the _encoded.uimf file.  If you want to re-demultiplex the _encoded.uimf file, you should rename the CalibrationLog.txt file";
                        LogError(msg);

                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        mRetData.CloseoutMsg = "Error calibrating UIMF file; see " + DemuxTools.CALIBRATION_LOG_FILE;
                        mRetData.EvalMsg =
                            "De-multiplexed but Calibration failed.  If you want to re-demultiplex the _encoded.uimf file, you should rename the CalibrationLog.txt file";

                        return mRetData;
                    }
                }
            }
            else
            {
                // Was the file zero bytes? If so, delete it
                if (existingUimfFile.Exists && existingUimfFile.Length == 0)
                {
                    try
                    {
                        existingUimfFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        var msg = "Exception deleting 0-byte uimf_encoded file";
                        LogError(msg, ex);

                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        mRetData.CloseoutMsg = msg;

                        return mRetData;
                    }
                }

                // If we got to here, _encoded uimf file doesn't exist. So, use the other uimf file
                uimfFileName = mDataset + ".uimf";
                if (!File.Exists(Path.Combine(datasetDirectoryPath, uimfFileName)))
                {
                    // IMS08_AgQTOF05 datasets acquired in QTOF only mode do not have .UIMF files; check for this

                    var dotDDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    var dotDDirectoryPath = Path.Combine(datasetDirectoryPath, dotDDirectoryName);
                    var dotDDirectory = new DirectoryInfo(dotDDirectoryPath);
                    string msg;

                    if (!dotDDirectory.Exists)
                    {
                        msg = "Dataset .d directory not found: " + dotDDirectory.FullName;
                        LogError(msg);

                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        mRetData.CloseoutMsg = msg;
                        return mRetData;
                    }

                    if (!IsAgilentIMSDataset(dotDDirectory))
                    {
                        msg = "Skipped demultiplexing since not an IMS dataset (no .UIMF file or IMS files)";
                        LogMessage(msg);

                        mRetData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                        mRetData.EvalMsg = msg;

                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                        return mRetData;
                    }

                    msg = "UIMF file not found: " + uimfFileName;
                    LogError(msg);

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    mRetData.CloseoutMsg = msg;
                    return mRetData;
                }
            }

            // Query to determine if demux is needed.
            mUimfFilePath = Path.Combine(datasetDirectoryPath, uimfFileName);
            mNeedToDemultiplex = true;

            var sqLiteTools = new SQLiteTools();
            RegisterEvents(sqLiteTools);

            var queryResult = sqLiteTools.GetUimfMuxStatus(mUimfFilePath, out var numBitsForEncoding);
            if (queryResult == MultiplexingStatus.NonMultiplexed)
            {
                // De-multiplexing not required, but we should still attempt calibration (if enabled)
                var msg = "No demultiplexing required for dataset " + mDataset;
                LogMessage(msg);
                mRetData.EvalMsg = "Non-Multiplexed";
                mNeedToDemultiplex = false;
            }
            else if (queryResult == MultiplexingStatus.Error)
            {
                // There was a problem determining the UIMF file status. Set state and exit
                var msg = "Problem determining UIMF file status for dataset " + mDataset;
                LogMessage(msg);

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                mRetData.CloseoutMsg = msg;

                return mRetData;
            }

            if (mNeedToDemultiplex)
            {
                // De-multiplexing is needed
                mRetData = mDemuxTools.PerformDemux(mMgrParams, mTaskParams, uimfFileName, numBitsForEncoding);

                if (mDemuxTools.OutOfMemoryException)
                {
                    if (mRetData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                        {
                            mRetData.CloseoutMsg = "Out of memory";
                        }
                    }

                    mNeedToAbortProcessing = true;
                }

                mDemultiplexingPerformed = true;
            }

            if (mRetData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return mRetData;
            }

            // Lookup the current .uimf file size
            var uimfFile = new FileInfo(mUimfFilePath);
            if (!uimfFile.Exists)
            {
                string msg;
                if (mNeedToDemultiplex)
                {
                    msg = "UIMF File not found after demultiplexing: " + mUimfFilePath;
                }
                else
                {
                    msg = "UIMF File not found (skipped demultiplexing): " + mUimfFilePath;
                }

                LogError(msg);
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                if (calibrationMode == CalibrationMode.AutoCalibration)
                {
                    mRetData = mDemuxTools.PerformCalibration(mMgrParams, mTaskParams, mRetData);
                }
                else if (calibrationMode == CalibrationMode.ManualCalibration)
                {
                    mRetData = mDemuxTools.PerformManualCalibration(mMgrParams, mTaskParams, mRetData, calibrationSlope, calibrationIntercept);
                }
            }

            return mRetData;
        }

        private ToolReturnData RunToolAgilentDotD()
        {
            var agilentToUimf = new AgilentToUimfConversion(ResetTimestampForQueueWaitTimeLogging);
            RegisterEvents(agilentToUimf);
            agilentToUimf.ProgressUpdate += AgilentToUimf_ConvertProgress;

            if (agilentToUimf.InFailureState)
            {
                mRetData.CloseoutMsg = agilentToUimf.ErrorMessage;
                LogError(agilentToUimf.ErrorMessage);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            // Store the version info in the database
            if (!StoreToolVersionInfoAgilent(agilentToUimf.GetToolDllPaths()))
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mRetData.CloseoutMsg = "Error determining version of PNNL-PreProcessor";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            // Locate data file on storage server
            var svrPath = Path.Combine(mTaskParams.GetParam("Storage_Vol_External"), mTaskParams.GetParam("Storage_Path"));
            var datasetDirectory = mTaskParams.GetParam(mTaskParams.HasParam("Directory") ? "Directory" : "Folder");

            var datasetDirectoryPath = Path.Combine(svrPath, datasetDirectory);

            // IMS08_AgQTOF05 datasets acquired in QTOF only mode do not have .UIMF files; check for this

            var dotDDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
            var dotDDirectoryPath = Path.Combine(datasetDirectoryPath, dotDDirectoryName);
            var dotDDirectory = new DirectoryInfo(dotDDirectoryPath);

            if (!dotDDirectory.Exists)
            {
                var msg = "Dataset .d directory not found: " + dotDDirectory.FullName;
                LogError(msg);

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                mRetData.CloseoutMsg = msg;
                return mRetData;
            }

            if (!IsAgilentIMSDataset(dotDDirectory))
            {
                var msg = "Skipped since not an IMS dataset (no .UIMF file or IMS files)";
                LogMessage(msg);

                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                mRetData.EvalMsg = msg;

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                return mRetData;
            }

            var dotDTools = new AgilentDotDTools();
            RegisterEvents(dotDTools);

            // Use this name first to test if demux has already been performed once
            var dotDDirName = mDataset + AgilentDemuxTools.ENCODED_dotD_SUFFIX;
            var existingDotDDir = new DirectoryInfo(Path.Combine(datasetDirectoryPath, dotDDirName));
            if (existingDotDDir.Exists && IsAgilentIMSDataset(existingDotDDir) && dotDTools.GetDotDMuxStatus(existingDotDDir.FullName, out _) == MultiplexingStatus.Multiplexed)
            {
                // The _muxed.d file will be used for demultiplexing
            }
            else
            {
                // Does the existing file have necessary files? If not, delete it
                if (existingDotDDir.Exists)
                {
                    try
                    {
                        mFileTools.DeleteDirectory(existingDotDDir.FullName);
                    }
                    catch (Exception ex)
                    {
                        var msg = "Exception deleting incomplete Agilent .D IMS _muxed.d directory";
                        LogError(msg, ex);

                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        mRetData.CloseoutMsg = msg;

                        return mRetData;
                    }
                }

                // If we got to here, _muxed.d file doesn't exist. So, use the other .d file
                dotDDirName = mDataset + ".d";
            }

            // Query to determine if demux is needed.
            var dotDDirPath = Path.Combine(datasetDirectoryPath, dotDDirName);
            mNeedToDemultiplex = true;

            var queryResult = dotDTools.GetDotDMuxStatus(dotDDirPath, out var muxSequence);
            if (queryResult == MultiplexingStatus.NonMultiplexed)
            {
                // De-multiplexing not required, but we should still attempt calibration (if enabled)
                var msg = "No demultiplexing required for dataset " + mDataset;
                LogMessage(msg);
                mRetData.EvalMsg = "Non-Multiplexed";
                mNeedToDemultiplex = false;
            }
            else if (queryResult == MultiplexingStatus.Error)
            {
                // There was a problem determining the Agilent IMS .D file status. Set state and exit
                var msg = "Problem determining Agilent IMS .D file status for dataset " + mDataset;
                LogMessage(msg);

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                mRetData.CloseoutMsg = msg;

                return mRetData;
            }

            var doNotDeleteLocalOutput = true;

            if (mNeedToDemultiplex)
            {
                // De-multiplexing is needed
                mRetData = mAgDemuxTools.PerformDemux(mMgrParams, mTaskParams, dotDDirName, doNotDeleteLocalOutput);

                if (mAgDemuxTools.OutOfMemoryException)
                {
                    if (mRetData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                        {
                            mRetData.CloseoutMsg = "Out of memory";
                        }
                    }

                    mNeedToAbortProcessing = true;
                }

                mDemultiplexingPerformed = true;
            }

            if (mRetData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return mRetData;
            }

            // Convert to UIMF
            agilentToUimf.RunConvert(mRetData, mMgrParams, mTaskParams, mFileTools);

            if (!agilentToUimf.RunConvert(mRetData, mMgrParams, mTaskParams, mFileTools))
            {
                return mRetData;
            }

            mUimfFilePath = Path.Combine(datasetDirectoryPath, mDataset + ".uimf");

            // Lookup the current .uimf file size
            var uimfFile = new FileInfo(mUimfFilePath);
            if (!uimfFile.Exists)
            {
                string msg;
                if (mNeedToDemultiplex)
                {
                    msg = "UIMF File not found after demultiplexing: " + mUimfFilePath;
                }
                else
                {
                    msg = "UIMF File not found (skipped demultiplexing): " + mUimfFilePath;
                }

                LogError(msg);
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            return mRetData;
        }

        /// <summary>
        /// Initializes the demux tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting PluginMain.Setup()";
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed PluginMain.Setup()";
            LogDebug(msg);

            // Determine the path to UIMFDemultiplexer_Console.exe
            var uimfDemultiplexerProgLoc = GetUimfDemultiplexerPath();

            ResetTimestampForQueueWaitTimeLogging();

            mDemuxTools = new DemuxTools(uimfDemultiplexerProgLoc, mFileTools);
            RegisterEvents(mDemuxTools);

            // Add a handler to catch progress events
            mDemuxTools.DemuxProgress += DemuxTools_DemuxProgress;
            mDemuxTools.BinCentricTableProgress += DemuxTools_BinCentricTableProgress;
            mDemuxTools.CopyFileWithRetryEvent += DemuxTools_CopyFileWithRetryEvent;

            // Determine the path to PNNL-PreProcessor.exe
            var preprocessorProgLoc = GetPNNLPreProcessorPath();

            mAgDemuxTools = new AgilentDemuxTools(preprocessorProgLoc, mFileTools);
            RegisterEvents(mAgDemuxTools);

            // Add a handler to catch progress events
            mAgDemuxTools.DemuxProgress += DemuxTools_DemuxProgress;
            mAgDemuxTools.CopyFileWithRetryEvent += DemuxTools_CopyFileWithRetryEvent;
        }

        private bool CheckForCalibrationError(string datasetDirectoryPath)
        {
            var calibrationLogPath = Path.Combine(datasetDirectoryPath, DemuxTools.CALIBRATION_LOG_FILE);

            if (!File.Exists(calibrationLogPath))
            {
                return false;
            }

            var calibrationError = false;

            using var reader = new StreamReader(new FileStream(calibrationLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(dataLine))
                {
                    continue;
                }

                if (dataLine.Contains(COULD_NOT_OBTAIN_GOOD_CALIBRATION))
                {
                    calibrationError = true;
                }
                else
                {
                    // Only count this as a calibration error if the last non-blank line of the file contains the error message
                    calibrationError = false;
                }
            }

            return calibrationError;
        }

        private bool CheckForManualCalibration(string decodedUimfFilePath, out double calibrationSlope, out double calibrationIntercept)
        {
            calibrationSlope = 0;
            calibrationIntercept = 0;

            if (!File.Exists(decodedUimfFilePath))
            {
                LogMessage("Decoded UIMF file does not exist (" + decodedUimfFilePath + "); cannot determine manual calibration coefficients");
                return false;
            }

            using var uimfReader = new DataReader(decodedUimfFilePath);

            var manuallyCalibrated = false;

            if (!uimfReader.TableExists("Log_Entries"))
            {
                return false;
            }

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in ReadOnly folders
            var connectionString = "Data Source = " + decodedUimfFilePath + "; Version=3; DateTimeFormat=Ticks;";

            using (var cnUIMF = new SQLiteConnection(connectionString, true))
            {
                cnUIMF.Open();
                var cmdLogEntries = cnUIMF.CreateCommand();

                cmdLogEntries.CommandText = "SELECT Message FROM Log_Entries where Posted_By = '" +
                                            DemuxTools.UIMF_CALIBRATION_UPDATER_NAME +
                                            "' order by Entry_ID desc";

                using var logEntriesReader = cmdLogEntries.ExecuteReader();

                while (logEntriesReader.Read())
                {
                    var message = logEntriesReader.GetString(0);
                    if (message.StartsWith("New calibration coefficients"))
                    {
                        // Extract out the coefficients
                        var coefficientsMatcher = new Regex("slope = ([0-9.+-]+), intercept = ([0-9.+-]+)", RegexOptions.IgnoreCase);
                        var match = coefficientsMatcher.Match(message);
                        if (match.Success)
                        {
                            double.TryParse(match.Groups[1].Value, out calibrationSlope);
                            double.TryParse(match.Groups[2].Value, out calibrationIntercept);
                        }
                    }
                    else if (message.StartsWith("Manually applied calibration coefficients"))
                    {
                        manuallyCalibrated = true;
                    }
                }
            }

            if (manuallyCalibrated && Math.Abs(calibrationSlope) < double.Epsilon)
            {
                LogError("Found message 'Manually applied calibration coefficients' but could not determine slope or intercept manually applied");
                manuallyCalibrated = false;
            }

            return manuallyCalibrated;
        }

        /// <summary>
        /// Construct the full path to UIMFDemultiplexer_Console.exe
        /// </summary>
        private string GetUimfDemultiplexerPath()
        {
            var uimfDemuxFolder = mMgrParams.GetParam("UimfDemultiplexerProgLoc", string.Empty);

            if (string.IsNullOrEmpty(uimfDemuxFolder))
            {
                LogError("Manager parameter UimfDemultiplexerProgLoc not defined");
                return string.Empty;
            }

            return Path.Combine(uimfDemuxFolder, "UIMFDemultiplexer_Console.exe");
        }

        /// <summary>
        /// Construct the full path to PNNL-Preprocessor.exe
        /// </summary>
        private string GetPNNLPreProcessorPath()
        {
            var preprocessorFolder = mMgrParams.GetParam("PNNLPreProcessorProgLoc", string.Empty);

            if (string.IsNullOrEmpty(preprocessorFolder))
            {
                LogError("Manager parameter PNNLPreProcessorProgLoc not defined");
                return string.Empty;
            }

            return Path.Combine(preprocessorFolder, "PNNL-PreProcessor.exe");
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            var uimfDemultiplexerProgLoc = GetUimfDemultiplexerPath();
            if (string.IsNullOrEmpty(uimfDemultiplexerProgLoc))
            {
                return false;
            }

            var uimfDemultiplexer = new FileInfo(uimfDemultiplexerProgLoc);

            LogDebug("Determining tool version info");

            if (uimfDemultiplexer.DirectoryName == null)
            {
                return false;
            }

            // Lookup the version of UIMFDemultiplexer_Console
            var success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, uimfDemultiplexer.FullName);
            if (!success)
            {
                return false;
            }

            // Lookup the version of the IMSDemultiplexer (in the UIMFDemultiplexer folder)
            var demultiplexerPath = Path.Combine(uimfDemultiplexer.DirectoryName, "IMSDemultiplexer.dll");
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, demultiplexerPath);
            if (!success)
            {
                return false;
            }

            var autoCalibrateUIMFPath = Path.Combine(uimfDemultiplexer.DirectoryName, "AutoCalibrateUIMF.dll");
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, autoCalibrateUIMFPath);
            if (!success)
            {
                return false;
            }

            var uimfLibraryPath = Path.Combine(uimfDemultiplexer.DirectoryName, "UIMFLibrary.dll");
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, uimfLibraryPath);
            if (!success)
            {
                return false;
            }

            // Store path to the demultiplexer DLL in toolFiles
            var toolFiles = new System.Collections.Generic.List<FileInfo>
            {
                new(demultiplexerPath)
            };

            try
            {
                const bool saveToolVersionTextFile = false;
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfoAgilent(List<string> additionalPaths)
        {
            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;

            var appDirectoryPath = CTMUtilities.GetAppDirectoryPath();

            if (string.IsNullOrEmpty(appDirectoryPath))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfoAgilent for the ImsDemux plugin");
                return false;
            }

            // Lookup the version of the Capture tool plugin
            var pluginPath = Path.Combine(appDirectoryPath, "ImsDemuxPlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
            {
                return false;
            }

            var preprocessorProgLoc = GetPNNLPreProcessorPath();
            if (string.IsNullOrEmpty(preprocessorProgLoc))
            {
                return false;
            }

            var preprocessor = new FileInfo(preprocessorProgLoc);

            if (preprocessor.DirectoryName == null)
            {
                return false;
            }

            // Lookup the version of UIMFDemultiplexer_Console
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, preprocessor.FullName);
            if (!success)
            {
                return false;
            }

            // Lookup the version of the IMSDemultiplexer (in the PNNL-PreProcessor folder)
            var demultiplexerPath = Path.Combine(preprocessor.DirectoryName, "IMSDemultiplexer.dll");
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, demultiplexerPath);
            if (!success)
            {
                return false;
            }

            // Lookup the version of SQLite
            var sqLitePath = Path.Combine(appDirectoryPath, "System.Data.SQLite.dll");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, sqLitePath);
            if (!success)
            {
                return false;
            }

            //var uimfLibraryPath = Path.Combine(preprocessor.DirectoryName, "UIMFLibrary.dll");
            var uimfLibraryPath = Path.Combine(appDirectoryPath, "UIMFLibrary.dll");
            success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, uimfLibraryPath);
            if (!success)
            {
                return false;
            }

            foreach (var path in additionalPaths)
            {
                success = StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, path);
                if (!success)
                {
                    return false;
                }
            }

            // Store path to the demultiplexer DLL in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(preprocessorProgLoc),
                new(demultiplexerPath)
            };

            try
            {
                const bool saveToolVersionTextFile = false;
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Reports progress from demux DLL
        /// </summary>
        /// <param name="newProgress">Current progress (value between 0 and 100)</param>
        private void DemuxTools_DemuxProgress(float newProgress)
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (ADD_BIN_CENTRIC_TABLES)
            {
#pragma warning disable 162
                // Multiplying by 0.9 since we're assuming that demultiplexing will take 90% of the time while addition of bin-centric tables will take 10% of the time
                mStatusTools.UpdateAndWrite(0 + newProgress * 0.90f);
#pragma warning restore 162
            }

            // Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTES
            UpdateMgrSettings();
        }

        private void AgilentToUimf_ConvertProgress(string progressMessage, float percentComplete)
        {
            // TODO: Currently disabled, should be made functional.
        }

        /// <summary>
        /// Reports progress for the addition of bin-centric tables
        /// </summary>
        /// <param name="newProgress">Current progress (value between 0 and 100)</param>
        private void DemuxTools_BinCentricTableProgress(float newProgress)
        {
            float progressOverall;

            if (mDemultiplexingPerformed)
            {
                // Multiplying by 0.1 since we're assuming that demultiplexing will take 90% of the time while addition of bin-centric tables will take 10% of the time
                progressOverall = 90 + newProgress * 0.10f;
            }
            else
            {
                progressOverall = newProgress;
            }

            mStatusTools.UpdateAndWrite(progressOverall);

            // Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTES
            UpdateMgrSettings();
        }

        private void DemuxTools_CopyFileWithRetryEvent(string message)
        {
            ResetTimestampForQueueWaitTimeLogging();
        }
    }
}
