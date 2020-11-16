//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//*********************************************************************************************************

using CaptureTaskManager;
using System;
using System.Data.SQLite;
using System.IO;
using System.Text.RegularExpressions;
using UIMFLibrary;

namespace ImsDemuxPlugin
{
    #region "Delegates"

    /// <summary>
    /// Progress event delegate
    /// </summary>
    /// <param name="newProgress">Value between 0 and 100</param>
    public delegate void DelDemuxProgressHandler(float newProgress);

    #endregion

    /// <summary>
    /// IMS Demultiplexer plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {
        // Ignore Spelling: Demultiplexer, demux, demultiplexing, demultiplexed, uimf, desc

        #region "Constants"

        public const int MANAGER_UPDATE_INTERVAL_MINUTES = 10;
        private const string COULD_NOT_OBTAIN_GOOD_CALIBRATION = "Could not obtain a good calibration";

        private const bool ADD_BIN_CENTRIC_TABLES = false;

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

        private clsDemuxTools mDemuxTools;
        private bool mDemultiplexingPerformed;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the IMS demux step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting ImsDemuxPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);
            mDemultiplexingPerformed = false;

            // Perform base class operations, if any
            var returnData = base.RunTool();
            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return returnData;
            }

            // Initialize the config DB update interval
            mLastConfigDbUpdate = DateTime.UtcNow;
            mMinutesBetweenConfigDbUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                returnData.CloseoutMsg = "Error determining version of IMSDemultiplexer";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
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

            var dsPath = Path.Combine(svrPath, datasetDirectory);

            // Use this name first to test if demux has already been performed once
            var uimfFileName = mDataset + "_encoded.uimf";
            var existingUimfFile = new FileInfo(Path.Combine(dsPath, uimfFileName));
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

                var calibrationError = CheckForCalibrationError(dsPath);

                if (calibrationError)
                {
                    var decodedUIMFFile = new FileInfo(Path.Combine(dsPath, mDataset + ".uimf"));

                    var manuallyCalibrated = CheckForManualCalibration(decodedUIMFFile.FullName, out calibrationSlope, out calibrationIntercept);

                    if (manuallyCalibrated)
                    {
                        // Update the calibration mode
                        calibrationMode = CalibrationMode.ManualCalibration;
                    }
                    else
                    {
                        msg = "CalibrationLog.txt file ends with '" + COULD_NOT_OBTAIN_GOOD_CALIBRATION +
                              "'; will not attempt to re-demultiplex the _encoded.uimf file.  If you want to re-demultiplex the _encoded.uimf file, you should rename the CalibrationLog.txt file";
                        LogError(msg);

                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        returnData.CloseoutMsg = "Error calibrating UIMF file; see " + clsDemuxTools.CALIBRATION_LOG_FILE;
                        returnData.EvalMsg =
                            "De-multiplexed but Calibration failed.  If you want to re-demultiplex the _encoded.uimf file, you should rename the CalibrationLog.txt file";

                        msg = "Completed clsPluginMain.RunTool()";
                        LogDebug(msg);
                        return returnData;
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
                        msg = "Exception deleting 0-byte uimf_encoded file";
                        LogError(msg, ex);

                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        returnData.CloseoutMsg = msg;

                        msg = "Completed clsPluginMain.RunTool()";
                        LogDebug(msg);
                        return returnData;
                    }
                }

                // If we got to here, _encoded uimf file doesn't exist. So, use the other uimf file
                uimfFileName = mDataset + ".uimf";
                if (!File.Exists(Path.Combine(dsPath, uimfFileName)))
                {
                    msg = "UIMF file not found: " + uimfFileName;
                    LogError(msg);

                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    returnData.CloseoutMsg = msg;

                    msg = "Completed clsPluginMain.RunTool()";
                    LogDebug(msg);
                    return returnData;
                }
            }

            // Query to determine if demux is needed.
            var uimfFilePath = Path.Combine(dsPath, uimfFileName);
            var needToDemultiplex = true;

            var sqLiteTools = new clsSQLiteTools();
            RegisterEvents(sqLiteTools);

            var queryResult = sqLiteTools.GetUimfMuxStatus(uimfFilePath, out var numBitsForEncoding);
            if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
            {
                // De-multiplexing not required, but we should still attempt calibration (if enabled)
                msg = "No demultiplexing required for dataset " + mDataset;
                LogMessage(msg);
                returnData.EvalMsg = "Non-Multiplexed";
                needToDemultiplex = false;
            }
            else if (queryResult == clsSQLiteTools.UimfQueryResults.Error)
            {
                // There was a problem determining the UIMF file status. Set state and exit
                msg = "Problem determining UIMF file status for dataset " + mDataset;

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = msg;

                msg = "Completed clsPluginMain.RunTool()";
                LogDebug(msg);
                return returnData;
            }

            if (needToDemultiplex)
            {
                // De-multiplexing is needed
                returnData = mDemuxTools.PerformDemux(mMgrParams, mTaskParams, uimfFileName, numBitsForEncoding);

                if (mDemuxTools.OutOfMemoryException)
                {
                    if (returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        if (string.IsNullOrEmpty(returnData.CloseoutMsg))
                        {
                            returnData.CloseoutMsg = "Out of memory";
                        }
                    }

                    mNeedToAbortProcessing = true;
                }

                mDemultiplexingPerformed = true;
            }

            if (returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return returnData;
            }

            // Demultiplexing succeeded (or skipped)

            // Lookup the current .uimf file size
            var uimfFile = new FileInfo(uimfFilePath);
            if (!uimfFile.Exists)
            {
                if (needToDemultiplex)
                {
                    msg = "UIMF File not found after demultiplexing: " + uimfFilePath;
                }
                else
                {
                    msg = "UIMF File not found (skipped demultiplexing): " + uimfFilePath;
                }

                LogError(msg);
                returnData.CloseoutMsg = msg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            // October 2013: Disabled the addition of bin-centric tables since datasets currently being acquired on the IMS platform will not have IQ run on them
            // March 2015: Re-enabled automatic addition of bin-centric tables
            // May 22, 2015: Now adding bin-centric tables only if the original .UIMF file is less than 2 GB in size
            // October 12, 2017: Again disabled the addition of bin-centric tables since they can greatly increase .UIMF file size and because usage of the bin-centric tables is low

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (ADD_BIN_CENTRIC_TABLES)
#pragma warning disable 162
            {
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
                    returnData = mDemuxTools.AddBinCentricTablesIfMissing(mMgrParams, mTaskParams, returnData);

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

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                if (calibrationMode == CalibrationMode.AutoCalibration)
                {
                    returnData = mDemuxTools.PerformCalibration(mMgrParams, mTaskParams, returnData);
                }
                else if (calibrationMode == CalibrationMode.ManualCalibration)
                {
                    returnData = mDemuxTools.PerformManualCalibration(mMgrParams, mTaskParams, returnData, calibrationSlope, calibrationIntercept);
                }
            }

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return returnData;
        }

        /// <summary>
        /// Initializes the demux tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting clsPluginMain.Setup()";
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed clsPluginMain.Setup()";
            LogDebug(msg);

            // Determine the path to UIMFDemultiplexer_Console.exe
            var uimfDemultiplexerProgLoc = GetUimfDemultiplexerPath();

            ResetTimestampForQueueWaitTimeLogging();

            mDemuxTools = new clsDemuxTools(uimfDemultiplexerProgLoc, mMgrName, mFileTools);
            RegisterEvents(mDemuxTools);

            // Add a handler to catch progress events
            mDemuxTools.DemuxProgress += clsDemuxTools_DemuxProgress;
            mDemuxTools.BinCentricTableProgress += clsDemuxTools_BinCentricTableProgress;
            mDemuxTools.CopyFileWithRetryEvent += clsDemuxTools_CopyFileWithRetryEvent;
        }

        protected bool CheckForCalibrationError(string dsPath)
        {
            var calibrationLogPath = Path.Combine(dsPath, clsDemuxTools.CALIBRATION_LOG_FILE);

            if (!File.Exists(calibrationLogPath))
            {
                return false;
            }

            var calibrationError = false;

            using (var reader = new StreamReader(new FileStream(calibrationLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
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
            }

            return calibrationError;
        }

        protected bool CheckForManualCalibration(string decodedUimfFilePath, out double calibrationSlope, out double calibrationIntercept)
        {
            calibrationSlope = 0;
            calibrationIntercept = 0;

            if (!File.Exists(decodedUimfFilePath))
            {
                LogMessage("Decoded UIMF file does not exist (" + decodedUimfFilePath + "); cannot determine manual calibration coefficients");
                return false;
            }

            using (var uimfReader = new DataReader(decodedUimfFilePath))
            {
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
                                                clsDemuxTools.UIMF_CALIBRATION_UPDATER_NAME +
                                                "' order by Entry_ID desc";
                    using (var logEntriesReader = cmdLogEntries.ExecuteReader())
                    {
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
                }

                if (manuallyCalibrated && Math.Abs(calibrationSlope) < double.Epsilon)
                {
                    LogError("Found message 'Manually applied calibration coefficients' but could not determine slope or intercept manually applied");
                    manuallyCalibrated = false;
                }

                return manuallyCalibrated;
            }
        }

        /// <summary>
        /// Construct the full path to UIMFDemultiplexer_Console.exe
        /// </summary>
        /// <returns></returns>
        protected string GetUimfDemultiplexerPath()
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
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
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
                new FileInfo(demultiplexerPath)
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

        #endregion

        #region "Event handlers"

        /// <summary>
        /// Reports progress from demux dll
        /// </summary>
        /// <param name="newProgress">Current progress (value between 0 and 100)</param>
        void clsDemuxTools_DemuxProgress(float newProgress)
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

        /// <summary>
        /// Reports progress for the addition of bin-centric tables
        /// </summary>
        /// <param name="newProgress">Current progress (value between 0 and 100)</param>
        void clsDemuxTools_BinCentricTableProgress(float newProgress)
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

        private void clsDemuxTools_CopyFileWithRetryEvent(string message)
        {
            ResetTimestampForQueueWaitTimeLogging();
        }

        #endregion
    }
}
