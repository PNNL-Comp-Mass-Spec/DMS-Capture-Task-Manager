//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//*********************************************************************************************************

using System;
using CaptureTaskManager;
using System.IO;
using System.Data.SQLite;
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
    public class clsPluginMain : clsToolRunnerBase
    {

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
            var retData = base.RunTool();
            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return retData;

            // Initialize the config DB update interval
            m_LastConfigDBUpdate = DateTime.UtcNow;
            m_MinutesBetweenConfigDBUpdates = MANAGER_UPDATE_INTERVAL_MINUTES;

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                retData.CloseoutMsg = "Error determining version of IMSDemultiplexer";
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
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

            if (m_TaskParams.GetParam("PerformCalibration", true))
                calibrationMode = CalibrationMode.AutoCalibration;
            else
                calibrationMode = CalibrationMode.NoCalibration;

            // Locate data file on storage server
            var svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
            var dsPath = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

            // Use this name first to test if demux has already been performed once
            var uimfFileName = m_Dataset + "_encoded.uimf";
            var fiUIMFFile = new FileInfo(Path.Combine(dsPath, uimfFileName));
            if (fiUIMFFile.Exists && (fiUIMFFile.Length != 0))
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
                    var fiDecodedUIMFFile = new FileInfo(Path.Combine(dsPath, m_Dataset + ".uimf"));

                    var manuallyCalibrated = CheckForManualCalibration(fiDecodedUIMFFile.FullName, out calibrationSlope, out calibrationIntercept);

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

                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        retData.CloseoutMsg = "Error calibrating UIMF file; see " + clsDemuxTools.CALIBRATION_LOG_FILE;
                        retData.EvalMsg =
                            "De-multiplexed but Calibration failed.  If you want to re-demultiplex the _encoded.uimf file, you should rename the CalibrationLog.txt file";

                        msg = "Completed clsPluginMain.RunTool()";
                        LogDebug(msg);
                        return retData;
                    }
                }
            }
            else
            {
                // Was the file zero bytes? If so, delete it
                if (fiUIMFFile.Exists && (fiUIMFFile.Length == 0))
                {
                    try
                    {
                        fiUIMFFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        msg = "Exception deleting 0-byte uimf_encoded file";
                        LogError(msg, ex);

                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        retData.CloseoutMsg = msg;

                        msg = "Completed clsPluginMain.RunTool()";
                        LogDebug(msg);
                        return retData;
                    }
                }

                // If we got to here, _encoded uimf file doesn't exist. So, use the other uimf file
                uimfFileName = m_Dataset + ".uimf";
                if (!File.Exists(Path.Combine(dsPath, uimfFileName)))
                {
                    msg = "UIMF file not found: " + uimfFileName;
                    LogError(msg);

                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    retData.CloseoutMsg = msg;

                    msg = "Completed clsPluginMain.RunTool()";
                    LogDebug(msg);
                    return retData;
                }
            }

            // Query to determine if demux is needed.
            var uimfFilePath = Path.Combine(dsPath, uimfFileName);
            var needToDemultiplex = true;

            var oSQLiteTools = new clsSQLiteTools();
            RegisterEvents(oSQLiteTools);

            var queryResult = oSQLiteTools.GetUimfMuxStatus(uimfFilePath, out var numBitsForEncoding);
            if (queryResult == clsSQLiteTools.UimfQueryResults.NonMultiplexed)
            {
                // De-multiplexing not required, but we should still attempt calibration (if enabled)
                msg = "No de-multiplexing required for dataset " + m_Dataset;
                LogMessage(msg);
                retData.EvalMsg = "Non-Multiplexed";
                needToDemultiplex = false;
            }
            else if (queryResult == clsSQLiteTools.UimfQueryResults.Error)
            {
                // There was a problem determining the UIMF file status. Set state and exit
                msg = "Problem determining UIMF file status for dataset " + m_Dataset;

                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.CloseoutMsg = msg;

                msg = "Completed clsPluginMain.RunTool()";
                LogDebug(msg);
                return retData;
            }

            if (needToDemultiplex)
            {
                // De-multiplexing is needed
                retData = mDemuxTools.PerformDemux(m_MgrParams, m_TaskParams, uimfFileName, numBitsForEncoding);

                if (mDemuxTools.OutOfMemoryException)
                {
                    if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        if (string.IsNullOrEmpty(retData.CloseoutMsg))
                            retData.CloseoutMsg = "Out of memory";
                    }

                    m_NeedToAbortProcessing = true;
                }

                mDemultiplexingPerformed = true;
            }

            if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return retData;
            }

            // Demultiplexing succeeded (or skipped)

            // Lookup the current .uimf file size
            var fiUIMF = new FileInfo(uimfFilePath);
            if (!fiUIMF.Exists)
            {
                if (needToDemultiplex)
                    msg = "UIMF File not found after demultiplexing: " + uimfFilePath;
                else
                    msg = "UIMF File not found (skipped demultiplexing): " + uimfFilePath;

                LogError(msg);
                retData.CloseoutMsg = msg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            // October 2013: Disabled the addition of bin-centric tables since datasets currently being acquired on the IMS platform will not have IQ run on them
            // March 2015: Re-enabled automatic addition of bin-centric tables
            // May 22, 2015: Now adding bin-centric tables only if the original .UIMF file is less than 2 GB in size
            // October 12, 2017: Again disabled the addition of bin-centric tables since they can greatly increase .UIMF file size and because usage of the bin-centric tables is low

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (ADD_BIN_CENTRIC_TABLES)
#pragma warning disable 162
            {
                var fileSizeGBStart = fiUIMF.Length / 1024.0 / 1024.0 / 1024.0;
                var fileSizeText = " (" + Math.Round(fileSizeGBStart, 0).ToString("0") + " GB)";

                if (fileSizeGBStart > 2)
                {
                    LogMessage("Not adding bin-centric tables to " + fiUIMF.Name + " since over 2 GB in size" + fileSizeText);
                }
                else
                {
                    // Add the bin-centric tables if not yet present
                    LogMessage("Adding bin-centric tables to " + fiUIMF.Name + fileSizeText);
                    retData = mDemuxTools.AddBinCentricTablesIfMissing(m_MgrParams, m_TaskParams, retData);

                    fiUIMF.Refresh();
                    var fileSizeGBEnd = fiUIMF.Length / 1024.0 / 1024.0 / 1024.0;
                    double foldIncrease = 0;
                    if (fileSizeGBStart > 0)
                        foldIncrease = fileSizeGBEnd / fileSizeGBStart;

                    LogMessage("UIMF file size increased from " + fileSizeGBStart.ToString("0.00") + " GB to " + fileSizeGBEnd.ToString("0.00") +
                               " GB, a " + foldIncrease.ToString("0.0" + " fold increase"));
                }
#pragma warning restore 162
            }

            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {

                if (calibrationMode == CalibrationMode.AutoCalibration)
                    retData = mDemuxTools.PerformCalibration(m_MgrParams, m_TaskParams, retData);
                else if (calibrationMode == CalibrationMode.ManualCalibration)
                {
                    retData = mDemuxTools.PerformManualCalibration(m_MgrParams, m_TaskParams, retData, calibrationSlope, calibrationIntercept);
                }
            }

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return retData;
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


            mDemuxTools = new clsDemuxTools(uimfDemultiplexerProgLoc, m_MgrName, m_FileTools);
            RegisterEvents(mDemuxTools);

            // Add a handler to catch progress events
            mDemuxTools.DemuxProgress += clsDemuxTools_DemuxProgress;
            mDemuxTools.BinCentricTableProgress += clsDemuxTools_BinCentricTableProgress;
            mDemuxTools.CopyFileWithRetryEvent += clsDemuxTools_CopyFileWithRetryEvent;
        }

        protected bool CheckForCalibrationError(string dsPath)
        {
            var sCalibrationLogPath = Path.Combine(dsPath, clsDemuxTools.CALIBRATION_LOG_FILE);
            var bCalibrationError = false;

            if (File.Exists(sCalibrationLogPath))
            {
                var srInFile = new StreamReader(new FileStream(sCalibrationLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (srInFile.Peek() >= 0)
                {
                    var sLine = srInFile.ReadLine();

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

                // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in readonly folders
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

                if (manuallyCalibrated && Math.Abs(calibrationSlope) < double.Epsilon)
                {
                    LogError("Found message 'Manually applied calibration coefficients' but could not determine slope or intercept manually applied");
                    manuallyCalibrated = false;
                }

                return manuallyCalibrated;
            }

        }

        protected bool CopyFileToStorageServer(string sourceDirPath, string fileName)
        {

            string msg;
            var bSuccess = true;

            var svrPath = Path.Combine(m_TaskParams.GetParam("Storage_Vol_External"), m_TaskParams.GetParam("Storage_Path"));
            var sDatasetFolderPathRemote = Path.Combine(svrPath, m_TaskParams.GetParam("Folder"));

            // Copy file fileName from sourceDirPath to the dataset folder
            var sSourceFilePath = Path.Combine(sourceDirPath, fileName);
            var sTargetFilePath = Path.Combine(sDatasetFolderPathRemote, fileName);

            if (!File.Exists(sSourceFilePath))
            {
                msg = "File not found: " + sSourceFilePath;
                LogError(msg);
            }
            else
            {
                const int retryCount = 3;
                const bool backupDestFileBeforeCopy = false;

                if (!clsDemuxTools.CopyFileWithRetry(sSourceFilePath, sTargetFilePath, true, retryCount, backupDestFileBeforeCopy, m_MgrName, m_FileTools))
                {
                    msg = "Error copying " + fileName + " to storage server";
                    LogError(msg);
                    bSuccess = false;
                }
            }

            return bSuccess;

        }

        /// <summary>
        /// Construct the full path to UIMFDemultiplexer_Console.exe
        /// </summary>
        /// <returns></returns>
        protected string GetUimfDemultiplexerPath()
        {
            var uimfDemuxFolder = m_MgrParams.GetParam("UimfDemultiplexerProgLoc", string.Empty);

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

            var strToolVersionInfo = string.Empty;

            var uimfDemultiplexerProgLoc = GetUimfDemultiplexerPath();
            if (string.IsNullOrEmpty(uimfDemultiplexerProgLoc))
                return false;

            var fiUimfDemultiplexer = new FileInfo(uimfDemultiplexerProgLoc);

            LogDebug("Determining tool version info");

            if (fiUimfDemultiplexer.DirectoryName == null)
                return false;

            // Lookup the version of UIMFDemultiplexer_Console
            var bSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, fiUimfDemultiplexer.FullName);
            if (!bSuccess)
                return false;

            // Lookup the version of the IMSDemultiplexer (in the UIMFDemultiplexer folder)
            var strDemultiplexerPath = Path.Combine(fiUimfDemultiplexer.DirectoryName, "IMSDemultiplexer.dll");
            bSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strDemultiplexerPath);
            if (!bSuccess)
                return false;

            var strAutoCalibrateUIMFPath = Path.Combine(fiUimfDemultiplexer.DirectoryName, "AutoCalibrateUIMF.dll");
            bSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strAutoCalibrateUIMFPath);
            if (!bSuccess)
                return false;

            var strUIMFLibrary = Path.Combine(fiUimfDemultiplexer.DirectoryName, "UIMFLibrary.dll");
            bSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strUIMFLibrary);
            if (!bSuccess)
                return false;

            // Store path to the demultiplexer DLL in ioToolFiles
            var ioToolFiles = new System.Collections.Generic.List<FileInfo>
            {
                new FileInfo(strDemultiplexerPath)
            };

            try
            {
                const bool bSaveToolVersionTextFile = false;
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, bSaveToolVersionTextFile);
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
#pragma warning disable 162
            {
                // Multiplying by 0.9 since we're assuming that demultiplexing will take 90% of the time while addition of bin-centric tables will take 10% of the time
                m_StatusTools.UpdateAndWrite(0 + newProgress * 0.90f);
#pragma warning restore 162
            }

            // Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTESS
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
                progressOverall = newProgress;

            m_StatusTools.UpdateAndWrite(progressOverall);

            // Update the manager settings every MANAGER_UPDATE_INTERVAL_MINUTESS
            UpdateMgrSettings();
        }

        private void clsDemuxTools_CopyFileWithRetryEvent(string message)
        {
        }
        #endregion
    }
}
