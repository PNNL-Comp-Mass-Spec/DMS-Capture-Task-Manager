//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;
using MSFileInfoScannerInterfaces;
using PRISM;
using PRISMDatabaseUtils;

namespace DatasetInfoPlugin
{
    /// <summary>
    /// Dataset Info plugin: generates QC graphics using MSFileInfoScanner
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: acq, Altis, Bruker, fid, href, html, Illumina, labelling, maldi, mgf
        // Ignore Spelling: online, png, prepend, qgd, ser, Shimadzu, svc, Synapt, utf, wiff

        private const string BRUKER_MCF_FILE_EXTENSION = ".mcf";

        private const bool IGNORE_BRUKER_BAF_ERRORS = false;

        private const string INVALID_FILE_TYPE = "Invalid File Type";

        private const int MAX_INFO_SCANNER_RUNTIME_HOURS = 72;

        private const int MAX_INFO_SCANNER_RUNTIME_MINUTES = 60 * MAX_INFO_SCANNER_RUNTIME_HOURS;

        private const string MS_FILE_SCANNER_DS_INFO_SP = "cache_dataset_info_xml";

        private const string STORED_FILE_SUFFIX = ".stored";

        private const string UNKNOWN_FILE_TYPE = "Unknown File Type";

        private enum QCPlottingModes
        {
            NoPlots = 0,          // Use this if SkipPlots is enabled
            BpiAndTicOnly = 1,
            AllPlots = 2
        }

        private int mCurrentDatasetNumber;

        private int mFailedScanCount;

        /// <summary>
        /// Property to flag if this is being run specifically for LC data capture
        /// </summary>
        protected virtual bool IsLcDataCapture => false;

        private DateTime mLastProgressUpdate;

        private DateTime mLastStatusUpdate;

        private readonly LCMSDataPlotterOptions mLCMS2DPlotOptions;

        private string mMsFileInfoScannerExePath;

        private readonly InfoScannerOptions mMsFileScannerOptions;

        private string mMsg;

        private DateTime mProcessingStartTime;

        private ProcessingStatus mProcessingStatus;

        private string mStatusFilePath;

        private readonly StatusFileTools mStatusFileTools;

        private int mTotalDatasetFilesOrDirectories;

        /// <summary>
        /// Constructor
        /// </summary>
        public PluginMain()
        {
            mMsFileScannerOptions = new InfoScannerOptions();

            mLCMS2DPlotOptions = new LCMSDataPlotterOptions();

            mProcessingStatus = new ProcessingStatus();

            mStatusFileTools = new StatusFileTools();
            RegisterEvents(mStatusFileTools);
        }

        /// <summary>
        /// Runs the dataset info step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override ToolReturnData RunTool()
        {
            // Note that Debug messages are logged if mDebugLevel == 5

            LogDebug("Starting DatasetInfoPlugin.PluginMain.RunTool()");

            // Perform base class operations, if any
            var returnData = base.RunTool();

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return returnData;
            }

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                returnData.CloseoutMsg = "Error determining tool version info";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            LogMessage("Running DatasetInfo on dataset " + mDataset);

            returnData = RunMsFileInfoScanner();

            LogDebug("Completed PluginMain.RunTool()");

            return returnData;
        }

        /// <summary>
        /// Initializes MSFileInfoScanner options
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            LogDebug("Starting PluginMain.Setup()");

            base.Setup(mgrParams, taskParams, statusTools);

            mMsFileInfoScannerExePath = GetMSFileInfoScannerExePath();

            if (string.IsNullOrEmpty(mMsFileInfoScannerExePath))
            {
                throw new NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");
            }

            if (!File.Exists(mMsFileInfoScannerExePath))
            {
                throw new FileNotFoundException("File Not Found: " + mMsFileInfoScannerExePath);
            }

            mProcessingStartTime = DateTime.UtcNow;
            mProcessingStatus.ProgressPercentComplete = 0;
            mLastProgressUpdate = DateTime.UtcNow;
            mLastStatusUpdate = DateTime.UtcNow;

            // Initialize the MSFileInfoScanner options

            LogDebug("Completed PluginMain.Setup()");
        }

        /// <summary>
        /// Runs the MS_File_Info_Scanner tool
        /// </summary>
        private ToolReturnData RunMsFileInfoScanner()
        {
            var returnData = new ToolReturnData();

            // Always use client perspective for the source directory (allows MSFileInfoScanner to run from any CTM)
            var remoteSharePath = mTaskParams.GetParam("Storage_Vol_External");

            // Set up the rest of the paths
            var sourceDirectoryBase = Path.Combine(remoteSharePath, mTaskParams.GetParam("Storage_Path"));
            var datasetDirectory = mTaskParams.GetParam("Directory");

            var datasetDirectoryPath = Path.Combine(sourceDirectoryBase, datasetDirectory);
            var outputPathBase = Path.Combine(datasetDirectoryPath, "QC");

            // Set up the params for the MS file scanner
            mMsFileScannerOptions.PostResultsToDMS = false;
            mMsFileScannerOptions.SaveTICAndBPIPlots = mTaskParams.GetParam("SaveTICAndBPIPlots", true);
            mMsFileScannerOptions.SaveLCMS2DPlots = mTaskParams.GetParam("SaveLCMS2DPlots", true);
            mMsFileScannerOptions.ComputeOverallQualityScores = mTaskParams.GetParam("ComputeOverallQualityScores", false);
            mMsFileScannerOptions.CreateDatasetInfoFile = mTaskParams.GetParam("CreateDatasetInfoFile", true);

            mLCMS2DPlotOptions.MZResolution = mTaskParams.GetParam("LCMS2DPlotMZResolution", LCMSDataPlotterOptions.DEFAULT_MZ_RESOLUTION);
            mLCMS2DPlotOptions.MaxPointsToPlot = mTaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", LCMSDataPlotterOptions.DEFAULT_MAX_POINTS_TO_PLOT);
            mLCMS2DPlotOptions.MinPointsPerSpectrum = mTaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", LCMSDataPlotterOptions.DEFAULT_MIN_POINTS_PER_SPECTRUM);
            mLCMS2DPlotOptions.MinIntensity = mTaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
            mLCMS2DPlotOptions.OverviewPlotDivisor = mTaskParams.GetParam("LCMS2DOverviewPlotDivisor", LCMSDataPlotterOptions.DEFAULT_LCMS2D_OVERVIEW_PLOT_DIVISOR);

            var sampleLabelling = mTaskParams.GetParam("Meta_Experiment_sample_labelling", string.Empty);
            var experimentName = mTaskParams.GetParam("Meta_Experiment_Num", string.Empty);
            ConfigureMinimumMzValidation(experimentName, sampleLabelling);

            mMsFileScannerOptions.DatasetID = mDatasetID;
            mMsFileScannerOptions.CheckCentroidingStatus = true;
            mMsFileScannerOptions.PlotWithPython = true;

            // Debugging aids:

            // Uncomment to disable SHA-1 hashing of instrument files
            // mMsFileInfoScannerOptions.DisableInstrumentHash = true;

            // Uncomment to skip creating plots
            // mTaskParams.SetParam("SkipPlots", "true");

            // Get the input file or directory name (or names)
            var fileOrDirectoryRelativePaths = GetDataFileOrDirectoryName(
                datasetDirectoryPath,
                out var qcPlotMode,
                out var rawDataType,
                out var instrumentClass,
                out var brukerDotDBaf);

            mTotalDatasetFilesOrDirectories = fileOrDirectoryRelativePaths.Count;

            if (mTotalDatasetFilesOrDirectories > 0 && fileOrDirectoryRelativePaths.First() == UNKNOWN_FILE_TYPE)
            {
                // Raw_Data_Type not recognized
                returnData.CloseoutMsg = mMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            var rawDataTypeName = InstrumentClassInfo.GetDataFormatName(rawDataType);
            var instClass = InstrumentClassInfo.GetInstrumentClassName(instrumentClass);

            if (mTotalDatasetFilesOrDirectories > 0 && fileOrDirectoryRelativePaths.First() == INVALID_FILE_TYPE)
            {
                // DS Info not implemented for this file type
                returnData.CloseoutMsg = string.Empty;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                returnData.EvalMsg = AppendToComment(
                    returnData.EvalMsg,
                    string.Format(
                        "Dataset info analysis is not implemented for data type {0}, instrument class {1}",
                        rawDataTypeName, instClass));

                returnData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                return returnData;
            }

            if (IsLcDataCapture && rawDataType == DataFormat.WatersRawFolder)
            {
                // We currently cannot extract chromatograms from Waters Raw datasets, so report the step 'Skipped'
                // However, still run the MSFileInfoScanner so that it creates the DatasetInfo.xml file
                returnData.CloseoutMsg = string.Empty;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                returnData.EvalMsg = $"Dataset info is not implemented for data type {rawDataTypeName}, instrument class {instClass}";
            }

            if (mTotalDatasetFilesOrDirectories == 0 || string.IsNullOrEmpty(fileOrDirectoryRelativePaths.First()))
            {
                // There was a problem with getting the file name; Details reported by called method
                returnData.CloseoutMsg = mMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            if (mTaskParams.GetParam("SkipPlots", false))
            {
                // To add parameter SkipPlots for job 123456, use:
                // Call cap.add_update_task_parameter (123456, 'JobParameters', 'SkipPlots', 'true');
                qcPlotMode = QCPlottingModes.NoPlots;
            }

            switch (qcPlotMode)
            {
                case QCPlottingModes.NoPlots:
                    // Do not create any plots
                    mMsFileScannerOptions.SaveTICAndBPIPlots = false;
                    mMsFileScannerOptions.SaveLCMS2DPlots = false;
                    break;

                case QCPlottingModes.BpiAndTicOnly:
                    // Only create the BPI and TIC plots
                    mMsFileScannerOptions.SaveTICAndBPIPlots = true;
                    mMsFileScannerOptions.SaveLCMS2DPlots = false;
                    break;
            }

            if (IsLcDataCapture)
            {
                mMsFileScannerOptions.HideEmptyHTMLSections = true;
            }

            // Make the output directory
            if (!Directory.Exists(outputPathBase))
            {
                try
                {
                    Directory.CreateDirectory(outputPathBase);
                    LogDebug("PluginMain.RunMsFileInfoScanner: Created output directory " + outputPathBase);
                }
                catch (Exception ex)
                {
                    var msg = string.Format("PluginMain.RunMsFileInfoScanner: {0} {1}", EXCEPTION_CREATING_OUTPUT_DIRECTORY, outputPathBase);

                    if (System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase) &&
                        !Environment.UserName.StartsWith("svc", StringComparison.OrdinalIgnoreCase))
                    {
                        LogWarning(msg + ": " + ex.Message);
                    }
                    else
                    {
                        LogError(msg, ex);

                        returnData.CloseoutMsg = EXCEPTION_CREATING_OUTPUT_DIRECTORY + " " + outputPathBase;
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                        return returnData;
                    }
                }
            }

            var useLocalOutputDirectory =
                System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase) &&
                !Environment.UserName.StartsWith("svc", StringComparison.OrdinalIgnoreCase);

            // Call the MS File Info Scanner DLL
            // Typically only call it once, but for Bruker datasets with multiple .d directories, we'll call it once for each .d directory

            mMsg = string.Empty;
            var errorCodeToReturn = iMSFileInfoScanner.MSFileScannerErrorCodes.NoError;

            var cachedDatasetInfoXML = new List<string>();
            var datasetInfoFilesToRename = new List<FileInfo>();
            var outputDirectoryNames = new List<string>();
            var primaryFileOrDirectoryProcessed = false;

            var nextSubdirectorySuffix = 1;
            var successCount = 0;
            mCurrentDatasetNumber = 0;

            foreach (var datasetFileOrDirectory in fileOrDirectoryRelativePaths)
            {
                mFailedScanCount = 0;
                mCurrentDatasetNumber++;

                // Reset this for each input file or directory
                mProcessingStatus.ErrorCode = 0;

                var remoteFileOrDirectoryPath = Path.Combine(datasetDirectoryPath, datasetFileOrDirectory);

                string pathToProcess;

                bool fileCopiedLocally;
                bool directoryCopiedLocally;

                string localDirectoryPath;

                var datasetFile = new FileInfo(remoteFileOrDirectoryPath);

                if (datasetFile.Exists && datasetFile.Extension.Equals(InstrumentClassInfo.DOT_RAW_EXTENSION, StringComparison.OrdinalIgnoreCase))
                {
                    LogMessage("Copying instrument file to local disk: " + datasetFile.FullName, false, false);

                    ResetTimestampForQueueWaitTimeLogging();

                    // Thermo .raw file; copy it locally
                    var localFilePath = Path.Combine(mWorkDir, datasetFileOrDirectory);
                    var fileCopied = mFileTools.CopyFileUsingLocks(datasetFile, localFilePath, true);

                    if (!fileCopied)
                    {
                        returnData.CloseoutMsg = "Error copying instrument data file to local working directory";
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return returnData;
                    }

                    pathToProcess = localFilePath;
                    fileCopiedLocally = true;
                    directoryCopiedLocally = false;
                    localDirectoryPath = string.Empty;
                }
                else if (instrumentClass == InstrumentClass.BrukerFT_BAF && datasetFile.Exists)
                {
                    // ReSharper disable CommentTypo

                    // If the .D directory has file chromatography-data.sqlite, copy the directory locally so that MSFileInfoScanner can extract the EIC data from the SQLite file
                    // For example, see SciMax dataset QC_SRFAII_23_01_10ppm_WEOM_r2_Mag_04Mar24_300SA_p01_54_1_16858
                    // Even though MSFileInfoScanner includes "Read Only=true" in the connection string, the SQLite database cannot be opened if the field is read-only

                    // ReSharper restore CommentTypo

                    // datasetFileOrDirectory has a relative path to a file
                    var remoteDirectory = new DirectoryInfo(Path.Combine(datasetDirectoryPath, Path.GetDirectoryName(datasetFileOrDirectory)));

                    var sqliteFile = new FileInfo(Path.Combine(remoteDirectory.FullName, "chromatography-data.sqlite"));

                    if (!sqliteFile.Exists)
                    {
                        pathToProcess = remoteFileOrDirectoryPath;
                        fileCopiedLocally = false;
                        directoryCopiedLocally = false;
                        localDirectoryPath = string.Empty;
                    }
                    else
                    {
                        localDirectoryPath = Path.Combine(mWorkDir, remoteDirectory.Name);
                        pathToProcess = Path.Combine(mWorkDir, datasetFileOrDirectory);

                        if (!CopyRemoteDirectoryToLocal(remoteDirectory, localDirectoryPath, returnData))
                        {
                            return returnData;
                        }

                        fileCopiedLocally = false;
                        directoryCopiedLocally = true;
                    }
                }
                else if (instrumentClass == InstrumentClass.BrukerTOF_TDF)
                {
                    // BrukerTOF_TDF is the timsTOF; copy the .d directory locally to avoid network glitches during processing

                    // datasetFileOrDirectory has a relative path to a directory
                    var remoteDirectory = new DirectoryInfo(remoteFileOrDirectoryPath);
                    localDirectoryPath = Path.Combine(mWorkDir, datasetFileOrDirectory);
                    pathToProcess = localDirectoryPath;

                    if (!CopyRemoteDirectoryToLocal(remoteDirectory, localDirectoryPath, returnData))
                    {
                        return returnData;
                    }

                    fileCopiedLocally = false;
                    directoryCopiedLocally = true;
                }
                else
                {
                    pathToProcess = remoteFileOrDirectoryPath;
                    fileCopiedLocally = false;
                    directoryCopiedLocally = false;
                    localDirectoryPath = string.Empty;
                }

                var currentOutputDirectory = ConstructOutputDirectoryPath(
                    outputPathBase, datasetFileOrDirectory, mTotalDatasetFilesOrDirectories,
                    outputDirectoryNames, ref nextSubdirectorySuffix);

                if (string.IsNullOrWhiteSpace(currentOutputDirectory))
                {
                    returnData.CloseoutMsg = "ConstructOutputDirectoryPath returned an empty string; cannot process this dataset";
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return returnData;
                }

                if (useLocalOutputDirectory)
                {
                    // Override the output directory
                    var localOutputDir = Path.Combine(mWorkDir, Path.GetFileName(currentOutputDirectory));
                    ConsoleMsgUtils.ShowDebug("Overriding MSFileInfoScanner output directory from {0}\n  to {1}",
                                              currentOutputDirectory, localOutputDir);
                    currentOutputDirectory = localOutputDir;
                }

                mStatusFilePath = Path.Combine(mWorkDir, "MSFileInfoScanner_Status.xml");

                var argumentList = new List<string>
                {
                    string.Format("/I:{0}", PathUtils.PossiblyQuotePath(pathToProcess)),
                    string.Format("/O:{0}", currentOutputDirectory),
                    string.Format("/SF:{0}", mStatusFilePath)
                };

                if (!mMsFileScannerOptions.SaveTICAndBPIPlots)
                {
                    argumentList.Add("/NoTIC");
                }
                else if (IsLcDataCapture)
                {
                    argumentList.Add("/HideEmpty");
                }

                if (mMsFileScannerOptions.SaveLCMS2DPlots)
                {
                    argumentList.Add("/LC");
                }

                if (mMsFileScannerOptions.CreateDatasetInfoFile)
                {
                    argumentList.Add("/DI");
                }

                if (mMsFileScannerOptions.CheckCentroidingStatus)
                {
                    argumentList.Add("/CC");
                }

                if (mMsFileScannerOptions.PlotWithPython)
                {
                    argumentList.Add("/Python");
                }

                if (mMsFileScannerOptions.CheckCentroidingStatus)
                {
                    argumentList.Add("/SS");
                }

                if (mMsFileScannerOptions.ComputeOverallQualityScores)
                {
                    argumentList.Add("/QS");
                }

                if (mMsFileScannerOptions.MS2MzMin > 0)
                {
                    argumentList.Add(string.Format("/MS2MzMin:{0:F1}", mMsFileScannerOptions.MS2MzMin));
                }

                if (mMsFileScannerOptions.ScanStart > 0)
                {
                    argumentList.Add("/ScanStart:" + mMsFileScannerOptions.ScanStart);
                }

                if (mMsFileScannerOptions.ScanEnd > 0)
                {
                    argumentList.Add("/ScanEnd:" + mMsFileScannerOptions.ScanEnd);
                }

                if (mMsFileScannerOptions.ShowDebugInfo)
                {
                    argumentList.Add("/Debug");
                }

                var consoleOutputFilePath = Path.Combine(mWorkDir, "MSFileInfoScanner_ConsoleOutput.txt");

                var cmdRunner = new RunDosProgram(mWorkDir)
                {
                    CreateNoWindow = false,
                    EchoOutputToConsole = false,
                    CacheStandardOutput = false,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = consoleOutputFilePath
                };

                // This will also call RegisterEvents
                AttachCmdRunnerEvents(cmdRunner);

                mProcessingStartTime = DateTime.UtcNow;
                mProcessingStatus.ProgressPercentComplete = 0;
                mLastProgressUpdate = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;

                var arguments = string.Join(" ", argumentList);

                LogMessage("Starting MsFileInfoScanner: {0} {1}", mMsFileInfoScannerExePath, arguments);

                const int maxRuntimeSeconds = MAX_INFO_SCANNER_RUNTIME_MINUTES * 60;
                var successProcessing = cmdRunner.RunProgram(mMsFileInfoScannerExePath, arguments, "MsFileInfoScanner", true, maxRuntimeSeconds);

                DetachCmdRunnerEvents(cmdRunner);

                // Parse the status file one more time
                ParseStatusFile();

                iMSFileInfoScanner.MSFileScannerErrorCodes errorCodeToUse;

                if (!successProcessing)
                {
                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("MsFileInfoScanner returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MsFileInfoScanner failed (but exit code is 0)");
                    }

                    if (cmdRunner.ExitCode <= 0)
                    {
                        returnData.CloseoutMsg = "Error running MsFileInfoScanner";
                        LogError(returnData.CloseoutMsg);

                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return returnData;
                    }

                    if (mProcessingStatus.ErrorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.NoError)
                    {
                        errorCodeToUse = Enum.TryParse(cmdRunner.ExitCode.ToString(), out iMSFileInfoScanner.MSFileScannerErrorCodes errorCode)
                            ? errorCode
                            : iMSFileInfoScanner.MSFileScannerErrorCodes.UnspecifiedError;
                    }
                    else
                    {
                        errorCodeToUse = mProcessingStatus.ErrorCode;
                    }

                    if (errorCodeToReturn == iMSFileInfoScanner.MSFileScannerErrorCodes.NoError)
                    {
                        errorCodeToReturn = errorCodeToUse;
                    }
                }
                else
                {
                    LogMessage("MsFileInfoScanner Complete");
                    errorCodeToUse = iMSFileInfoScanner.MSFileScannerErrorCodes.NoError;
                }

                mStatusTools.UpdateAndWrite(99);

                if (errorCodeToUse != iMSFileInfoScanner.MSFileScannerErrorCodes.NoError)
                {
                    successProcessing = false;
                }

                if (errorCodeToUse == iMSFileInfoScanner.MSFileScannerErrorCodes.ThermoRawFileReaderError)
                {
                    // Call to .OpenRawFile failed
                    mMsg = "Error running MSFileInfoScanner: Call to .OpenRawFile failed";
                    LogError(mMsg);
                    successProcessing = false;
                }
                else if (errorCodeToUse == iMSFileInfoScanner.MSFileScannerErrorCodes.DatasetHasNoSpectra)
                {
                    // Dataset has no spectra
                    // If the instrument class is BrukerFT_BAF and the dataset has an analysis.baf file but no ser or fid file, treat this as a warning

                    bool logWarning;

                    if (successProcessing && instrumentClass == InstrumentClass.BrukerFT_BAF)
                    {
                        var directoryToSearch = new DirectoryInfo(datasetDirectoryPath);

                        var serFiles = PathUtils.FindFilesWildcard(directoryToSearch, "ser", true).Count;
                        var fidFiles = PathUtils.FindFilesWildcard(directoryToSearch, "fid", true).Count;

                        logWarning = serFiles == 0 && fidFiles == 0;
                    }
                    else
                    {
                        logWarning = false;
                    }

                    if (logWarning)
                    {
                        returnData.EvalMsg = AppendToComment(returnData.EvalMsg, "Dataset has no spectra; file format not supported by MSFileInfoScanner");

                        LogWarning(returnData.EvalMsg);

                        qcPlotMode = QCPlottingModes.NoPlots;
                    }
                    else if (returnData.EvalCode == EnumEvalCode.EVAL_CODE_SKIPPED && IsLcDataCapture)
                    {
                        successProcessing = true;
                    }
                    else
                    {
                        mMsg = "Error running MSFileInfoScanner: Dataset has no spectra (ScanCount = 0)";
                        LogError(mMsg);
                        successProcessing = false;
                    }
                }

                if (fileCopiedLocally)
                {
                    mFileTools.DeleteFileWithRetry(new FileInfo(pathToProcess), 2, out _);
                }
                else if (directoryCopiedLocally)
                {
                    mFileTools.DeleteDirectory(localDirectoryPath);
                }

                var mzMinValidationError = errorCodeToUse == iMSFileInfoScanner.MSFileScannerErrorCodes.MS2MzMinValidationError;

                if (mzMinValidationError)
                {
                    mMsg = mProcessingStatus.MS2MzMinValidationMessage;
                    successProcessing = false;
                }

                if (errorCodeToUse == iMSFileInfoScanner.MSFileScannerErrorCodes.MS2MzMinValidationWarning)
                {
                    var warningMsg = mProcessingStatus.MS2MzMinValidationMessage;
                    returnData.EvalMsg = AppendToComment(returnData.EvalMsg, "MS2MzMinValidationWarning: " + warningMsg);
                }

                bool validQcGraphics;

                if (returnData.EvalCode == EnumEvalCode.EVAL_CODE_SKIPPED)
                {
                    validQcGraphics = true;
                }
                else if (successProcessing && qcPlotMode == QCPlottingModes.AllPlots)
                {
                    validQcGraphics = ValidateQCGraphics(currentOutputDirectory, primaryFileOrDirectoryProcessed, returnData);

                    if (returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {
                        if (mTotalDatasetFilesOrDirectories == 1 || mCurrentDatasetNumber == mTotalDatasetFilesOrDirectories)
                        {
                            return returnData;
                        }

                        continue;
                    }
                }
                else
                {
                    validQcGraphics = true;
                }

                if (successProcessing)
                {
                    if (mMsFileScannerOptions.CreateDatasetInfoFile)
                    {
                        // Read the DatasetInfo XML file and store in cachedDatasetInfoXML
                        LoadDatasetInfoXML(currentOutputDirectory, cachedDatasetInfoXML, datasetInfoFilesToRename);
                    }

                    primaryFileOrDirectoryProcessed = true;

                    if (validQcGraphics)
                        successCount++;

                    continue;
                }

                if (mProcessingStatus.ErrorCountLoadDataForScan > 0)
                {
                    returnData.EvalMsg = AppendToComment(
                        returnData.EvalMsg, "Corrupt spectra found; inspect QC plots to decide if errors can be ignored");
                }

                // Either a non-zero error code was returned, or an error event was received

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                // ReSharper disable HeuristicUnreachableCode
                if (brukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
                {
                    // 12T_FTICR_B datasets (with .d directories and analysis.baf and/or fid files) sometimes work with MSFileInfoScanner, and sometimes don't
                    // The problem is that ProteoWizard doesn't support certain forms of these datasets
                    // In particular, small datasets (lasting just a few seconds) don't work

                    returnData.CloseoutMsg = string.Empty;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    returnData.EvalMsg = AppendToComment(
                        returnData.EvalMsg,
                        string.Format("MSFileInfoScanner error for data type {0}, instrument class {1}", rawDataTypeName, instClass));
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    return returnData;
                }
                // ReSharper restore HeuristicUnreachableCode

                if (primaryFileOrDirectoryProcessed || mTotalDatasetFilesOrDirectories > 1 && mCurrentDatasetNumber < mTotalDatasetFilesOrDirectories)
                {
                    // MSFileInfoScanner already processed the primary file or directory
                    // Mention this failure in the EvalMsg but still return success
                    returnData.EvalMsg = AppendToComment(
                        returnData.EvalMsg,
                        "ProcessMSFileOrFolder returned false for " + datasetFileOrDirectory);
                }
                else
                {
                    if (returnData.EvalCode != EnumEvalCode.EVAL_CODE_SKIPPED)
                    {
                        if (string.IsNullOrEmpty(mMsg))
                        {
                            mMsg = "ProcessMSFileOrFolder returned false. Message = " +
                                   mProcessingStatus.ErrorMessage +
                                   " returnData code = " + (int)errorCodeToUse;
                        }

                        LogError(mMsg);

                        returnData.CloseoutMsg = mMsg;
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    if (mzMinValidationError)
                    {
                        string jobParamNote;

                        var instrumentName = mTaskParams.GetParam("Instrument_Name");

                        if (mDataset.Contains("SRM_TMT"))
                        {
                            jobParamNote = "Ignoring m/z validation error since an SRM TMT dataset";
                            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                            if (successCount < 1)
                            {
                                successCount = 1;
                            }
                        }
                        else if (instrumentName.StartsWith("Altis", StringComparison.OrdinalIgnoreCase))
                        {
                            jobParamNote = "Ignoring m/z validation error since an Altis MRM dataset";
                            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                            if (successCount < 1)
                            {
                                successCount = 1;
                            }
                        }
                        else
                        {
                            jobParamNote = string.Format(
                                "To ignore this error, use Call cap.add_update_task_parameter ({0}, 'JobParameters', 'SkipMinimumMzValidation', 'true');",
                                mJob);
                        }

                        returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg, jobParamNote);
                    }

                    if (cachedDatasetInfoXML.Count > 0)
                    {
                        // Do not exit this method yet; we want to store the dataset info in the database
                        break;
                    }

                    return returnData;
                }

                if (mFailedScanCount > 10)
                {
                    LogWarning("Unable to load data for {0} spectra", mFailedScanCount);
                }
            } // for each file in fileOrDirectoryRelativePaths

            // Merge the dataset info defined in cachedDatasetInfoXML
            // If cachedDatasetInfoXml contains just one item, simply return it
            var datasetXmlMerger = new DatasetInfoXmlMerger();

            var combinedDatasetInfoXML = CombineDatasetInfoXML(datasetXmlMerger, cachedDatasetInfoXML, mTotalDatasetFilesOrDirectories);

            if (cachedDatasetInfoXML.Count > 1)
            {
                ProcessMultiDatasetInfoScannerResults(outputPathBase, datasetXmlMerger, combinedDatasetInfoXML, outputDirectoryNames);
            }

            // Remove ".stored" from the files in datasetInfoFilesToRename
            foreach (var datasetInfoFile in datasetInfoFilesToRename)
            {
                if (!datasetInfoFile.Name.EndsWith(STORED_FILE_SUFFIX))
                {
                    LogWarning("File in datasetInfoFilesToRename does not have suffix '.stored', which is unexpected: {0}", datasetInfoFile.FullName);
                    continue;
                }

                var renamedFile = new FileInfo(datasetInfoFile.FullName.Substring(0, datasetInfoFile.FullName.Length - STORED_FILE_SUFFIX.Length));

                if (renamedFile.Exists)
                {
                    LogWarning("Cannot remove '.stored' from dataset info file, since the target file already exists: {0}", renamedFile.FullName);
                    continue;
                }

                datasetInfoFile.MoveTo(renamedFile.FullName);
            }

            // Check for dataset acq time gap warnings
            // If any are found, CloseoutMsg is updated
            AcqTimeWarningsReported(datasetXmlMerger, returnData);

            bool success;
            string errorMessage;

            if (string.IsNullOrWhiteSpace(combinedDatasetInfoXML))
            {
                success = false;
                errorMessage = "Dataset info XML is an empty string";
            }
            else if (IsLcDataCapture)
            {
                // Do not post Dataset info XML to the database, since it is only LC data
                success = true;
                errorMessage = string.Empty;
            }
            else
            {
                // Call SP cache_dataset_info_xml to store datasetInfoXML in table T_Dataset_Info_XML
                success = PostDatasetInfoXml(combinedDatasetInfoXML, out errorMessage);
            }

            if (!success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg, errorMessage);
            }

            if (successCount == 0 && returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                if (string.IsNullOrEmpty(mMsg))
                {
                    mMsg = "ProcessMSFileOrFolder returned false. Message = " +
                           mProcessingStatus.ErrorMessage +
                           " returnData code = " + (int)errorCodeToReturn;
                }

                LogError(mMsg);

                returnData.CloseoutMsg = mMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (!useLocalOutputDirectory || returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return returnData;
            }

            // Set this to failed since we stored the QC graphics in the local working directory instead of on the storage server
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg,
                                                  "QC graphics were saved locally for debugging purposes; " +
                                                  "this job step needs to be processed by a manager that has write access to the storage server");

            return returnData;
        }

        /// <summary>
        /// Examine datasetXmlMerger.AcqTimeWarnings
        /// If non-empty, summarize the errors and update returnData
        /// </summary>
        /// <param name="datasetXmlMerger"></param>
        /// <param name="returnData"></param>
        /// <returns>True if warnings exist, otherwise false</returns>
        private void AcqTimeWarningsReported(DatasetInfoXmlMerger datasetXmlMerger, ToolReturnData returnData)
        {
            if (datasetXmlMerger.AcqTimeWarnings.Count == 0)
            {
                return;
            }

            // Large gap found
            // Log the error and do not post the XML file to the database
            // You could manually add the file later by reading from disk and adding to table T_Dataset_Info_XML

            foreach (var warning in datasetXmlMerger.AcqTimeWarnings)
            {
                mMsg = AppendToComment(mMsg, warning);
            }

            LogError(mMsg);

            returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg, "Large gap between acq times: " + datasetXmlMerger.AcqTimeWarnings.FirstOrDefault());
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Looks for a zip file matching "0_R*X*.zip"
        /// </summary>
        /// <param name="datasetDirectory">Dataset directory</param>
        /// <returns>Returns the file name if found, otherwise an empty string</returns>
        private static string CheckForBrukerImagingZipFiles(DirectoryInfo datasetDirectory)
        {
            var zipFiles = datasetDirectory.GetFiles("0_R*X*.zip");

            if (zipFiles.Length > 0)
            {
                return zipFiles[0].Name;
            }

            return string.Empty;
        }

        /// <summary>
        /// Merge the dataset info defined in cachedDatasetInfoXml
        /// If cachedDatasetInfoXml contains just one item, simply return it
        /// </summary>
        /// <param name="datasetXmlMerger">DatasetInfo XML Merger</param>
        /// <param name="cachedDatasetInfoXml">List of cached DatasetInfo XML</param>
        /// <param name="totalDatasetFilesOrDirectories">Total number of dataset files or input directories</param>
        /// <returns>Merged DatasetInfo XML</returns>
        private string CombineDatasetInfoXML(
            DatasetInfoXmlMerger datasetXmlMerger,
            List<string> cachedDatasetInfoXml,
            int totalDatasetFilesOrDirectories)
        {
            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (cachedDatasetInfoXml.Count == 0)
                return string.Empty;

            if (cachedDatasetInfoXml.Count == 1 && totalDatasetFilesOrDirectories <= 1)
            {
                return cachedDatasetInfoXml.First();
            }

            return datasetXmlMerger.CombineDatasetInfoXML(mDataset, cachedDatasetInfoXml, totalDatasetFilesOrDirectories);
        }

        /// <summary>
        /// Check whether the experiment for this dataset has a labelling value defined
        /// If it doesn't, or if it is Unknown or None, examine the dataset name
        /// </summary>
        /// <param name="experimentName">Experiment name</param>
        /// <param name="sampleLabelling">Sample labelling</param>
        private void ConfigureMinimumMzValidation(string experimentName, string sampleLabelling)
        {
            mMsFileScannerOptions.MS2MzMin = 0;

            if (mTaskParams.GetParam("SkipMinimumMzValidation", false))
            {
                // Skip minimum m/z validation
                return;
            }

            if (!string.IsNullOrEmpty(sampleLabelling))
            {
                // Check whether this label has a reporter ion minimum m/z value defined
                // If it does, instruct the MSFileInfoScanner to validate that all MS/MS spectra
                // have a scan range that starts below the minimum reporter ion m/z

                var reporterIonMzMinText = mTaskParams.GetParam("Meta_Experiment_labelling_reporter_mz_min", string.Empty);

                if (!string.IsNullOrEmpty(reporterIonMzMinText) && float.TryParse(reporterIonMzMinText, out var reporterIonMzMin))
                {
                    mMsFileScannerOptions.MS2MzMin = (int)Math.Floor(reporterIonMzMin);
                    LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since experiment {1} has {2} labelling",
                        mMsFileScannerOptions.MS2MzMin, experimentName, sampleLabelling);
                }
            }

            if (mMsFileScannerOptions.MS2MzMin > 0)
            {
                return;
            }

            // People sometimes forget to define the sample label for the experiment, but put iTRAQ or TMT in the dataset name
            // Check for this

            // Match names like:
            // _iTRAQ_
            // _iTRAQ4_
            // _iTRAQ8_
            // _iTRAQ-8_
            var iTRAQMatcher = new Regex("_iTRAQ[0-9-]*_", RegexOptions.IgnoreCase);

            // Match names like:
            // _TMT_
            // _TMT6_
            // _TMT10_
            // _TMT-10_
            var tmtMatcher = new Regex("_TMT[0-9-]*_", RegexOptions.IgnoreCase);

            var iTRAQMatch = iTRAQMatcher.Match(mDataset);

            if (iTRAQMatch.Success)
            {
                mMsFileScannerOptions.MS2MzMin = 113;
                LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                    mMsFileScannerOptions.MS2MzMin, iTRAQMatch.Value);
            }

            var tmtMatch = tmtMatcher.Match(mDataset);

            if (tmtMatch.Success && mDataset.IndexOf("NanoPOTS_mTIFF_TMT_24_01", StringComparison.OrdinalIgnoreCase) < 0)
            {
                mMsFileScannerOptions.MS2MzMin = 126;
                LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                    mMsFileScannerOptions.MS2MzMin, tmtMatch.Value);
            }
        }

        /// <summary>
        /// Determine the appropriate output directory path
        /// If we are only processing one dataset file or directory, the output directory path is simply outputPathBase
        /// Otherwise, it is based on the current file or directory being processed (datasetFileOrDirectory)
        /// nextSubdirectorySuffix is used to avoid directory name conflicts
        /// </summary>
        /// <param name="outputPathBase"></param>
        /// <param name="datasetFileOrDirectory"></param>
        /// <param name="totalDatasetFilesOrDirectories"></param>
        /// <param name="outputDirectoryNames"></param>
        /// <param name="nextSubdirectorySuffix">Input/output parameter</param>
        /// <returns>Full path to the output directory to use for the current file or directory being processed</returns>
        private string ConstructOutputDirectoryPath(
            string outputPathBase,
            string datasetFileOrDirectory,
            int totalDatasetFilesOrDirectories,
            // ReSharper disable once SuggestBaseTypeForParameter
            List<string> outputDirectoryNames,
            ref int nextSubdirectorySuffix)
        {
            string currentOutputDirectory;

            if (totalDatasetFilesOrDirectories > 1)
            {
                var subDirectory = Path.GetFileNameWithoutExtension(datasetFileOrDirectory);

                if (string.IsNullOrWhiteSpace(subDirectory))
                {
                    var subdirectoryToUse = mDataset + "_" + nextSubdirectorySuffix;

                    while (outputDirectoryNames.Contains(subdirectoryToUse))
                    {
                        nextSubdirectorySuffix++;
                        subdirectoryToUse = mDataset + "_" + nextSubdirectorySuffix;
                    }

                    currentOutputDirectory = Path.Combine(outputPathBase, subdirectoryToUse);
                    outputDirectoryNames.Add(subdirectoryToUse);
                }
                else
                {
                    var subdirectoryToUse = subDirectory;

                    while (outputDirectoryNames.Contains(subdirectoryToUse))
                    {
                        subdirectoryToUse = subDirectory + "_" + nextSubdirectorySuffix;
                        nextSubdirectorySuffix++;
                    }

                    currentOutputDirectory = Path.Combine(outputPathBase, subdirectoryToUse);
                    outputDirectoryNames.Add(subdirectoryToUse);
                }
            }
            else
            {
                currentOutputDirectory = outputPathBase;
            }
            return currentOutputDirectory;
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        private bool CopyRemoteDirectoryToLocal(DirectoryInfo remoteDirectory, string localDirectoryPath, ToolReturnData returnData)
        {
            try
            {
                mFileTools.CopyDirectory(remoteDirectory.FullName, localDirectoryPath, true);
                return true;
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = string.Format("Error copying remote .D directory to local working directory ({0} to {1})",
                    remoteDirectory.FullName, mWorkDir);

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                LogError(returnData.CloseoutMsg, ex);
                return false;
            }
        }

        /// <summary>
        /// Look for .d directories below datasetDirectory
        /// Add them to list fileOrDirectoryNames
        /// </summary>
        /// <param name="datasetDirectory">Dataset directory to examine</param>
        /// <param name="initialFileOrDirectoryName">Optional file to look for if a .mcf file is not found</param>
        /// <param name="fileOrDirectoryRelativePaths">List to append .d directories to (calling function must initialize)</param>
        private void FindDotDDirectories(
            DirectoryInfo datasetDirectory,
            string initialFileOrDirectoryName,
            // ReSharper disable once SuggestBaseTypeForParameter
            List<string> fileOrDirectoryRelativePaths)
        {
            var looseMatchDotD = mTaskParams.GetParam("LooseMatchDotD", false);

            var searchOption = looseMatchDotD ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            // Example log messages:
            // Looking for .d directories in the dataset directory: dataset_directory_path
            // Looking for .d directories in the dataset directory and its subdirectories: dataset_directory_path

            LogDebug("Looking for .d directories in the dataset directory{0}: {1}",
                looseMatchDotD ? " and its subdirectories" : string.Empty,
                datasetDirectory.FullName);

            var dotDDirectories = datasetDirectory.GetDirectories("*.d", searchOption);

            if (dotDDirectories.Length == 0)
            {
                return;
            }

            var alternateFileName = string.IsNullOrWhiteSpace(initialFileOrDirectoryName)
                ? string.Empty
                : Path.GetFileName(initialFileOrDirectoryName);

            // Look for a .mcf file in each of the .d directories
            foreach (var dotDDirectory in dotDDirectories)
            {
                LogDebug("Looking for .mcf files in: {0}", dotDDirectory.FullName);

                var mcfFileExists = LookForMcfFileInDotDDirectory(dotDDirectory, out _);

                if (!mcfFileExists)
                {
                    if (string.IsNullOrWhiteSpace(alternateFileName))
                        continue;

                    LogDebug("Looking for file {0} in: {1}", alternateFileName, dotDDirectory.FullName);
                    var matchingFiles = dotDDirectory.GetFiles(alternateFileName);

                    if (matchingFiles.Length == 0)
                        continue;

                    LogDebug("Found file {0}", matchingFiles[0].FullName);
                }

                var relativeDirectoryPath = looseMatchDotD
                    ? dotDDirectory.FullName.Substring(datasetDirectory.FullName.Length + 1)
                    : dotDDirectory.Name;

                if (!fileOrDirectoryRelativePaths.Contains(relativeDirectoryPath))
                {
                    LogDebug("Adding directory {0} to fileOrDirectoryRelativePaths", relativeDirectoryPath);
                    fileOrDirectoryRelativePaths.Add(relativeDirectoryPath);
                }
            }
        }

        /// <summary>
        /// Returns the file or directory name list for the specified dataset based on dataset type
        /// Most datasets only have a single dataset file or directory, but FTICR_Imaging datasets
        /// can have multiple .d directories below a parent directory
        /// </summary>
        /// <remarks>
        /// Returns UNKNOWN_FILE_TYPE for instrument types that are not recognized.
        /// Returns INVALID_FILE_TYPE for instruments for which we do not run MSFileInfoScanner
        /// </remarks>
        /// <returns>List of data file or directory names; empty list if not found</returns>
        private List<string> GetDataFileOrDirectoryName(
            string inputDirectory,
            out QCPlottingModes qcPlotMode,
            out DataFormat rawDataType,
            out InstrumentClass instrumentClass,
            out bool brukerDotDBaf)
        {
            bool isFile;

            qcPlotMode = QCPlottingModes.AllPlots;
            rawDataType = DataFormat.Unknown;
            brukerDotDBaf = false;

            // Determine the Instrument Class and RawDataType
            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var rawDataTypeName = mTaskParams.GetParam("RawDataType", "UnknownRawDataType");

            instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);

            if (instrumentClass == InstrumentClass.Unknown)
            {
                mMsg = "Instrument class not recognized: " + instClassName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            rawDataType = InstrumentClassInfo.GetDataFormat(rawDataTypeName);

            if (rawDataType == DataFormat.Unknown)
            {
                mMsg = "RawDataType not recognized: " + rawDataTypeName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            if (instrumentClass == InstrumentClass.IMS_Agilent_TOF_DotD)
            {
                // Operate directly on the .D file instead of the UIMF
                rawDataType = DataFormat.AgilentDFolder;
            }

            var datasetDirectory = new DirectoryInfo(inputDirectory);
            string fileOrDirectoryName;

            // Get the expected file name based on the dataset type
            switch (rawDataType)
            {
                case DataFormat.ThermoRawFile:
                    // LTQ_2, LTQ_4, etc.
                    // LTQ_Orb_1, LTQ_Orb_2, etc.
                    // VOrbiETD01, VOrbiETD02, etc.
                    // TSQ_3
                    // Thermo_GC_MS_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_RAW_EXTENSION;
                    isFile = true;
                    break;

                case DataFormat.ZippedSFolders:
                    // 9T_FTICR, 11T_FTICR_B, and 12T_FTICR
                    fileOrDirectoryName = "analysis.baf";
                    isFile = true;
                    break;

                case DataFormat.BrukerFTFolder:
                    // 12T_FTICR_B, 15T_FTICR, 9T_FTICR_B
                    // Also, Bruker_FT_IonTrap01, which is Bruker_Amazon_Ion_Trap
                    // 12T_FTICR_Imaging and 15T_FTICR_Imaging datasets with instrument class BrukerMALDI_Imaging_V2 will also have bruker_ft format;
                    // however, instead of an analysis.baf file, they might have a .mcf file and a ser file

                    isFile = true;

                    if (instrumentClass == InstrumentClass.Bruker_Amazon_Ion_Trap)
                    {
                        fileOrDirectoryName = Path.Combine(mDataset + InstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
                    }
                    else
                    {
                        fileOrDirectoryName = Path.Combine(mDataset + InstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
                        brukerDotDBaf = true;
                    }

                    if (!File.Exists(Path.Combine(datasetDirectory.FullName, fileOrDirectoryName)))
                    {
                        var zipFileName = CheckForBrukerImagingZipFiles(datasetDirectory);

                        if (!string.IsNullOrWhiteSpace(string.Empty))
                        {
                            fileOrDirectoryName = zipFileName;
                        }
                    }

                    break;

                case DataFormat.UIMF:
                    if (IsAgilentIMSDataset(datasetDirectory))
                    {
                        // IMS08_AcQTOF05, IMS09_AgQToF06, IMS11_AgQTOF08
                        fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                        isFile = false;
                    }
                    else
                    {
                        // IMS05 (retired), SLIM TOF/QTOF instruments
                        fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_UIMF_EXTENSION;
                        isFile = true;
                    }

                    break;

                case DataFormat.SciexWiffFile:
                    // QTrap01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_WIFF_EXTENSION;
                    isFile = true;
                    break;

                case DataFormat.AgilentDFolder:
                    // Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;

                    if (instrumentClass == InstrumentClass.PrepHPLC)
                    {
                        LogMessage("Skipping MSFileInfoScanner since PrepHPLC dataset");
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case DataFormat.BrukerMALDIImaging:
                    // bruker_maldi_imaging: 12T_FTICR_Imaging, 15T_FTICR_Imaging, and BrukerTOF_Imaging_01
                    // Find the name of the first zip file

                    fileOrDirectoryName = CheckForBrukerImagingZipFiles(datasetDirectory);
                    qcPlotMode = QCPlottingModes.NoPlots;
                    isFile = true;

                    if (string.IsNullOrEmpty(fileOrDirectoryName))
                    {
                        mMsg = "Did not find any 0_R*.zip files in the dataset directory";
                        LogWarning("PluginMain.GetDataFileOrDirectoryName: " + mMsg);
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case DataFormat.BrukerTOFBaf:
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;
                    break;

                case DataFormat.BrukerTOFTdf:
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;
                    break;

                case DataFormat.BrukerTOFTsf:
                    // This is used by Bruker timsTOF Flex MALDI Imaging datasets; treat it like BrukerFTFolder, which is used for FTICR imaging
                    // Note: this can be a .tsf or a .tdf file, so check both
                    var fileNameBase = Path.Combine(mDataset + InstrumentClassInfo.DOT_D_EXTENSION, "analysis");
                    fileOrDirectoryName = fileNameBase + (File.Exists(Path.Combine(datasetDirectory.FullName, fileNameBase + ".tdf")) ? ".tdf" : ".tsf");

                    isFile = true;
                    brukerDotDBaf = true;
                    break;

                case DataFormat.IlluminaFolder:
                    // fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_TXT_GZ_EXTENSION;
                    // isFile = true;

                    LogMessage("Skipping MSFileInfoScanner since Illumina RNASeq dataset");
                    return new List<string> { INVALID_FILE_TYPE };

                case DataFormat.ShimadzuQGDFile:
                    // 	Shimadzu_GC_MS_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_QGD_EXTENSION;
                    qcPlotMode = QCPlottingModes.NoPlots;
                    isFile = true;
                    break;

                case DataFormat.WatersRawFolder:
                    // 	SynaptG2_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_RAW_EXTENSION;
                    isFile = false;
                    break;

                case DataFormat.LCMSNetLCMethod:
                    // 	LCMSNet .lcmethod file: no data to do anything with anyway
                    //fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_LCMETHOD_EXTENSION;
                    //isFile = true;

                    LogMessage("Skipping MSFileInfoScanner since LCMSNet .lcmethod file");
                    return new List<string> { INVALID_FILE_TYPE };

                default:
                    // Other instruments; do not process them with MSFileInfoScanner

                    // Excluded instruments include:
                    // dot_wiff_files (AgilentQStarWiffFile): AgTOF02
                    // bruker_maldi_spot (BrukerMALDISpot): BrukerTOF_01
                    // dot_qgd_files (Shimadzu_GC): Shimadzu_GC_MS_01

                    mMsg = "Data type " + rawDataType + " not recognized";
                    LogWarning("PluginMain.GetDataFileOrDirectoryName: " + mMsg);
                    return new List<string> { INVALID_FILE_TYPE };
            }

            // Test to verify the file (or directory) exists
            var fileOrDirectoryPath = Path.Combine(datasetDirectory.FullName, fileOrDirectoryName);

            if (isFile)
            {
                // Expecting to match a file
                if (File.Exists(fileOrDirectoryPath))
                {
                    // File exists
                    // Even if it is in a .d directory, we will only examine this file
                    return new List<string> { fileOrDirectoryName };
                }

                // File not found; check for alternative files or directories
                // This function also looks for .d directories
                var fileOrDirectoryRelativePaths = LookForAlternateFileOrDirectory(datasetDirectory, fileOrDirectoryName);

                if (fileOrDirectoryRelativePaths.Count > 0)
                {
                    return fileOrDirectoryRelativePaths;
                }

                mMsg = "PluginMain.GetDataFileOrDirectoryName: File " + fileOrDirectoryPath + " not found";
                LogError(mMsg);

                mMsg = "File " + fileOrDirectoryPath + " not found";

                if (brukerDotDBaf && datasetDirectory.GetDirectories("*.D", SearchOption.AllDirectories).Length > 0)
                {
                    mMsg = string.Format(
                        "analysis.baf/.tsf/.tdf not found in the expected .d directory; to include all .D subdirectories, use " +
                        "Call cap.add_update_task_parameter ({0}, 'StepParameters', 'LooseMatchDotD', 'true');",
                        mJob);

                    LogWarning(mMsg);
                }

                return new List<string>();
            }

            // Expecting to match a directory
            if (Directory.Exists(fileOrDirectoryPath))
            {
                if (!string.Equals(Path.GetExtension(fileOrDirectoryName), ".D", StringComparison.OrdinalIgnoreCase))
                {
                    // Directory exists, and it does not end in .D
                    return new List<string> { fileOrDirectoryName };
                }

                // Look for other .d directories
                var fileOrDirectoryRelativePaths = new List<string> { fileOrDirectoryName };

                FindDotDDirectories(datasetDirectory, fileOrDirectoryName, fileOrDirectoryRelativePaths);

                return fileOrDirectoryRelativePaths;
            }

            mMsg = "PluginMain.GetDataFileOrDirectoryName; directory not found: " + fileOrDirectoryPath;
            LogError(mMsg);
            mMsg = "Directory not found: " + fileOrDirectoryPath;

            return new List<string>();
        }

        /// <summary>
        /// Construct the full path to the MSFileInfoScanner.exe
        /// </summary>
        private string GetMSFileInfoScannerExePath()
        {
            var msFileInfoScannerDir = mMgrParams.GetParam("MSFileInfoScannerDir", string.Empty);

            if (string.IsNullOrEmpty(msFileInfoScannerDir))
            {
                return string.Empty;
            }

            return Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.exe");
        }

        private static void LoadDatasetInfoXML(
            string currentOutputDirectory,
            // ReSharper disable SuggestBaseTypeForParameter
            List<string> cachedDatasetInfoXML,
            List<FileInfo> datasetInfoFilesToRename)
            // ReSharper restore SuggestBaseTypeForParameter
        {
            var outputDirectory = new DirectoryInfo(currentOutputDirectory);

            var filesToRename = new List<FileInfo>();

            foreach (var datasetInfoFile in outputDirectory.GetFiles("*_DatasetInfo.xml"))
            {
                using var streamReader = new StreamReader(new FileStream(datasetInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var datasetInfoXML = streamReader.ReadToEnd();

                cachedDatasetInfoXML.Add(datasetInfoXML);

                filesToRename.Add(datasetInfoFile);
            }

            foreach (var datasetInfoFile in filesToRename)
            {
                var renamedFile = new FileInfo(datasetInfoFile.FullName + STORED_FILE_SUFFIX);

                if (renamedFile.Exists)
                {
                    renamedFile.Delete();
                }

                datasetInfoFile.MoveTo(renamedFile.FullName);
                datasetInfoFilesToRename.Add(datasetInfoFile);
            }
        }

        /// <summary>
        /// A dataset file was not found
        /// Look for alternate dataset files, or look for .d directories
        /// </summary>
        /// <param name="datasetDirectory"></param>
        /// <param name="initialFileOrDirectoryName"></param>
        /// <returns>List of alternative files or directories that were found</returns>
        private List<string> LookForAlternateFileOrDirectory(DirectoryInfo datasetDirectory, string initialFileOrDirectoryName)
        {
            // Look for .mgf, .mzXML, and .mzML

            foreach (var altExtension in new List<string> { "mgf", "mzXML", "mzML" })
            {
                var dataFileNamePathAlt = Path.ChangeExtension(initialFileOrDirectoryName, altExtension);

                if (File.Exists(dataFileNamePathAlt))
                {
                    mMsg = "Data file not found, but ." + altExtension + " file exists";
                    LogMessage(mMsg);
                    return new List<string> { INVALID_FILE_TYPE };
                }
            }

            // Look for dataset directories
            var primaryDotDDirectory = new DirectoryInfo(Path.Combine(datasetDirectory.FullName, mDataset + InstrumentClassInfo.DOT_D_EXTENSION));

            var fileOrDirectoryNames = new List<string>();

            if (primaryDotDDirectory.Exists)
            {
                // Look for a .mcf file in the .d directory
                var mcfFileExists = LookForMcfFileInDotDDirectory(primaryDotDDirectory, out var dotDDirectoryName);

                if (mcfFileExists)
                {
                    fileOrDirectoryNames.Add(dotDDirectoryName);
                }
            }

            // With instrument class BrukerMALDI_Imaging_V2 (e.g. 15T_FTICR_Imaging) we allow multiple .d directories to be captured
            // Look for additional directories now
            FindDotDDirectories(datasetDirectory, initialFileOrDirectoryName, fileOrDirectoryNames);

            return fileOrDirectoryNames;
        }

        /// <summary>
        /// Look for any .mcf file in a Bruker .d directory
        /// </summary>
        /// <param name="dotDDirectory">Bruker .d directory to search</param>
        /// <param name="dotDDirectoryName">Output: name of the .d directory</param>
        /// <returns>True if a .mcf file is found</returns>
        private static bool LookForMcfFileInDotDDirectory(DirectoryInfo dotDDirectory, out string dotDDirectoryName)
        {
            var mcfFilesWithExtras = dotDDirectory.GetFiles("*" + BRUKER_MCF_FILE_EXTENSION);

            // The "*.mcf" sent to .GetFiles() matches both .mcf and .mcf_idx files
            // Filter the list to only include the .mcf files
            var mcfFiles = (from item in mcfFilesWithExtras
                            where item.Extension.Equals(BRUKER_MCF_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase)
                            select item).ToList();

            if (mcfFiles.Count > 0)
            {
                // Find the largest .mcf file (not .mcf_idx file)
                var largestMcf = (from item in mcfFiles
                                  orderby item.Length descending
                                  select item).First();

                if (largestMcf.Length > 0)
                {
                    dotDDirectoryName = dotDDirectory.Name;
                    return true;
                }
            }

            dotDDirectoryName = string.Empty;
            return false;
        }

        private void ParseStatusFile()
        {
            if (File.Exists(mStatusFilePath))
            {
                mStatusFileTools.ReadStatusFile(mStatusFilePath, out mProcessingStatus);
                return;
            }

            var runtimeMinutes = DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes;

            if (runtimeMinutes < 2)
                return;

            LogWarning("Status file not found ({0:F0} minutes elapsed): {1}", runtimeMinutes, mStatusFilePath);
        }

        private bool PostDatasetInfoXml(string datasetInfoXML, out string errorMessage)
        {
            var postCount = 0;

            // This connection string points to the DMS database on prismdb2 (previously, DMS_Capture on Gigasax)
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var applicationName = string.Format("{0}_DatasetInfo", mMgrParams.ManagerName);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

            var datasetID = mTaskParams.GetParam("Dataset_ID", 0);

            var successPosting = false;

            while (postCount <= 2)
            {
                successPosting = PostDatasetInfoXml(
                    datasetID, datasetInfoXML, connectionStringToUse, MS_FILE_SCANNER_DS_INFO_SP);

                if (successPosting)
                {
                    break;
                }

                // If the error message contains the text "timeout expired" then try again, up to 2 times
                if (mMsg.IndexOf("timeout expired", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    break;
                }

                System.Threading.Thread.Sleep(1500);
                postCount++;
            }

            if (successPosting)
            {
                errorMessage = string.Empty;
                return true;
            }

            // Either a non-zero error code was returned, or an error event was received
            errorMessage = "Error posting dataset info XML";
            return false;
        }

        /// <summary>
        /// Post the dataset info in datasetInfoXML to the database, using the specified connection string and stored procedure
        /// This version assumes the stored procedure takes DatasetID as the first parameter
        /// </summary>
        /// <param name="datasetID">Dataset ID to send to the stored procedure</param>
        /// <param name="datasetInfoXML">Database info XML</param>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="storedProcedureName">Stored procedure</param>
        /// <returns>True if success; false if failure</returns>
        private bool PostDatasetInfoXml(int datasetID, string datasetInfoXML, string connectionString, string storedProcedureName)
        {
            bool success;

            try
            {
                LogMessage("  Posting DatasetInfo XML to the database (using Dataset ID " + datasetID + ")");

                // We need to remove the encoding line from datasetInfoXML before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-16" standalone="yes"?>

                var startIndex = datasetInfoXML.IndexOf("?>", StringComparison.Ordinal);
                string dsInfoXMLClean;

                if (startIndex > 0)
                {
                    dsInfoXMLClean = datasetInfoXML.Substring(startIndex + 2).Trim();
                }
                else
                {
                    dsInfoXMLClean = datasetInfoXML;
                }

                // Call stored procedure storedProcedure using connection string connectionString

                if (string.IsNullOrEmpty(connectionString))
                {
                    LogError("Connection string not defined; unable to post the dataset info to the database");
                    return false;
                }

                if (string.IsNullOrEmpty(storedProcedureName))
                {
                    storedProcedureName = "cache_dataset_info_xml";
                }

                var applicationName = "MSFileInfoScanner_DatasetID_" + datasetID;

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse);
                RegisterEvents(dbTools);

                var cmd = dbTools.CreateCommand(storedProcedureName, CommandType.StoredProcedure);

                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@DatasetID", SqlType.Int).Value = datasetID;
                dbTools.AddParameter(cmd, "@DatasetInfoXML", SqlType.XML).Value = dsInfoXMLClean;

                var result = dbTools.ExecuteSP(cmd);

                if (result == DbUtilsConstants.RET_VAL_OK)
                {
                    // No errors
                    success = true;
                }
                else
                {
                    LogError("Error calling stored procedure to store dataset info XML, return code = " + returnParam.Value.CastDBVal<string>());
                    success = false;
                }
            }
            catch (Exception ex)
            {
                LogError("Error calling stored procedure to store dataset info XML", ex);
                success = false;
            }

            return success;
        }

        private void ProcessMultiDatasetInfoScannerResults(
            string outputPathBase,
            DatasetInfoXmlMerger datasetXmlMerger,
            string datasetInfoXML,
            IEnumerable<string> outputDirectoryNames)
        {
            var combinedDatasetInfoFilename = mDataset + "_Combined_DatasetInfo.xml";

            try
            {
                // Write the combined XML to disk
                var combinedXmlFilePath = Path.Combine(outputPathBase, combinedDatasetInfoFilename);

                using var xmlWriter = new StreamWriter(new FileStream(combinedXmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                xmlWriter.WriteLine(datasetInfoXML);
            }
            catch (Exception ex)
            {
                var msg = "Exception creating the combined _DatasetInfo.xml file for " + mDataset;

                if (System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase) &&
                    !Environment.UserName.StartsWith("svc", StringComparison.OrdinalIgnoreCase))
                {
                    LogWarning(msg + ": " + ex.Message);

                    var localFilePath = Path.Combine(mWorkDir, combinedDatasetInfoFilename);

                    try
                    {
                        using var xmlWriter = new StreamWriter(new FileStream(localFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                        xmlWriter.WriteLine(datasetInfoXML);
                    }
                    catch (Exception ex2)
                    {
                        var msg2 = "Exception creating the combined _DatasetInfo.xml file in directory " + mWorkDir;
                        LogWarning(msg2 + ": " + ex2.Message);
                    }

                    return;
                }

                LogError(msg, ex);
            }

            try
            {
                var pngMatcher = new Regex(@"""(?<Filename>[^""]+\.png)""", RegexOptions.IgnoreCase);

                // Create an index.html file that shows all the plots in the subdirectories
                var indexHtmlFilePath = Path.Combine(outputPathBase, "index.html");

                using var htmlWriter = new StreamWriter(new FileStream(indexHtmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // ReSharper disable once StringLiteralTypo
                htmlWriter.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 3.2//EN\">");
                htmlWriter.WriteLine("<html>");
                htmlWriter.WriteLine("<head>");
                htmlWriter.WriteLine("  <title>" + mDataset + "</title>");
                htmlWriter.WriteLine("  <style>");
                htmlWriter.WriteLine("     table.DataTable {");
                htmlWriter.WriteLine("       margin: 10px 5px 5px 5px;");
                htmlWriter.WriteLine("       border: 1px solid black;");
                htmlWriter.WriteLine("       border-collapse: collapse;");
                htmlWriter.WriteLine("     }");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("     th.DataHead {");
                htmlWriter.WriteLine("       border: 1px solid black;");
                htmlWriter.WriteLine("       padding: 2px 4px 2px 2px; ");
                htmlWriter.WriteLine("       text-align: left;");
                htmlWriter.WriteLine("     }");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("     td.DataCell {");
                htmlWriter.WriteLine("       border: 1px solid black;");
                htmlWriter.WriteLine("       padding: 2px 4px 2px 4px;");
                htmlWriter.WriteLine("     }");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("     td.DataCentered {");
                htmlWriter.WriteLine("       border: 1px solid black;");
                htmlWriter.WriteLine("       padding: 2px 4px 2px 4px;");
                htmlWriter.WriteLine("       text-align: center;");
                htmlWriter.WriteLine("     }");
                htmlWriter.WriteLine("  </style>");
                htmlWriter.WriteLine("</head>");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("<body>");
                htmlWriter.WriteLine("  <h2>" + mDataset + "</h2>");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("  <table>");

                foreach (var subdirectoryName in outputDirectoryNames)
                {
                    var subdirectoryInfo = new DirectoryInfo(Path.Combine(outputPathBase, subdirectoryName));
                    var htmlFiles = subdirectoryInfo.GetFiles("index.html");

                    if (htmlFiles.Length == 0)
                    {
                        continue;
                    }

                    using var htmlReader = new StreamReader(new FileStream(htmlFiles[0].FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    var processingTable = false;
                    var htmlToAppend = new List<string>();
                    var htmlHasImageInfo = false;
                    var rowDepth = 0;

                    while (!htmlReader.EndOfStream)
                    {
                        var dataLine = htmlReader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        var lineTrimmed = dataLine.Trim();

                        if (processingTable)
                        {
                            var rowAdded = false;

                            // Look for png files
                            if (pngMatcher.IsMatch(dataLine))
                            {
                                // Match found; prepend the subdirectory name
                                dataLine = pngMatcher.Replace(dataLine, '"' + subdirectoryInfo.Name + "/${Filename}" + '"');
                                htmlHasImageInfo = true;
                            }

                            if (lineTrimmed.StartsWith("<tr>"))
                            {
                                // Start of a table row
                                rowDepth++;

                                htmlToAppend.Add(dataLine);
                                rowAdded = true;
                            }

                            if (lineTrimmed.EndsWith("</tr>"))
                            {
                                // End of a table row
                                if (!rowAdded)
                                {
                                    htmlToAppend.Add(dataLine);
                                    rowAdded = true;
                                }
                                rowDepth--;

                                if (rowDepth == 0 && htmlToAppend.Count > 0)
                                {
                                    if (htmlHasImageInfo)
                                    {
                                        // Write this set of rows out to the new index.html file
                                        foreach (var outRow in htmlToAppend)
                                        {
                                            htmlWriter.WriteLine(outRow);
                                        }
                                    }
                                    htmlToAppend.Clear();
                                    htmlHasImageInfo = false;
                                }
                            }

                            if (rowDepth == 0 && lineTrimmed.StartsWith("</table>"))
                            {
                                // Done processing the main table
                                // Stop parsing this file
                                break;
                            }

                            if (!rowAdded)
                            {
                                htmlToAppend.Add(dataLine);
                            }
                        }
                        else if (dataLine.Trim().StartsWith("<table>"))
                        {
                            processingTable = true;
                        }
                    }
                }

                // Add the combined stats
                htmlWriter.WriteLine("    <tr>");
                htmlWriter.WriteLine("        <td colspan=\"3\"><hr/></td>");
                htmlWriter.WriteLine("    </tr>");
                htmlWriter.WriteLine("    <tr>");
                htmlWriter.WriteLine("      <td>&nbsp;</td>");
                htmlWriter.WriteLine("      <td align=\"right\">Combined Stats:</td>");
                // ReSharper disable once StringLiteralTypo
                htmlWriter.WriteLine("      <td valign=\"middle\">");
                htmlWriter.WriteLine("        <table class=\"DataTable\">");
                htmlWriter.WriteLine("          <tr><th class=\"DataHead\">Scan Type</th><th class=\"DataHead\">Scan Count</th><th class=\"DataHead\">Scan Filter Text</th></tr>");

                foreach (var item in datasetXmlMerger.ScanTypes)
                {
                    var scanTypeName = item.Key.Key;
                    var scanCount = item.Value;

                    htmlWriter.WriteLine("          <tr><td class=\"DataCell\">" + scanTypeName + "</td><td class=\"DataCentered\">" + scanCount + "</td><td class=\"DataCell\"</td></tr>");
                }

                htmlWriter.WriteLine("        </table>");
                htmlWriter.WriteLine("      </td>");
                htmlWriter.WriteLine("    </tr>");
                htmlWriter.WriteLine("    <tr>");
                htmlWriter.WriteLine("        <td colspan=\"3\"><hr/></td>");
                htmlWriter.WriteLine("    </tr>");

                // Add a link to the Dataset detail report
                htmlWriter.WriteLine("    <tr>");
                htmlWriter.WriteLine("      <td>&nbsp;</td>");
                htmlWriter.WriteLine("      <td align=\"center\">DMS <a href=\"http://dms2.pnl.gov/dataset/show/" + mDataset + "\">Dataset Detail Report</a></td>");
                htmlWriter.WriteLine("      <td align=\"center\"><a href=\"" + combinedDatasetInfoFilename + "\">Dataset Info XML file</a></td>");
                htmlWriter.WriteLine("    </tr>");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("  </table>");
                htmlWriter.WriteLine();
                htmlWriter.WriteLine("</body>");
                htmlWriter.WriteLine("</html>");
                htmlWriter.WriteLine();
            }
            catch (Exception ex)
            {
                LogError("Exception creating the combined _DatasetInfo.xml file for " + mDataset, ex);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = CTMUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Info plugin");
                return false;
            }

            // Lookup the version of the dataset info plugin
            var pluginPath = Path.Combine(appDirectory, "DatasetInfoPlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);

            if (!success)
            {
                return false;
            }

            // Lookup the version of the MSFileInfoScanner Exe
            var msFileInfoScannerExePath = GetMSFileInfoScannerExePath();

            if (!string.IsNullOrEmpty(msFileInfoScannerExePath))
            {
                success = StoreToolVersionInfoOneFile(ref toolVersionInfo, msFileInfoScannerExePath);

                if (!success)
                {
                    return false;
                }
            }

            // Lookup the version of the UIMFLibrary DLL
            var uimfLibraryPath = Path.Combine(appDirectory, "UIMFLibrary.dll");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, uimfLibraryPath);

            if (!success)
            {
                return false;
            }

            // Store path to CaptureToolPlugin.dll and MSFileInfoScanner.exe in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(pluginPath)
            };

            if (!string.IsNullOrEmpty(msFileInfoScannerExePath))
            {
                toolFiles.Add(new FileInfo(msFileInfoScannerExePath));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        private static bool ValidateQCGraphics(string currentOutputDirectory, bool primaryFileOrDirectoryProcessed, ToolReturnData returnData)
        {
            // Make sure at least one of the PNG files created by MSFileInfoScanner is over 10 KB in size
            var outputDirectory = new DirectoryInfo(currentOutputDirectory);

            if (!outputDirectory.Exists)
            {
                var errMsg = "Output directory not found: " + currentOutputDirectory;

                if (primaryFileOrDirectoryProcessed)
                {
                    LogWarning(errMsg);
                    returnData.EvalMsg = AppendToComment(returnData.EvalMsg, errMsg);
                    return false;
                }

                LogError(errMsg);
                returnData.CloseoutMsg = errMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var pngFiles = outputDirectory.GetFiles("*.png");

            if (pngFiles.Length == 0)
            {
                const string errMsg = "No PNG files were created";

                if (primaryFileOrDirectoryProcessed)
                {
                    LogWarning(errMsg);
                    returnData.EvalMsg = AppendToComment(returnData.EvalMsg, errMsg);
                    return false;
                }

                LogError(errMsg);
                returnData.CloseoutMsg = errMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            const int minimumGraphicsSizeKB = 10;

            var validGraphics = false;

            foreach (var pngFile in pngFiles)
            {
                if (pngFile.Length >= 1024 * minimumGraphicsSizeKB)
                {
                    validGraphics = true;
                    break;
                }
            }

            if (validGraphics)
            {
                return true;
            }

            var errMsg2 = string.Format("All {0} PNG files created by MSFileInfoScanner are less than {1} KB and likely blank graphics", pngFiles.Length, minimumGraphicsSizeKB);

            if (primaryFileOrDirectoryProcessed)
            {
                LogWarning(errMsg2);
                returnData.EvalMsg = AppendToComment(returnData.EvalMsg, errMsg2);
                return false;
            }

            LogError(errMsg2);
            returnData.CloseoutMsg = errMsg2;
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return false;
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

        private void DetachCmdRunnerEvents(RunDosProgram cmdRunner)
        {
            try
            {
                if (cmdRunner != null)
                {
                    cmdRunner.LoopWaiting -= CmdRunner_LoopWaiting;
                    cmdRunner.Timeout -= CmdRunner_Timeout;
                }
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        private void CmdRunner_Timeout()
        {
            LogError("CmdRunner timeout reported (MSFileInfoScanner has been running for over {0} hours)", MAX_INFO_SCANNER_RUNTIME_HOURS);
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdate).TotalSeconds < 30)
            {
                return;
            }

            mLastProgressUpdate = DateTime.UtcNow;

            ParseStatusFile();

            var overallProgress = ComputeIncrementalProgress(
                mCurrentDatasetNumber,
                mTotalDatasetFilesOrDirectories,
                mProcessingStatus.ProgressPercentComplete);

            mStatusTools.UpdateAndWrite(EnumTaskStatusDetail.Running_Tool, overallProgress);

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds < 300)
                return;

            mLastStatusUpdate = DateTime.UtcNow;

            LogDebug("{0} running; {1:F1} minutes elapsed, {2:F1}% complete",
                "MSFileInfoScanner",
                DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes,
                mProcessingStatus.ProgressPercentComplete);
        }
    }
}
