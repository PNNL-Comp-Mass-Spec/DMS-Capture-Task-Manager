//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//*********************************************************************************************************

using CaptureTaskManager;
using MSFileInfoScannerInterfaces;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PRISMDatabaseUtils;

namespace DatasetInfoPlugin
{
    /// <summary>
    /// Dataset Info plugin: generates QC graphics using MSFileInfoScanner
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: acq, Bruker, fid, href, html, Illumina, labelling, maldi, mgf
        // Ignore Spelling: online, png, prepend, qgd, ser, Shimadzu, svc, Synapt, wiff

        private const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";

        private const string UNKNOWN_FILE_TYPE = "Unknown File Type";

        private const string INVALID_FILE_TYPE = "Invalid File Type";

        private const bool IGNORE_BRUKER_BAF_ERRORS = false;

        private enum QCPlottingModes
        {
            NoPlots = 0,          // Use this if SkipPlots is enabled
            BpiAndTicOnly = 1,
            AllPlots = 2
        }

        private iMSFileInfoScanner mMsFileScanner;

        private string mMsg;

        private bool mErrorOccurred;

        private int mErrorCountLoadDataForScan;

        private int mErrorCountUnknownScanFilterFormat;

        private int mFailedScanCount;

        private DateTime mProcessingStartTime;

        private DateTime mLastProgressUpdate;

        private DateTime mLastStatusUpdate;

        private int mStatusUpdateIntervalMinutes;

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

        private iMSFileInfoScanner LoadMSFileInfoScanner(string msFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.MSFileInfoScanner";

            iMSFileInfoScanner msFileInfoScanner = null;
            string msg;

            try
            {
                if (!File.Exists(msFileInfoScannerDLLPath))
                {
                    msg = "DLL not found: " + msFileInfoScannerDLLPath;
                    LogError(msg);
                }
                else
                {
                    var newInstance = LoadObject(MsDataFileReaderClass, msFileInfoScannerDLLPath);
                    if (newInstance != null)
                    {
                        msFileInfoScanner = (iMSFileInfoScanner)newInstance;
                        msg = "Loaded MSFileInfoScanner from " + msFileInfoScannerDLLPath;
                        LogMessage(msg);
                    }
                    else
                    {
                        LogError("LoadObject was unable to load class {0} from {1}; it returned null",
                            MsDataFileReaderClass, msFileInfoScannerDLLPath);
                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
                LogError(msg, ex);
            }

            return msFileInfoScanner;
        }

        private object LoadObject(string className, string dllFilePath)
        {
            try
            {
                // Dynamically load the specified class from dllFilePath
                var assembly = Assembly.LoadFrom(dllFilePath);
                var dllType = assembly.GetType(className, false, true);
                return Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                LogError(ex, "Exception loading DLL {0}: {1}", dllFilePath, ex.Message);
                return null;
            }
        }

        /// <summary>
        /// Initializes MSFileInfoScanner
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            LogDebug("Starting PluginMain.Setup()");

            base.Setup(mgrParams, taskParams, statusTools);

            var msFileInfoScannerDLLPath = GetMSFileInfoScannerDLLPath();
            if (string.IsNullOrEmpty(msFileInfoScannerDLLPath))
            {
                throw new NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");
            }

            if (!File.Exists(msFileInfoScannerDLLPath))
            {
                throw new FileNotFoundException("File Not Found: " + msFileInfoScannerDLLPath);
            }

            mProcessingStartTime = DateTime.UtcNow;
            mLastProgressUpdate = DateTime.UtcNow;
            mLastStatusUpdate = DateTime.UtcNow;
            mStatusUpdateIntervalMinutes = 5;

            // Initialize the MSFileScanner class
            mMsFileScanner = LoadMSFileInfoScanner(msFileInfoScannerDLLPath);
            RegisterEvents(mMsFileScanner);

            // Add custom error and warning handlers
            UnregisterEventHandler(mMsFileScanner, BaseLogger.LogLevels.ERROR);
            UnregisterEventHandler(mMsFileScanner, BaseLogger.LogLevels.WARN);

            mMsFileScanner.ErrorEvent += MsFileScanner_ErrorEvent;
            mMsFileScanner.WarningEvent += MsFileScanner_WarningEvent;

            // Monitor progress reported by MSFileInfoScanner
            mMsFileScanner.ProgressUpdate += ProgressUpdateHandler;

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
            var datasetDirectory = mTaskParams.GetParam(mTaskParams.HasParam("Directory") ? "Directory" : "Folder");

            var datasetDirectoryPath = Path.Combine(sourceDirectoryBase, datasetDirectory);
            var outputPathBase = Path.Combine(datasetDirectoryPath, "QC");

            // Set up the params for the MS file scanner
            mMsFileScanner.Options.PostResultsToDMS = false;
            mMsFileScanner.Options.SaveTICAndBPIPlots = mTaskParams.GetParam("SaveTICAndBPIPlots", true);
            mMsFileScanner.Options.SaveLCMS2DPlots = mTaskParams.GetParam("SaveLCMS2DPlots", true);
            mMsFileScanner.Options.ComputeOverallQualityScores = mTaskParams.GetParam("ComputeOverallQualityScores", false);
            mMsFileScanner.Options.CreateDatasetInfoFile = mTaskParams.GetParam("CreateDatasetInfoFile", true);

            mMsFileScanner.LCMS2DPlotOptions.MZResolution = mTaskParams.GetParam("LCMS2DPlotMZResolution", LCMSDataPlotterOptions.DEFAULT_MZ_RESOLUTION);
            mMsFileScanner.LCMS2DPlotOptions.MaxPointsToPlot = mTaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", LCMSDataPlotterOptions.DEFAULT_MAX_POINTS_TO_PLOT);
            mMsFileScanner.LCMS2DPlotOptions.MinPointsPerSpectrum = mTaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", LCMSDataPlotterOptions.DEFAULT_MIN_POINTS_PER_SPECTRUM);
            mMsFileScanner.LCMS2DPlotOptions.MinIntensity = mTaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
            mMsFileScanner.LCMS2DPlotOptions.OverviewPlotDivisor = mTaskParams.GetParam("LCMS2DOverviewPlotDivisor", LCMSDataPlotterOptions.DEFAULT_LCMS2D_OVERVIEW_PLOT_DIVISOR);

            var sampleLabelling = mTaskParams.GetParam("Meta_Experiment_sample_labelling", string.Empty);
            var experimentName = mTaskParams.GetParam("Meta_Experiment_Num", string.Empty);
            ConfigureMinimumMzValidation(mMsFileScanner, experimentName, sampleLabelling);

            mMsFileScanner.Options.DatasetID = mDatasetID;
            mMsFileScanner.Options.CheckCentroidingStatus = true;
            mMsFileScanner.Options.PlotWithPython = true;

            // Get the input file or directory name (or names)
            var fileOrDirectoryRelativePaths = GetDataFileOrDirectoryName(
                datasetDirectoryPath,
                out var qcPlotMode,
                out var rawDataType,
                out var instrumentClass,
                out var brukerDotDBaf);

            if (fileOrDirectoryRelativePaths.Count > 0 && fileOrDirectoryRelativePaths.First() == UNKNOWN_FILE_TYPE)
            {
                // Raw_Data_Type not recognized
                returnData.CloseoutMsg = mMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            if (fileOrDirectoryRelativePaths.Count > 0 && fileOrDirectoryRelativePaths.First() == INVALID_FILE_TYPE)
            {
                // DS quality test not implemented for this file type
                returnData.CloseoutMsg = string.Empty;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                returnData.EvalMsg = "Dataset info test not implemented for data type " + InstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " + InstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                return returnData;
            }

            if (fileOrDirectoryRelativePaths.Count == 0 || string.IsNullOrEmpty(fileOrDirectoryRelativePaths.First()))
            {
                // There was a problem with getting the file name; Details reported by called method
                returnData.CloseoutMsg = mMsg;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            if (mTaskParams.GetParam("SkipPlots", false))
            {
                // To add parameter SkipPlots for job 123456, use:
                // Exec AddUpdateJobParameter 123456, 'JobParameters', 'SkipPlots', 'true'
                qcPlotMode = QCPlottingModes.NoPlots;
            }

            switch (qcPlotMode)
            {
                case QCPlottingModes.NoPlots:
                    // Do not create any plots
                    mMsFileScanner.Options.SaveTICAndBPIPlots = false;
                    mMsFileScanner.Options.SaveLCMS2DPlots = false;
                    break;

                case QCPlottingModes.BpiAndTicOnly:
                    // Only create the BPI and TIC plots
                    mMsFileScanner.Options.SaveTICAndBPIPlots = true;
                    mMsFileScanner.Options.SaveLCMS2DPlots = false;
                    break;
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

            // Call the file scanner DLL
            // Typically only call it once, but for Bruker datasets with multiple .D directories, we'll call it once for each .D directory

            mErrorOccurred = false;
            mMsg = string.Empty;

            var cachedDatasetInfoXML = new List<string>();
            var outputDirectoryNames = new List<string>();
            var primaryFileOrDirectoryProcessed = false;
            var nextSubdirectorySuffix = 1;

            foreach (var datasetFileOrDirectory in fileOrDirectoryRelativePaths)
            {
                mFailedScanCount = 0;

                var remoteFileOrDirectoryPath = Path.Combine(datasetDirectoryPath, datasetFileOrDirectory);

                string pathToProcess;
                bool fileCopiedLocally;

                var datasetFile = new FileInfo(remoteFileOrDirectoryPath);
                if (datasetFile.Exists && string.Equals(datasetFile.Extension, InstrumentClassInfo.DOT_RAW_EXTENSION,
                                                        StringComparison.OrdinalIgnoreCase))
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
                }
                else
                {
                    pathToProcess = remoteFileOrDirectoryPath;
                    fileCopiedLocally = false;
                }

                var currentOutputDirectory = ConstructOutputDirectoryPath(
                    outputPathBase, datasetFileOrDirectory, fileOrDirectoryRelativePaths.Count,
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

                bool successProcessing;
                mErrorCountLoadDataForScan = 0;
                mErrorCountUnknownScanFilterFormat = 0;

                try
                {
                    var equivalentCommandLineArguments = GetEquivalentCommandLineArgs(mMsFileScanner, pathToProcess);

                    LogMessage("Processing with MSFileInfoScanner.dll; equivalent executable call: " +
                               "MSFileInfoScanner.exe {0}",
                        equivalentCommandLineArguments);

                    successProcessing = mMsFileScanner.ProcessMSFileOrDirectory(pathToProcess, currentOutputDirectory);
                }
                catch (Exception ex)
                {
                    LogError("Error running MSFileInfoScanner", ex);
                    successProcessing = false;
                }

                if (mErrorOccurred)
                {
                    successProcessing = false;
                }

                if (mMsFileScanner.ErrorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.ThermoRawFileReaderError)
                {
                    // Call to .OpenRawFile failed
                    mMsg = "Error running MSFileInfoScanner: Call to .OpenRawFile failed";
                    LogError(mMsg);
                    successProcessing = false;
                }
                else if (mMsFileScanner.ErrorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.DatasetHasNoSpectra)
                {
                    // Dataset has no spectra
                    mMsg = "Error running MSFileInfoScanner: Dataset has no spectra (ScanCount = 0)";
                    LogError(mMsg);
                    successProcessing = false;
                }

                if (fileCopiedLocally)
                {
                    mFileTools.DeleteFileWithRetry(new FileInfo(pathToProcess), 2, out _);
                }

                var mzMinValidationError = mMsFileScanner.ErrorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.MS2MzMinValidationError;
                if (mzMinValidationError)
                {
                    mMsg = mMsFileScanner.GetErrorMessage();
                    successProcessing = false;
                }

                if (mMsFileScanner.ErrorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.MS2MzMinValidationWarning)
                {
                    var warningMsg = mMsFileScanner.GetErrorMessage();
                    returnData.EvalMsg = AppendToComment(returnData.EvalMsg, "MS2MzMinValidationWarning: " + warningMsg);
                }

                if (successProcessing && qcPlotMode == QCPlottingModes.AllPlots)
                {
                    var validQcGraphics = ValidateQCGraphics(currentOutputDirectory, primaryFileOrDirectoryProcessed, returnData);
                    if (returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return returnData;
                    }

                    if (!validQcGraphics)
                    {
                        continue;
                    }
                }

                if (successProcessing)
                {
                    cachedDatasetInfoXML.Add(mMsFileScanner.DatasetInfoXML);
                    primaryFileOrDirectoryProcessed = true;
                    continue;
                }

                // Either a non-zero error code was returned, or an error event was received

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                // ReSharper disable HeuristicUnreachableCode
                if (brukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
                {
                    // 12T_FTICR_B datasets (with .D directories and analysis.baf and/or fid files) sometimes work with MSFileInfoScanner, and sometimes don't
                    // The problem is that ProteoWizard doesn't support certain forms of these datasets
                    // In particular, small datasets (lasting just a few seconds) don't work

                    returnData.CloseoutMsg = string.Empty;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    returnData.EvalMsg = "MSFileInfoScanner error for data type " +
                                     InstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " +
                                     InstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    return returnData;
                }
                // ReSharper restore HeuristicUnreachableCode

                if (primaryFileOrDirectoryProcessed)
                {
                    // MSFileInfoScanner already processed the primary file or directory
                    // Mention this failure in the EvalMsg but still return success
                    returnData.EvalMsg = AppendToComment(returnData.EvalMsg,
                                                     "ProcessMSFileOrFolder returned false for " + datasetFileOrDirectory);
                }
                else
                {
                    if (string.IsNullOrEmpty(mMsg))
                    {
                        mMsg = "ProcessMSFileOrFolder returned false. Message = " +
                                mMsFileScanner.GetErrorMessage() +
                                " returnData code = " + (int)mMsFileScanner.ErrorCode;
                    }

                    LogError(mMsg);

                    returnData.CloseoutMsg = mMsg;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    if (!string.IsNullOrWhiteSpace(mMsFileScanner.DatasetInfoXML))
                    {
                        cachedDatasetInfoXML.Add(mMsFileScanner.DatasetInfoXML);
                    }

                    if (mzMinValidationError)
                    {
                        var jobParamNote = string.Format(
                            "To ignore this error, use Exec AddUpdateJobParameter {0}, 'JobParameters', 'SkipMinimumMzValidation', 'true'",
                            mJob);

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
            var datasetInfoXML = CombineDatasetInfoXML(datasetXmlMerger, cachedDatasetInfoXML);

            if (cachedDatasetInfoXML.Count > 1)
            {
                ProcessMultiDatasetInfoScannerResults(outputPathBase, datasetXmlMerger, datasetInfoXML, outputDirectoryNames);
            }

            // Check for dataset acq time gap warnings
            // If any are found, CloseoutMsg is updated
            AcqTimeWarningsReported(datasetXmlMerger, returnData);

            // Call SP CacheDatasetInfoXML to store datasetInfoXML in table T_Dataset_Info_XML
            var success = PostDatasetInfoXml(datasetInfoXML, out var errorMessage);
            if (!success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg, errorMessage);
            }

            if (!useLocalOutputDirectory || returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return returnData;
            }

            // Set this to failed since we stored the QC graphics in the local working directory instead of on the storage server
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            returnData.CloseoutMsg = AppendToComment(returnData.CloseoutMsg,
                                                  "QC graphics were saved locally for debugging purposes; " +
                                                  "need to run this job step with a manager that has write access to the storage server");

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

        private bool PostDatasetInfoXml(string datasetInfoXML, out string errorMessage)
        {
            var postCount = 0;
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var applicationName = string.Format("{0}_DatasetInfo", mMgrParams.ManagerName);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

            var datasetID = mTaskParams.GetParam("Dataset_ID", 0);

            var successPosting = false;

            while (postCount <= 2)
            {
                successPosting = mMsFileScanner.PostDatasetInfoUseDatasetID(
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

            iMSFileInfoScanner.MSFileScannerErrorCodes errorCode;
            if (successPosting)
            {
                errorCode = iMSFileInfoScanner.MSFileScannerErrorCodes.NoError;
            }
            else
            {
                errorCode = mMsFileScanner.ErrorCode;
                mMsg = "Error posting dataset info XML. Message = " +
                        mMsFileScanner.GetErrorMessage() + " returnData code = " + (int)mMsFileScanner.ErrorCode;
                LogError(mMsg);
            }

            if (errorCode == iMSFileInfoScanner.MSFileScannerErrorCodes.NoError)
            {
                // Everything went wonderfully
                errorMessage = string.Empty;
                return true;
            }

            // Either a non-zero error code was returned, or an error event was received
            errorMessage = "Error posting dataset info XML";
            return false;
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
                LogError("Exception creating the combined _DatasetInfo.xml file for " + mDataset, ex);
            }

            try
            {
                var pngMatcher = new Regex(@"""(?<Filename>[^""]+\.png)""", RegexOptions.IgnoreCase);

                // Create an index.html file that shows all of the plots in the subdirectories
                var indexHtmlFilePath = Path.Combine(outputPathBase, "index.html");

                using var htmlWriter = new StreamWriter(new FileStream(indexHtmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // ReSharper disable once StringLiteralTypo
                htmlWriter.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 3.2//EN\">");
                htmlWriter.WriteLine("<html>");
                htmlWriter.WriteLine("<head>");
                htmlWriter.WriteLine("  <title>" + mDataset + "</title>");
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
                htmlWriter.WriteLine("        <table border=\"1\">");
                htmlWriter.WriteLine("          <tr><th>Scan Type</th><th>Scan Count</th><th>Scan Filter Text</th></tr>");

                foreach (var item in datasetXmlMerger.ScanTypes)
                {
                    var scanTypeName = item.Key.Key;
                    var scanCount = item.Value;

                    htmlWriter.WriteLine("          <tr><td>" + scanTypeName + "</td><td align=\"center\">" + scanCount + "</td><td></td></tr>");
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
        /// Merge the dataset info defined in cachedDatasetInfoXml
        /// If cachedDatasetInfoXml contains just one item, simply return it
        /// </summary>
        /// <param name="datasetXmlMerger">DatasetInfo XML Merger</param>
        /// <param name="cachedDatasetInfoXml">List of cached DatasetInfo XML</param>
        /// <returns>Merged DatasetInfo XML</returns>
        private string CombineDatasetInfoXML(DatasetInfoXmlMerger datasetXmlMerger, List<string> cachedDatasetInfoXml)
        {
            if (cachedDatasetInfoXml.Count == 1)
            {
                return cachedDatasetInfoXml.First();
            }

            var combinedXML = datasetXmlMerger.CombineDatasetInfoXML(mDataset, cachedDatasetInfoXml);

            return combinedXML;
        }

        /// <summary>
        /// Looks for a zip file matching "0_R*X*.zip"
        /// </summary>
        /// <param name="datasetDirectory">Dataset directory</param>
        /// <returns>Returns the file name if found, otherwise an empty string</returns>
        private string CheckForBrukerImagingZipFiles(DirectoryInfo datasetDirectory)
        {
            var zipFiles = datasetDirectory.GetFiles("0_R*X*.zip");

            if (zipFiles.Length > 0)
            {
                return zipFiles[0].Name;
            }

            return string.Empty;
        }

        /// <summary>
        /// Check whether the experiment for this dataset has labelling value defined
        /// If it doesn't, or if it is Unknown or None, examine the dataset name
        /// </summary>
        /// <param name="msFileInfoScanner"></param>
        /// <param name="experimentName"></param>
        /// <param name="sampleLabelling"></param>
        private void ConfigureMinimumMzValidation(iMSFileInfoScanner msFileInfoScanner, string experimentName, string sampleLabelling)
        {
            mMsFileScanner.Options.MS2MzMin = 0;

            if (mTaskParams.GetParam("SkipMinimumMzValidation", false))
            {
                // Skip minimum m/z validation
                return;
            }

            if (!string.IsNullOrEmpty(sampleLabelling))
            {
                // Check whether this label has a reporter ion minimum m/z value defined
                // If it does, instruct the MSFileInfoScanner to validate that all of the MS/MS spectra
                // have a scan range that starts below the minimum reporter ion m/z

                var reporterIonMzMinText = mTaskParams.GetParam("Meta_Experiment_labelling_reporter_mz_min", string.Empty);
                if (!string.IsNullOrEmpty(reporterIonMzMinText))
                {
                    if (float.TryParse(reporterIonMzMinText, out var reporterIonMzMin))
                    {
                        msFileInfoScanner.Options.MS2MzMin = (int)Math.Floor(reporterIonMzMin);
                        LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since experiment {1} has {2} labelling",
                            msFileInfoScanner.Options.MS2MzMin, experimentName, sampleLabelling);
                    }
                }
            }

            if (msFileInfoScanner.Options.MS2MzMin > 0)
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
                msFileInfoScanner.Options.MS2MzMin = 113;
                LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                    msFileInfoScanner.Options.MS2MzMin, iTRAQMatch.Value);
            }

            var tmtMatch = tmtMatcher.Match(mDataset);
            if (tmtMatch.Success)
            {
                msFileInfoScanner.Options.MS2MzMin = 126;
                LogMessage("Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                    msFileInfoScanner.Options.MS2MzMin, tmtMatch.Value);
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
            ICollection<string> outputDirectoryNames,
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

        /// <summary>
        /// Look for .D directories below datasetDirectory
        /// Add them to list fileOrDirectoryNames
        /// </summary>
        /// <param name="datasetDirectory">Dataset directory to examine</param>
        /// <param name="fileOrDirectoryRelativePaths">List to append .D directories to (calling function must initialize)</param>
        private void FindDotDDirectories(DirectoryInfo datasetDirectory, ICollection<string> fileOrDirectoryRelativePaths)
        {
            var looseMatchDotD = mTaskParams.GetParam("LooseMatchDotD", false);

            var searchOption = looseMatchDotD ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            var dotDDirectories = datasetDirectory.GetDirectories("*.d", searchOption);
            if (dotDDirectories.Length == 0)
            {
                return;
            }

            // Look for a .mcf file in each of the .D directories
            foreach (var dotDDirectory in dotDDirectories)
            {
                var mcfFileExists = LookForMcfFileInDotDDirectory(dotDDirectory, out _);
                if (!mcfFileExists)
                {
                    continue;
                }

                string relativeDirectoryPath;
                if (looseMatchDotD)
                {
                    relativeDirectoryPath = dotDDirectory.FullName.Substring(datasetDirectory.FullName.Length + 1);
                }
                else
                {
                    relativeDirectoryPath = dotDDirectory.Name;
                }

                if (!fileOrDirectoryRelativePaths.Contains(relativeDirectoryPath))
                {
                    fileOrDirectoryRelativePaths.Add(relativeDirectoryPath);
                }
            }
        }

        /// <summary>
        /// Returns the file or directory name list for the specified dataset based on dataset type
        /// Most datasets only have a single dataset file or directory, but FTICR_Imaging datasets
        /// can have multiple .D directories below a parent directory
        /// </summary>
        /// <remarks>
        /// Returns UNKNOWN_FILE_TYPE for instrument types that are not recognized.
        /// Returns INVALID_FILE_TYPE for instruments for which we do not run MSFileInfoScanner
        /// </remarks>
        /// <returns>List of data file or directory names; empty list if not found</returns>
        private List<string> GetDataFileOrDirectoryName(
            string inputDirectory,
            out QCPlottingModes qcPlotMode,
            out InstrumentClassInfo.RawDataType rawDataType,
            out InstrumentClassInfo.InstrumentClass instrumentClass,
            out bool brukerDotDBaf)
        {
            bool isFile;

            qcPlotMode = QCPlottingModes.AllPlots;
            rawDataType = InstrumentClassInfo.RawDataType.Unknown;
            brukerDotDBaf = false;

            // Determine the Instrument Class and RawDataType
            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var rawDataTypeName = mTaskParams.GetParam("RawDataType", "UnknownRawDataType");

            instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);
            if (instrumentClass == InstrumentClassInfo.InstrumentClass.Unknown)
            {
                mMsg = "Instrument class not recognized: " + instClassName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            rawDataType = InstrumentClassInfo.GetRawDataType(rawDataTypeName);
            if (rawDataType == InstrumentClassInfo.RawDataType.Unknown)
            {
                mMsg = "RawDataType not recognized: " + rawDataTypeName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.IMS_Agilent_TOF_DotD)
            {
                // Operate directly on the .D file instead of the UIMF
                rawDataType = InstrumentClassInfo.RawDataType.AgilentDFolder;
            }

            var datasetDirectory = new DirectoryInfo(inputDirectory);
            string fileOrDirectoryName;

            // Get the expected file name based on the dataset type
            switch (rawDataType)
            {
                case InstrumentClassInfo.RawDataType.ThermoRawFile:
                    // LTQ_2, LTQ_4, etc.
                    // LTQ_Orb_1, LTQ_Orb_2, etc.
                    // VOrbiETD01, VOrbiETD02, etc.
                    // TSQ_3
                    // Thermo_GC_MS_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_RAW_EXTENSION;
                    isFile = true;
                    break;

                case InstrumentClassInfo.RawDataType.ZippedSFolders:
                    // 9T_FTICR, 11T_FTICR_B, and 12T_FTICR
                    fileOrDirectoryName = "analysis.baf";
                    isFile = true;
                    break;

                case InstrumentClassInfo.RawDataType.BrukerFTFolder:
                    // 12T_FTICR_B, 15T_FTICR, 9T_FTICR_B
                    // Also, Bruker_FT_IonTrap01, which is Bruker_Amazon_Ion_Trap
                    // 12T_FTICR_Imaging and 15T_FTICR_Imaging datasets with instrument class BrukerMALDI_Imaging_V2 will also have bruker_ft format;
                    // however, instead of an analysis.baf file, they might have a .mcf file and a ser file

                    isFile = true;
                    if (instrumentClass == InstrumentClassInfo.InstrumentClass.Bruker_Amazon_Ion_Trap)
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

                case InstrumentClassInfo.RawDataType.UIMF:
                    // IMS09_AgQToF06, IMS10_AgQTOF07
                    if (IsAgilentIMSDataset(datasetDirectory))
                    {
                        fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_UIMF_EXTENSION;
                        isFile = true;
                    }
                    else
                    {
                        fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                        isFile = false;
                    }

                    break;

                case InstrumentClassInfo.RawDataType.SciexWiffFile:
                    // QTrap01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_WIFF_EXTENSION;
                    isFile = true;
                    break;

                case InstrumentClassInfo.RawDataType.AgilentDFolder:
                    // Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;

                    if (instrumentClass == InstrumentClassInfo.InstrumentClass.PrepHPLC)
                    {
                        LogMessage("Skipping MSFileInfoScanner since PrepHPLC dataset");
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case InstrumentClassInfo.RawDataType.BrukerMALDIImaging:
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

                case InstrumentClassInfo.RawDataType.BrukerTOFBaf:
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;
                    break;

                case InstrumentClassInfo.RawDataType.BrukerTOFTdf:
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_D_EXTENSION;
                    qcPlotMode = QCPlottingModes.BpiAndTicOnly;
                    isFile = false;
                    break;

                case InstrumentClassInfo.RawDataType.IlluminaFolder:
                    // fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_TXT_GZ_EXTENSION;
                    // isFile = true;

                    LogMessage("Skipping MSFileInfoScanner since Illumina RNASeq dataset");
                    return new List<string> { INVALID_FILE_TYPE };

                case InstrumentClassInfo.RawDataType.ShimadzuQGDFile:
                    // 	Shimadzu_GC_MS_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_QGD_EXTENSION;
                    qcPlotMode = QCPlottingModes.NoPlots;
                    isFile = true;
                    break;

                case InstrumentClassInfo.RawDataType.WatersRawFolder:
                    // 	SynaptG2_01
                    fileOrDirectoryName = mDataset + InstrumentClassInfo.DOT_RAW_EXTENSION;
                    isFile = false;
                    break;

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
                    // Even if it is in a .D directory, we will only examine this file
                    return new List<string> { fileOrDirectoryName };
                }

                // File not found; check for alternative files or directories
                // This function also looks for .D directories
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
                        "analysis.baf not found in the expected .D directory; to include all .D subdirectories, use " +
                        "Exec AddUpdateJobParameter {0}, 'StepParameters', 'LooseMatchDotD', 'true'",
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

                // Look for other .D directories
                var fileOrDirectoryRelativePaths = new List<string> { fileOrDirectoryName };
                FindDotDDirectories(datasetDirectory, fileOrDirectoryRelativePaths);

                return fileOrDirectoryRelativePaths;
            }

            mMsg = "PluginMain.GetDataFileOrDirectoryName; directory not found: " + fileOrDirectoryPath;
            LogError(mMsg);
            mMsg = "Directory not found: " + fileOrDirectoryPath;

            return new List<string>();
        }

        /// <summary>
        /// Convert the options defined in msFileScanner to the
        /// equivalent list of command line arguments that could be sent to MSFileInfoScanner.exe
        /// </summary>
        /// <param name="msFileScanner"></param>
        /// <param name="datasetFileName"></param>
        private string GetEquivalentCommandLineArgs(iMSFileInfoScanner msFileScanner, string datasetFileName)
        {
            var argumentList = new List<string>();

            if (!msFileScanner.Options.SaveTICAndBPIPlots)
            {
                argumentList.Add("/NoTIC");
            }

            if (msFileScanner.Options.SaveLCMS2DPlots)
            {
                argumentList.Add("/LC");
            }

            if (msFileScanner.Options.CreateDatasetInfoFile)
            {
                argumentList.Add("/DI");
            }

            if (msFileScanner.Options.CheckCentroidingStatus)
            {
                argumentList.Add("/CC");
            }

            if (msFileScanner.Options.PlotWithPython)
            {
                argumentList.Add("/Python");
            }

            if (msFileScanner.Options.CheckCentroidingStatus)
            {
                argumentList.Add("/SS");
            }

            if (msFileScanner.Options.ComputeOverallQualityScores)
            {
                argumentList.Add("/QS");
            }

            if (msFileScanner.Options.MS2MzMin > 0)
            {
                argumentList.Add(string.Format("/MS2MzMin:{0:F1}", msFileScanner.Options.MS2MzMin));
            }

            if (msFileScanner.Options.ScanStart > 0)
            {
                argumentList.Add("/ScanStart:" + msFileScanner.Options.ScanStart);
            }

            if (msFileScanner.Options.ScanEnd > 0)
            {
                argumentList.Add("/ScanEnd:" + msFileScanner.Options.ScanEnd);
            }

            if (msFileScanner.Options.ShowDebugInfo)
            {
                argumentList.Add("/Debug");
            }

            return datasetFileName + " " + string.Join(" ", argumentList);
        }

        /// <summary>
        /// Construct the full path to the MSFileInfoScanner.DLL
        /// </summary>
        private string GetMSFileInfoScannerDLLPath()
        {
            var msFileInfoScannerDir = mMgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
            if (string.IsNullOrEmpty(msFileInfoScannerDir))
            {
                return string.Empty;
            }

            return Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");
        }

        /// <summary>
        /// A dataset file was not found
        /// Look for alternate dataset files, or look for .D directories
        /// </summary>
        /// <param name="datasetDirectory"></param>
        /// <param name="initialFileOrDirectoryName"></param>
        /// <returns>List of alternative files or directories that were found</returns>
        private List<string> LookForAlternateFileOrDirectory(DirectoryInfo datasetDirectory, string initialFileOrDirectoryName)
        {
            // Alternate extensions to find
            var alternateExtensions = new List<string> { "mgf", "mzXML", "mzML" };

            foreach (var altExtension in alternateExtensions)
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
                // Look for a .mcf file in the .D directory
                var mcfFileExists = LookForMcfFileInDotDDirectory(primaryDotDDirectory, out var dotDDirectoryName);
                if (mcfFileExists)
                {
                    fileOrDirectoryNames.Add(dotDDirectoryName);
                }
            }

            // With instrument class BrukerMALDI_Imaging_V2 (e.g. 15T_FTICR_Imaging) we allow multiple .D directories to be captured
            // Look for additional directories now
            FindDotDDirectories(datasetDirectory, fileOrDirectoryNames);

            return fileOrDirectoryNames;
        }

        /// <summary>
        /// Look for any .mcf file in a Bruker .D directory
        /// </summary>
        /// <param name="dotDDirectory"></param>
        /// <param name="dotDDirectoryName">Output: name of the .D directory</param>
        /// <returns>True if a .mcf file is found</returns>
        private bool LookForMcfFileInDotDDirectory(DirectoryInfo dotDDirectory, out string dotDDirectoryName)
        {
            long mcfFileSizeBytes = 0;
            dotDDirectoryName = string.Empty;

            foreach (var mcfFile in dotDDirectory.GetFiles("*.mcf"))
            {
                // Return the .mcf file that is the largest
                if (mcfFile.Length > mcfFileSizeBytes)
                {
                    mcfFileSizeBytes = mcfFile.Length;
                    dotDDirectoryName = dotDDirectory.Name;
                }
            }

            return mcfFileSizeBytes > 0;
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

            // Lookup the version of the MSFileInfoScanner DLL
            var msFileInfoScannerDLLPath = GetMSFileInfoScannerDLLPath();
            if (!string.IsNullOrEmpty(msFileInfoScannerDLLPath))
            {
                success = StoreToolVersionInfoOneFile(ref toolVersionInfo, msFileInfoScannerDLLPath);
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

            // Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(pluginPath)
            };

            if (!string.IsNullOrEmpty(msFileInfoScannerDLLPath))
            {
                toolFiles.Add(new FileInfo(msFileInfoScannerDLLPath));
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

        private bool ValidateQCGraphics(string currentOutputDirectory, bool primaryFileOrDirectoryProcessed, ToolReturnData returnData)
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

        /// <summary>
        /// Handles an error event from MS file scanner
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="ex"></param>
        private void MsFileScanner_ErrorEvent(string message, Exception ex)
        {
            var errorMsg = "Error running MSFileInfoScanner: " + message;

            if (ex != null)
            {
                errorMsg += "; " + ex.Message;
            }

            if (message.StartsWith("Error using ProteoWizard reader"))
            {
                // This is not always a critical error; log it as a warning
                LogWarning(errorMsg);
            }
            else
            {
                mErrorOccurred = true;

                // Limit the logging of messages similar to:

                if (message.StartsWith("Unable to load data for scan"))
                {
                    mErrorCountLoadDataForScan++;
                    if (mErrorCountLoadDataForScan > 25 && mErrorCountLoadDataForScan % 1000 != 0)
                    {
                        ConsoleMsgUtils.ShowWarning("Error running MSFileInfoScanner: " + message);
                        return;
                    }
                }
                else if (message.StartsWith("Unknown format for Scan Filter"))
                {
                    mErrorCountUnknownScanFilterFormat++;
                    if (mErrorCountUnknownScanFilterFormat > 25 && mErrorCountUnknownScanFilterFormat % 1000 != 0)
                    {
                        ConsoleMsgUtils.ShowWarning("Error running MSFileInfoScanner: " + message);
                        return;
                    }
                }

                // Message often contains long paths; check for this and shorten them.
                // For example, switch
                // from: \\proto-6\LTQ_Orb_1\2015_4\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw
                // to:   QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw

                // Match text of the form         \\server\share\directory<anything>DatasetName.Extension
                // or                             C:\directory<anything>DatasetName.Extension
                var filenameMatcher = new Regex(@"([a-z]:|\\\\[^\\]+\\[^\\]+)\\[^\\]+.+(" + mDataset + @"\.[a-z0-9]+)", RegexOptions.IgnoreCase);

                if (filenameMatcher.IsMatch(message))
                {
                    mMsg = "Error running MSFileInfoScanner: " + filenameMatcher.Replace(message, "$2");
                }
                else
                {
                    mMsg = "Error running MSFileInfoScanner: " + message;
                }

                LogError(errorMsg);
            }
        }

        /// <summary>
        /// Handles a warning event from MS file scanner
        /// </summary>
        /// <param name="message"></param>
        private void MsFileScanner_WarningEvent(string message)
        {
            if (message.StartsWith("Unable to load data for scan"))
            {
                mFailedScanCount++;

                if (mFailedScanCount < 10)
                {
                    LogWarning(message);
                }
                else if (
                    mFailedScanCount < 100 && mFailedScanCount % 25 == 0 ||
                    mFailedScanCount < 1000 && mFailedScanCount % 250 == 0 ||
                    mFailedScanCount % 500 == 0)
                {
                    LogWarning("Unable to load data for {0} spectra", mFailedScanCount);
                }
            }
            else
            {
                LogWarning(message);
            }
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdate).TotalSeconds < 30)
            {
                return;
            }

            mLastProgressUpdate = DateTime.UtcNow;

            mStatusTools.UpdateAndWrite(EnumTaskStatusDetail.Running_Tool, percentComplete);

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalMinutes >= mStatusUpdateIntervalMinutes)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                LogMessage("MSFileInfoScanner running; {0:F1} minutes elapsed",
                    DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes);

                // Increment mStatusUpdateIntervalMinutes by 1 minute every time the status is logged, up to a maximum of 30 minutes
                if (mStatusUpdateIntervalMinutes < 30)
                {
                    mStatusUpdateIntervalMinutes++;
                }
            }
        }
    }
}
