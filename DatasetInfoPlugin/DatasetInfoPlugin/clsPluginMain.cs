//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Reflection;
using CaptureTaskManager;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using MSFileInfoScannerInterfaces;
using PRISM;
using PRISM.Logging;

namespace DatasetInfoPlugin
{
    /// <summary>
    /// Dataset Info plugin: generates QC graphics using MSFileInfoScanner
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Constants"

        private const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";

        private const string UNKNOWN_FILE_TYPE = "Unknown File Type";

        private const string INVALID_FILE_TYPE = "Invalid File Type";

        private const bool IGNORE_BRUKER_BAF_ERRORS = false;

        #endregion

        #region "Class-wide variables"

        iMSFileInfoScanner mMsFileScanner;

        string mMsg;

        bool mErrOccurred;

        private int mFailedScanCount;

        private DateTime mProcessingStartTime;

        private DateTime mLastProgressUpdate;

        private DateTime mLastStatusUpdate;

        private int mStatusUpdateIntervalMinutes;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset info step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            // Note that Debug messages are logged if mDebugLevel == 5

            var msg = "Starting DatasetInfoPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            var retData = base.RunTool();
            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return retData;

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                retData.CloseoutMsg = "Error determining tool version info";
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            msg = "Running DatasetInfo on dataset '" + mDataset + "'";
            LogMessage(msg);

            retData = RunMsFileInfoScanner();

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return retData;
        }

        private iMSFileInfoScanner LoadMSFileInfoScanner(string msFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

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
            object obj = null;
            try
            {
                // Dynamically load the specified class from dllFilePath
                var assembly = Assembly.LoadFrom(dllFilePath);
                var dllType = assembly.GetType(className, false, true);
                obj = Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                var msg = "Exception loading DLL " + dllFilePath + ": " + ex.Message;
                LogError(msg, ex);
            }
            return obj;
        }

        /// <summary>
        /// Initializes MSFileInfoScanner
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting clsPluginMain.Setup()";
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            var msFileInfoScannerDLLPath = GetMSFileInfoScannerDLLPath();
            if (string.IsNullOrEmpty(msFileInfoScannerDLLPath))
                throw new NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");

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

            msg = "Completed clsPluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Runs the MS_File_Info_Scanner tool
        /// </summary>
        /// <returns></returns>
        private clsToolReturnData RunMsFileInfoScanner()
        {
            var retData = new clsToolReturnData();

            // Always use client perspective for the source directory (allows MSFileInfoScanner to run from any CTM)
            var sourceDirectory = mTaskParams.GetParam("Storage_Vol_External");

            // Set up the rest of the paths
            sourceDirectory = Path.Combine(sourceDirectory, mTaskParams.GetParam("Storage_Path"));
            sourceDirectory = Path.Combine(sourceDirectory, mTaskParams.GetParam("Folder"));
            var outputPathBase = Path.Combine(sourceDirectory, "QC");

            // Set up the params for the MS file scanner
            mMsFileScanner.DSInfoDBPostingEnabled = false;
            mMsFileScanner.SaveTICAndBPIPlots = mTaskParams.GetParam("SaveTICAndBPIPlots", true);
            mMsFileScanner.SaveLCMS2DPlots = mTaskParams.GetParam("SaveLCMS2DPlots", true);
            mMsFileScanner.ComputeOverallQualityScores = mTaskParams.GetParam("ComputeOverallQualityScores", false);
            mMsFileScanner.CreateDatasetInfoFile = mTaskParams.GetParam("CreateDatasetInfoFile", true);

            mMsFileScanner.LCMS2DPlotMZResolution = mTaskParams.GetParam("LCMS2DPlotMZResolution", clsLCMSDataPlotterOptions.DEFAULT_MZ_RESOLUTION);
            mMsFileScanner.LCMS2DPlotMaxPointsToPlot = mTaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", clsLCMSDataPlotterOptions.DEFAULT_MAX_POINTS_TO_PLOT);
            mMsFileScanner.LCMS2DPlotMinPointsPerSpectrum = mTaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", clsLCMSDataPlotterOptions.DEFAULT_MIN_POINTS_PER_SPECTRUM);
            mMsFileScanner.LCMS2DPlotMinIntensity = mTaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
            mMsFileScanner.LCMS2DOverviewPlotDivisor = mTaskParams.GetParam("LCMS2DOverviewPlotDivisor", clsLCMSDataPlotterOptions.DEFAULT_LCMS2D_OVERVIEW_PLOT_DIVISOR);

            var sampleLabelling = mTaskParams.GetParam("Meta_Experiment_sample_labelling", "");
            ConfigureMinimumMzValidation(mMsFileScanner, sampleLabelling);

            mMsFileScanner.CheckCentroidingStatus = true;
            mMsFileScanner.PlotWithPython = true;

            // Get the input file name
            var fileOrDirectoryNames = GetDataFileOrDirectoryName(sourceDirectory, out var skipPlots, out var rawDataType, out var instrumentClass, out var brukerDotDBaf);

            if (fileOrDirectoryNames.Count > 0 && fileOrDirectoryNames.First() == UNKNOWN_FILE_TYPE)
            {
                // Raw_Data_Type not recognized
                retData.CloseoutMsg = mMsg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            if (fileOrDirectoryNames.Count > 0 && fileOrDirectoryNames.First() == INVALID_FILE_TYPE)
            {
                // DS quality test not implemented for this file type
                retData.CloseoutMsg = string.Empty;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                retData.EvalMsg = "Dataset info test not implemented for data type " + clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " + clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                retData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                return retData;
            }

            if (fileOrDirectoryNames.Count == 0 || string.IsNullOrEmpty(fileOrDirectoryNames.First()))
            {
                // There was a problem with getting the file name; Details reported by called method
                retData.CloseoutMsg = mMsg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            if (skipPlots)
            {
                // Do not create any plots
                mMsFileScanner.SaveTICAndBPIPlots = false;
                mMsFileScanner.SaveLCMS2DPlots = false;
            }

            // Make the output directory
            if (!Directory.Exists(outputPathBase))
            {
                try
                {
                    Directory.CreateDirectory(outputPathBase);
                    var msg = "clsPluginMain.RunMsFileInfoScanner: Created output directory " + outputPathBase;
                    LogDebug(msg);
                }
                catch (Exception ex)
                {
                    var msg = "clsPluginMain.RunMsFileInfoScanner: Exception creating output directory " + outputPathBase;
                    LogError(msg, ex);

                    retData.CloseoutMsg = "Exception creating output directory " + outputPathBase;
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }
            }

            bool useLocalOutputDirectory;
            if (Environment.MachineName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) &&
                !Environment.UserName.StartsWith("svc", StringComparison.OrdinalIgnoreCase))
            {
                useLocalOutputDirectory = true;
            }
            else
            {
                useLocalOutputDirectory = false;
            }

            // Call the file scanner DLL
            // Typically only call it once, but for Bruker datasets with multiple .D directories, we'll call it once for each .D directory

            mErrOccurred = false;
            mMsg = string.Empty;

            var cachedDatasetInfoXML = new List<string>();
            var outputDirectoryNames = new List<string>();
            var primaryFileOrDirectoryProcessed = false;
            var nextSubdirectorySuffix = 1;

            foreach (var datasetFileOrDirectory in fileOrDirectoryNames)
            {
                mFailedScanCount = 0;

                var remoteFileOrDirectoryPath = Path.Combine(sourceDirectory, datasetFileOrDirectory);

                string pathToProcess;
                bool fileCopiedLocally;

                var datasetFile = new FileInfo(remoteFileOrDirectoryPath);
                if (datasetFile.Exists && string.Equals(datasetFile.Extension, clsInstrumentClassInfo.DOT_RAW_EXTENSION,
                                                        StringComparison.OrdinalIgnoreCase))
                {
                    LogMessage("Copying instrument file to local disk: " + datasetFile.FullName, false, false);

                    ResetTimestampForQueueWaitTimeLogging();

                    // Thermo .raw file; copy it locally
                    var localFilePath = Path.Combine(mWorkDir, datasetFileOrDirectory);
                    var fileCopied = mFileTools.CopyFileUsingLocks(datasetFile, localFilePath, true);

                    if (!fileCopied)
                    {
                        retData.CloseoutMsg = "Error copying instrument data file to local working directory";
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return retData;
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
                    outputPathBase, datasetFileOrDirectory, fileOrDirectoryNames.Count,
                    outputDirectoryNames, ref nextSubdirectorySuffix);

                if (string.IsNullOrWhiteSpace(currentOutputDirectory))
                {
                    retData.CloseoutMsg = "ConstructOutputDirectoryPath returned an empty string; cannot process this dataset";
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

                if (useLocalOutputDirectory)
                {
                    // Override the output directory
                    var localOutputDir = Path.Combine(mWorkDir, Path.GetFileName(currentOutputDirectory));
                    ConsoleMsgUtils.ShowDebug(string.Format(
                                                  "Overriding MSFileInfoScanner output directory from {0}\n  to {1}",
                                                  currentOutputDirectory, localOutputDir));
                    currentOutputDirectory = localOutputDir;
                }

                var successProcessing = mMsFileScanner.ProcessMSFileOrDirectory(pathToProcess, currentOutputDirectory);

                if (mErrOccurred)
                {
                    successProcessing = false;
                }

                if (fileCopiedLocally)
                {
                    mFileTools.DeleteFileWithRetry(new FileInfo(pathToProcess), 2, out _);
                }

                var mzMinValidationError = mMsFileScanner.ErrorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.MS2MzMinValidationError;
                if (mzMinValidationError)
                {
                    mMsg = mMsFileScanner.GetErrorMessage();
                    successProcessing = false;
                }

                if (mMsFileScanner.ErrorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.MS2MzMinValidationWarning)
                {
                    var warningMsg = mMsFileScanner.GetErrorMessage();
                    retData.EvalMsg = AppendToComment(retData.EvalMsg, "MS2MzMinValidationWarning: " + warningMsg);
                }

                if (successProcessing && !skipPlots)
                {
                    var validQcGraphics = ValidateQCGraphics(currentOutputDirectory, primaryFileOrDirectoryProcessed, retData);
                    if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
                        return retData;

                    if (!validQcGraphics)
                        continue;
                }

                if (successProcessing)
                {
                    cachedDatasetInfoXML.Add(mMsFileScanner.DatasetInfoXML);
                    primaryFileOrDirectoryProcessed = true;
                    continue;
                }

                // Either a non-zero error code was returned, or an error event was received

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (brukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
                {
                    // 12T_FTICR_B datasets (with .D directories and analysis.baf and/or fid files) sometimes work with MSFileInfoScanner, and sometimes don't
                    // The problem is that ProteoWizard doesn't support certain forms of these datasets
                    // In particular, small datasets (lasting just a few seconds) don't work

                    retData.CloseoutMsg = string.Empty;
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    retData.EvalMsg = "MSFileInfoScanner error for data type " +
                                     clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " +
                                     clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                    retData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    return retData;

                }

                if (primaryFileOrDirectoryProcessed)
                {
                    // MSFileInfoScanner already processed the primary file or directory
                    // Mention this failure in the EvalMsg but still return success
                    retData.EvalMsg = AppendToComment(retData.EvalMsg,
                                                     "ProcessMSFileOrFolder returned false for " + datasetFileOrDirectory);
                }
                else
                {
                    if (string.IsNullOrEmpty(mMsg))
                    {
                        mMsg = "ProcessMSFileOrFolder returned false. Message = " +
                                mMsFileScanner.GetErrorMessage() +
                                " retData code = " + (int)mMsFileScanner.ErrorCode;
                    }

                    LogError(mMsg);

                    retData.CloseoutMsg = mMsg;
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    if (!string.IsNullOrWhiteSpace(mMsFileScanner.DatasetInfoXML))
                    {
                        cachedDatasetInfoXML.Add(mMsFileScanner.DatasetInfoXML);
                    }

                    if (mzMinValidationError)
                    {
                        var jobParamNote = string.Format(
                            "To ignore this error, use Exec AddUpdateJobParameter {0}, 'JobParameters', 'SkipMinimumMzValidation', 'true'",
                            mJob);

                        retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg, jobParamNote);
                    }

                    if (cachedDatasetInfoXML.Count > 0)
                    {
                        // Do not exit this method yet; we want to store the dataset info in the database
                        break;
                    }

                    return retData;
                }

                if (mFailedScanCount > 10)
                {
                    LogWarning(string.Format("Unable to load data for {0} spectra", mFailedScanCount));
                }

            } // foreach file in fileOrDirectoryNames

            // Merge the dataset info defined in cachedDatasetInfoXML
            // If cachedDatasetInfoXml contains just one item, simply return it
            var datasetXmlMerger = new clsDatasetInfoXmlMerger();
            var dsInfoXML = CombineDatasetInfoXML(datasetXmlMerger, cachedDatasetInfoXML);

            if (cachedDatasetInfoXML.Count > 1)
            {
                ProcessMultiDatasetInfoScannerResults(outputPathBase, datasetXmlMerger, dsInfoXML, outputDirectoryNames);
            }

            // Check for dataset acq time gap warnings
            // If any are found, CloseoutMsg is updated
            AcqTimeWarningsReported(datasetXmlMerger, retData);

            // Call SP CacheDatasetInfoXML to store dsInfoXML in table T_Dataset_Info_XML
            var success = PostDatasetInfoXml(dsInfoXML, out var errorMessage);
            if (!success)
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg, errorMessage);
            }

            if (!useLocalOutputDirectory || retData.CloseoutType != EnumCloseOutType.CLOSEOUT_SUCCESS)
                return retData;

            // Set this to failed since we stored the QC graphics in the local work dir instead of on the storage server
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg,
                                                  "QC graphics were saved locally for debugging purposes; " +
                                                  "need to run this job step with a manager that has write access to the storage server");

            return retData;

        }

        /// <summary>
        /// Examine datasetXmlMerger.AcqTimeWarnings
        /// If non-empty, summarize the errors and update retData
        /// </summary>
        /// <param name="datasetXmlMerger"></param>
        /// <param name="retData"></param>
        /// <returns>True if warnings exist, otherwise false</returns>
        private void AcqTimeWarningsReported(clsDatasetInfoXmlMerger datasetXmlMerger, clsToolReturnData retData)
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

            retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg, "Large gap between acq times: " + datasetXmlMerger.AcqTimeWarnings.FirstOrDefault());
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
        }

        private bool PostDatasetInfoXml(string dsInfoXML, out string errorMessage)
        {
            var iPostCount = 0;
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var iDatasetID = mTaskParams.GetParam("Dataset_ID", 0);

            var successPosting = false;

            while (iPostCount <= 2)
            {
                successPosting = mMsFileScanner.PostDatasetInfoUseDatasetID(
                    iDatasetID, dsInfoXML, connectionString, MS_FILE_SCANNER_DS_INFO_SP);

                if (successPosting)
                    break;

                // If the error message contains the text "timeout expired" then try again, up to 2 times
                if (!mMsg.ToLower().Contains("timeout expired"))
                    break;

                System.Threading.Thread.Sleep(1500);
                iPostCount += 1;
            }

            iMSFileInfoScanner.eMSFileScannerErrorCodes errorCode;
            if (successPosting)
            {
                errorCode = iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError;
            }
            else
            {
                errorCode = mMsFileScanner.ErrorCode;
                mMsg = "Error posting dataset info XML. Message = " +
                        mMsFileScanner.GetErrorMessage() + " retData code = " + (int)mMsFileScanner.ErrorCode;
                LogError(mMsg);
            }

            if (errorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError)
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
            clsDatasetInfoXmlMerger datasetXmlMerger,
            string dsInfoXML,
            IEnumerable<string> outputDirectoryNames)
        {

            var combinedDatasetInfoFilename = mDataset + "_Combined_DatasetInfo.xml";

            try
            {
                // Write the combined XML to disk
                var combinedXmlFilePath = Path.Combine(outputPathBase, combinedDatasetInfoFilename);
                using (var xmlWriter = new StreamWriter(new FileStream(combinedXmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    xmlWriter.WriteLine(dsInfoXML);
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception creating the combined _DatasetInfo.xml file for " + mDataset + ": " + ex.Message;
                LogError(msg, ex);
            }

            try
            {
                var pngMatcher = new Regex(@"""(?<Filename>[^""]+\.png)""");

                // Create an index.html file that shows all of the plots in the subdirectories
                var indexHtmlFilePath = Path.Combine(outputPathBase, "index.html");
                using (var htmlWriter = new StreamWriter(new FileStream(indexHtmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
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
                            continue;

                        using (var htmlReader = new StreamReader(new FileStream(htmlFiles[0].FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                        {
                            var processingTable = false;
                            var htmlToAppend = new List<string>();
                            var htmlHasImageInfo = false;
                            var rowDepth = 0;

                            while (!htmlReader.EndOfStream)
                            {
                                var dataLine = htmlReader.ReadLine();
                                if (string.IsNullOrWhiteSpace(dataLine))
                                    continue;

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

                    }

                    // Add the combined stats
                    htmlWriter.WriteLine("    <tr>");
                    htmlWriter.WriteLine("        <td colspan=\"3\"><hr/></td>");
                    htmlWriter.WriteLine("    </tr>");
                    htmlWriter.WriteLine("    <tr>");
                    htmlWriter.WriteLine("      <td>&nbsp;</td>");
                    htmlWriter.WriteLine("      <td align=\"right\">Combined Stats:</td>");
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
                    htmlWriter.WriteLine("");
                    htmlWriter.WriteLine("  </table>");
                    htmlWriter.WriteLine("");
                    htmlWriter.WriteLine("</body>");
                    htmlWriter.WriteLine("</html>");
                    htmlWriter.WriteLine();
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception creating the combined _DatasetInfo.xml file for " + mDataset + ": " + ex.Message;
                LogError(msg, ex);
            }

        }

        /// <summary>
        /// Merge the dataset info defined in cachedDatasetInfoXml
        /// If cachedDatasetInfoXml contains just one item, simply return it
        /// </summary>
        /// <param name="datasetXmlMerger">DatasetInfo XML Merger</param>
        /// <param name="cachedDatasetInfoXml">List of cached DatasetInfo XML</param>
        /// <returns>Merged DatasetInfo XML</returns>
        private string CombineDatasetInfoXML(clsDatasetInfoXmlMerger datasetXmlMerger, List<string> cachedDatasetInfoXml)
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
        /// <param name="diDatasetDirectory">Dataset directory</param>
        /// <returns>Returns the file name if found, otherwise an empty string</returns>
        private string CheckForBrukerImagingZipFiles(DirectoryInfo diDatasetDirectory)
        {
            var fiFiles = diDatasetDirectory.GetFiles("0_R*X*.zip");

            if (fiFiles.Length > 0)
            {
                return fiFiles[0].Name;
            }

            return string.Empty;
        }

        /// <summary>
        /// Check whether the experiment for this dataset has labelling value defined
        /// If it doesn't, or if it is Unknown or None, examine the dataset name
        /// </summary>
        /// <param name="msFileInfoScanner"></param>
        /// <param name="sampleLabelling"></param>
        private void ConfigureMinimumMzValidation(iMSFileInfoScanner msFileInfoScanner, string sampleLabelling)
        {

            mMsFileScanner.MS2MzMin = 0;

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

                var reporterIonMzMinText = mTaskParams.GetParam("Meta_Experiment_labelling_reporter_mz_min", "");
                if (!string.IsNullOrEmpty(reporterIonMzMinText))
                {
                    if (float.TryParse(reporterIonMzMinText, out var reporterIonMzMin))
                    {
                        msFileInfoScanner.MS2MzMin = (int)Math.Floor(reporterIonMzMin);
                        LogMessage(string.Format(
                                       "Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the experiment labelling is {1}",
                                       msFileInfoScanner.MS2MzMin, sampleLabelling));
                    }
                }
            }

            if (msFileInfoScanner.MS2MzMin > 0)
                return;

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
                msFileInfoScanner.MS2MzMin = 113;
                LogMessage(string.Format(
                               "Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                               msFileInfoScanner.MS2MzMin, iTRAQMatch.Value));
            }

            var tmtMatch = tmtMatcher.Match(mDataset);
            if (tmtMatch.Success)
            {
                msFileInfoScanner.MS2MzMin = 126;
                LogMessage(string.Format(
                               "Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                               msFileInfoScanner.MS2MzMin, tmtMatch.Value));
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
                    var subdirectoryToUse = string.Copy(subDirectory);
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
        /// Look for .D directories below diDatasetDirectory
        /// Add them to list fileOrDirectoryNames
        /// </summary>
        /// <param name="diDatasetDirectory">Dataset directory to examine</param>
        /// <param name="fileOrDirectoryNames">List to append .D directories to (calling function must initialize)</param>
        private void FindDotDDirectories(DirectoryInfo diDatasetDirectory, ICollection<string> fileOrDirectoryNames)
        {
            var diDotDDirectories = diDatasetDirectory.GetDirectories("*.d");
            if (diDotDDirectories.Length <= 0)
                return;

            // Look for a .mcf file in each of the .D directories
            foreach (var dotDDirectory in diDotDDirectories)
            {
                var mcfFileExists = LookForMcfFileInDotDDirectory(dotDDirectory, out var dotDDirectoryName);
                if (mcfFileExists && !fileOrDirectoryNames.Contains(dotDDirectoryName))
                {
                    fileOrDirectoryNames.Add(dotDDirectoryName);
                }
            }

        }

        /// <summary>
        /// Returns the file or directory name list for the specified dataset based on dataset type
        /// Most datasets only have a single dataset file or directory, but FTICR_Imaging datasets
        /// can have multiple .D directories below a parent directory
        /// </summary>
        /// <returns>List of data file file or directory names; empty list if not found</returns>
        /// <remarks>
        /// Returns UNKNOWN_FILE_TYPE for instrument types that are not recognized.
        /// Returns INVALID_FILE_TYPE for instruments for which we do not run MSFileInfoScanner
        /// </remarks>
        private List<string> GetDataFileOrDirectoryName(
            string inputDirectory,
            out bool bSkipPlots,
            out clsInstrumentClassInfo.eRawDataType rawDataType,
            out clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            out bool bBrukerDotDBaf)
        {
            bool isFile;

            bSkipPlots = false;
            rawDataType = clsInstrumentClassInfo.eRawDataType.Unknown;
            bBrukerDotDBaf = false;

            // Determine the Instrument Class and RawDataType
            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var rawDataTypeName = mTaskParams.GetParam("RawDataType", "UnknownRawDataType");

            instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
            {
                mMsg = "Instrument class not recognized: " + instClassName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            rawDataType = clsInstrumentClassInfo.GetRawDataType(rawDataTypeName);
            if (rawDataType == clsInstrumentClassInfo.eRawDataType.Unknown)
            {
                mMsg = "RawDataType not recognized: " + rawDataTypeName;
                LogError(mMsg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            var diDatasetDirectory = new DirectoryInfo(inputDirectory);
            string fileOrDirectoryName;

            // Get the expected file name based on the dataset type
            switch (rawDataType)
            {
                case clsInstrumentClassInfo.eRawDataType.ThermoRawFile:
                    // LTQ_2, LTQ_4, etc.
                    // LTQ_Orb_1, LTQ_Orb_2, etc.
                    // VOrbiETD01, VOrbiETD02, etc.
                    // TSQ_3
                    // Thermo_GC_MS_01
                    fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION;
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.ZippedSFolders:
                    // 9T_FTICR, 11T_FTICR_B, and 12T_FTICR
                    fileOrDirectoryName = "analysis.baf";
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerFTFolder:
                    // 12T_FTICR_B, 15T_FTICR, 9T_FTICR_B
                    // Also, Bruker_FT_IonTrap01, which is Bruker_Amazon_Ion_Trap
                    // 12T_FTICR_Imaging and 15T_FTICR_Imaging datasets with instrument class BrukerMALDI_Imaging_V2 will also have bruker_ft format;
                    // however, instead of an analysis.baf file, they might have a .mcf file

                    isFile = true;
                    if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Bruker_Amazon_Ion_Trap)
                    {
                        fileOrDirectoryName = Path.Combine(mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
                    }
                    else
                    {
                        fileOrDirectoryName = Path.Combine(mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
                        bBrukerDotDBaf = true;
                    }

                    if (!File.Exists(Path.Combine(diDatasetDirectory.FullName, fileOrDirectoryName)))
                        fileOrDirectoryName = CheckForBrukerImagingZipFiles(diDatasetDirectory);

                    break;

                case clsInstrumentClassInfo.eRawDataType.UIMF:
                    // IMS_TOF_2, IMS_TOF_3, IMS_TOF_4, IMS_TOF_5, IMS_TOF_6, etc.
                    fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION;
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.SciexWiffFile:
                    // QTrap01
                    fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_WIFF_EXTENSION;
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.AgilentDFolder:
                    // Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
                    fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;

                    if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.PrepHPLC)
                    {
                        LogMessage("Skipping MSFileInfoScanner since PrepHPLC dataset");
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerMALDIImaging:
                    // bruker_maldi_imaging: 12T_FTICR_Imaging, 15T_FTICR_Imaging, and BrukerTOF_Imaging_01
                    // Find the name of the first zip file

                    fileOrDirectoryName = CheckForBrukerImagingZipFiles(diDatasetDirectory);
                    bSkipPlots = true;
                    isFile = true;

                    if (string.IsNullOrEmpty(fileOrDirectoryName))
                    {
                        mMsg = "Did not find any 0_R*.zip files in the dataset directory";
                        LogWarning("clsPluginMain.GetDataFileOrDirectoryName: " + mMsg);
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerTOFBaf:
                    fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;
                    break;
                case clsInstrumentClassInfo.eRawDataType.IlluminaFolder:
                    // fileOrDirectoryName = mDataset + clsInstrumentClassInfo.DOT_TXT_GZ_EXTENSION;
                    // isFile = true;

                    LogMessage("Skipping MSFileInfoScanner since Illumina RNASeq dataset");
                    return new List<string> { INVALID_FILE_TYPE };

                default:
                    // Other instruments; do not process them with MSFileInfoScanner

                    // Excluded instruments include:
                    // dot_wiff_files (AgilentQStarWiffFile): AgTOF02
                    // bruker_maldi_spot (BrukerMALDISpot): BrukerTOF_01
                    mMsg = "Data type " + rawDataType + " not recognized";
                    LogWarning("clsPluginMain.GetDataFileOrDirectoryName: " + mMsg);
                    return new List<string> { INVALID_FILE_TYPE };
            }

            // Test to verify the file (or directory) exists
            var fileOrDirectoryPath = Path.Combine(diDatasetDirectory.FullName, fileOrDirectoryName);

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
                var fileOrDirectoryNames = LookForAlternateFileOrDirectory(diDatasetDirectory, fileOrDirectoryName);

                if (fileOrDirectoryNames.Count > 0)
                    return fileOrDirectoryNames;

                mMsg = "clsPluginMain.GetDataFileOrDirectoryName: File " + fileOrDirectoryPath + " not found";
                LogError(mMsg);
                mMsg = "File " + fileOrDirectoryPath + " not found";

                return new List<string>();
            }

            // Expecting to match a directory
            if (Directory.Exists(fileOrDirectoryPath))
            {
                if (Path.GetExtension(fileOrDirectoryName).ToUpper() != ".D")
                {
                    // Directory exists, and it does not end in .D
                    return new List<string> { fileOrDirectoryName };
                }

                // Look for other .D directories
                var fileOrDirectoryNames = new List<string> { fileOrDirectoryName };
                FindDotDDirectories(diDatasetDirectory, fileOrDirectoryNames);

                return fileOrDirectoryNames;
            }

            mMsg = "clsPluginMain.GetDataFileOrDirectoryName; directory not found: " + fileOrDirectoryPath;
            LogError(mMsg);
            mMsg = "Directory not found: " + fileOrDirectoryPath;

            return new List<string>();

        }

        /// <summary>
        /// Construct the full path to the MSFileInfoScanner.DLL
        /// </summary>
        /// <returns></returns>
        private string GetMSFileInfoScannerDLLPath()
        {
            var msFileInfoScannerDir = mMgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
            if (string.IsNullOrEmpty(msFileInfoScannerDir))
                return string.Empty;

            return Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");
        }

        /// <summary>
        /// A dataset file was not found
        /// Look for alternate dataset files, or look for .D directories
        /// </summary>
        /// <param name="diDatasetDirectory"></param>
        /// <param name="initialFileOrDirectoryName"></param>
        /// <returns></returns>
        private List<string> LookForAlternateFileOrDirectory(DirectoryInfo diDatasetDirectory, string initialFileOrDirectoryName)
        {

            // File not found; look for alternate extensions
            var lstAlternateExtensions = new List<string> { "mgf", "mzXML", "mzML" };

            foreach (var altExtension in lstAlternateExtensions)
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
            var primaryDotDDirectory = new DirectoryInfo(Path.Combine(diDatasetDirectory.FullName, mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

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
            FindDotDDirectories(diDatasetDirectory, fileOrDirectoryNames);

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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {

            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = clsUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Info plugin");
                return false;
            }

            // Lookup the version of the dataset info plugin
            var pluginPath = Path.Combine(appDirectory, "DatasetInfoPlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!bSuccess)
                return false;

            // Lookup the version of the MSFileInfoScanner DLL
            var msFileInfoScannerDLLPath = GetMSFileInfoScannerDLLPath();
            if (!string.IsNullOrEmpty(msFileInfoScannerDLLPath))
            {
                bSuccess = StoreToolVersionInfoOneFile(ref toolVersionInfo, msFileInfoScannerDLLPath);
                if (!bSuccess)
                    return false;
            }

            // Lookup the version of the UIMFLibrary DLL
            var uimfLibraryPath = Path.Combine(appDirectory, "UIMFLibrary.dll");
            bSuccess = StoreToolVersionInfoOneFile(ref toolVersionInfo, uimfLibraryPath);
            if (!bSuccess)
                return false;

            // Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(pluginPath)
            };

            if (!string.IsNullOrEmpty(msFileInfoScannerDLLPath))
                toolFiles.Add(new FileInfo(msFileInfoScannerDLLPath));

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

        private bool ValidateQCGraphics(string currentOutputDirectory, bool primaryFileOrDirectoryProcessed, clsToolReturnData retData)
        {
            // Make sure at least one of the PNG files created by MSFileInfoScanner is over 10 KB in size
            var outputDirectory = new DirectoryInfo(currentOutputDirectory);
            if (!outputDirectory.Exists)
            {
                var errMsg = "Output directory not found: " + currentOutputDirectory;

                if (primaryFileOrDirectoryProcessed)
                {
                    LogWarning(errMsg);
                    retData.EvalMsg = AppendToComment(retData.EvalMsg, errMsg);
                    return false;
                }

                LogError(errMsg);
                retData.CloseoutMsg = errMsg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var pngFiles = outputDirectory.GetFiles("*.png");

            if (pngFiles.Length == 0)
            {
                var errMsg = "No PNG files were created";
                if (primaryFileOrDirectoryProcessed)
                {
                    LogWarning(errMsg);
                    retData.EvalMsg = AppendToComment(retData.EvalMsg, errMsg);
                    return false;
                }

                LogError(errMsg);
                retData.CloseoutMsg = errMsg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var minimumGraphicsSizeKB = 10;

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
                return true;

            var errMsg2 = string.Format("All {0} PNG files created by MSFileInfoScanner are less than {1} KB and likely blank graphics", pngFiles.Length, minimumGraphicsSizeKB);

            if (primaryFileOrDirectoryProcessed)
            {
                LogWarning(errMsg2);
                retData.EvalMsg = AppendToComment(retData.EvalMsg, errMsg2);
                return false;
            }

            LogError(errMsg2);
            retData.CloseoutMsg = errMsg2;
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return false;

        }

        #endregion

        #region "Event handlers"

        /// <summary>
        /// Handles an error event from MS file scanner
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="ex"></param>
        void MsFileScanner_ErrorEvent(string message, Exception ex)
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
            else if (message.StartsWith("Call to .OpenRawFile failed for:"))
            {
                mMsg = "Error running MSFileInfoScanner: Call to .OpenRawFile failed";
                LogError(errorMsg);

            } else
            {
                mErrOccurred = true;

                // Message often contains long paths; check for this and shorten them.
                // For example, switch
                // from: \\proto-6\LTQ_Orb_1\2015_4\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw
                // to:   QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw

                // Match text of the form         \\server\share\directory<anything>DatasetName.Extension
                var reDatasetFile = new Regex(@"\\\\[^\\]+\\[^\\]+\\[^\\]+.+(" + mDataset + @"\.[a-z0-9]+)");

                if (reDatasetFile.IsMatch(message))
                {
                    mMsg = "Error running MSFileInfoScanner: " + reDatasetFile.Replace(message, "$1");
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
        void MsFileScanner_WarningEvent(string message)
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
                    LogWarning(string.Format("Unable to load data for {0} spectra", mFailedScanCount));
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
                return;

            mLastProgressUpdate = DateTime.UtcNow;

            mStatusTools.UpdateAndWrite(EnumTaskStatusDetail.Running_Tool, percentComplete);


            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalMinutes >= mStatusUpdateIntervalMinutes)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                LogMessage(string.Format("MSFileInfoScanner running; {0:F1} minutes elapsed",
                                         DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes));

                // Increment mStatusUpdateIntervalMinutes by 1 minute every time the status is logged, up to a maximum of 30 minutes
                if (mStatusUpdateIntervalMinutes < 30)
                {
                    mStatusUpdateIntervalMinutes += 1;
                }
            }
        }

        #endregion
    }

}
