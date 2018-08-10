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
    /// Dataset Info plugin: generates QC graphics
    /// </summary>
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Constants"

        private const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";

        private const string UNKNOWN_FILE_TYPE = "Unknown File Type";

        private const string INVALID_FILE_TYPE = "Invalid File Type";

        private const bool IGNORE_BRUKER_BAF_ERRORS = false;

        #endregion

        #region "Class-wide variables"

        iMSFileInfoScanner m_MsFileScanner;

        string m_Msg;

        bool m_ErrOccurred;

        private int m_FailedScanCount;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset info step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
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

            msg = "Running DatasetInfo on dataset '" + m_Dataset + "'";
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
                var assem = Assembly.LoadFrom(dllFilePath);
                var dllType = assem.GetType(className, false, true);
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
        /// Initializes the dataset info tool
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

            // Initialize the MSFileScanner class
            m_MsFileScanner = LoadMSFileInfoScanner(msFileInfoScannerDLLPath);
            RegisterEvents(m_MsFileScanner);

            // Add custom error and warning handlers
            UnregisterEventHandler(m_MsFileScanner, BaseLogger.LogLevels.ERROR);
            UnregisterEventHandler(m_MsFileScanner, BaseLogger.LogLevels.WARN);

            m_MsFileScanner.ErrorEvent += m_MsFileScanner_ErrorEvent;
            m_MsFileScanner.WarningEvent += m_MsFileScanner_WarningEvent;

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
            var sourceDirectory = m_TaskParams.GetParam("Storage_Vol_External");

            // Set up the rest of the paths
            sourceDirectory = Path.Combine(sourceDirectory, m_TaskParams.GetParam("Storage_Path"));
            sourceDirectory = Path.Combine(sourceDirectory, m_TaskParams.GetParam("Folder"));
            var outputPathBase = Path.Combine(sourceDirectory, "QC");

            // Set up the params for the MS file scanner
            m_MsFileScanner.DSInfoDBPostingEnabled = false;
            m_MsFileScanner.SaveTICAndBPIPlots = m_TaskParams.GetParam("SaveTICAndBPIPlots", true);
            m_MsFileScanner.SaveLCMS2DPlots = m_TaskParams.GetParam("SaveLCMS2DPlots", true);
            m_MsFileScanner.ComputeOverallQualityScores = m_TaskParams.GetParam("ComputeOverallQualityScores", false);
            m_MsFileScanner.CreateDatasetInfoFile = m_TaskParams.GetParam("CreateDatasetInfoFile", true);

            m_MsFileScanner.LCMS2DPlotMZResolution = m_TaskParams.GetParam("LCMS2DPlotMZResolution", clsLCMSDataPlotterOptions.DEFAULT_MZ_RESOLUTION);
            m_MsFileScanner.LCMS2DPlotMaxPointsToPlot = m_TaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", clsLCMSDataPlotterOptions.DEFAULT_MAX_POINTS_TO_PLOT);
            m_MsFileScanner.LCMS2DPlotMinPointsPerSpectrum = m_TaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", clsLCMSDataPlotterOptions.DEFAULT_MIN_POINTS_PER_SPECTRUM);
            m_MsFileScanner.LCMS2DPlotMinIntensity = m_TaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
            m_MsFileScanner.LCMS2DOverviewPlotDivisor = m_TaskParams.GetParam("LCMS2DOverviewPlotDivisor", clsLCMSDataPlotterOptions.DEFAULT_LCMS2D_OVERVIEW_PLOT_DIVISOR);

            var sampleLabelling = m_TaskParams.GetParam("Meta_Experiment_sample_labelling", "");
            ConfigureMinimumMzValidation(m_MsFileScanner, sampleLabelling);

            m_MsFileScanner.CheckCentroidingStatus = true;
            m_MsFileScanner.PlotWithPython = true;

            // Get the input file name
            var fileOrDirectoryNames = GetDataFileOrDirectoryName(sourceDirectory, out var skipPlots, out var rawDataType, out var instrumentClass, out var brukerDotDBaf);

            if (fileOrDirectoryNames.Count > 0 && fileOrDirectoryNames.First() == UNKNOWN_FILE_TYPE)
            {
                // Raw_Data_Type not recognized
                retData.CloseoutMsg = m_Msg;
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
                retData.CloseoutMsg = m_Msg;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            if (skipPlots)
            {
                // Do not create any plots
                m_MsFileScanner.SaveTICAndBPIPlots = false;
                m_MsFileScanner.SaveLCMS2DPlots = false;
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

            m_ErrOccurred = false;
            m_Msg = string.Empty;

            var cachedDatasetInfoXML = new List<string>();
            var outputDirectoryNames = new List<string>();
            var primaryFileOrDirectoryProcessed = false;
            var nextSubdirectorySuffix = 1;

            foreach (var datasetFileOrDirectory in fileOrDirectoryNames)
            {
                m_FailedScanCount = 0;

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
                    var localFilePath = Path.Combine(m_WorkDir, datasetFileOrDirectory);
                    var fileCopied = m_FileTools.CopyFileUsingLocks(datasetFile, localFilePath, true);

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
                    var localOutputDir = Path.Combine(m_WorkDir, Path.GetFileName(currentOutputDirectory));
                    ConsoleMsgUtils.ShowDebug(string.Format(
                                                  "Overriding MSFileInfoScanner output directory from {0}\n  to {1}",
                                                  currentOutputDirectory, localOutputDir));
                    currentOutputDirectory = localOutputDir;
                }

                var successProcessing = m_MsFileScanner.ProcessMSFileOrFolder(pathToProcess, currentOutputDirectory);

                if (m_ErrOccurred)
                {
                    successProcessing = false;
                }

                if (fileCopiedLocally)
                {
                    m_FileTools.DeleteFileWithRetry(new FileInfo(pathToProcess), 2, out _);
                }

                var mzMinValidationError = m_MsFileScanner.ErrorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.MS2MzMinValidationError;
                if (mzMinValidationError)
                {
                    m_Msg = m_MsFileScanner.GetErrorMessage();
                    successProcessing = false;
                }

                if (m_MsFileScanner.ErrorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.MS2MzMinValidationWarning)
                {
                    var warningMsg = m_MsFileScanner.GetErrorMessage();
                    retData.EvalMsg = AppendToComment(retData.EvalMsg,
                                                     "MS2MzMinValidationWarning: " + warningMsg);
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
                    cachedDatasetInfoXML.Add(m_MsFileScanner.DatasetInfoXML);
                    primaryFileOrDirectoryProcessed = true;
                    continue;
                }

                // Either a non-zero error code was returned, or an error event was received

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (brukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
                {
                    // 12T_FTICR_B datasets (with .D directories and analysis.baf and/or fid files) sometimes work with MSFileInfoscanner, and sometimes don't
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
                    if (string.IsNullOrEmpty(m_Msg))
                    {
                        m_Msg = "ProcessMSFileOrFolder returned false. Message = " +
                                m_MsFileScanner.GetErrorMessage() +
                                " retData code = " + (int)m_MsFileScanner.ErrorCode;
                    }

                    LogError(m_Msg);

                    retData.CloseoutMsg = m_Msg;
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    if (mzMinValidationError && !string.IsNullOrWhiteSpace(m_MsFileScanner.DatasetInfoXML))
                    {
                        cachedDatasetInfoXML.Add(m_MsFileScanner.DatasetInfoXML);

                        var jobParamNote = string.Format(
                            "To ignore this error, use Exec AddUpdateJobParameter {0}, 'JobParameters', 'SkipMinimumMzValidation', 'true'",
                            m_Job);

                        retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg, jobParamNote);

                        // Do not exit this method yet; we want to store the dataset info in the database
                        break;
                    }

                    return retData;
                }

                if (m_FailedScanCount > 10)
                {
                    LogWarning(string.Format("Unable to load data for {0} spectra", m_FailedScanCount));
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

            if (!useLocalOutputDirectory)
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
                m_Msg = AppendToComment(m_Msg, warning);
            }

            LogError(m_Msg);

            retData.CloseoutMsg = AppendToComment(retData.CloseoutMsg, "Large gap between acq times: " + datasetXmlMerger.AcqTimeWarnings.FirstOrDefault());
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
        }

        private bool PostDatasetInfoXml(string dsInfoXML, out string errorMessage)
        {
            var iPostCount = 0;
            var connectionString = m_MgrParams.GetParam("connectionstring");

            var iDatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

            var successPosting = false;

            while (iPostCount <= 2)
            {
                successPosting = m_MsFileScanner.PostDatasetInfoUseDatasetID(
                    iDatasetID, dsInfoXML, connectionString, MS_FILE_SCANNER_DS_INFO_SP);

                if (successPosting)
                    break;

                // If the error message contains the text "timeout expired" then try again, up to 2 times
                if (!m_Msg.ToLower().Contains("timeout expired"))
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
                errorCode = m_MsFileScanner.ErrorCode;
                m_Msg = "Error posting dataset info XML. Message = " +
                        m_MsFileScanner.GetErrorMessage() + " retData code = " + (int)m_MsFileScanner.ErrorCode;
                LogError(m_Msg);
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

            var combinedDatasetInfoFilename = m_Dataset + "_Combined_DatasetInfo.xml";

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
                var msg = "Exception creating the combined _DatasetInfo.xml file for " + m_Dataset + ": " + ex.Message;
                LogError(msg, ex);
            }

            try
            {
                var pngMatcher = new Regex(@"""(?<Filename>[^""]+\.png)""");

                // Create an index.html file that shows all of the plots in the subdierctories
                var indexHtmlFilePath = Path.Combine(outputPathBase, "index.html");
                using (var htmlWriter = new StreamWriter(new FileStream(indexHtmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    htmlWriter.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 3.2//EN\">");
                    htmlWriter.WriteLine("<html>");
                    htmlWriter.WriteLine("<head>");
                    htmlWriter.WriteLine("  <title>" + m_Dataset + "</title>");
                    htmlWriter.WriteLine("</head>");
                    htmlWriter.WriteLine();
                    htmlWriter.WriteLine("<body>");
                    htmlWriter.WriteLine("  <h2>" + m_Dataset + "</h2>");
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
                    htmlWriter.WriteLine("      <td align=\"center\">DMS <a href=\"http://dms2.pnl.gov/dataset/show/" + m_Dataset + "\">Dataset Detail Report</a></td>");
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
                var msg = "Exception creating the combined _DatasetInfo.xml file for " + m_Dataset + ": " + ex.Message;
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

            var combinedXML = datasetXmlMerger.CombineDatasetInfoXML(m_Dataset, cachedDatasetInfoXml);

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

            m_MsFileScanner.MS2MzMin = 0;

            if (m_TaskParams.GetParam("SkipMinimumMzValidation", false))
            {
                // Skip minimum m/z validation
                return;
            }

            if (!string.IsNullOrEmpty(sampleLabelling))
            {

                // Check whether this label has a reporter ion minimum m/z value defined
                // If it does, instruct the MSFileInfoScanner to validate that all of the MS/MS spectra
                // have a scan range that starts below the minimum reporter ion m/z

                var reporterIonMzMinText = m_TaskParams.GetParam("Meta_Experiment_labelling_reporter_mz_min", "");
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
            var itraqMatcher = new Regex("_iTRAQ[0-9-]*_", RegexOptions.IgnoreCase);

            // Match names like:
            // _TMT_
            // _TMT6_
            // _TMT10_
            // _TMT-10_
            var tmtMatcher = new Regex("_TMT[0-9-]*_", RegexOptions.IgnoreCase);

            var itraqMatch = itraqMatcher.Match(m_Dataset);
            if (itraqMatch.Success)
            {
                msFileInfoScanner.MS2MzMin = 113;
                LogMessage(string.Format(
                               "Verifying that MS/MS spectra have minimum m/z values below {0:N0} since the dataset name contains {1}",
                               msFileInfoScanner.MS2MzMin, itraqMatch.Value));
            }

            var tmtMatch = tmtMatcher.Match(m_Dataset);
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
                    var subdirectoryToUse = m_Dataset + "_" + nextSubdirectorySuffix;
                    while (outputDirectoryNames.Contains(subdirectoryToUse))
                    {
                        nextSubdirectorySuffix++;
                        subdirectoryToUse = m_Dataset + "_" + nextSubdirectorySuffix;
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
                var mcfFileExists = LookForMcfFileIndotDDirectory(dotDDirectory, out var dotDDirectoryName);
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
            var instClassName = m_TaskParams.GetParam("Instrument_Class");
            var rawDataTypeName = m_TaskParams.GetParam("rawdatatype", "UnknownRawDataType");

            instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
            {
                m_Msg = "Instrument class not recognized: " + instClassName;
                LogError(m_Msg);
                return new List<string> { UNKNOWN_FILE_TYPE };
            }

            rawDataType = clsInstrumentClassInfo.GetRawDataType(rawDataTypeName);
            if (rawDataType == clsInstrumentClassInfo.eRawDataType.Unknown)
            {
                m_Msg = "RawDataType not recognized: " + rawDataTypeName;
                LogError(m_Msg);
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
                    fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION;
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
                        fileOrDirectoryName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
                    }
                    else
                    {
                        fileOrDirectoryName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
                        bBrukerDotDBaf = true;
                    }

                    if (!File.Exists(Path.Combine(diDatasetDirectory.FullName, fileOrDirectoryName)))
                        fileOrDirectoryName = CheckForBrukerImagingZipFiles(diDatasetDirectory);

                    break;

                case clsInstrumentClassInfo.eRawDataType.UIMF:
                    // IMS_TOF_2, IMS_TOF_3, IMS_TOF_4, IMS_TOF_5, IMS_TOF_6, etc.
                    fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION;
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.SciexWiffFile:
                    // QTrap01
                    fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_WIFF_EXTENSION;
                    isFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.AgilentDFolder:
                    // Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
                    fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
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
                        m_Msg = "Did not find any 0_R*.zip files in the dataset directory";
                        LogWarning("clsPluginMain.GetDataFileOrDirectoryName: " + m_Msg);
                        return new List<string> { INVALID_FILE_TYPE };
                    }

                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerTOFBaf:
                    fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                    isFile = false;
                    break;
                case clsInstrumentClassInfo.eRawDataType.IlluminaFolder:
                    // fileOrDirectoryName = m_Dataset + clsInstrumentClassInfo.DOT_TXT_GZ_EXTENSION;
                    // isFile = true;

                    LogMessage("Skipping MSFileInfoScanner since Illumina RNASeq dataset");
                    return new List<string> { INVALID_FILE_TYPE };

                default:
                    // Other instruments; do not process them with MSFileInfoScanner

                    // Excluded instruments include:
                    // dot_wiff_files (AgilentQStarWiffFile): AgTOF02
                    // bruker_maldi_spot (BrukerMALDISpot): BrukerTOF_01
                    m_Msg = "Data type " + rawDataType + " not recognized";
                    LogWarning("clsPluginMain.GetDataFileOrDirectoryName: " + m_Msg);
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

                // File not found; check for alternative files or directores
                // This function also looks for .D directories
                var fileOrDirectoryNames = LookForAlternateFileOrDirectory(diDatasetDirectory, fileOrDirectoryName);

                if (fileOrDirectoryNames.Count > 0)
                    return fileOrDirectoryNames;

                m_Msg = "clsPluginMain.GetDataFileOrDirectoryName: File " + fileOrDirectoryPath + " not found";
                LogError(m_Msg);
                m_Msg = "File " + fileOrDirectoryPath + " not found";

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

            m_Msg = "clsPluginMain.GetDataFileOrDirectoryName; directory not found: " + fileOrDirectoryPath;
            LogError(m_Msg);
            m_Msg = "Directory not found: " + fileOrDirectoryPath;

            return new List<string>();

        }

        /// <summary>
        /// Construct the full path to the MSFileInfoScanner.DLL
        /// </summary>
        /// <returns></returns>
        private string GetMSFileInfoScannerDLLPath()
        {
            var msFileInfoScannerDir = m_MgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
            if (string.IsNullOrEmpty(msFileInfoScannerDir))
                return string.Empty;

            return Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");
        }

        /// <summary>
        /// A dataset file was not found
        /// Look for alternate dataset files, or look for .D directories
        /// </summary>
        /// <param name="diDatasetDirectory"></param>
        /// <param name="initialfileOrDirectoryName"></param>
        /// <returns></returns>
        private List<string> LookForAlternateFileOrDirectory(DirectoryInfo diDatasetDirectory, string initialfileOrDirectoryName)
        {

            // File not found; look for alternate extensions
            var lstAlternateExtensions = new List<string> { "mgf", "mzXML", "mzML" };

            foreach (var altExtension in lstAlternateExtensions)
            {
                var dataFileNamePathAlt = Path.ChangeExtension(initialfileOrDirectoryName, altExtension);
                if (File.Exists(dataFileNamePathAlt))
                {
                    m_Msg = "Data file not found, but ." + altExtension + " file exists";
                    LogMessage(m_Msg);
                    return new List<string> { INVALID_FILE_TYPE };
                }
            }

            // Look for dataset directories
            var primarydotDDirectory = new DirectoryInfo(Path.Combine(diDatasetDirectory.FullName, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

            var fileOrDirectoryNames = new List<string>();

            if (primarydotDDirectory.Exists)
            {
                // Look for a .mcf file in the .D directory
                var mcfFileExists = LookForMcfFileIndotDDirectory(primarydotDDirectory, out var dotDDirectoryName);
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
        /// <param name="didotDDirectory"></param>
        /// <param name="dotDDirectoryName">Output: name of the .D directory</param>
        /// <returns>True if a .mcf file is found</returns>
        private bool LookForMcfFileIndotDDirectory(DirectoryInfo didotDDirectory, out string dotDDirectoryName)
        {

            long mcfFileSizeBytes = 0;
            dotDDirectoryName = string.Empty;

            foreach (var fiFile in didotDDirectory.GetFiles("*.mcf"))
            {
                // Return the .mcf file that is the largest
                if (fiFile.Length > mcfFileSizeBytes)
                {
                    mcfFileSizeBytes = fiFile.Length;
                    dotDDirectoryName = didotDDirectory.Name;
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
            var appDirectory = clsUtilities.GetAppFolderPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppFolderPath returned an empty directory path to StoreToolVersionInfo for the Dataset Info plugin");
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

            // Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(pluginPath)
            };

            if (!string.IsNullOrEmpty(msFileInfoScannerDLLPath))
                ioToolFiles.Add(new FileInfo(msFileInfoScannerDLLPath));

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles, false);
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
        void m_MsFileScanner_ErrorEvent(string message, Exception ex)
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
                m_ErrOccurred = true;

                // Message often contains long paths; check for this and shorten them.
                // For example, switch
                // from: \\proto-6\LTQ_Orb_1\2015_4\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw
                // to:   QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw

                // Match text of the form         \\server\share\directory<anything>DatasetName.Extension
                var reDatasetFile = new Regex(@"\\\\[^\\]+\\[^\\]+\\[^\\]+.+(" + m_Dataset + @"\.[a-z0-9]+)");

                if (reDatasetFile.IsMatch(message))
                {
                    m_Msg = "Error running MSFileInfoScanner: " + reDatasetFile.Replace(message, "$1");
                }
                else
                {
                    m_Msg = "Error running MSFileInfoScanner: " + message;
                }

                LogError(errorMsg);
            }

        }

        /// <summary>
        /// Handles a warning event from MS file scanner
        /// </summary>
        /// <param name="message"></param>
        void m_MsFileScanner_WarningEvent(string message)
        {
            if (message.StartsWith("Unable to load data for scan"))
            {
                m_FailedScanCount++;

                if (m_FailedScanCount < 10)
                {
                    LogWarning(message);
                }
                else if (
                    m_FailedScanCount < 100 && m_FailedScanCount % 25 == 0 ||
                    m_FailedScanCount < 1000 && m_FailedScanCount % 250 == 0 ||
                    m_FailedScanCount % 500 == 0)
                {
                    LogWarning(string.Format("Unable to load data for {0} spectra", m_FailedScanCount));
                }
            }
            else
            {
                LogWarning(message);
            }

        }

        #endregion
    }

}
