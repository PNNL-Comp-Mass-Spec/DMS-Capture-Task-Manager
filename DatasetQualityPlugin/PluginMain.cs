//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//*********************************************************************************************************

using CaptureTaskManager;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using PRISMDatabaseUtils;

namespace DatasetQualityPlugin
{
    /// <summary>
    /// Dataset quality plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: Quameter, utf, frac, Lumos, orbi, Roc, idfree, monoisotope, Filepath, cpus, cfg, cmd

        private const int MAX_QUAMETER_RUNTIME_MINUTES = 150;

        private const string STORE_QUAMETER_RESULTS_SP_NAME = "StoreQuameterResults";
        private const string QUAMETER_IDFREE_METRICS_FILE = "Quameter_IDFree.tsv";
        private const string QUAMETER_CONSOLE_OUTPUT_FILE = "Quameter_Console_Output.txt";

        private const string FATAL_SPLINE_ERROR = "SPLINE_PCHIP_SET - Fatal error";
        private const string X_ARRAY_NOT_INCREASING = "X array not strictly increasing";

        private ToolReturnData mRetData = new();

        private bool mFatalSplineError;

        private DateTime mProcessingStartTime;

        private string mConsoleOutputFilePath;

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

            var msg = "Starting DatasetQualityPlugin.PluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return mRetData;
            }

            msg = "Creating dataset info for dataset '" + mDataset + "'";
            LogDebug(msg);

            if (MetaDataFile.CreateMetadataFile(mTaskParams))
            {
                // Everything was good
                msg = "Metadata file created for dataset " + mDataset;
                LogMessage(msg);

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                // There was a problem
                msg = "Problem creating metadata file for dataset " + mDataset + ". See local log for details";
                LogError(msg, true);
                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
                mRetData.EvalMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            var success = ConditionallyRunQuameter();

            ClearWorkDir();

            if (!success)
            {
                return mRetData;
            }

            msg = "Completed PluginMain.RunTool()";
            LogDebug(msg);

            return mRetData;
        }

        /// <summary>
        /// Determine whether or not we will run Quameter
        /// </summary>
        /// <returns>True if success (including if Quameter was skipped); false if an error</returns>
        /// <remarks>At present we only process Thermo .Raw files. Furthermore, if the file only contains MS/MS spectra, then it cannot be processed with Quameter</remarks>
        private bool ConditionallyRunQuameter()
        {
            // Set up the file paths
            var storageVolExt = mTaskParams.GetParam("Storage_Vol_External");
            var storagePath = mTaskParams.GetParam("Storage_Path");
            var datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, mDataset));
            string dataFilePathRemote;
            var runQuameter = false;

            var instClassName = mTaskParams.GetParam("Instrument_Class");

            var msg = "Instrument class: " + instClassName;
            LogDebug(msg);

            var instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);
            if (instrumentClass == InstrumentClassInfo.InstrumentClass.Unknown)
            {
                msg = "Instrument class not recognized: " + instClassName;
                LogError(msg);
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            // Define the generic Quameter skip reason
            var skipReason = "instrument class " + instClassName;

            switch (instrumentClass)
            {
                case InstrumentClassInfo.InstrumentClass.Finnigan_Ion_Trap:
                case InstrumentClassInfo.InstrumentClass.GC_QExactive:
                case InstrumentClassInfo.InstrumentClass.LTQ_FT:
                case InstrumentClassInfo.InstrumentClass.Thermo_Exactive:
                    dataFilePathRemote = Path.Combine(datasetFolder, mDataset + InstrumentClassInfo.DOT_RAW_EXTENSION);

                    // Confirm that the file has MS1 spectra (since Quameter requires that they be present)
                    if (!QuameterCanProcessDataset(mDatasetID, mDataset, datasetFolder, ref skipReason))
                    {
                        dataFilePathRemote = string.Empty;
                    }
                    break;

                case InstrumentClassInfo.InstrumentClass.Triple_Quad:
                    // Quameter crashes on TSQ files; skip them
                    dataFilePathRemote = string.Empty;
                    break;

                default:
                    dataFilePathRemote = string.Empty;
                    break;
            }

            if (!string.IsNullOrEmpty(dataFilePathRemote))
            {
                runQuameter = true;
            }

            // Store the version info in the database
            // Store the Quameter version if dataFileNamePath is not empty
            if (!StoreToolVersionInfo(runQuameter))
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mRetData.CloseoutMsg = "Error determining tool version info";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            if (!runQuameter)
            {
                msg = "Skipped Quameter since " + skipReason;
                mRetData.EvalMsg = msg;
                LogMessage(msg);
                return true;
            }

            var instrumentName = mTaskParams.GetParam("Instrument_Name");

            // Lumos datasets will fail with Quameter if they have unsupported scan types
            // 21T datasets will fail with Quameter if they only have one scan
            var ignoreQuameterFailure = instrumentName.StartsWith("Lumos", StringComparison.OrdinalIgnoreCase) ||
                                        instrumentName.StartsWith("21T", StringComparison.OrdinalIgnoreCase);

            var quameterExePath = GetQuameterPath();
            var quameterProgram = new FileInfo(quameterExePath);

            if (!quameterProgram.Exists)
            {
                mRetData.CloseoutMsg = "Quameter not found at " + quameterExePath;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var instrumentDataFile = new FileInfo(dataFilePathRemote);
            if (!instrumentDataFile.Exists)
            {
                // File has likely been purged from the storage server

                var datasetDirectory = mTaskParams.GetParam(mTaskParams.HasParam("Directory") ? "Directory" : "Folder");

                // Look in the Aurora archive (aurora.emsl.pnl.gov) using samba; was previously a2.emsl.pnl.gov
                var dataFilePathArchive = Path.Combine(mTaskParams.GetParam("Archive_Network_Share_Path"), datasetDirectory, instrumentDataFile.Name);

                var archiveFile = new FileInfo(dataFilePathArchive);
                if (archiveFile.Exists)
                {
                    // Update dataFilePathRemote using the archive file path
                    msg = "Dataset file not found on storage server (" + dataFilePathRemote + "), but was found in the archive at " + dataFilePathArchive;
                    LogMessage(msg);
                    dataFilePathRemote = dataFilePathArchive;
                }
                else
                {
                    dataFilePathRemote = string.Empty;
                    msg = "Dataset file not found on storage server (" + dataFilePathRemote + ") or in the archive (" + dataFilePathRemote + ")";
                    LogError(msg);
                    mRetData.CloseoutMsg = "Dataset file not found on storage server or in the archive";
                }
            }

            if (string.IsNullOrEmpty(dataFilePathRemote))
            {
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var success = ProcessThermoRawFile(dataFilePathRemote, instrumentClass, quameterProgram, ignoreQuameterFailure, instrumentName);

            if (success)
            {
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                // Quameter failed
                // Copy the Quameter log file to the Dataset QC folder
                // We only save the log file if an error occurs since it typically doesn't contain any useful information
                success = CopyFilesToDatasetFolder(datasetFolder);

                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            return success;
        }

        private void ClearWorkDir()
        {
            try
            {
                var workDir = new DirectoryInfo(mWorkDir);

                // Delete any files that start with the dataset name
                foreach (var file in workDir.GetFiles(mDataset + "*.*"))
                {
                    DeleteFileIgnoreErrors(file.FullName);
                }

                // Delete any files that contain Quameter
                foreach (var file in workDir.GetFiles("*Quameter*.*"))
                {
                    DeleteFileIgnoreErrors(file.FullName);
                }
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Convert the Quameter results to XML
        /// </summary>
        /// <param name="results"></param>
        /// <param name="xmlResults"></param>
        /// <returns></returns>
        private bool ConvertResultsToXML(IEnumerable<KeyValuePair<string, string>> results, out string xmlResults)
        {
            // XML will look like:

            // <?xml version="1.0" encoding="utf-8" standalone="yes"?>
            // <Quameter_Results>
            //   <Dataset>Shew119-01_17july02_earth_0402-10_4-20</Dataset>
            //   <Job>780000</Job>
            //   <Measurements>
            //     <Measurement Name="XIC-WideFrac">0.35347</Measurement>
            //     <Measurement Name="XIC-FWHM-Q1">20.7009</Measurement>
            //     <Measurement Name="XIC-FWHM-Q2">22.3192</Measurement>
            //     <Measurement Name="XIC-FWHM-Q3">24.794</Measurement>
            //     <Measurement Name="XIC-Height-Q2">1.08473</Measurement>
            //     etc.
            //   </Measurements>
            // </Quameter_Results>

            var xmlText = new StringBuilder();
            xmlResults = string.Empty;

            try
            {
                xmlText.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
                xmlText.Append("<Quameter_Results>");

                xmlText.Append("<Dataset>" + mDataset + "</Dataset>");
                xmlText.Append("<Job>" + mJob + "</Job>");

                xmlText.Append("<Measurements>");

                foreach (var result in results)
                {
                    xmlText.Append("<Measurement Name=\"" + result.Key + "\">" + result.Value + "</Measurement>");
                }

                xmlText.Append("</Measurements>");
                xmlText.Append("</Quameter_Results>");

                xmlResults = xmlText.ToString();
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Error converting Quameter results to XML";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool CopyFilesToDatasetFolder(string datasetDirectoryPath)
        {
            try
            {
                var datasetQCDirectory= new DirectoryInfo(Path.Combine(datasetDirectoryPath, "QC"));

                if (!datasetQCDirectory.Exists)
                {
                    datasetQCDirectory.Create();
                }

                if (!CopyFileToServer(QUAMETER_CONSOLE_OUTPUT_FILE, mWorkDir, datasetQCDirectory.FullName))
                {
                    return false;
                }

                // Uncomment the following to copy the Metrics file to the server
                //if (!CopyFileToServer(QUAMETER_IDFREE_METRICS_FILE, mWorkDir, datasetQCDirectory.FullName)) return false;

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Error creating the Dataset QC folder";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool CopyFileToServer(string sFileName, string sSourceFolder, string sTargetFolder)
        {
            try
            {
                var sSourceFilePath = Path.Combine(sSourceFolder, sFileName);

                if (File.Exists(sSourceFilePath))
                {
                    var sTargetFilePath = Path.Combine(sTargetFolder, sFileName);
                    mFileTools.CopyFile(sSourceFilePath, sTargetFilePath, true);
                }
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Error copying file " + sFileName + " to Dataset folder";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Construct the full path to Quameter.exe
        /// </summary>
        /// <returns></returns>
        private string GetQuameterPath()
        {
            // Typically C:\DMS_Programs\Quameter\x64\
            var sQuameterFolder = mMgrParams.GetParam("QuameterProgLoc", string.Empty);

            if (string.IsNullOrEmpty(sQuameterFolder))
            {
                return string.Empty;
            }

            return Path.Combine(sQuameterFolder, "Quameter.exe");
        }

        /// <summary>
        /// Extract the results from the Quameter results file
        /// </summary>
        /// <param name="ResultsFilePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private List<KeyValuePair<string, string>> LoadQuameterResults(string ResultsFilePath)
        {
            // The Quameter results file has two rows, a header row and a data row
            // Filename StartTimeStamp   XIC-WideFrac   XIC-FWHM-Q1   XIC-FWHM-Q2   XIC-FWHM-Q3   XIC-Height-Q2   etc.
            // QC_Shew_12_02_Run-06_4Sep12_Roc_12-03-30.RAW   2012-09-04T20:33:29Z   0.35347   20.7009   22.3192   24.794   etc.

            // The measurements are returned via this list
            var results = new List<KeyValuePair<string, string>>();

            if (!File.Exists(ResultsFilePath))
            {
                mRetData.CloseoutMsg = "Quameter Results file not found";
                LogWarning(mRetData.CloseoutMsg + ": " + ResultsFilePath);
                return results;
            }

            if (mDebugLevel >= 5)
            {
                LogDebug("Parsing Quameter Results file " + ResultsFilePath);
            }

            using var reader = new StreamReader(new FileStream(ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            string headerLine;
            if (!reader.EndOfStream)
            {
                // Read the header line
                headerLine = reader.ReadLine();
            }
            else
            {
                headerLine = string.Empty;
            }

            if (string.IsNullOrWhiteSpace(headerLine))
            {
                mRetData.CloseoutMsg = "Quameter Results file is empty (no header line)";
                LogWarning(mRetData.CloseoutMsg);
                return results;
            }

            // Parse the headers
            var headerNames = headerLine.Split('\t');

            string dataLine;
            if (!reader.EndOfStream)
            {
                // Read the data line
                dataLine = reader.ReadLine();
            }
            else
            {
                dataLine = string.Empty;
            }

            if (string.IsNullOrWhiteSpace(dataLine))
            {
                mRetData.CloseoutMsg = "Quameter Results file is empty (headers, but no data)";
                LogWarning(mRetData.CloseoutMsg);
                return results;
            }

            // Parse the data
            var dataValues = dataLine.Split('\t');

            if (headerNames.Length > dataValues.Length)
            {
                // More headers than data values
                mRetData.CloseoutMsg = "Quameter Results file data line (" + dataValues.Length + " items) does not match the header line (" + headerNames.Length + " items)";
                LogWarning(mRetData.CloseoutMsg);
                return results;
            }

            // Store the results by stepping through the arrays
            // Skip the first two items provided they are "filename" and "StartTimeStamp")
            var indexStart = 0;
            if (string.Equals(headerNames[indexStart], "filename", StringComparison.OrdinalIgnoreCase))
            {
                indexStart++;

                if (headerNames[indexStart].Equals("StartTimestamp", StringComparison.OrdinalIgnoreCase))
                {
                    indexStart++;
                }
                else
                {
                    LogWarning("The second column in the Quameter metrics file is not StartTimeStamp; this is unexpected");
                }
            }
            else
            {
                LogWarning("The first column in the Quameter metrics file is not Filename; this is unexpected");
            }

            for (var index = indexStart; index < headerNames.Length; index++)
            {
                if (string.IsNullOrWhiteSpace(headerNames[index]))
                {
                    LogWarning("Column " + (index + 1) + " in the Quameter metrics file is empty; this is unexpected");
                }
                else
                {
                    // Replace dashes with underscores in the metric names
                    var headerName = headerNames[index].Trim().Replace("-", "_");

                    string dataItem;
                    if (string.IsNullOrWhiteSpace(dataValues[index]))
                    {
                        dataItem = string.Empty;
                    }
                    else
                    {
                        dataItem = dataValues[index].Trim();
                    }

                    results.Add(new KeyValuePair<string, string>(headerName, dataItem));
                }
            }

            return results;
        }

        private void ParseConsoleOutputFile()
        {
            var unhandledException = false;
            var exceptionText = string.Empty;
            float metadataPercentComplete = 0;
            float peaksPercentComplete = 0;
            float precursorPercentComplete = 0;

            var metadataProgressMatcher = new Regex(@"Reading metadata: (?<ScansRead>\d+)/(?<TotalScans>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var peaksProgressMatcher = new Regex(@"Reading peaks: (?<ScansRead>\d+)/(?<TotalScans>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var precursorProgressMatcher = new Regex(@"Finding precursor peaks: (?<ScansRead>\d+)/(?<TotalScans>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // ReSharper disable CommentTypo

            // Example output from Quameter

            // Quameter 1.1.18254 (c18e43d39)
            // Vanderbilt University (c) 2012, D.Tabb/M.Chambers/S.Dasari
            // Licensed under the Apache License, Version 2.0
            //
            // ChromatogramMzLowerOffset: "10ppm"
            // ChromatogramMzUpperOffset: "10ppm"
            //        ChromatogramOutput: "0"
            //                Instrument: "orbi"
            //               MetricsType: "idfree"
            //  MonoisotopeAdjustmentSet: "[0,0] "
            //           NumChargeStates: "3"
            //            OutputFilepath: "Quameter_IDFree.tsv"
            //             RawDataFormat: "RAW"
            //               RawDataPath: ""
            //               ScoreCutoff: "0.050000000000000003"
            //       SpectrumListFilters: "peakPicking true 1-;threshold absolute 0.00000000001 most-intense"
            //     StatusUpdateFrequency: "5"
            //     UseMultipleProcessors: "1"
            //          WorkingDirectory: "C:\CapMan_WorkDir"
            // Opening source file Dataset_18Sep18.raw
            // Started processing file Dataset_18Sep18.raw
            // Reading metadata: 4347/4347
            // Reading peaks: 4347/4347
            // Finding precursor peaks: 3817/3817

            // ReSharper restore CommentTypo

            try
            {
                if (!File.Exists(mConsoleOutputFilePath))
                {
                    return;
                }

                using var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var trimmedLine = dataLine.Trim();

                    var metadataProgressMatched = UpdateProgress(dataLine, metadataProgressMatcher, ref metadataPercentComplete);
                    var peaksProgressMatched = UpdateProgress(dataLine, peaksProgressMatcher, ref peaksPercentComplete);
                    var precursorProgressMatched = UpdateProgress(dataLine, precursorProgressMatcher, ref precursorPercentComplete);

                    if (metadataProgressMatched || peaksProgressMatched || precursorProgressMatched)
                    {
                        continue;
                    }

                    if (unhandledException)
                    {
                        if (string.IsNullOrEmpty(exceptionText))
                        {
                            exceptionText = trimmedLine;
                        }
                        else
                        {
                            exceptionText = "; " + trimmedLine;
                        }
                    }
                    else if (trimmedLine.StartsWith("Error:"))
                    {
                        LogError("Quameter error: " + trimmedLine);
                    }
                    else if (trimmedLine.StartsWith("Unhandled Exception"))
                    {
                        LogError("Quameter error: " + trimmedLine);
                        unhandledException = true;
                    }
                    else if (trimmedLine.StartsWith(FATAL_SPLINE_ERROR))
                    {
                        mFatalSplineError = true;
                    }
                    else if (trimmedLine.StartsWith(X_ARRAY_NOT_INCREASING))
                    {
                        if (mFatalSplineError)
                        {
                            LogError(string.Format("Quameter error: {0}; {1}", FATAL_SPLINE_ERROR, trimmedLine));
                        }
                        else
                        {
                            LogError("Quameter error: " + trimmedLine);
                        }
                    }
                }

                if (!string.IsNullOrEmpty(exceptionText))
                {
                    LogError(exceptionText);
                }

                var percentComplete = metadataPercentComplete / 3 + peaksPercentComplete / 3 + precursorPercentComplete / 3;

                mStatusTools.UpdateAndWrite(EnumTaskStatusDetail.Running_Tool, percentComplete);
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseConsoleOutputFile: " + ex.Message);
            }
        }

        private void ParseDatasetInfoFile(string datasetFolderPath, string datasetName, ICollection<string> scanTypes, out int scanCount, out int scanCountMS)
        {
            var datasetInfoFile = new FileInfo(Path.Combine(datasetFolderPath, "QC", datasetName + "_DatasetInfo.xml"));

            scanCount = 0;
            scanCountMS = 0;
            scanTypes.Clear();

            if (!datasetInfoFile.Exists)
            {
                LogWarning("DatasetInfo.xml file not found at " + datasetInfoFile.FullName);
                return;
            }

            LogDebug("Reading scan counts from " + datasetInfoFile.FullName);

            using var reader = new XmlTextReader(new FileStream(datasetInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Write));

            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                {
                    continue;
                }

                switch (reader.Name)
                {
                    case "ScanCount":
                        scanCount = reader.ReadElementContentAsInt();
                        break;
                    case "ScanCountMS":
                        scanCountMS = reader.ReadElementContentAsInt();
                        break;

                    case "ScanType":
                        var scanType = reader.ReadElementContentAsString();
                        scanTypes.Add(scanType);
                        break;
                }
            }
        }

        private void PostProcessMetricsFile(string metricsOutputFileName)
        {
            var replaceOriginal = false;

            try
            {
                var correctedFilePath = metricsOutputFileName + ".new";

                using (var correctedFileWriter = new StreamWriter(new FileStream(correctedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    using var metricsReader = new StreamReader(new FileStream(metricsOutputFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    while (!metricsReader.EndOfStream)
                    {
                        var dataLine = metricsReader.ReadLine();

                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            if (dataLine.IndexOf("-1.#IND", StringComparison.Ordinal) > 0)
                            {
                                dataLine = dataLine.Replace("-1.#IND", string.Empty);
                                replaceOriginal = true;
                            }
                            correctedFileWriter.WriteLine(dataLine);
                        }
                        else
                        {
                            correctedFileWriter.WriteLine();
                        }
                    }
                }

                if (replaceOriginal)
                {
                    System.Threading.Thread.Sleep(100);

                    // Corrections were made; replace the original file
                    File.Copy(correctedFilePath, metricsOutputFileName, true);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error in PostProcessMetricsFile: " + ex.Message, ex);
            }
        }

        private bool PostQuameterResultsToDB(string xmlResults)
        {
            // Note that mDatasetID gets populated by runTool
            return PostQuameterResultsToDB(mDatasetID, xmlResults);
        }

        public bool PostQuameterResultsToDB(int datasetID, string xmlResults)
        {
            const int MAX_RETRY_COUNT = 3;
            const int SEC_BETWEEN_RETRIES = 20;

            try
            {
                var writeLog = mDebugLevel >= 5;
                LogDebug("Posting Quameter Results to the database (using Dataset ID " + datasetID + ")", writeLog);

                // We need to remove the encoding line from xmlResults before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

                var startIndex = xmlResults.IndexOf("?>", StringComparison.Ordinal);
                string xmlResultsClean;
                if (startIndex > 0)
                {
                    xmlResultsClean = xmlResults.Substring(startIndex + 2).Trim();
                }
                else
                {
                    xmlResultsClean = xmlResults;
                }

                // Call stored procedure StoreQuameterResults in the DMS_Capture database
                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(STORE_QUAMETER_RESULTS_SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@DatasetID", SqlType.Int).Value = datasetID;
                dbTools.AddParameter(cmd, "@ResultsXML", SqlType.XML).Value = xmlResultsClean;

                var resultCode = dbTools.ExecuteSP(cmd, MAX_RETRY_COUNT, SEC_BETWEEN_RETRIES);

                if (resultCode == DbUtilsConstants.RET_VAL_OK)
                {
                    // No errors
                    return true;
                }

                mRetData.CloseoutMsg = "Error storing Quameter Results in database, " + STORE_QUAMETER_RESULTS_SP_NAME + " returned " + resultCode;
                LogError(mRetData.CloseoutMsg);
                return false;
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception storing Quameter Results in database";
                LogError(mRetData.CloseoutMsg, ex);
                return false;
            }
        }

        private bool ProcessThermoRawFile(
            string dataFilePathRemote,
            InstrumentClassInfo.InstrumentClass instrumentClass,
            FileInfo quameterProgram,
            bool ignoreQuameterFailure,
            string instrumentName)
        {
            try
            {
                // Copy the appropriate config file to the working directory
                string configFileNameSource;

                switch (instrumentClass)
                {
                    case InstrumentClassInfo.InstrumentClass.Finnigan_Ion_Trap:
                        // Assume low-res precursor spectra
                        configFileNameSource = "quameter_ltq.cfg";
                        break;

                    case InstrumentClassInfo.InstrumentClass.GC_QExactive:
                    case InstrumentClassInfo.InstrumentClass.LTQ_FT:
                    case InstrumentClassInfo.InstrumentClass.Thermo_Exactive:
                    case InstrumentClassInfo.InstrumentClass.Triple_Quad:
                        // Assume high-res precursor spectra
                        configFileNameSource = "quameter_orbitrap.cfg";
                        break;
                    default:
                        // Assume high-res precursor spectra
                        configFileNameSource = "quameter_orbitrap.cfg";
                        LogWarning("Unexpected Thermo instrumentClass; will assume high-res precursor spectra");
                        break;
                }

                if (quameterProgram.DirectoryName == null)
                {
                    LogError("Unable to determine the parent directory path for " + quameterProgram.FullName);
                    return false;
                }

                var configFilePathSource = Path.Combine(quameterProgram.DirectoryName, configFileNameSource);
                var configFilePathTarget = Path.Combine(mWorkDir, configFileNameSource);

                if (!File.Exists(configFilePathSource) && quameterProgram.DirectoryName.EndsWith("x64", StringComparison.OrdinalIgnoreCase))
                {
                    // Using the 64-bit version of quameter
                    // Look for the .cfg file up one directory
                    var parentFolder = quameterProgram.Directory?.Parent;
                    if (parentFolder != null)
                    {
                        configFilePathSource = Path.Combine(parentFolder.FullName, configFileNameSource);
                    }
                }

                if (!File.Exists(configFilePathSource))
                {
                    mRetData.CloseoutMsg = "Quameter parameter file not found " + configFilePathSource;
                    LogWarning(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                File.Copy(configFilePathSource, configFilePathTarget, true);

                if (string.IsNullOrWhiteSpace(dataFilePathRemote))
                {
                    mRetData.CloseoutMsg = "Empty file path sent to ProcessThermoRawFile";
                    LogError(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                // Copy the .Raw file to the working directory
                // This message is logged if mDebugLevel == 5
                LogDebug("Copying the .Raw file from " + dataFilePathRemote);

                var dataFilePathLocal = Path.Combine(mWorkDir, Path.GetFileName(dataFilePathRemote));

                try
                {
                    mFileTools.CopyFile(dataFilePathRemote, dataFilePathLocal, true);
                }
                catch (Exception ex)
                {
                    mRetData.CloseoutMsg = "Exception copying the .Raw file locally";
                    LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                /*
                 *

                // Determine the first scan that is MS1 and the last scan number
                // Prior to September 2018, this was required for .raw files that start with MS2 spectra
                var scanInfoSuccess = ExamineThermoRawFileScans(dataFilePathLocal, out var firstMS1Scan, out var lastScan);
                if (!scanInfoSuccess)
                {
                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Error examining scans in the .raw file to determine the first MS1 scan";
                        LogError(mRetData.CloseoutMsg);
                    }

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }
                 *
                 */

                // Run Quameter
                mRetData.CloseoutMsg = string.Empty;
                var success = RunQuameter(quameterProgram, Path.GetFileName(dataFilePathLocal),
                                          QUAMETER_IDFREE_METRICS_FILE, ignoreQuameterFailure,
                                          instrumentName, configFilePathTarget);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Unknown error running Quameter";
                        LogError(mRetData.CloseoutMsg);
                    }

                    if (mTaskParams.HasParam("IgnoreQuameterErrors"))
                    {
                        var ignoreQuameterErrors = mTaskParams.GetParam("IgnoreQuameterErrors", false);
                        if (ignoreQuameterErrors)
                        {
                            mRetData.CloseoutMsg = "Quameter failed; ignoring because job parameter IgnoreQuameterErrors is True";
                            LogWarning(mRetData.CloseoutMsg);
                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                            mRetData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                            return true;
                        }
                    }

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception in ProcessThermoRawFile";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            return true;
        }

        private bool QuameterCanProcessDataset(int datasetID, string datasetName, string datasetFolderPath, ref string skipReason)
        {
            var sql =
                " SELECT Scan_Type, Scan_Count" +
                " FROM S_DMS_V_Dataset_Scans " +
                " WHERE (Dataset_ID = " + datasetID + ") ";

            var connectionString = mMgrParams.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var scanCount = 0;
            var scanCountMS = 0;
            var scanTypes = new List<string>();

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mTraceMode);
            RegisterEvents(dbTools);

            if (dbTools.GetQueryResultsDataTable(sql, out var table))
            {
                foreach (DataRow row in table.Rows)
                {
                    var scanType = row[0].CastDBVal(string.Empty);
                    var scanCountForType = row[1].CastDBVal(0);

                    scanTypes.Add(scanType);
                    scanCount += scanCountForType;

                    if (scanType.Equals("HMS", StringComparison.OrdinalIgnoreCase) ||
                        scanType.Equals("MS", StringComparison.OrdinalIgnoreCase) ||
                        scanType.Equals("Zoom-MS", StringComparison.OrdinalIgnoreCase))
                    {
                        scanCountMS += scanCountForType;
                    }
                }
            }

            if (scanCount == 0)
            {
                // Scan stats data is not yet in DMS
                // Look for the _DatasetInfo.xml file in the QC folder below the dataset folder

                ParseDatasetInfoFile(datasetFolderPath, datasetName, scanTypes, out scanCount, out scanCountMS);
            }

            if (scanCount > 0)
            {
                if (scanCountMS == 0)
                {
                    skipReason = "dataset does not have any HMS or MS spectra";
                    return false;
                }

                if (scanTypes.Count == 1 && scanTypes[0].Equals("SIM ms", StringComparison.OrdinalIgnoreCase))
                {
                    // The dataset only has SIM scans; Quameter does not support that
                    skipReason = "dataset only has SIM scans";
                    return false;
                }

                return true;
            }

            // The DatasetInfo.xml file was not found
            // We don't know if Quameter can process the dataset or not, so we'll err on the side of "Sure, let's give it a try"
            return true;
        }

        /// <summary>
        /// Read the Quameter results files, convert to XML, and post to DMS
        /// </summary>
        /// <param name="ResultsFilePath">Path to the Quameter results file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ReadAndStoreQuameterResults(string ResultsFilePath)
        {
            try
            {
                var results = LoadQuameterResults(ResultsFilePath);

                if (results.Count == 0)
                {
                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "No Quameter results were found";
                        LogError(mRetData.CloseoutMsg + ": results.Count == 0");
                    }

                    return false;
                }

                // Convert the results to XML format

                var success = ConvertResultsToXML(results, out var xmlResults);

                if (!success)
                {
                    return false;
                }

                // Store the results in the database
                var postSuccess = PostQuameterResultsToDB(xmlResults);

                if (postSuccess)
                {
                    return true;
                }

                if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                {
                    mRetData.CloseoutMsg = "Unknown error posting quameter results to the database";
                }

                return false;
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception parsing Quameter results";
                LogError("Exception parsing Quameter results and posting to the database", ex);
                return false;
            }
        }

        private bool RunQuameter(
            FileSystemInfo quameterProgram,
            string dataFileName,
            string metricsOutputFileName,
            bool ignoreQuameterFailure,
            string instrumentName,
            string configFilePath)
        {
            try
            {
                mFatalSplineError = false;

                // Construct the command line arguments
                // Always use "cpus 1" since it guarantees that the metrics will always be written out in the same order

                // Note that we could filter on scan range using the -SpectrumListFilters argument, e.g.
                // -SpectrumListFilters: "peakPicking true 1-;threshold absolute 0.00000000001 most-intense; scanNumber [14,24843]"

                var quameterArgs = new StringBuilder();

                quameterArgs.Append(Conversion.PossiblyQuotePath(dataFileName));
                quameterArgs.Append(" -MetricsType idfree");
                quameterArgs.Append(" -cfg " + Conversion.PossiblyQuotePath(configFilePath));
                quameterArgs.Append(" -OutputFilepath " + Conversion.PossiblyQuotePath(metricsOutputFileName));
                quameterArgs.Append(" -cpus 1");
                quameterArgs.Append(" -dump");

                // Create a batch file to run the command
                // Capture the console output (including output to the error stream) via redirection symbols:
                //    exePath arguments > ConsoleOutputFile.txt 2>&1

                const string batchFileName = "Run_Quameter.bat";

                // Update the Exe path to point to the RunProgram batch file; update arguments to be empty
                var exePath = Path.Combine(mWorkDir, batchFileName);
                var arguments = string.Empty;

                const string consoleOutputFileName = QUAMETER_CONSOLE_OUTPUT_FILE;

                // Create the batch file
                using (var batchFileWriter = new StreamWriter(new FileStream(exePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var batchCommand = quameterProgram.FullName + " " + quameterArgs + " > " + consoleOutputFileName + " 2>&1";

                    LogMessage("Creating " + batchFileName + " with: " + batchCommand);
                    batchFileWriter.WriteLine(batchCommand);
                }

                mConsoleOutputFilePath = Path.Combine(mWorkDir, consoleOutputFileName);

                var cmdRunner = new RunDosProgram(mWorkDir)
                {
                    CreateNoWindow = false,
                    EchoOutputToConsole = false,
                    CacheStandardOutput = false,
                    WriteConsoleOutputToFile = false        // We are using a batch file to capture the console output
                };

                // This will also call RegisterEvents
                AttachCmdRunnerEvents(cmdRunner);

                mProcessingStartTime = DateTime.UtcNow;
                mLastProgressUpdate = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusUpdateIntervalMinutes = 5;

                const int maxRuntimeSeconds = MAX_QUAMETER_RUNTIME_MINUTES * 60;
                var success = cmdRunner.RunProgram(exePath, arguments, "Quameter", true, maxRuntimeSeconds);

                // Parse the console output file one more time to check for errors
                ParseConsoleOutputFile();

                DetachCmdRunnerEvents(cmdRunner);

                if (!success)
                {
                    if (mFatalSplineError)
                    {
                        mRetData.CloseoutMsg = "Quameter failed; ignoring because a spline data validation error";
                        mRetData.EvalMsg = "Spline error; " + X_ARRAY_NOT_INCREASING;
                        LogWarning(mRetData.CloseoutMsg);
                        return true;
                    }

                    if (ignoreQuameterFailure)
                    {
                        mRetData.CloseoutMsg = "Quameter failed; ignoring because instrument " + instrumentName;
                        LogWarning(mRetData.CloseoutMsg);
                        return true;
                    }

                    mRetData.CloseoutMsg = "Error running Quameter";
                    LogError(mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("Quameter returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to Quameter failed (but exit code is 0)");
                    }

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                LogMessage("Quameter Complete");

                System.Threading.Thread.Sleep(100);

                var metricsOutputFilePath = Path.Combine(mWorkDir, metricsOutputFileName);

                if (!File.Exists(metricsOutputFilePath))
                {
                    mRetData.CloseoutMsg = "Metrics file was not created";
                    LogError(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                // Post-process the metrics output file to replace -1.#IND with empty strings
                PostProcessMetricsFile(metricsOutputFilePath);

                // Parse the metrics file and post to the database
                if (!ReadAndStoreQuameterResults(metricsOutputFilePath))
                {
                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Error parsing Quameter results";
                    }
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return false;
                }
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception in RunQuameter";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Initializes the dataset info tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting PluginMain.Setup()";

            // This message is logged if mDebugLevel == 5
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed PluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(bool storeQuameterVersion)
        {
            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = CTMUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Info plugin");
                return false;
            }

            // Lookup the version of the dataset quality plugin
            var pluginPath = Path.Combine(appDirectory, "DatasetQualityPlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
            {
                return false;
            }

            // Store path to CaptureToolPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(pluginPath)
            };

            if (storeQuameterVersion)
            {
                // Quameter is a C++ program, so we can only store the date
                toolFiles.Add(new FileInfo(GetQuameterPath()));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Examine the data line with the RegEx matcher
        /// If a match, compute the new progress
        /// </summary>
        /// <param name="dataLine"></param>
        /// <param name="progressMatcher"></param>
        /// <param name="percentComplete"></param>
        /// <returns></returns>
        private bool UpdateProgress(string dataLine, Regex progressMatcher, ref float percentComplete)
        {
            var match = progressMatcher.Match(dataLine);

            if (!match.Success)
            {
                return false;
            }

            var scansRead = int.Parse(match.Groups["ScansRead"].Value);
            var totalScans = int.Parse(match.Groups["TotalScans"].Value);

            percentComplete = scansRead / (float)totalScans * 100;
            return true;
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
            LogError("CmdRunner timeout reported");
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdate).TotalSeconds >= 30)
            {
                mLastProgressUpdate = DateTime.UtcNow;
                ParseConsoleOutputFile();
                return;
            }

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalMinutes >= mStatusUpdateIntervalMinutes)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                LogMessage(string.Format("Quameter running; {0:F1} minutes elapsed",
                                         DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes));

                // Increment mStatusUpdateIntervalMinutes by 5 minutes every time the status is logged, up to a maximum of 30
                if (mStatusUpdateIntervalMinutes < 30)
                {
                    mStatusUpdateIntervalMinutes += 5;
                }
            }
        }
    }
}
