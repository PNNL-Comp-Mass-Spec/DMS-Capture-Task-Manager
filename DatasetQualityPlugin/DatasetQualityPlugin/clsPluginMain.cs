//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Xml;
using CaptureTaskManager;

namespace DatasetQualityPlugin
{
    /// <summary>
    /// Dataset quality plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Constants and Enums"

        private const int MAX_QUAMETER_RUNTIME_MINUTES = 150;

        private const string STORE_QUAMETER_RESULTS_SP_NAME = "StoreQuameterResults";
        private const string QUAMETER_IDFREE_METRICS_FILE = "Quameter_IDFree.tsv";
        private const string QUAMETER_CONSOLE_OUTPUT_FILE = "Quameter_Console_Output.txt";

        #endregion

        #region "Class-wide variables"
        clsToolReturnData mRetData = new clsToolReturnData();

        DateTime mLastStatusUpdate = DateTime.UtcNow;
        DateTime mQuameterStartTime = DateTime.UtcNow;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset info step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {

            // Note that Debug messages are logged if m_DebugLevel == 5

            var msg = "Starting DatasetQualityPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            msg = "Creating dataset info for dataset '" + m_Dataset + "'";
            LogDebug(msg);

            if (clsMetaDataFile.CreateMetadataFile(m_MgrParams, m_TaskParams))
            {
                // Everything was good
                msg = "Metadata file created for dataset " + m_Dataset;
                LogMessage(msg);

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                // There was a problem
                msg = "Problem creating metadata file for dataset " + m_Dataset + ". See local log for details";
                LogError(msg, true);
                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
                mRetData.EvalMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            var success = ConditionallyRunQuameter();

            ClearWorkDir();

            if (!success)
                return mRetData;

            msg = "Completed clsPluginMain.RunTool()";
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
            var storageVolExt = m_TaskParams.GetParam("Storage_Vol_External");
            var storagePath = m_TaskParams.GetParam("Storage_Path");
            var datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, m_Dataset));
            string dataFilePathRemote;
            var runQuameter = false;

            var instClassName = m_TaskParams.GetParam("Instrument_Class");

            var msg = "Instrument class: " + instClassName;
            LogDebug(msg);

            var instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
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
                case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
                case clsInstrumentClassInfo.eInstrumentClass.GC_QExactive:
                case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
                case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
                    dataFilePathRemote = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);

                    // Confirm that the file has MS1 spectra (since Quameter requires that they be present)
                    if (!QuameterCanProcessDataset(m_DatasetID, m_Dataset, datasetFolder, ref skipReason))
                    {
                        dataFilePathRemote = string.Empty;
                    }

                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
                    // Quameter crashes on TSQ files; skip them
                    dataFilePathRemote = string.Empty;
                    break;
                default:
                    dataFilePathRemote = string.Empty;
                    break;
            }

            if (!string.IsNullOrEmpty(dataFilePathRemote))
                runQuameter = true;

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
                mRetData.EvalMsg = string.Copy(msg);
                LogMessage(msg);
                return true;
            }

            var instrumentName = m_TaskParams.GetParam("Instrument_Name");

            // Lumos datasets will fail with Quameter if they have unsupported scan types
            // 21T datasets will fail with Quameter if they only have one scan
            var ignoreQuameterFailure = instrumentName.StartsWith("Lumos", StringComparison.OrdinalIgnoreCase) ||
                                        instrumentName.StartsWith("21T", StringComparison.OrdinalIgnoreCase);

            var quameterExePath = GetQuameterPath();
            var fiQuameter = new FileInfo(quameterExePath);

            if (!fiQuameter.Exists)
            {
                mRetData.CloseoutMsg = "Quameter not found at " + quameterExePath;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            var instrumentDataFile = new FileInfo(dataFilePathRemote);
            if (!instrumentDataFile.Exists)
            {
                // File has likely been purged from the storage server
                // Look in the Aurora archive (aurora.emsl.pnl.gov) using samba; was previously a2.emsl.pnl.gov
                var dataFilePathArchive = Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"), m_TaskParams.GetParam("Folder"), instrumentDataFile.Name);

                var fiDataFileInArchive = new FileInfo(dataFilePathArchive);
                if (fiDataFileInArchive.Exists)
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

            var bSuccess = ProcessThermoRawFile(dataFilePathRemote, instrumentClass, fiQuameter, ignoreQuameterFailure, instrumentName);

            if (bSuccess)
            {
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                // Quameter failed
                // Copy the Quameter log file to the Dataset QC folder
                // We only save the log file if an error occurs since it typically doesn't contain any useful information
                bSuccess = CopyFilesToDatasetFolder(datasetFolder);

                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

            }

            return bSuccess;

        }

        private void ClearWorkDir()
        {

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                // Delete any files that start with the dataset name
                foreach (var fiFile in diWorkDir.GetFiles(m_Dataset + "*.*"))
                {
                    DeleteFileIgnoreErrors(fiFile.FullName);
                }

                // Delete any files that contain Quameter
                foreach (var fiFile in diWorkDir.GetFiles("*Quameter*.*"))
                {
                    DeleteFileIgnoreErrors(fiFile.FullName);
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
        /// <param name="lstResults"></param>
        /// <param name="sXMLResults"></param>
        /// <returns></returns>
        private bool ConvertResultsToXML(List<KeyValuePair<string, string>> lstResults, out string sXMLResults)
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

            var sbXML = new StringBuilder();
            sXMLResults = string.Empty;

            try
            {
                sbXML.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
                sbXML.Append("<Quameter_Results>");

                sbXML.Append("<Dataset>" + m_Dataset + "</Dataset>");
                sbXML.Append("<Job>" + m_Job + "</Job>");

                sbXML.Append("<Measurements>");

                foreach (var kvResult in lstResults)
                {
                    sbXML.Append("<Measurement Name=\"" + kvResult.Key + "\">" + kvResult.Value + "</Measurement>");
                }

                sbXML.Append("</Measurements>");
                sbXML.Append("</Quameter_Results>");

                sXMLResults = sbXML.ToString();

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Error converting Quameter results to XML";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;

        }

        private bool CopyFilesToDatasetFolder(string datasetFolder)
        {

            try
            {
                var diDatasetQCFolder = new DirectoryInfo(Path.Combine(datasetFolder, "QC"));

                if (!diDatasetQCFolder.Exists)
                {
                    diDatasetQCFolder.Create();
                }

                if (!CopyFileToServer(QUAMETER_CONSOLE_OUTPUT_FILE, m_WorkDir, diDatasetQCFolder.FullName))
                    return false;

                // Uncomment the following to copy the Metrics file to the server
                //if (!CopyFileToServer(QUAMETER_IDFREE_METRICS_FILE, m_WorkDir, diDatasetQCFolder.FullName)) return false;

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
                    m_FileTools.CopyFile(sSourceFilePath, sTargetFilePath, true);
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
        /// Open a Thermo .raw file and determine both the first MS1 scan and the scan count
        /// </summary>
        /// <param name="dataFilePath">Thermo .raw file path</param>
        /// <param name="firstMS1Scan">First MS1 scan; typically 1, but greater than one for some datasets</param>
        /// <param name="lastScan">Last Scan Number</param>
        /// <returns></returns>
        [Obsolete("Unused")]
        private bool ExamineThermoRawFileScans(string dataFilePath, out int firstMS1Scan, out int lastScan)
        {
            firstMS1Scan = 0;
            lastScan = 0;

            var firstMS2Scan = 0;

            try
            {
                var reader = new ThermoRawFileReader.XRawFileIO(dataFilePath);
                RegisterEvents(reader);

                var scanStart = reader.FileInfo.ScanStart;
                lastScan = reader.FileInfo.ScanEnd;

                for (var scanNumber = scanStart; scanNumber <= lastScan; scanNumber++)
                {
                    var msLevel = reader.GetMSLevel(scanNumber);
                    if (msLevel == 1)
                    {
                        firstMS1Scan = scanNumber;
                        break;
                    }

                    if (msLevel > 1 && firstMS2Scan == 0)
                    {
                        firstMS2Scan = scanNumber;
                    }
                }

                if (firstMS1Scan > 0)
                {
                    if (firstMS1Scan > 1)
                    {
                        LogMessage(string.Format("First MS1 scan for {0} is {1}", Path.GetFileName(dataFilePath), firstMS1Scan));
                    }
                    return true;
                }

                if (firstMS1Scan == 0 && firstMS2Scan == 0)
                {
                    mRetData.CloseoutMsg = "Dataset has no MS1 or MS2 spectra and is either corrupt or not supported; cannot process with Quameter";
                    LogWarning(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }

                if (firstMS1Scan == 0 && firstMS2Scan > 0)
                {
                    mRetData.CloseoutMsg = "Dataset only has MS2 spectra; cannot process with Quameter";
                    LogWarning(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }

                return false;
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception examining scans in the .raw file to determine the first MS1 scan";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

        }

        /// <summary>
        /// Construct the full path to Quameter.exe
        /// </summary>
        /// <returns></returns>
        private string GetQuameterPath()
        {

            // Typically C:\DMS_Programs\Quameter\x64\
            var sQuameterFolder = m_MgrParams.GetParam("QuameterProgLoc", string.Empty);

            if (string.IsNullOrEmpty(sQuameterFolder))
                return string.Empty;
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
            var lstResults = new List<KeyValuePair<string, string>>();

            if (!File.Exists(ResultsFilePath))
            {
                mRetData.CloseoutMsg = "Quameter Results file not found";
                LogDebug(mRetData.CloseoutMsg + ": " + ResultsFilePath);
                return lstResults;
            }

            if (m_DebugLevel >= 5)
            {
                LogDebug("Parsing Quameter Results file " + ResultsFilePath);
            }

            using (var reader = new StreamReader(new FileStream(ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
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
                    LogDebug(mRetData.CloseoutMsg);
                    return lstResults;
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
                    LogDebug(mRetData.CloseoutMsg);
                    return lstResults;
                }

                // Parse the data
                var dataValues = dataLine.Split('\t');

                if (headerNames.Length > dataValues.Length)
                {
                    // More headers than data values
                    mRetData.CloseoutMsg = "Quameter Results file data line (" + dataValues.Length + " items) does not match the header line (" + headerNames.Length + " items)";
                    LogDebug(mRetData.CloseoutMsg);
                    return lstResults;
                }

                // Store the results by stepping through the arrays
                // Skip the first two items provided they are "filename" and "StartTimeStamp")
                var indexStart = 0;
                if (headerNames[indexStart].ToLower() == "filename")
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
                        var sHeaderName = headerNames[index].Trim().Replace("-", "_");

                        string sDataItem;
                        if (string.IsNullOrWhiteSpace(dataValues[index]))
                            sDataItem = string.Empty;
                        else
                            sDataItem = string.Copy(dataValues[index]).Trim();

                        lstResults.Add(new KeyValuePair<string, string>(sHeaderName, sDataItem));
                    }

                }

            }

            return lstResults;

        }

        private void ParseConsoleOutputFileForErrors(string sConsoleOutputFilePath)
        {
            var unhandledException = false;
            var exceptionText = string.Empty;

            try
            {
                if (!File.Exists(mConsoleOutputFilePath))
                    return;

                using (var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (unhandledException)
                        {
                            if (string.IsNullOrEmpty(exceptionText))
                            {
                                exceptionText = string.Copy(dataLine);
                            }
                            else
                            {
                                exceptionText = "; " + dataLine;
                            }

                        }
                        else if (dataLine.StartsWith("Error:"))
                        {
                            LogError("Quameter error: " + dataLine);

                        }
                        else if (dataLine.StartsWith("Unhandled Exception"))
                        {
                            LogError("Quameter error: " + dataLine);
                            unhandledException = true;
                        }
                    }
                }

                if (!string.IsNullOrEmpty(exceptionText))
                {
                    LogError(exceptionText);
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in ParseConsoleOutputFileForErrors: " + ex.Message);
            }

        }

        private void ParseDatasetInfoFile(string datasetFolderPath, string datasetName, out int scanCount, out int scanCountMS)
        {
            var datasetInfoFile = new FileInfo(Path.Combine(datasetFolderPath, "QC", datasetName + "_DatasetInfo.xml"));

            scanCount = 0;
            scanCountMS = 0;

            if (!datasetInfoFile.Exists)
                return;

            using (var reader = new XmlTextReader(new FileStream(datasetInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Write)))
            {
                while (reader.Read())
                {
                    if (reader.NodeType != XmlNodeType.Element)
                        continue;

                    switch (reader.Name)
                    {
                        case "ScanCount":
                            scanCount = reader.ReadElementContentAsInt();
                            break;
                        case "ScanCountMS":
                            scanCountMS = reader.ReadElementContentAsInt();
                            break;
                    }
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
                    using (var metricsReader = new StreamReader(new FileStream(metricsOutputFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!metricsReader.EndOfStream)
                        {
                            var dataLine = metricsReader.ReadLine();

                            if (!string.IsNullOrEmpty(dataLine))
                            {
                                if (dataLine.IndexOf("-1.#IND", StringComparison.Ordinal) > 0)
                                {
                                    dataLine = dataLine.Replace("-1.#IND", "");
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

        private bool PostQuameterResultsToDB(string sXMLResults)
        {

            // Note that m_DatasetID gets populated by runTool
            return PostQuameterResultsToDB(m_DatasetID, sXMLResults);

        }

        public bool PostQuameterResultsToDB(int intDatasetID, string sXMLResults)
        {

            const int MAX_RETRY_COUNT = 3;
            const int SEC_BETWEEN_RETRIES = 20;

            bool blnSuccess;

            try
            {
                var writeLog = m_DebugLevel >= 5;
                LogDebug("Posting Quameter Results to the database (using Dataset ID " + intDatasetID + ")", writeLog);

                // We need to remove the encoding line from sXMLResults before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

                var intStartIndex = sXMLResults.IndexOf("?>", StringComparison.Ordinal);
                string sXMLResultsClean;
                if (intStartIndex > 0)
                {
                    sXMLResultsClean = sXMLResults.Substring(intStartIndex + 2).Trim();
                }
                else
                {
                    sXMLResultsClean = sXMLResults;
                }

                // Call stored procedure StoreQuameterResults in the DMS_Capture database

                var spCmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = STORE_QUAMETER_RESULTS_SP_NAME
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID;
                spCmd.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = sXMLResultsClean;

                var resultCode = m_CaptureDBProcedureExecutor.ExecuteSP(spCmd, MAX_RETRY_COUNT, SEC_BETWEEN_RETRIES);

                if (resultCode == PRISM.clsExecuteDatabaseSP.RET_VAL_OK)
                {
                    // No errors
                    blnSuccess = true;
                }
                else
                {
                    mRetData.CloseoutMsg = "Error storing Quameter Results in database, " + STORE_QUAMETER_RESULTS_SP_NAME + " returned " + resultCode;
                    LogError(mRetData.CloseoutMsg);
                    blnSuccess = false;
                }

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception storing Quameter Results in database";
                LogError(mRetData.CloseoutMsg, ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        private bool ProcessThermoRawFile(
            string dataFilePathRemote,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            FileInfo fiQuameter,
            bool ignoreQuameterFailure,
            string instrumentName)
        {

            try
            {

                // Copy the appropriate config file to the working directory
                string configFileNameSource;

                switch (instrumentClass)
                {
                    case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
                        // Assume low-res precursor spectra
                        configFileNameSource = "quameter_ltq.cfg";
                        break;

                    case clsInstrumentClassInfo.eInstrumentClass.GC_QExactive:
                    case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
                    case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
                    case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
                        // Assume high-res precursor spectra
                        configFileNameSource = "quameter_orbitrap.cfg";
                        break;
                    default:
                        // Assume high-res precursor spectra
                        configFileNameSource = "quameter_orbitrap.cfg";
                        LogWarning("Unexpected Thermo instrumentClass; will assume high-res precursor spectra");
                        break;
                }

                if (fiQuameter.DirectoryName == null)
                {
                    LogError("Unable to determine the parent directory path for " + fiQuameter.FullName);
                    return false;
                }

                var configFilePathSource = Path.Combine(fiQuameter.DirectoryName, configFileNameSource);
                var configFilePathTarget = Path.Combine(m_WorkDir, configFileNameSource);

                if (!File.Exists(configFilePathSource) && fiQuameter.DirectoryName.ToLower().EndsWith("x64"))
                {
                    // Using the 64-bit version of quameter
                    // Look for the .cfg file up one directory
                    var parentFolder = fiQuameter.Directory?.Parent;
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
                // This message is logged if m_DebugLevel == 5
                LogDebug("Copying the .Raw file from " + dataFilePathRemote);

                var dataFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(dataFilePathRemote));

                try
                {
                    m_FileTools.CopyFile(dataFilePathRemote, dataFilePathLocal, true);
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
                var success = RunQuameter(fiQuameter, Path.GetFileName(dataFilePathLocal),
                                           QUAMETER_IDFREE_METRICS_FILE, ignoreQuameterFailure,
                                           instrumentName, configFilePathTarget);

                if (!success)
                {

                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Unknown error running Quameter";
                        LogError(mRetData.CloseoutMsg);
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
                " SELECT SUM(Scan_Count) AS Scans, " +
                       " SUM(CASE WHEN Scan_Type IN ('HMS', 'MS', 'Zoom-MS') THEN Scan_Count ELSE 0 END) AS MS_Scans" +
                " FROM S_DMS_V_Dataset_Scans " +
                " WHERE (Dataset_ID = " + datasetID + ") ";

            var sConnectionString = m_MgrParams.GetParam("ConnectionString");

            var scanCount = 0;
            var scanCountMS = 0;

            using (var cnDB = new SqlConnection(sConnectionString))
            {
                cnDB.Open();

                var spCmd = new SqlCommand(sql, cnDB);
                var reader = spCmd.ExecuteReader();

                if (reader.Read())
                {
                    if (!reader.IsDBNull(0))
                        scanCount = reader.GetInt32(0);

                    if (!reader.IsDBNull(1))
                        scanCountMS = reader.GetInt32(1);
                }
            }

            if (scanCount == 0)
            {
                // Scan stats data is not yet in DMS
                // Look for the _DatasetInfo.xml file in the QC folder below the dataset folder

                ParseDatasetInfoFile(datasetFolderPath, datasetName, out scanCount, out scanCountMS);

            }

            if (scanCount > 0)
            {
                if (scanCountMS == 0)
                {
                    skipReason = "dataset does not have any HMS or MS spectra";
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

            var blnSuccess = false;

            try
            {
                var lstResults = LoadQuameterResults(ResultsFilePath);

                if (lstResults.Count == 0)
                {
                    if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "No Quameter results were found";
                        LogError(mRetData.CloseoutMsg + ": lstResults.Count == 0");
                    }

                }
                else
                {
                    // Convert the results to XML format

                    blnSuccess = ConvertResultsToXML(lstResults, out var sXMLResults);

                    if (blnSuccess)
                    {
                        // Store the results in the database
                        blnSuccess = PostQuameterResultsToDB(sXMLResults);

                        if (!blnSuccess)
                        {
                            if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                            {
                                mRetData.CloseoutMsg = "Unknown error posting quameter results to the database";
                            }
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception parsing Quameter results";
                LogError("Exception parsing Quameter results and posting to the database", ex);
                blnSuccess = false;
            }

            return blnSuccess;

        }

        private bool RunQuameter(
            FileSystemInfo fiQuameter,
            string dataFileName,
            string metricsOutputFileName,
            bool ignoreQuameterFailure,
            string instrumentName,
            string configFilePath,
            int firstMS1Scan = 0,
            int lastScan = 0)
        {
            clsRunDosProgram cmdRunner = null;
            try
            {
                // Construct the command line arguments
                // Always use "cpus 1" since it guarantees that the metrics will always be written out in the same order
                var quameterArgs = new StringBuilder();

                quameterArgs.Append(clsConversion.PossiblyQuotePath(dataFileName));
                quameterArgs.Append(" -MetricsType idfree");
                quameterArgs.Append(" -cfg " + clsConversion.PossiblyQuotePath(configFilePath));
                quameterArgs.Append(" -OutputFilepath " + clsConversion.PossiblyQuotePath(metricsOutputFileName));
                quameterArgs.Append(" -cpus 1");
                quameterArgs.Append(" -dump");

                if (firstMS1Scan > 1)
                {
                    // Append the -SpectrumListFilters argument, e.g.
                    // -SpectrumListFilters: "peakPicking true 1-;threshold absolute 0.00000000001 most-intense; scanNumber [14,24843]"
                    quameterArgs.Append(" -SpectrumListFilters: \"");
                    quameterArgs.Append("peakPicking true 1-;");
                    quameterArgs.Append("threshold absolute 0.00000000001 most-intense;");
                    quameterArgs.Append(string.Format("scanNumber [{0},{1}]", firstMS1Scan, lastScan));
                    quameterArgs.Append("\"");
                }

                cmdRunner = new clsRunDosProgram(m_WorkDir);
                mQuameterStartTime = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;

                // This will also call RegisterEvents
                AttachCmdRunnerEvents(cmdRunner);

                // Create a batch file to run the command
                // Capture the console output (including output to the error stream) via redirection symbols:
                //    strExePath CmdStr > ConsoleOutputFile.txt 2>&1

                const string batchFileName = "Run_Quameter.bat";

                // Update the Exe path to point to the RunProgram batch file; update CmdStr to be empty
                var exePath = Path.Combine(m_WorkDir, batchFileName);
                var cmdStr = string.Empty;

                const string consoleOutputFileName = QUAMETER_CONSOLE_OUTPUT_FILE;

                // Create the batch file
                using (var batchFileWriter = new StreamWriter(new FileStream(exePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var batchCommand = fiQuameter.FullName + " " + quameterArgs + " > " + consoleOutputFileName + " 2>&1";

                    LogMessage("Creating " + batchFileName + " with: " + batchCommand);
                    batchFileWriter.WriteLine(batchCommand);
                }

                System.Threading.Thread.Sleep(100);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = false;

                const int iMaxRuntimeSeconds = MAX_QUAMETER_RUNTIME_MINUTES * 60;
                var bSuccess = cmdRunner.RunProgram(sExePath, cmdStr, "Quameter", true, iMaxRuntimeSeconds);

                ParseConsoleOutputFileForErrors(Path.Combine(m_WorkDir, consoleOutputFileName));

                if (!bSuccess)
                {
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

                // This message is logged if m_DebugLevel == 5
                LogDebug("Quameter Complete");

                System.Threading.Thread.Sleep(100);

                var metricsOutputFilePath = Path.Combine(m_WorkDir, metricsOutputFileName);

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
            finally
            {
                DetachCmdRunnerEvents(cmdRunner);
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
            var msg = "Starting clsPluginMain.Setup()";

            // This message is logged if m_DebugLevel == 5
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed clsPluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(bool storeQuameterVersion)
        {

            LogDebug("Determining tool version info");

            var strToolVersionInfo = string.Empty;
            var appDirectory = clsUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Info plugin");
                return false;
            }

            // Lookup the version of the dataset quality plugin
            var sPluginPath = Path.Combine(appDirectory, "DatasetQualityPlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, sPluginPath);
            if (!bSuccess)
                return false;

            // Store path to CaptureToolPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(sPluginPath)
            };

            if (storeQuameterVersion)
            {
                // Quameter is a C++ program, so we can only store the date
                toolFiles.Add(new FileInfo(GetQuameterPath()));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        #endregion

        #region "Event handlers"

        private void AttachCmdRunnerEvents(clsRunDosProgram cmdRunner)
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

        private void DetachCmdRunnerEvents(clsRunDosProgram cmdRunner)
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

        void CmdRunner_Timeout()
        {
            LogError("CmdRunner timeout reported");
        }

        void CmdRunner_LoopWaiting()
        {

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 300)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                // This message is logged to disk m_DebugLevel == 5
                LogDebug("Quameter running; " + DateTime.UtcNow.Subtract(mQuameterStartTime).TotalMinutes + " minutes elapsed");
            }
        }

        #endregion

    }

}
