
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//
// Last modified 10/06/2009
//               05/04/2010 dac - Added Matt Monroe MSFileScanner for quality checks
//               09/17/2012 mem - Moved from the DatasetQuality plugin to the Dataset Info plugin
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Reflection;
using CaptureTaskManager;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using MSFileInfoScannerInterfaces;

namespace DatasetInfoPlugin
{

    public class clsPluginMain : clsToolRunnerBase
    {
        //*********************************************************************************************************
        // Main class for plugin
        //**********************************************************************************************************

        #region "Constants"
        const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";
        const string UNKNOWN_FILE_TYPE = "Unknown File Type";
        const string INVALID_FILE_TYPE = "Invalid File Type";

        private const bool IGNORE_BRUKER_BAF_ERRORS = false;
        #endregion

        #region "Class-wide variables"
        iMSFileInfoScanner m_MsFileScanner;
        string m_Msg;
        bool m_ErrOccurred;
        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset info step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting DatasetInfoPlugin.clsPluginMain.RunTool()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            // Perform base class operations, if any
            var retData = base.RunTool();
            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return retData;

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                retData.CloseoutMsg = "Error determining tool version info";
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            msg = "Running DatasetInfo on dataset '" + m_Dataset + "'";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

            retData = RunMsFileInfoScanner();

            msg = "Completed clsPluginMain.RunTool()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            return retData;
        }


        private iMSFileInfoScanner LoadMSFileInfoScanner(string strMSFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

            iMSFileInfoScanner objMSFileInfoScanner = null;
            string msg;

            try
            {
                if (!File.Exists(strMSFileInfoScannerDLLPath))
                {
                    msg = "DLL not found: " + strMSFileInfoScannerDLLPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                }
                else
                {
                    var obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath);
                    if (obj != null)
                    {
                        objMSFileInfoScanner = (iMSFileInfoScanner)obj;
                        msg = "Loaded MSFileInfoScanner from " + strMSFileInfoScannerDLLPath;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }

            return objMSFileInfoScanner;
        }

        private object LoadObject(string className, string strDLLFilePath)
        {
            object obj = null;
            try
            {
                // Dynamically load the specified class from strDLLFilePath
                var assem = Assembly.LoadFrom(strDLLFilePath);
                var dllType = assem.GetType(className, false, true);
                obj = Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                var msg = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            base.Setup(mgrParams, taskParams, statusTools);

            var strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
            if (string.IsNullOrEmpty(strMSFileInfoScannerPath))
                throw new NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");

            if (!File.Exists(strMSFileInfoScannerPath))
            {
                throw new FileNotFoundException("File Not Found: " + strMSFileInfoScannerPath);
            }

            // Initialize the MSFileScanner class
            m_MsFileScanner = LoadMSFileInfoScanner(strMSFileInfoScannerPath);

            m_MsFileScanner.ErrorEvent += m_MsFileScanner_ErrorEvent;
            m_MsFileScanner.MessageEvent += m_MsFileScanner_MessageEvent;

            msg = "Completed clsPluginMain.Setup()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
        }

        /// <summary>
        /// Runs the MS_File_Info_Scanner tool
        /// </summary>
        /// <returns></returns>
        private clsToolReturnData RunMsFileInfoScanner()
        {
            var result = new clsToolReturnData();

            // Always use client perspective for the source folder (allows MSFileInfoScanner to run from any CTM)
            var sourceFolder = m_TaskParams.GetParam("Storage_Vol_External");

            // Set up the rest of the paths
            sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Storage_Path"));
            sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Folder"));
            var outputPathBase = Path.Combine(sourceFolder, "QC");
            bool bSkipPlots;

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

            m_MsFileScanner.CheckCentroidingStatus = true;

            // Get the input file name
            clsInstrumentClassInfo.eRawDataType rawDataType;
            clsInstrumentClassInfo.eInstrumentClass instrumentClass;
            bool bBrukerDotDBaf;
            var sFileOrFolderNames = GetDataFileOrFolderName(sourceFolder, out bSkipPlots, out rawDataType, out instrumentClass, out bBrukerDotDBaf);

            if (sFileOrFolderNames.Count > 0 && sFileOrFolderNames.First() == UNKNOWN_FILE_TYPE)
            {
                // Raw_Data_Type not recognized
                result.CloseoutMsg = m_Msg;
                result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return result;
            }

            if (sFileOrFolderNames.Count > 0 && sFileOrFolderNames.First() == INVALID_FILE_TYPE)
            {
                // DS quality test not implemented for this file type
                result.CloseoutMsg = string.Empty;
                result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                result.EvalMsg = "Dataset info test not implemented for data type " + clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " + clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                result.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                return result;
            }

            if (sFileOrFolderNames.Count == 0 || string.IsNullOrEmpty(sFileOrFolderNames.First()))
            {
                // There was a problem with getting the file name; Details reported by called method
                result.CloseoutMsg = m_Msg;
                result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return result;
            }

            if (bSkipPlots)
            {
                // Do not create any plots
                m_MsFileScanner.SaveTICAndBPIPlots = false;
                m_MsFileScanner.SaveLCMS2DPlots = false;
            }

            // Make the output folder
            if (!Directory.Exists(outputPathBase))
            {
                try
                {
                    Directory.CreateDirectory(outputPathBase);
                    var msg = "clsPluginMain.RunMsFileInfoScanner: Created output folder " + outputPathBase;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                }
                catch (Exception ex)
                {
                    var msg = "clsPluginMain.RunMsFileInfoScanner: Exception creating output folder " + outputPathBase;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                    result.CloseoutMsg = "Exception creating output folder " + outputPathBase;
                    result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return result;
                }
            }

            // Call the file scanner DLL
            // Typically only call it once, but for Bruker datasets with multiple .D folders, we'll call it once for each .D folder

            m_ErrOccurred = false;
            m_Msg = string.Empty;

            var cachedDatasetInfoXML = new List<string>();
            var outputFolderNames = new List<string>();
            var primaryFileOrFolderProcessed = false;
            var nextSubFolderSuffix = 1;

            foreach (var datasetFileOrFolder in sFileOrFolderNames)
            {
                var currentOutputFolder = ConstructOutputFolderPath(
                    outputPathBase, datasetFileOrFolder, sFileOrFolderNames.Count,
                    outputFolderNames, ref nextSubFolderSuffix);

                var successProcessing = m_MsFileScanner.ProcessMSFileOrFolder(Path.Combine(sourceFolder, datasetFileOrFolder), currentOutputFolder);

                if (m_ErrOccurred)
                    successProcessing = false;

                if (successProcessing)
                {
                    cachedDatasetInfoXML.Add(m_MsFileScanner.DatasetInfoXML);
                    primaryFileOrFolderProcessed = true;
                    continue;
                }

                // Either a bad result code was returned, or an error event was received

                if (bBrukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
                {
                    // 12T_FTICR_B datasets (with .D folders and analysis.baf and/or fid files) sometimes work with MSFileInfoscanner, and sometimes don't
                    // The problem is that ProteoWizard doesn't support certain forms of these datasets
                    // In particular, small datasets (lasting just a few seconds) don't work

                    result.CloseoutMsg = string.Empty;
                    result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    result.EvalMsg = "MSFileInfoScanner error for data type " +
                                     clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " +
                                     clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
                    result.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    return result;

                }

                if (primaryFileOrFolderProcessed)
                {
                    // MSFileInfoScanner already processed the primary file or folder
                    // Mention this failure in the EvalMsg but still return success
                    result.EvalMsg = AppendToComment(result.EvalMsg,
                                                     "ProcessMSFileOrFolder returned false for " + datasetFileOrFolder);
                }
                else
                {
                    if (string.IsNullOrEmpty(m_Msg))
                    {
                        m_Msg = "ProcessMSFileOrFolder returned false. Message = " +
                                m_MsFileScanner.GetErrorMessage() +
                                " Result code = " + (int)m_MsFileScanner.ErrorCode;
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);

                    result.CloseoutMsg = m_Msg;
                    result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return result;
                }

            } // foreach file in sFileOrFolderNames

            // Merge the dataset info defined in cachedDatasetInfoXML 
            // If cachedDatasetInfoXml contains just one item, simply return it
            var datasetXmlMerger = new clsDatasetInfoXmlMerger();
            var dsInfoXML = CombineDatasetInfoXML(datasetXmlMerger, cachedDatasetInfoXML);

            if (cachedDatasetInfoXML.Count > 1)
            {
                ProcessMultiDatasetInfoScannerResults(outputPathBase, datasetXmlMerger, dsInfoXML, outputFolderNames);
            }

            // Check for dataset acq time gap warnings            
            if (AcqTimeWarningsReported(datasetXmlMerger, result))
                return result;

            // Call SP CacheDatasetInfoXML to store dsInfoXML in table T_Dataset_Info_XML
            PostDatasetInfoXml(dsInfoXML, result);

            return result;

        }

        /// <summary>
        /// Examine datasetXmlMerger.AcqTimeWarnings
        /// If non-empty, summarize the errors and update result
        /// </summary>
        /// <param name="datasetXmlMerger"></param>
        /// <param name="result"></param>
        /// <returns>True if warnings exist, otherwise false</returns>
        private bool AcqTimeWarningsReported(clsDatasetInfoXmlMerger datasetXmlMerger, clsToolReturnData result)
        {
            if (datasetXmlMerger.AcqTimeWarnings.Count == 0)
            {
                return false;
            }

            // Large gap found
            // Log the error and do not post the XML file to the database
            // You could manually add the file later by reading from disk and adding to table T_Dataset_Info_XML

            foreach (var warning in datasetXmlMerger.AcqTimeWarnings)
            {
                m_Msg = AppendToComment(m_Msg, warning);
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);

            result.CloseoutMsg = "Large gap between acq times: " + datasetXmlMerger.AcqTimeWarnings.FirstOrDefault();
            result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

            return true;
        }

        private void PostDatasetInfoXml(string dsInfoXML, clsToolReturnData result)
        {
            var iPostCount = 0;
            var connectionString = m_MgrParams.GetParam("connectionstring");

            var iDatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

            var successPosting = false;

            while (iPostCount <= 2)
            {
                successPosting = m_MsFileScanner.PostDatasetInfoUseDatasetID(iDatasetID, dsInfoXML, connectionString,
                                                                             MS_FILE_SCANNER_DS_INFO_SP);

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
                m_Msg = "Error running info scanner. Message = " +
                        m_MsFileScanner.GetErrorMessage() + " Result code = " + (int)m_MsFileScanner.ErrorCode;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
            }

            if (errorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError)
            {
                // Everything went wonderfully
                result.CloseoutMsg = string.Empty;
                result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                // Either a bad result code was returned, or an error event was received
                result.CloseoutMsg = "MSFileInfoScanner error";
                result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ProcessMultiDatasetInfoScannerResults(
            string outputPathBase,
            clsDatasetInfoXmlMerger datasetXmlMerger,
            string dsInfoXML, 
            IEnumerable<string> outputFolderNames)
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
            }

            try
            {
                var pngMatcher = new Regex(@"""(?<Filename>[^""]+\.png)""");

                // Create an index.html file that shows all of the plots in the subfolders
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

                    foreach (var subfolderName in outputFolderNames)
                    {
                        var diSubFolder = new DirectoryInfo(Path.Combine(outputPathBase, subfolderName));
                        var htmlFiles = diSubFolder.GetFiles("index.html");
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
                                        // Match found; prepend the subfolder name
                                        dataLine = pngMatcher.Replace(dataLine, '"' + subfolderName + "/${Filename}" + '"');
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

                        htmlWriter.WriteLine("          <tr><td>" + scanTypeName  + "</td><td align=\"center\">" + scanCount  + "</td><td></td></tr>");
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
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
        /// <param name="diDatasetFolder">Dataset folder</param>
        /// <returns>Returns the file name if found, otherwise an empty string</returns>
        private string CheckForBrukerImagingZipFiles(DirectoryInfo diDatasetFolder)
        {
            var fiFiles = diDatasetFolder.GetFiles("0_R*X*.zip");

            if (fiFiles.Length > 0)
            {
                return fiFiles[0].Name;
            }

            return string.Empty;
        }

        /// <summary>
        /// Determine the appropriate output folder path
        /// If we are only processing one dataset file or folder, the output folder path is simply outputPathBase
        /// Otherwise, it is based on the current file or folder being processed (datasetFileOrFolder)
        /// nextSubFolderSuffix is used to avoid folder name conflicts
        /// </summary>
        /// <param name="outputPathBase"></param>
        /// <param name="datasetFileOrFolder"></param>
        /// <param name="totalDatasetFilesOrFolders"></param>
        /// <param name="outputFolderNames"></param>
        /// <param name="nextSubFolderSuffix">Input/output parameter</param>
        /// <returns>Full path to the output folder to use for the current file or folder being processed</returns>
        private string ConstructOutputFolderPath(
            string outputPathBase,
            string datasetFileOrFolder,
            int totalDatasetFilesOrFolders,
            ICollection<string> outputFolderNames,
            ref int nextSubFolderSuffix)
        {

            string currentOutputFolder;

            if (totalDatasetFilesOrFolders > 1)
            {
                var subFolder = Path.GetFileNameWithoutExtension(datasetFileOrFolder);
                if (string.IsNullOrWhiteSpace(subFolder))
                {
                    var subFolderToUse = m_Dataset + "_" + nextSubFolderSuffix;
                    while (outputFolderNames.Contains(subFolderToUse))
                    {
                        nextSubFolderSuffix++;
                        subFolderToUse = m_Dataset + "_" + nextSubFolderSuffix;
                    }

                    currentOutputFolder = Path.Combine(outputPathBase, subFolderToUse);
                    outputFolderNames.Add(subFolderToUse);
                }
                else
                {
                    var subFolderToUse = string.Copy(subFolder);
                    while (outputFolderNames.Contains(subFolderToUse))
                    {
                        subFolderToUse = subFolder + "_" + nextSubFolderSuffix;
                        nextSubFolderSuffix++;
                    }

                    currentOutputFolder = Path.Combine(outputPathBase, subFolderToUse);
                    outputFolderNames.Add(subFolderToUse);
                }
            }
            else
            {
                currentOutputFolder = outputPathBase;
            }
            return currentOutputFolder;
        }

        /// <summary>
        /// Look for .D folders below diDatasetFolder
        /// Add them to list fileOrFolderNames
        /// </summary>
        /// <param name="diDatasetFolder">Dataset folder to examine</param>
        /// <param name="fileOrFolderNames">List to append .D folders to (calling function must initialize)</param>
        private void FindDotDFolders(DirectoryInfo diDatasetFolder, ICollection<string> fileOrFolderNames)
        {
            var diDotDFolders = diDatasetFolder.GetDirectories("*.d");
            if (diDotDFolders.Length <= 0)
                return;

            // Look for a .mcf file in each of the .D folders
            foreach (var dotDFolder in diDotDFolders)
            {
                string dotDFolderName;
                var mcfFileExists = LookForMcfFileInDotDFolder(dotDFolder, out dotDFolderName);
                if (mcfFileExists && !fileOrFolderNames.Contains(dotDFolderName))
                {
                    fileOrFolderNames.Add(dotDFolderName);
                }
            }

        }

        /// <summary>
        /// Returns the file or folder name list for the specified dataset based on dataset type
        /// Most datasets only have a single dataset file or folder, but FTICR_Imaging datasets 
        /// can have multiple .D folders below a parent folder
        /// </summary>
        /// <returns>List of data file file or folder names; empty list if not found</returns>
        /// <remarks>
        /// Returns UNKNOWN_FILE_TYPE for instrument types that are not recognized.
        /// Returns INVALID_FILE_TYPE for instruments for which we do not run MSFileInfoScanner
        /// </remarks>
        private List<string> GetDataFileOrFolderName(
            string inputFolder,
            out bool bSkipPlots,
            out clsInstrumentClassInfo.eRawDataType rawDataType,
            out clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            out bool bBrukerDotDBaf)
        {
            var bIsFile = false;

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
                return new List<string>() { UNKNOWN_FILE_TYPE };
            }

            rawDataType = clsInstrumentClassInfo.GetRawDataType(rawDataTypeName);
            if (rawDataType == clsInstrumentClassInfo.eRawDataType.Unknown)
            {
                m_Msg = "RawDataType not recognized: " + rawDataTypeName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
                return new List<string>() { UNKNOWN_FILE_TYPE };
            }

            var diDatasetFolder = new DirectoryInfo(inputFolder);
            string sFileOrFolderName;

            // Get the expected file name based on the dataset type
            switch (rawDataType)
            {
                case clsInstrumentClassInfo.eRawDataType.ThermoRawFile:
                    // LTQ_2, LTQ_4, etc.
                    // LTQ_Orb_1, LTQ_Orb_2, etc.
                    // VOrbiETD01, VOrbiETD02, etc.
                    // TSQ_3
                    // Thermo_GC_MS_01
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION;
                    bIsFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.ZippedSFolders:
                    // 9T_FTICR, 11T_FTICR_B, and 12T_FTICR
                    sFileOrFolderName = "analysis.baf";
                    bIsFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerFTFolder:
                    // 12T_FTICR_B, 15T_FTICR, 9T_FTICR_B
                    // Also, Bruker_FT_IonTrap01, which is Bruker_Amazon_Ion_Trap
                    // 12T_FTICR_Imaging and 15T_FTICR_Imaging datasets with instrument class BrukerMALDI_Imaging_V2 will also have bruker_ft format; 
                    // however, instead of an analysis.baf file, they might have a .mcf file

                    bIsFile = true;
                    if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Bruker_Amazon_Ion_Trap)
                    {
                        sFileOrFolderName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
                    }
                    else
                    {
                        sFileOrFolderName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
                        bBrukerDotDBaf = true;
                    }

                    if (!File.Exists(Path.Combine(diDatasetFolder.FullName, sFileOrFolderName)))
                        sFileOrFolderName = CheckForBrukerImagingZipFiles(diDatasetFolder);

                    break;

                case clsInstrumentClassInfo.eRawDataType.UIMF:
                    // IMS_TOF_2, IMS_TOF_3, IMS_TOF_4, IMS_TOF_5, IMS_TOF_6, etc.
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION;
                    bIsFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.SciexWiffFile:
                    // QTrap01
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_WIFF_EXTENSION;
                    bIsFile = true;
                    break;

                case clsInstrumentClassInfo.eRawDataType.AgilentDFolder:
                    // Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                    bIsFile = false;

                    if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.PrepHPLC)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Skipping MSFileInfoScanner since PrepHPLC dataset");
                        return new List<string>() { INVALID_FILE_TYPE };
                    }

                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerMALDIImaging:
                    // bruker_maldi_imaging: 12T_FTICR_Imaging, 15T_FTICR_Imaging, and BrukerTOF_Imaging_01
                    // Find the name of the first zip file

                    sFileOrFolderName = CheckForBrukerImagingZipFiles(diDatasetFolder);
                    bSkipPlots = true;
                    bIsFile = true;

                    if (string.IsNullOrEmpty(sFileOrFolderName))
                    {
                        m_Msg = "Did not find any 0_R*.zip files in the dataset folder";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
                        return new List<string>() { INVALID_FILE_TYPE };
                    }

                    break;

                case clsInstrumentClassInfo.eRawDataType.BrukerTOFBaf:
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                    bIsFile = false;
                    break;
                case clsInstrumentClassInfo.eRawDataType.IlluminaFolder:
                    sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_TXT_GZ_EXTENSION;
                    bIsFile = true;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Skipping MSFileInfoScanner since Illumina RNASeq dataset");
                    return new List<string>() { INVALID_FILE_TYPE };

                default:
                    // Other instruments; do not process them with MSFileInfoScanner

                    // Excluded instruments include:
                    // dot_wiff_files (AgilentQStarWiffFile): AgTOF02
                    // bruker_maldi_spot (BrukerMALDISpot): BrukerTOF_01
                    m_Msg = "Data type " + rawDataType + " not recognized";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
                    return new List<string>() { INVALID_FILE_TYPE };
            }

            // Test to verify the file (or folder) exists
            var sFileOrFolderPath = Path.Combine(diDatasetFolder.FullName, sFileOrFolderName);

            if (bIsFile)
            {
                // Expecting to match a file
                if (File.Exists(sFileOrFolderPath))
                {
                    // File exists
                    // Even if it is in a .D folder, we will only examine this file
                    return new List<string>() { sFileOrFolderName };
                }

                // File not found; check for alternative files or folders
                // This function also looks for .D folders
                var fileOrFolderNames = LookForAlternateFileOrFolder(diDatasetFolder, sFileOrFolderName);

                if (fileOrFolderNames.Count > 0)
                    return fileOrFolderNames;

                m_Msg = "clsPluginMain.GetDataFileOrFolderName: File " + sFileOrFolderPath + " not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
                m_Msg = "File " + sFileOrFolderPath + " not found";

                return new List<string>();
            }

            // Expecting to match a folder
            if (Directory.Exists(sFileOrFolderPath))
            {
                if (Path.GetExtension(sFileOrFolderName).ToUpper() != ".D")
                {
                    // Folder exists, and it does not end in .D
                    return new List<string>() { sFileOrFolderName };
                }

                // Look for other .D folders
                var fileOrFolderNames = new List<string>() { sFileOrFolderName };
                FindDotDFolders(diDatasetFolder, fileOrFolderNames);

                return fileOrFolderNames;
            }

            m_Msg = "clsPluginMain.GetDataFileOrFolderName: Folder " + sFileOrFolderPath + " not found";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
            m_Msg = "Folder " + sFileOrFolderPath + " not found";

            return new List<string>();

        }

        /// <summary>
        /// Construct the full path to the MSFileInfoScanner.DLL
        /// </summary>
        /// <returns></returns>
        protected string GetMSFileInfoScannerDLLPath()
        {
            var strMSFileInfoScannerFolder = m_MgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
            if (string.IsNullOrEmpty(strMSFileInfoScannerFolder))
                return string.Empty;

            return Path.Combine(strMSFileInfoScannerFolder, "MSFileInfoScanner.dll");
        }

        /// <summary>
        /// A dataset file was not found
        /// Look for alternate dataset files, or look for .D folders
        /// </summary>
        /// <param name="diDatasetFolder"></param>
        /// <param name="initialFileOrFolderName"></param>
        /// <returns></returns>
        private List<string> LookForAlternateFileOrFolder(DirectoryInfo diDatasetFolder, string initialFileOrFolderName)
        {

            // File not found; look for alternate extensions
            var lstAlternateExtensions = new List<string> { "mgf", "mzXML", "mzML" };

            foreach (var altExtension in lstAlternateExtensions)
            {
                var dataFileNamePathAlt = Path.ChangeExtension(initialFileOrFolderName, altExtension);
                if (File.Exists(dataFileNamePathAlt))
                {
                    m_Msg = "Data file not found, but ." + altExtension + " file exists";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
                    return new List<string>() { INVALID_FILE_TYPE };
                }
            }


            // Look for dataset folders
            var primaryDotDFolder = new DirectoryInfo(Path.Combine(diDatasetFolder.FullName, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

            var fileOrFolderNames = new List<string>();

            if (primaryDotDFolder.Exists)
            {
                // Look for a .mcf file in the .D folder
                string dotDFolderName;
                var mcfFileExists = LookForMcfFileInDotDFolder(primaryDotDFolder, out dotDFolderName);
                if (mcfFileExists)
                {
                    fileOrFolderNames.Add(dotDFolderName);
                }
            }

            // With instrument class BrukerMALDI_Imaging_V2 (e.g. 15T_FTICR_Imaging) we allow multiple .D folders to be captured
            // Look for additional folders now
            FindDotDFolders(diDatasetFolder, fileOrFolderNames);

            return fileOrFolderNames;

        }

        /// <summary>
        /// Look for any .mcf file in a Bruker .D folder
        /// </summary>
        /// <param name="diDotDFolder"></param>
        /// <param name="dotDFolderName">Output: name of the .D folder</param>
        /// <returns>True if a .mcf file is found</returns>
        private bool LookForMcfFileInDotDFolder(DirectoryInfo diDotDFolder, out string dotDFolderName)
        {

            long mcfFileSizeBytes = 0;
            dotDFolderName = string.Empty;

            foreach (var fiFile in diDotDFolder.GetFiles("*.mcf"))
            {
                // Return the .mcf file that is the largest
                if (fiFile.Length > mcfFileSizeBytes)
                {
                    mcfFileSizeBytes = fiFile.Length;
                    dotDFolderName = diDotDFolder.Name;
                }
            }

            return mcfFileSizeBytes > 0;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {

            var strToolVersionInfo = string.Empty;
            var fiExecutingAssembly = new FileInfo(Assembly.GetExecutingAssembly().Location);
            if (fiExecutingAssembly.DirectoryName == null)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FileInfo object for the executing assembly has a null value for DirectoryName");
                return false;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

            // Lookup the version of the Capture tool plugin
            var strPluginPath = Path.Combine(fiExecutingAssembly.DirectoryName, "DatasetInfoPlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
            if (!bSuccess)
                return false;

            // Lookup the version of the MSFileInfoScanner DLL
            var strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
            if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
            {
                bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMSFileInfoScannerPath);
                if (!bSuccess)
                    return false;
            }

            // Lookup the version of the UIMFLibrary DLL
            var strUIMFLibraryPath = Path.Combine(fiExecutingAssembly.DirectoryName, "UIMFLibrary.dll");
            bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
            if (!bSuccess)
                return false;

            // Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(strPluginPath)
            };

            if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
                ioToolFiles.Add(new FileInfo(strMSFileInfoScannerPath));

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }        

        #endregion

        #region "Event handlers"
        /// <summary>
        /// Handles message event from MS file scanner
        /// </summary>
        /// <param name="Message">Event message</param>
        void m_MsFileScanner_MessageEvent(string Message)
        {
            m_Msg = "Message from MSFileInfoScanner = " + Message;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
        }

        /// <summary>
        /// Handles error event from MS file scanner
        /// </summary>
        /// <param name="message">Error message</param>
        void m_MsFileScanner_ErrorEvent(string message)
        {
            var errorMsg = "clsPluginMain.RunMsFileInfoScanner, Error running MSFileInfoScanner: " + message;

            if (message.StartsWith("Error using ProteoWizard reader"))
            {
                // This is not always a critical error; log it as a warning
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, errorMsg);
            }
            else
            {
                m_ErrOccurred = true;

                // Message often contains long folder paths; check for this and shorten them.
                // For example, switch
                // from: \\proto-6\LTQ_Orb_1\2015_4\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13\QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw
                // to:   QC_Shew_pep_Online_Dig_v12_c0pt5_05_10-08-13.raw

                // Match text of the form         \\server\share\folder<anything>DatasetName.Extension
                var reDatasetFile = new Regex(@"\\\\[^\\]+\\[^\\]+\\[^\\]+.+(" + m_Dataset + @"\.[a-z0-9]+)");

                if (reDatasetFile.IsMatch(message))
                {
                    m_Msg = "Error running MSFileInfoScanner: " + reDatasetFile.Replace(message, "$1");
                }
                else
                {
                    m_Msg = "Error running MSFileInfoScanner: " + message;
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
            }

        }

        #endregion
    }

}	// End namespace

