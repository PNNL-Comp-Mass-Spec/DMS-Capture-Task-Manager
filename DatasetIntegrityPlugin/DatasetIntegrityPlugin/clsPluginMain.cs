
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//
// Last modified 08/11/2015 
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using CaptureTaskManager;
using PRISM.Files;
using PRISM.Processes;
using ThermoRawFileReader;
using UIMFLibrary;

namespace DatasetIntegrityPlugin
{
    public class clsPluginMain : clsToolRunnerBase
    {
        //*********************************************************************************************************
        // Main class for plugin
        //**********************************************************************************************************

        #region "Constants"
        const float RAW_FILE_MIN_SIZE_KB = 50;
        const float RAW_FILE_MAX_SIZE_MB_LTQ = 2048;
        const float RAW_FILE_MAX_SIZE_MB_ORBITRAP = 100000;
        const float BAF_FILE_MIN_SIZE_KB = 16;
        const float SER_FILE_MIN_SIZE_KB = 16;
        const float FID_FILE_MIN_SIZE_KB = 16;
        const float ACQ_METHOD_FILE_MIN_SIZE_KB = 5;
        const float SCIEX_WIFF_FILE_MIN_SIZE_KB = 50;
        const float SCIEX_WIFF_SCAN_FILE_MIN_SIZE_KB = 0.03F;
        const float UIMF_FILE_MIN_SIZE_KB = 50;
        const float TIMS_UIMF_FILE_MIN_SIZE_KB = 5;
        const float AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB = 50;
        const float AGILENT_DATA_MS_FILE_MIN_SIZE_KB = 75;
        const float MCF_FILE_MIN_SIZE_KB = 0.1F;		// Malding imaging file; Prior to May 2014, used a minimum of 4 KB; however, seeing 12T_FTICR_B datasets where this file is as small as 120 Bytes

        const int MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES = 30;
        const int MAX_AGILENT_TO_CDF_RUNTIME_MINUTES = 10;

        #endregion

        #region "Class-wide variables"

        protected clsToolReturnData mRetData = new clsToolReturnData();
        protected DateTime mAgilentToUIMFStartTime;
        protected DateTime mAgilentToCDFStartTime;
        protected DateTime mLastStatusUpdate;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset integrity step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting DatasetIntegrityPlugin.clsPluginMain.RunTool()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            var instrumentName = m_TaskParams.GetParam("Instrument_Name");
            var instClassName = m_TaskParams.GetParam("Instrument_Class");
            var instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);

            var agilentToUimfConverterPath = string.Empty;
            var openChromProgPath = string.Empty;

            // Check whether we need to convert from a .D folder to a .UIMF file
            var agilentDFolderToUimfConversionRequired = (instrumentName.StartsWith("IMS08") || instrumentName.StartsWith("IMS09"));

            if (agilentDFolderToUimfConversionRequired)
            {
                agilentToUimfConverterPath = GetAgilentToUIMFProgPath();

                if (!File.Exists(agilentToUimfConverterPath))
                {
                    mRetData.CloseoutMsg = "AgilentToUIMFConverter not found at " + agilentToUimfConverterPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return mRetData;
                }
            }

            // Check whether we need to convert from a .D folder to a .CDF file
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap)
            {
                openChromProgPath = GetOpenChromProgPath();

                if (!File.Exists(openChromProgPath))
                {
                    mRetData.CloseoutMsg = "OpenChrom not found at " + openChromProgPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return mRetData;
                }
            }

            // Store the version info in the database
            if (!StoreToolVersionInfo(agilentToUimfConverterPath, openChromProgPath))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                mRetData.CloseoutMsg = "Error determining tool version info";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            msg = "Performing integrity test, dataset '" + m_Dataset + "'";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

            // Set up the file paths
            var storageVolExt = m_TaskParams.GetParam("Storage_Vol_External");
            var storagePath = m_TaskParams.GetParam("Storage_Path");
            var datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, m_Dataset));
            string dataFileNamePath;

            // Select which tests will be performed based on instrument class

            msg = "Instrument class: " + instClassName;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
            {
                msg = "Instrument class not recognized: " + instClassName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
                    dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestFinniganIonTrapFile(dataFileNamePath);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
                    dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestLTQFTFile(dataFileNamePath);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BRUKERFTMS:
                    mRetData.CloseoutType = TestBrukerFolder(datasetFolder);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
                    dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestThermoExactiveFile(dataFileNamePath);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
                    dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestTripleQuadFile(dataFileNamePath);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
                    if (agilentDFolderToUimfConversionRequired)
                    {
                        // Need to first convert the .d folder to a .UIMF file
                        if (!ConvertAgilentDFolderToUIMF(datasetFolder, agilentToUimfConverterPath))
                        {
                            if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                            {
                                mRetData.CloseoutMsg = "Unknown error converting the Agilent .D to folder to a .UIMF file";
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
                            }

                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }

                    dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION);
                    mRetData.CloseoutType = TestIMSAgilentTOF(dataFileNamePath, instrumentName);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
                    mRetData.CloseoutType = TestBrukerFT_Folder(datasetFolder, requireBAFFile: true, requireMCFFile: false, instrumentClass: instrumentClass, instrumentName: instrumentName);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                    mRetData.CloseoutType = TestBrukerMaldiImagingFolder(datasetFolder);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                    mRetData.CloseoutType = TestBrukerFT_Folder(datasetFolder, requireBAFFile: false, requireMCFFile: false, instrumentClass: instrumentClass, instrumentName: instrumentName);

                    // Check for message "Multiple .d folders"
                    if (mRetData.EvalMsg.Contains("Multiple " + clsInstrumentClassInfo.DOT_D_EXTENSION + " folders"))
                        break;

                    if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                    {
                        // Try BrukerMALDI_Imaging
                        var oRetDataAlt = new clsToolReturnData
                        {
                            CloseoutType = TestBrukerMaldiImagingFolder(datasetFolder)
                        };

                        if (oRetDataAlt.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                        {
                            // The dataset actually consists of a series of .Zip files, not a .D folder
                            // Count this as a success
                            msg = "Dataset marked eInstrumentClass.BrukerMALDI_Imaging_V2 is actually eInstrumentClass.BrukerMALDI_Imaging (series of .Zip files); assuming integrity is correct";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                            mRetData = oRetDataAlt;
                            mRetData.EvalMsg = "Dataset is BrukerMALDI_Imaging (series of .Zip files) not BrukerMALDI_Imaging_V2 (.D folder)";
                        }
                    }
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:
                    mRetData.CloseoutType = TestBrukerMaldiSpotFolder(datasetFolder);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerTOF_BAF:
                    mRetData.CloseoutType = TestBrukerTof_BafFolder(datasetFolder, instrumentName);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Sciex_QTrap:
                    mRetData.CloseoutType = TestSciexQtrapFile(datasetFolder, m_Dataset);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap:
                    // .D folder with a DATA.MS file
                    mRetData.CloseoutType = TestAgilentIonTrapFolder(datasetFolder);

                    if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {

                        // Convert the .d folder to a .CDF file
                        if (!ConvertAgilentDFolderToCDF(datasetFolder, openChromProgPath))
                        {
                            if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                            {
                                mRetData.CloseoutMsg = "Unknown error converting the Agilent .D to folder to a .CDF file";
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
                            }

                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        }
                    }

                    break;
                case clsInstrumentClassInfo.eInstrumentClass.Agilent_TOF_V2:
                    mRetData.CloseoutType = TestAgilentTOFV2Folder(datasetFolder);
                    break;
                default:
                    msg = "No integrity test available for instrument class " + instClassName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                    mRetData.EvalMsg = msg;
                    mRetData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    break;
            }	// End switch

            msg = "Completed clsPluginMain.RunTool()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            return mRetData;
        }	// End sub

        private bool ConvertAgilentDFolderToCDF(string datasetFolderPath, string exePath)
        {
            try
            {

                // Make sure the CDF plugin is installed and that the SerialKey is defined
                if (!ValidateCDFPlugin())
                {
                    return false;
                }

                var mgrName = m_MgrParams.GetParam("MgrName", "CTM");
                var oFileTools = new clsFileTools(mgrName, m_DebugLevel);
                var dotDFolderPathLocal = Path.Combine(m_WorkDir, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION);

                var success = CopyDotDFolderToLocal(oFileTools, datasetFolderPath, dotDFolderPathLocal, false);
                if (!success)
                    return false;

                // Create the BatchJob.obj file
                // This is an XML file with the information required by OpenChrom to create CDF file from the .D folder

                var batchJobFilePath = CreateOpenChromCDFJobFile(dotDFolderPathLocal);
                if (string.IsNullOrEmpty(batchJobFilePath))
                {
                    return false;
                }

                // Construct the command line arguments to run the OpenChrom

                // Syntax:
                // OpenChrom.exe -cli -batchfile E:\CTM_WorkDir\BatchJob.obj 
                //
                // Optional: -nosplash

                var cmdStr = "-cli -batchfile " + batchJobFilePath;
                var consoleOutputFilePath = Path.Combine(m_WorkDir, "OpenChrom_ConsoleOutput_" + mgrName + ".txt");

                var cmdRunner = new clsRunDosProgram(m_WorkDir);
                mAgilentToCDFStartTime = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;

                AttachCmdrunnerEvents(cmdRunner);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;

                // OpenChrom does not produce any console output; so no point in creating it 
                cmdRunner.WriteConsoleOutputToFile = false;
                cmdRunner.ConsoleOutputFilePath = consoleOutputFilePath;

                var msg = "Converting .D folder to .CDF: " + exePath + " " + cmdStr;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                const int iMaxRuntimeSeconds = MAX_AGILENT_TO_CDF_RUNTIME_MINUTES * 60;
                success = cmdRunner.RunProgram(exePath, cmdStr, "OpenChrom", true, iMaxRuntimeSeconds);

                // Delete the locally cached .D folder
                try
                {
                    clsProgRunner.GarbageCollectNow();
                    oFileTools.DeleteDirectory(dotDFolderPathLocal, ignoreErrors: true);
                }
                catch (Exception ex)
                {
                    // Do not treat this as a fatal error
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception deleting locally cached .D folder (" + dotDFolderPathLocal + "): " + ex.Message);
                }

                if (!success)
                {
                    mRetData.CloseoutMsg = "Error running OpenChrom";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "OpenChrom returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to OpenChrom failed (but exit code is 0)");
                    }

                    return false;
                }

                Thread.Sleep(100);

                // Copy the .CDF file to the dataset folder
                success = CopyCDFToDatasetFolder(oFileTools, mgrName, datasetFolderPath);
                if (!success)
                {
                    return false;
                }

                // Delete the batch job  file
                DeleteFileIgnoreErrors(batchJobFilePath);

                // Delete the console output file
                if (File.Exists(consoleOutputFilePath))
                    DeleteFileIgnoreErrors(consoleOutputFilePath);

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception converting .D folder to a CDF file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }          

            return true;
        }

        private bool ConvertAgilentDFolderToUIMF(string datasetFolderPath, string exePath)
        {
            try
            {

                var mgrName = m_MgrParams.GetParam("MgrName", "CTM");
                var oFileTools = new clsFileTools(mgrName, m_DebugLevel);
                var dotDFolderPathLocal = Path.Combine(m_WorkDir, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION);

                var success = CopyDotDFolderToLocal(oFileTools, datasetFolderPath, dotDFolderPathLocal, true);
                if (!success)
                    return false;

                // Construct the command line arguments to run the AgilentToUIMFConverter

                // Syntax:
                // AgilentToUIMFConverter.exe [Agilent .d Folder] [Directory to insert file (optional)]
                //
                var cmdStr = clsConversion.PossiblyQuotePath(dotDFolderPathLocal) + " " + clsConversion.PossiblyQuotePath(m_WorkDir);
                var consoleOutputFilePath = Path.Combine(m_WorkDir, "AgilentToUIMF_ConsoleOutput_" + mgrName + ".txt");

                var cmdRunner = new clsRunDosProgram(m_WorkDir);
                mAgilentToUIMFStartTime = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;

                AttachCmdrunnerEvents(cmdRunner);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = consoleOutputFilePath;

                var msg = "Converting .D folder to .UIMF: " + exePath + " " + cmdStr;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                const int iMaxRuntimeSeconds = MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES * 60;
                success = cmdRunner.RunProgram(exePath, cmdStr, "AgilentToUIMFConverter", true, iMaxRuntimeSeconds);

                ParseConsoleOutputFileForErrors(consoleOutputFilePath);

                // Delete the locally cached .D folder
                try
                {
                    clsProgRunner.GarbageCollectNow();
                    oFileTools.DeleteDirectory(dotDFolderPathLocal, ignoreErrors: true);
                }
                catch (Exception ex)
                {
                    // Do not treat this as a fatal error
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception deleting locally cached .D folder (" + dotDFolderPathLocal + "): " + ex.Message);
                }

                if (!success)
                {
                    mRetData.CloseoutMsg = "Error running the AgilentToUIMFConverter";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "AgilentToUIMFConverter returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to AgilentToUIMFConverter failed (but exit code is 0)");
                    }

                    return false;
                }

                Thread.Sleep(100);

                // Copy the .UIMF file to the dataset folder
                success = CopyUIMFToDatasetFolder(oFileTools, mgrName, datasetFolderPath);
                if (!success)
                {
                    return false;
                }

                // Delete the console output file
                DeleteFileIgnoreErrors(consoleOutputFilePath);

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception converting .D folder to a UIMF file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }
         
            return true;
        }

        private bool CopyDotDFolderToLocal(
            clsFileTools oFileTools,
            string datasetFolderPath,
            string dotDFolderPathLocal,
            bool requireIMSFiles)
        {
            var dotDFolderPathRemote = new DirectoryInfo(Path.Combine(datasetFolderPath, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

            if (requireIMSFiles)
            {
                // Make sure the .D folder has the required files
                // Older datasets may have had their larger files purged, which will cause the AgilentToUIMFConverter to fail

                var binFiles = dotDFolderPathRemote.GetFiles("*.bin", SearchOption.AllDirectories).ToList();
                var fileNames = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

                foreach (var file in binFiles)
                    fileNames.Add(file.Name);

                var requiredFiles = new List<string>
                {
                    "IMSFrame.bin",
                    "MSPeak.bin",
                    "MSPeriodicActuals.bin",
                    "MSProfile.bin",
                    "MSScan.bin"
                };

                // Construct a list of the missing files
                var missingFiles = requiredFiles.Where(requiredFile => !fileNames.Contains(requiredFile)).ToList();

                var errorMessage = string.Empty;
                if (missingFiles.Count == 1)
                    errorMessage = "Cannot convert .D to .UIMF; missing file " + missingFiles.First();

                if (missingFiles.Count > 1)
                    errorMessage = "Cannot convert .D to .UIMF; missing files " + string.Join(", ", missingFiles);

                if (errorMessage.Length > 0)
                {
                    mRetData.CloseoutMsg = errorMessage;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         mRetData.CloseoutMsg);
                    return false;
                }
            }

            // Copy the dataset folder locally using Prism.DLL
            // Note that lock files will be used when copying large files (over 20 MB)

            oFileTools.CopyDirectory(dotDFolderPathRemote.FullName, dotDFolderPathLocal, true);

            return true;
        }

        private bool CopyCDFToDatasetFolder(clsFileTools oFileTools, string mgrName, string datasetFolderPath)
        {
            var fiCDF = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + ".cdf"));

            if (!fiCDF.Exists)
            {
                mRetData.CloseoutMsg = "OpenChrom did not create a .CDF file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + fiCDF.FullName);
                return false;
            }

            var success = CopyFileToDatasetFolder(oFileTools, mgrName, fiCDF, datasetFolderPath);
            return success;
        }


        private bool CopyUIMFToDatasetFolder(clsFileTools oFileTools, string mgrName, string datasetFolderPath)
        {

            var fiUIMF = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION));

            if (!fiUIMF.Exists)
            {
                mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + fiUIMF.FullName);
                return false;
            }

            var success = CopyFileToDatasetFolder(oFileTools, mgrName, fiUIMF, datasetFolderPath);
            return success;
        }

        private bool CopyFileToDatasetFolder(clsFileTools oFileTools, string mgrName, FileInfo dataFile, string datasetFolderPath)
        {
            if (m_DebugLevel >= 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying " + dataFile.Extension + " file to the dataset folder");
            }

            var targetFilePath = Path.Combine(datasetFolderPath, dataFile.Name);
            oFileTools.CopyFileUsingLocks(dataFile.FullName, targetFilePath, mgrName, Overwrite: true);

            if (m_DebugLevel >= 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copy complete");
            }

            try
            {
                // Delete the local copy
                dataFile.Delete();
            }
            catch (Exception ex)
            {
                // Do not treat this as a fatal error
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception deleting local copy of the new .UIMF file " + dataFile.FullName + ": " + ex.Message);
            }

            return true;
        }

        private string CreateOpenChromCDFJobFile(string dotDFolderPathLocal)
        {
            try
            {
                var sbXML = new StringBuilder();

                sbXML.Append(@"<?xml version=""1.0"" encoding=""utf-8""?>");
                sbXML.Append(@"<BatchProcessJob>");
                sbXML.Append(@"<Header></Header>");

                sbXML.Append(@"<!--Load the following chromatograms.-->");
                sbXML.Append(@"<InputEntries><InputEntry>");
                sbXML.Append(@"<![CDATA[" + dotDFolderPathLocal + "]]>");
                sbXML.Append(@"</InputEntry></InputEntries>");

                sbXML.Append(@"<!--Process each chromatogram with the listed methods.-->");
                sbXML.Append(@"<ProcessEntries></ProcessEntries>");

                sbXML.Append(@"<!--Write each processed chromatogram to the given output formats.-->");
                sbXML.Append(@"<OutputEntries>");
                sbXML.Append(@"<OutputEntry converterId=""net.openchrom.msd.converter.supplier.cdf"">");
                sbXML.Append(@"<![CDATA[" + m_WorkDir + "]]>");
                sbXML.Append(@"</OutputEntry>");
                sbXML.Append(@"</OutputEntries>");

                sbXML.Append(@"<!--Process each chromatogram with the listed report suppliers.-->");
                sbXML.Append(@"<ReportEntries></ReportEntries>");
                sbXML.Append(@"</BatchProcessJob>");

                var jobFilePath = Path.Combine(m_WorkDir, "CDFBatchJob.obj");

                using (var writer = new StreamWriter(new FileStream(jobFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(sbXML);
                }

                return jobFilePath;

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = ".D to CDF conversion failed; error in CreateOpenChromCDFJobFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateOpenChromCDFJobFile: " + ex.Message);
                return string.Empty;
            }
        }

        /// <summary>
        /// Processes folders in folderList to compare the x_ folder to the non x_ folder
        /// If the x_ folder is empty or if every file in the x_ folder is also in the non x_ folder, then returns True and optionally deletes the x_ folder
        /// </summary>
        /// <param name="folderList">List of folders; must contain exactly 2 entries</param>
        /// <param name="bDeleteIfSuperseded"></param>
        /// <returns>True if this is a superseded folder and it is safe to delete</returns>
        private bool DetectSupersededFolder(List<DirectoryInfo> folderList, bool bDeleteIfSuperseded)
        {
            string msg;

            try
            {
                if (folderList.Count != 2)
                {
                    msg = "folderList passed into DetectSupersededFolder does not contain 2 folders; cannot check for a superseded folder";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return false;
                }

                DirectoryInfo diNewFolder;
                DirectoryInfo diOldFolder;

                if (folderList[0].Name.ToLower().StartsWith("x_"))
                {
                    diNewFolder = folderList[1];
                    diOldFolder = folderList[0];
                }
                else
                {
                    diNewFolder = folderList[0];
                    diOldFolder = folderList[1];
                }

                if (diOldFolder.Name == "x_" + diNewFolder.Name)
                {
                    // Yes, we have a case of a likely superseded folder
                    // Examine diOldFolder

                    msg = "Comparing files in superseded folder (" + diOldFolder.FullName + ") to newer folder (" + diNewFolder.FullName + ")";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                    var bFolderIsSuperseded = true;

                    var fiSupersededFiles = diOldFolder.GetFiles("*", SearchOption.AllDirectories);

                    foreach (var fiFile in fiSupersededFiles)
                    {
                        var sNewfilePath = fiFile.FullName.Replace(diOldFolder.FullName, diNewFolder.FullName);
                        var fiNewFile = new FileInfo(sNewfilePath);

                        if (!fiNewFile.Exists)
                        {
                            msg = "File not found in newer folder: " + fiNewFile.FullName;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                            bFolderIsSuperseded = false;
                            break;
                        }

                        if (fiNewFile.Length < fiFile.Length)
                        {
                            msg = "Newer file is smaller than version in superseded folder: " + fiNewFile.FullName;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                            bFolderIsSuperseded = false;
                            break;
                        }
                    }

                    if (bFolderIsSuperseded && bDeleteIfSuperseded)
                    {
                        // Delete diOldFolder
                        msg = "Deleting superseded folder: " + diOldFolder.FullName;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

                        diOldFolder.Delete(true);
                    }

                    return bFolderIsSuperseded;

                }

                msg = "Folder " + diOldFolder.FullName + " is not a superseded folder for " + diNewFolder.FullName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                return false;
            }
            catch (Exception ex)
            {
                msg = "Error in DetectSupersededFolder: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }


        }

        private string GetAgilentToUIMFProgPath()
        {
            var exeName = m_MgrParams.GetParam("AgilentToUIMFProgLoc");
            var exePath = Path.Combine(exeName, "AgilentToUimfConverter.exe");
            return exePath;
        }

        private string GetOpenChromProgPath()
        {
            var exeName = m_MgrParams.GetParam("OpenChromProgLoc");
            var exePath = Path.Combine(exeName, "openchrom.exe");
            return exePath;
        }

        /// <summary>
        /// Looks files matching fileSpec
        /// For matching files, copies the or moves them up one folder
        /// If matchDatasetName is true then requires that the file start with the name of the dataset
        /// </summary>
        /// <param name="diDatasetFolder"></param>
        /// <param name="fileSpec">Files to match, for example *.mis</param>
        /// <param name="matchDatasetName">True if filenames must start with m_Dataset</param>
        /// <param name="copyFile">True to copy the file, false to move it</param>
        private void MoveOrCopyUpOneLevel(DirectoryInfo diDatasetFolder, string fileSpec, bool matchDatasetName, bool copyFile)
        {
            foreach (var fiFile in diDatasetFolder.GetFiles(fileSpec))
            {
                if (matchDatasetName &&
                    !Path.GetFileNameWithoutExtension(fiFile.Name).ToLower().StartsWith(m_Dataset.ToLower()))
                {
                    continue;
                }

                if (fiFile.Directory == null || fiFile.Directory.Parent == null)
                {
                    continue;
                }

                var newPath = Path.Combine(fiFile.Directory.Parent.FullName, fiFile.Name);
                if (File.Exists(newPath))
                {
                    continue;
                }

                if (copyFile)
                    fiFile.CopyTo(newPath, true);
                else
                    fiFile.MoveTo(newPath);
            }
        }

        protected void ParseConsoleOutputFileForErrors(string sConsoleOutputFilePath)
        {
            var blnUnhandledException = false;
            var sExceptionText = string.Empty;

            try
            {
                if (!File.Exists(sConsoleOutputFilePath))
                {
                    return;
                }

                using (var srInFile = new StreamReader(new FileStream(sConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (srInFile.Peek() > -1)
                    {
                        var sLineIn = srInFile.ReadLine();

                        if (string.IsNullOrEmpty(sLineIn))
                        {
                            continue;
                        }

                        if (blnUnhandledException)
                        {
                            if (string.IsNullOrEmpty(sExceptionText))
                            {
                                sExceptionText = string.Copy(sLineIn);
                            }
                            else
                            {
                                sExceptionText = ";" + sLineIn;
                            }
                        }
                        else if (sLineIn.StartsWith("Error:"))
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AgilentToUIMFConverter error: " + sLineIn);
                        }
                        else if (sLineIn.StartsWith("Exception in"))
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AgilentToUIMFConverter error: " + sLineIn);
                        }
                        else if (sLineIn.StartsWith("Unhandled Exception"))
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AgilentToUIMFConverter error: " + sLineIn);
                            blnUnhandledException = true;
                        }
                    }
                }

                if (!string.IsNullOrEmpty(sExceptionText))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, sExceptionText);
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ParseConsoleOutputFileForErrors: " + ex.Message);
            }

        }

        /// <summary>
        /// If folderList contains exactly two folders then calls DetectSupersededFolder Detect and delete the extra x_ folder (if appropriate)
        /// Returns True if folderList contains just one folder, or if able to successfully delete the extra x_ folder
        /// </summary>
        /// <param name="folderList"></param>
        /// <param name="folderDescription"></param>
        /// <returns>True if success; false if a problem</returns>
        private bool PossiblyRenameSupersededFolder(List<DirectoryInfo> folderList, string folderDescription)
        {
            var bInvalid = true;

            if (folderList.Count == 1)
                return true;

            if (folderList.Count == 2)
            {
                // If two folders are present and one starts with x_ and all of the files inside the one that start with x_ are also in the folder without x_,
                // then delete the x_ folder
                const bool bDeleteIfSuperseded = true;

                if (DetectSupersededFolder(folderList, bDeleteIfSuperseded))
                {
                    bInvalid = false;
                }
            }

            if (bInvalid)
            {
                mRetData.EvalMsg = "Invalid dataset. Multiple " + folderDescription + " folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return false;
            }

            return true;
        }


        private bool PositiveNegativeMethodFolders(List<DirectoryInfo> lstMethodFolders)
        {
            if (lstMethodFolders.Count != 2)
            {
                return false;
            }

            if (lstMethodFolders[0].Name.IndexOf("_neg", 0, StringComparison.CurrentCultureIgnoreCase) >= 0 &&
                lstMethodFolders[1].Name.IndexOf("_pos", 0, StringComparison.CurrentCultureIgnoreCase) >= 0)
            {
                return true;
            }

            if (lstMethodFolders[1].Name.IndexOf("_neg", 0, StringComparison.CurrentCultureIgnoreCase) >= 0 &&
                lstMethodFolders[0].Name.IndexOf("_pos", 0, StringComparison.CurrentCultureIgnoreCase) >= 0)
            {
                return true;
            }

            return false;
        }


        private void ReportFileSizeTooLarge(string sDataFileDescription, string sFilePath, float fActualSize, float fMaxSize)
        {
            string sMaxSize;

            if (fMaxSize / 1024.0 > 1)
                sMaxSize = (fMaxSize / 1024.0).ToString("#0.0") + " MB";
            else
                sMaxSize = fMaxSize.ToString("#0") + " KB";

            var msg = sDataFileDescription + " file may be corrupt. Actual file size is " +
                      fActualSize.ToString("####0.0") + " KB; " +
                      "max allowable size is " + sMaxSize + "; see " + sFilePath;

            mRetData.EvalMsg = sDataFileDescription + " file size is more than " + sMaxSize;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }

        private void ReportFileSizeTooSmall(string sDataFileDescription, string sFilePath, float fActualSizeKB, float fMinSizeKB)
        {
            // Example messages:
            // Data file size is less than 100 KB
            // ser file size is less than 16 KB

            var sMinSize = fMinSizeKB.ToString("#0") + " KB";

            var msg = sDataFileDescription + " file may be corrupt. Actual file size is " +
                      fActualSizeKB.ToString("####0.0") + " KB; " +
                      "min allowable size is " + sMinSize + "; see " + sFilePath;

            mRetData.EvalMsg = sDataFileDescription + " file size is less than " + sMinSize;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }

        /// <summary>
        /// Tests a Agilent_Ion_Trap folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestAgilentIonTrapFolder(string datasetFolderPath)
        {

            // Verify only one .D folder in dataset
            var diDatasetFolder = new DirectoryInfo(datasetFolderPath);
            var lstDotDFolders = diDatasetFolder.GetDirectories("*.D").ToList();

            if (lstDotDFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .D folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstDotDFolders.Count > 1)
            {
                if (!PossiblyRenameSupersededFolder(lstDotDFolders, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Look for Data.MS file in the .D folder
            var lstInstrumentData = lstDotDFolders[0].GetFiles("DATA.MS");
            if (lstDotDFolders[0].GetFiles("DATA.MS").Length == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. DATA.MS file not found in the .D folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the DATA.MS file
            var dataFileSizeKB = GetFileSize(lstInstrumentData.First());
            if (dataFileSizeKB <= AGILENT_DATA_MS_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("DATA.MS", lstInstrumentData.First().FullName, dataFileSizeKB, AGILENT_DATA_MS_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;

        }	// End sub

        /// <summary>
        /// Tests a Agilent_TOF_V2 folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestAgilentTOFV2Folder(string datasetFolderPath)
        {
            // Verify only one .D folder in dataset
            var diDatasetFolder = new DirectoryInfo(datasetFolderPath);
            var lstDotDFolders = diDatasetFolder.GetDirectories("*.D").ToList();

            if (lstDotDFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .D folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstDotDFolders.Count > 1)
            {
                if (!PossiblyRenameSupersededFolder(lstDotDFolders, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Look for AcqData folder below .D folder
            var acqDataFolderList = lstDotDFolders[0].GetDirectories("AcqData").ToList();
            if (acqDataFolderList.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. .D folder does not contain an AcqData subfolder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (acqDataFolderList.Count > 1)
            {
                mRetData.EvalMsg = "Invalid dataset. Multiple AcqData folders found in .D folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // The AcqData folder should contain one or more .Bin files, for example MSScan.bin, MSPeak.bin, and MSProfile.bin
            // Verify that the MSScan.bin file exists
            var lstInstrumentData = acqDataFolderList[0].GetFiles("MSScan.bin").ToList();
            if (lstInstrumentData.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. MSScan.bin file not found in the AcqData folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the MSScan.bin file
            var dataFileSizeKB = GetFileSize(lstInstrumentData.First());
            if (dataFileSizeKB <= AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("MSScan.bin", lstInstrumentData.First().FullName, dataFileSizeKB, AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // The AcqData folder should contain file MSTS.xml
            var lstMSTS = acqDataFolderList[0].GetFiles("MSTS.xml").ToList();
            if (lstMSTS.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. MSTS.xml file not found in the AcqData folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Check to see if a .M folder exists
            var lstMethodFolders = acqDataFolderList[0].GetDirectories("*.m").ToList();
            if (lstMethodFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .m folders found found in the AcqData folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstMethodFolders.Count > 1)
            {
                mRetData.EvalMsg = "Invalid dataset. Multiple .m folders found in the AcqData folder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Tests a Sciex QTrap dataset's integrity
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <param name="datasetName"></param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestSciexQtrapFile(string dataFileNamePath, string datasetName)
        {
            // Verify .wiff file exists in storage folder
            var tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + clsInstrumentClassInfo.DOT_WIFF_EXTENSION);
            if (!File.Exists(tempFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + tempFileNamePath + " not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of .wiff file
            var dataFileSizeKB = GetFileSize(tempFileNamePath);

            // Check .wiff file min size
            if (dataFileSizeKB < SCIEX_WIFF_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Data", tempFileNamePath, dataFileSizeKB, SCIEX_WIFF_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify .wiff.scan file exists in storage folder
            tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + ".wiff.scan");
            if (!File.Exists(tempFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + tempFileNamePath + " not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of .wiff.scan file
            dataFileSizeKB = GetFileSize(tempFileNamePath);

            // Check .wiff.scan file min size
            if (dataFileSizeKB < SCIEX_WIFF_SCAN_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Data", tempFileNamePath, dataFileSizeKB, SCIEX_WIFF_SCAN_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Tests a Finnigan Ion Trap dataset's integrity
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestFinniganIonTrapFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_LTQ, true);
        }

        /// <summary>
        /// Tests an Orbitrap (LTQ_FT) dataset's integrity
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestLTQFTFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, false);
        }

        /// <summary>
        /// Tests an Thermo_Exactive dataset's integrity
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestThermoExactiveFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, false);
        }

        /// <summary>
        /// Tests an Triple Quad (TSQ) dataset's integrity
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestTripleQuadFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, true);
        }

        /// <summary>
        /// Test a Thermo .Raw file's integrity
        /// If the .Raw file is not found, then looks for a .mgf file, .mzXML, or .mzML file
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <param name="minFileSizeKB">Minimum allowed file size</param>
        /// <param name="maxFileSizeMB">Maximum allowed file size</param>
        /// <param name="openRawFileIfTooSmall">
        /// When true, if the file is less than minFileSizeKB, we try to open it with the ThermoRawFileReader
        /// If we can successfully open the file and get the first scan's data, then we declare the file to be valid
        /// </param>
        /// <returns></returns>
        private EnumCloseOutType TestThermoRawFile(string dataFileNamePath, float minFileSizeKB, float maxFileSizeMB, bool openRawFileIfTooSmall)
        {
            // Verify file exists in storage folder
            if (!File.Exists(dataFileNamePath))
            {
                // File not found; look for alternate extensions
                var lstAlternateExtensions = new List<string>();
                var bAlternateFound = false;

                lstAlternateExtensions.Add("mgf");
                lstAlternateExtensions.Add("mzXML");
                lstAlternateExtensions.Add("mzML");

                foreach (var altExtension in lstAlternateExtensions)
                {
                    var dataFileNamePathAlt = Path.ChangeExtension(dataFileNamePath, altExtension);
                    if (File.Exists(dataFileNamePathAlt))
                    {
                        mRetData.EvalMsg = "Raw file not found, but ." + altExtension + " file exists";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, mRetData.EvalMsg);
                        minFileSizeKB = 25;
                        maxFileSizeMB = RAW_FILE_MAX_SIZE_MB_ORBITRAP;
                        dataFileNamePath = dataFileNamePathAlt;
                        bAlternateFound = true;
                        openRawFileIfTooSmall = false;
                        break;
                    }
                }

                if (!bAlternateFound)
                {
                    mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(dataFileNamePath);

            // Check min size
            if (dataFileSizeKB < minFileSizeKB)
            {
                var validFile = false;

                if (openRawFileIfTooSmall)
                {

                    try
                    {
                        var reader = new XRawFileIO();
                        reader.OpenRawFile(dataFileNamePath);

                        var scanCount = reader.GetNumScans();

                        if (scanCount > 0)
                        {
                            double[,] massIntensityPairs;

                            var dataCount = reader.GetScanData2D(1, out massIntensityPairs);

                            if (dataCount > 0)
                            {
                                validFile = true;
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception opening .Raw file: " + ex.Message);
                        validFile = false;
                    }

                }

                if (!validFile)
                {
                    ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, minFileSizeKB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }

            }

            // Check max size
            if (dataFileSizeKB > maxFileSizeMB * 1024)
            {
                ReportFileSizeTooLarge("Data", dataFileNamePath, dataFileSizeKB, maxFileSizeMB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests a bruker folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <returns></returns>
        private EnumCloseOutType TestBrukerFolder(string datasetFolderPath)
        {
            // Verify 0.ser folder exists
            if (!Directory.Exists(Path.Combine(datasetFolderPath, "0.ser")))
            {
                mRetData.EvalMsg = "Invalid dataset. 0.ser folder not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify acqus file exists
            var dataFolder = Path.Combine(datasetFolderPath, "0.ser");
            if (!File.Exists(Path.Combine(dataFolder, "acqus")))
            {
                mRetData.EvalMsg = "Invalid dataset. acqus file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the acqus file
            var dataFileSizeKB = GetFileSize(Path.Combine(dataFolder, "acqus"));
            if (dataFileSizeKB <= 0F)
            {
                mRetData.EvalMsg = "Invalid dataset. acqus file contains no data";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify ser file present
            if (!File.Exists(Path.Combine(dataFolder, "ser")))
            {
                mRetData.EvalMsg = "Invalid dataset. ser file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the ser file
            dataFileSizeKB = GetFileSize(Path.Combine(dataFolder, "ser"));
            if (dataFileSizeKB <= 100)
            {
                mRetData.EvalMsg = "Invalid dataset. ser file too small";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Tests a BrukerTOF_BAF folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <param name="instrumentName"></param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerTof_BafFolder(string datasetFolderPath, string instrumentName)
        {
            // Verify only one .D folder in dataset
            var diDatasetFolder = new DirectoryInfo(datasetFolderPath);
            var lstDotDFolders = diDatasetFolder.GetDirectories("*.D").ToList();

            if (lstDotDFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .D folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstDotDFolders.Count > 1)
            {
                if (!PossiblyRenameSupersededFolder(lstDotDFolders, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify analysis.baf file exists
            var lstBafFile = lstDotDFolders[0].GetFiles("analysis.baf").ToList();
            if (lstBafFile.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. analysis.baf file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the analysis.baf file			
            var dataFileSizeKB = GetFileSize(lstBafFile.First());
            if (dataFileSizeKB <= BAF_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Analysis.baf", lstBafFile.First().FullName, dataFileSizeKB, BAF_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Check to see if at least one .M folder exists
            var lstMethodFolders = lstDotDFolders[0].GetDirectories("*.m").ToList();
            if (lstMethodFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .M folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstMethodFolders.Count > 1)
            {
                // Multiple .M folders
                // This is OK for the Buker Imaging instruments and for Maxis_01
                var instrumentNameLCase = instrumentName.ToLower();
                if (!instrumentNameLCase.Contains("imaging") && !instrumentNameLCase.Contains("maxis"))
                {
                    // It's also OK if there are two folders, and one contains _neg and one contains _pos
                    if (!PositiveNegativeMethodFolders(lstMethodFolders))
                    {
                        mRetData.EvalMsg = "Invalid dataset. Multiple .M folders found";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            // Determine if at least one .method file exists
            var lstMethodFiles = lstMethodFolders.First().GetFiles("*.method").ToList();
            if (lstMethodFiles.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. No .method files found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub


        /// <summary>
        /// Tests a BrukerFT folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <param name="requireBAFFile">Set to True to require that the analysis.baf file be present</param>
        /// <param name="requireMCFFile">Set to True to require that the analysis.baf file be present</param>
        /// <param name="instrumentClass"></param>
        /// <param name="instrumentName"></param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerFT_Folder(string datasetFolderPath, bool requireBAFFile, bool requireMCFFile, clsInstrumentClassInfo.eInstrumentClass instrumentClass, string instrumentName)
        {
            float dataFileSizeKB;
            float bafFileSizeKB = 0;

            // Verify only one .D folder in dataset
            var diDatasetFolder = new DirectoryInfo(datasetFolderPath);
            var lstDotDFolders = diDatasetFolder.GetDirectories("*.D").ToList();

            if (lstDotDFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .D folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstDotDFolders.Count > 1)
            {
                var allowMultipleFolders = false;

                if (lstDotDFolders.Count == 2)
                {
                    // If one folder contains a ser file and the other folder contains an analysis.baf, then we'll allow this
                    // This is somtimes the case for the 15T_FTICR_Imaging
                    var serCount = 0;
                    var bafCount = 0;
                    foreach (var diFolder in lstDotDFolders)
                    {
                        if (diFolder.GetFiles("ser", SearchOption.TopDirectoryOnly).Length == 1)
                            serCount += 1;

                        if (diFolder.GetFiles("analysis.baf", SearchOption.TopDirectoryOnly).Length == 1)
                            bafCount += 1;
                    }

                    if (bafCount == 1 && serCount == 1)
                        allowMultipleFolders = true;
                }

                if (!allowMultipleFolders)
                {
                    if (!PossiblyRenameSupersededFolder(lstDotDFolders, clsInstrumentClassInfo.DOT_D_EXTENSION))
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                }

            }

            // Verify analysis.baf file exists
            var lstBafFile = lstDotDFolders[0].GetFiles("analysis.baf").ToList();
            var fileExists = lstBafFile.Count > 0;

            if (!fileExists && requireBAFFile)
            {
                mRetData.EvalMsg = "Invalid dataset. analysis.baf file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (fileExists)
            {
                // Verify size of the analysis.baf file
                dataFileSizeKB = GetFileSize(lstBafFile.First());
                if (dataFileSizeKB <= BAF_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall("Analysis.baf", lstBafFile.First().FullName, dataFileSizeKB, BAF_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
                bafFileSizeKB = dataFileSizeKB;
            }


            // Check whether any .mcf files exist
            // Note that "*.mcf" will match files with extension .mcf and files with extension .mcf_idx		

            var mctFileName = string.Empty;
            dataFileSizeKB = 0;
            fileExists = false;
            long mcfFileSizeMax = 0;

            foreach (var fiFile in lstDotDFolders[0].GetFiles("*.mcf"))
            {
                if (fiFile.Length > dataFileSizeKB * 1024)
                {
                    dataFileSizeKB = fiFile.Length / (1024F);
                    mctFileName = fiFile.Name;
                    fileExists = true;
                }

                if (fiFile.Length > mcfFileSizeMax)
                    mcfFileSizeMax = fiFile.Length;
            }

            if (!fileExists && requireMCFFile)
            {
                mRetData.EvalMsg = "Invalid dataset; .mcf file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (fileExists)
            {
                // Verify size of the largest .mcf file
                float minSizeKB;
                if (mctFileName.ToLower() == "Storage.mcf_idx".ToLower())
                    minSizeKB = 4;
                else
                    minSizeKB = MCF_FILE_MIN_SIZE_KB;

                if (dataFileSizeKB <= minSizeKB)
                {
                    ReportFileSizeTooSmall(".MCF", mctFileName, dataFileSizeKB, minSizeKB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Verify ser file (if it exists)
            var lstSerFile = lstDotDFolders[0].GetFiles("ser").ToList();
            if (lstSerFile.Count > 0)
            {
                // ser file found; verify its size				
                dataFileSizeKB = GetFileSize(lstSerFile.First());
                if (dataFileSizeKB <= SER_FILE_MIN_SIZE_KB)
                {
                    // If on the 15T and the ser file is small but the .mcf file is not empty, then this is OK
                    if (!(instrumentName == "15T_FTICR" && mcfFileSizeMax > 0))
                    {
                        ReportFileSizeTooSmall("ser", lstSerFile.First().FullName, dataFileSizeKB, SER_FILE_MIN_SIZE_KB);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            else
            {
                // Check to see if a fid file exists instead of a ser file
                var lstFidFile = lstDotDFolders[0].GetFiles("fid").ToList();
                if (lstFidFile.Count > 0)
                {
                    // fid file found; verify size					
                    dataFileSizeKB = GetFileSize(lstFidFile.First());
                    if (dataFileSizeKB <= FID_FILE_MIN_SIZE_KB)
                    {
                        ReportFileSizeTooSmall("fid", lstFidFile.First().FullName, dataFileSizeKB, FID_FILE_MIN_SIZE_KB);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    // No ser or fid file found
                    // Ignore this error if on the 15T
                    if (instrumentName != "15T_FTICR")
                    {
                        mRetData.EvalMsg = "Invalid dataset. No ser or fid file found";
                        if (bafFileSizeKB > 0 && bafFileSizeKB < 100)
                        {
                            mRetData.EvalMsg += "; additionally, the analysis.baf file is quite small";
                        }
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2)
            {
                // Look for any files that match Dataset.mis or Dataset.jpg, and, if found, copy them up one folder

                MoveOrCopyUpOneLevel(diDatasetFolder, "*.mis", matchDatasetName: true, copyFile: true);
                MoveOrCopyUpOneLevel(diDatasetFolder, "*.bak", matchDatasetName: true, copyFile: true);
                MoveOrCopyUpOneLevel(diDatasetFolder, "*.jpg", matchDatasetName: false, copyFile: true);
            }


            // Check to see if a .M folder exists
            var lstMethodFolders = lstDotDFolders[0].GetDirectories("*.m").ToList();
            if (lstMethodFolders.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No .M folders found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstMethodFolders.Count > 1)
            {
                // Multiple .M folders
                // Allow this if there are two folders, and one contains _neg and one contains _pos
                // Also allow this if on the 12T or on the 15T
                var instrumentNameLCase = instrumentName.ToLower();

                if (!PositiveNegativeMethodFolders(lstMethodFolders) &&
                    instrumentNameLCase.Contains("15t_fticr") &&
                    instrumentNameLCase.Contains("12t_fticr") &&
                    instrumentNameLCase.Contains("imaging"))
                {
                    mRetData.EvalMsg = "Invalid dataset. Multiple .M folders found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Determine if apexAcquisition.method file exists and meets minimum size requirements
            var apexAcqMethod = lstMethodFolders.First().GetFiles("apexAcquisition.method").ToList();
            if (apexAcqMethod.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset. apexAcquisition.method file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            dataFileSizeKB = GetFileSize(apexAcqMethod.First());
            if (dataFileSizeKB <= ACQ_METHOD_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("apexAcquisition.method", apexAcqMethod.First().FullName, dataFileSizeKB, ACQ_METHOD_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Tests a BrukerMALDI_Imaging folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestBrukerMaldiImagingFolder(string datasetFolderPath)
        {
            // Verify at least one zip file exists in dataset folder
            var fileList = Directory.GetFiles(datasetFolderPath, "*.zip");
            if (fileList.Length < 1)
            {
                mRetData.EvalMsg = "Invalid dataset. No zip files found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Tests a BrukerMALDI_Spot folder for integrity
        /// </summary>
        /// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestBrukerMaldiSpotFolder(string datasetFolderPath)
        {

            // Verify the dataset folder doesn't contain any .zip files
            var zipFiles = Directory.GetFiles(datasetFolderPath, "*.zip");
            if (zipFiles.Length > 0)
            {
                mRetData.EvalMsg = "Zip files found in dataset folder " + datasetFolderPath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether the dataset folder contains just one data folder or multiple data folders
            var dataFolders = Directory.GetDirectories(datasetFolderPath);

            if (dataFolders.Length < 1)
            {
                mRetData.EvalMsg = "No subfolders were found in the dataset folder " + datasetFolderPath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dataFolders.Length > 1)
            {
                // Make sure the subfolders match the naming convention for MALDI spot folders
                // Example folder names:
                //  0_D4
                //  0_E10
                //  0_N4

                const string MALDI_SPOT_FOLDER_REGEX = "^\\d_[A-Z]\\d+$";
                var reMaldiSpotFolder = new Regex(MALDI_SPOT_FOLDER_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var folder in dataFolders)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Test folder " + folder + " against RegEx " + reMaldiSpotFolder);

                    var sDirName = Path.GetFileName(folder);
                    if (sDirName != null && !reMaldiSpotFolder.IsMatch(sDirName, 0))
                    {
                        mRetData.EvalMsg = "Dataset folder contains multiple subfolders, but folder " + sDirName + " does not match the expected pattern (" + reMaldiSpotFolder + "); see " + datasetFolderPath;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        private EnumCloseOutType TestIMSAgilentTOF(string dataFileNamePath, string instrumentName)
        {
            // Verify file exists in storage folder
            if (!File.Exists(dataFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(dataFileNamePath);

            // Check min size
            if (instrumentName.ToLower().StartsWith("TIMS_Maxis".ToLower()))
            {
                if (dataFileSizeKB < TIMS_UIMF_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, TIMS_UIMF_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                if (dataFileSizeKB < UIMF_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, UIMF_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }


            // Verify that the pressure columns are in the correct order
            if (!ValidatePressureInfo(dataFileNamePath, instrumentName))
            {
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }


            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }	// End sub

        /// <summary>
        /// Extracts the pressure data from the Frame_Parameters table
        /// </summary>
        /// <param name="dataFileNamePath"></param>
        /// <param name="instrumentName"></param>
        /// <returns>True if the pressure values are correct; false if the columns have swapped data</returns>
        protected bool ValidatePressureInfo(string dataFileNamePath, string instrumentName)
        {

            // Example of correct pressures:
            //   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
            //   8.33844                     3.87086                3.92628                0.23302

            // Example of incorrect pressures:
            //   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
            //   4.06285                     9.02253                0.41679                4.13393

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Opening UIMF file to read pressure data");

            var ignorePressureErrors = m_TaskParams.GetParam("IgnorePressureInfoErrors", false);
            var loggedPressureErrorWarning = false;

            // Open the file with the UIMFRader
            using (var uimfReader = new DataReader(dataFileNamePath))
            {
                var dctMasterFrameList = uimfReader.GetMasterFrameList();

                foreach (var frameNumber in dctMasterFrameList.Keys)
                {
                    var frameParams = uimfReader.GetFrameParams(frameNumber);

                    var highPressureFunnel = frameParams.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure);
                    var rearIonFunnel = frameParams.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure);
                    var quadPressure = frameParams.GetValueDouble(FrameParamKeyType.QuadrupolePressure);
                    var ionFunnelTrap = frameParams.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure);

                    if (instrumentName.ToLower().StartsWith("ims05"))
                    {
                        // As of September 2014, IMS05 does not have a high pressure ion funnel
                        // In order for the logic checks to work, we will override the HighPressureFunnelPressure value listed using RearIonFunnelPressure 
                        if (highPressureFunnel < rearIonFunnel)
                            highPressureFunnel = rearIonFunnel;
                    }

                    var pressureColumnsArePresent = (quadPressure > 0 &&
                                                     rearIonFunnel > 0 &&
                                                     highPressureFunnel > 0 &&
                                                     ionFunnelTrap > 0);

                    if (pressureColumnsArePresent)
                    {
                        // Example pressure values:
                        // HighPressureFunnelPressure = 4.077
                        // RearIonFunnelPressure = 4.048
                        // QuadrupolePressure = 0.262

                        // Multiplying the comparison pressure by 1.1 to give a 10% buffer in case the two pressure values are similar
                        var pressuresAreInCorrectOrder = (quadPressure < rearIonFunnel * 1.1 &&
                                                          rearIonFunnel < highPressureFunnel * 1.1);

                        if (!pressuresAreInCorrectOrder)
                        {
                            mRetData.EvalMsg = "Invalid pressure info in the Frame_Parameters table for frame " + frameNumber + ", dataset " + m_Dataset + "; QuadrupolePressure should be less than the RearIonFunnelPressure and the RearIonFunnelPressure should be less than the HighPressureFunnelPressure";

                            if (ignorePressureErrors)
                            {
                                if (!loggedPressureErrorWarning)
                                {                                
                                    loggedPressureErrorWarning = true;
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, mRetData.EvalMsg);
                                }
                            }
                            else
                            {

                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);

                                uimfReader.Dispose();
                                return false;
                            }
                        }
                    }

                    if (frameNumber % 100 == 0)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Validated frame " + frameNumber);

                }

            }

            return true;
        }

        /// <summary>
        /// Initializes the dataset integrity tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting clsPluginMain.Setup()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed clsPluginMain.Setup()";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
        }	// End sub


        /// <summary>
        /// Gets the length of a single file in KB
        /// </summary>
        /// <param name="fileNamePath">Fully qualified path to input file</param>
        /// <returns>File size in KB</returns>
        private float GetFileSize(string fileNamePath)
        {
            var fiFile = new FileInfo(fileNamePath);
            return GetFileSize(fiFile);
        }

        /// <summary>
        /// Gets the length of a single file in KB
        /// </summary>
        /// <param name="fiFile">File info object</param>
        /// <returns>File size in KB</returns>
        private float GetFileSize(FileInfo fiFile)
        {
            var fileLengthKB = fiFile.Length / (1024F);
            return fileLengthKB;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string agilentToUimfConverterPath, string openChromProgPath)
        {

            var strToolVersionInfo = string.Empty;
            var ioAppFileInfo = new FileInfo(Assembly.GetExecutingAssembly().Location);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

            if (ioAppFileInfo.DirectoryName == null)
                return false;

            // Lookup the version of the Capture tool plugin
            var strPluginPath = Path.Combine(ioAppFileInfo.DirectoryName, "DatasetIntegrityPlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
            if (!bSuccess)
                return false;

            // Lookup the version of SQLite
            var strSQLitePath = Path.Combine(ioAppFileInfo.DirectoryName, "System.Data.SQLite.dll");
            bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strSQLitePath);
            if (!bSuccess)
                return false;

            // Lookup the version of the UIMFLibrary
            var strUIMFLibraryPath = Path.Combine(ioAppFileInfo.DirectoryName, "UIMFLibrary.dll");
            bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
            if (!bSuccess)
                return false;

            if (!string.IsNullOrWhiteSpace(agilentToUimfConverterPath))
            {
                bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, agilentToUimfConverterPath);
                if (!bSuccess)
                    return false;
            }

            // Store path to CaptureToolPlugin.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
			{
				new FileInfo(strPluginPath)
			};

            if (!string.IsNullOrWhiteSpace(openChromProgPath))
            {
                ioToolFiles.Add(new FileInfo(openChromProgPath));
            }

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

        private bool ValidateCDFPlugin()
        {
            try
            {
                var diSettingsFolder = new DirectoryInfo(
                    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                                 @"OpenChromCE\1.0.x\.metadata\.plugins\org.eclipse.core.runtime\.settings"));

                if (!diSettingsFolder.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating OpenChrom settings file folder at " + diSettingsFolder.FullName);

                    diSettingsFolder.Create();
                    return false;
                }

                var fiSettingsFile = new FileInfo(
                    Path.Combine(diSettingsFolder.FullName,
                                 "net.openchrom.msd.converter.supplier.agilent.hp.prefs"));

                if (!fiSettingsFile.Exists)
                {
                    // Create the file

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating OpenChrom settings file at " + fiSettingsFile.FullName);

                    using (var writer = fiSettingsFile.CreateText())
                    {
                        writer.WriteLine("eclipse.preferences.version=1");
                        writer.WriteLine("productSerialKey=wlkXZsvC-miP6A2KH-DgAuTix2");
                        writer.WriteLine("productTrialKey=false");
                        writer.WriteLine("productTrialStartDateKey=1439335966145");
                        writer.WriteLine("trace=1");
                    }

                    return true;
                }

                var settingsData = new List<string>();
                var settingsFileUpdateRequired = false;
                var traceFound = false;

                using (var reader = new StreamReader(new FileStream(fiSettingsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (string.Equals(dataLine.Trim(), "productTrialKey=true", StringComparison.CurrentCultureIgnoreCase))
                        {
                            settingsData.Add("productSerialKey=wlkXZsvC-miP6A2KH-DgAuTix2");
                            settingsData.Add("productTrialKey=false");
                            settingsFileUpdateRequired = true;
                        }
                        else
                        {
                            if (dataLine.StartsWith("trace"))
                                traceFound = true;

                            settingsData.Add(dataLine);
                        }

                    }
                }

                if (!settingsFileUpdateRequired)
                {
                    if (m_DebugLevel >= 2)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "OpenChrom settings file is up to date at " + fiSettingsFile.FullName);

                    return true;
                }

                // Need to update the settings file with the SerialKey entry
                Thread.Sleep(50);
                    
                // Possibly add the trace= line
                if (!traceFound)
                    settingsData.Add("trace=1");

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding productSerialKey entry to OpenChrom settings file at " + fiSettingsFile.FullName);

                // Actually update the file
                using (var writer = new StreamWriter(new FileStream(fiSettingsFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var dataLine in settingsData)
                        writer.WriteLine(dataLine);
                }

                return true;
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception validating the OpenChrom CDF plugin";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

        }

        #endregion


        #region "Event handlers"

        private void AttachCmdrunnerEvents(clsRunDosProgram cmdRunner)
        {
            try
            {
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
                cmdRunner.Timeout += CmdRunner_Timeout;
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        void CmdRunner_Timeout()
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "cmdRunner timeout reported");
        }

        void CmdRunner_LoopWaiting()
        {

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 300)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "AgilentToUIMFConverter running; " + DateTime.UtcNow.Subtract(mAgilentToUIMFStartTime).TotalMinutes + " minutes elapsed");
            }
        }

        #endregion

    }
}
