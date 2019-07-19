//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//*********************************************************************************************************

using CaptureTaskManager;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using ThermoRawFileReader;
using UIMFLibrary;

namespace DatasetIntegrityPlugin
{
    /// <summary>
    /// Dataset Integrity plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Constants"
        private const float RAW_FILE_MIN_SIZE_KB = 50;
        private const float RAW_FILE_MIN_SIZE_KB_21T = 30;
        private const float RAW_FILE_MAX_SIZE_MB_LTQ = 2048;
        private const float RAW_FILE_MAX_SIZE_MB_ORBITRAP = 100000;
        private const float BAF_FILE_MIN_SIZE_KB = 16;
        private const float TDF_FILE_MIN_SIZE_KB = 50;
        private const float TDF_BIN_FILE_MIN_SIZE_KB = 50;
        private const float SER_FILE_MIN_SIZE_KB = 16;
        private const float FID_FILE_MIN_SIZE_KB = 16;
        private const float ACQ_METHOD_FILE_MIN_SIZE_KB = 5;
        private const float SCIEX_WIFF_FILE_MIN_SIZE_KB = 50;
        private const float SCIEX_WIFF_SCAN_FILE_MIN_SIZE_KB = 0.03F;
        private const float UIMF_FILE_MIN_SIZE_KB = 5;
        private const float UIMF_FILE_SMALL_SIZE_KB = 50;
        private const float TIMS_UIMF_FILE_MIN_SIZE_KB = 5;
        private const float AGILENT_MS_SCAN_BIN_FILE_MIN_SIZE_KB = 5;
        private const float AGILENT_MS_SCAN_BIN_FILE_SMALL_SIZE_KB = 50;
        private const float AGILENT_MS_PEAK_BIN_FILE_MIN_SIZE_KB = 50;
        private const float AGILENT_MS_PEAK_BIN_FILE_SMALL_SIZE_KB = 500;
        private const float AGILENT_DATA_MS_FILE_MIN_SIZE_KB = 75;
        private const float SHIMADZU_QGD_FILE_MIN_SIZE_KB = 50;
        private const float WATERS_FUNC_DAT_FILE_MIN_SIZE_KB = 10;
        private const float WATERS_FUNC_IND_FILE_MIN_SIZE_KB = 25;

        // MALDI imaging file
        // Prior to May 2014, used a minimum of 4 KB
        // However, seeing 12T_FTICR_B datasets where this file is as small as 120 Bytes
        private const float MCF_FILE_MIN_SIZE_KB = 0.1F;
        private const float ILLUMINA_TXT_GZ_FILE_MIN_SIZE_KB = 500;
        private const float ILLUMINA_TXT_GZ_FILE_SMALL_SIZE_KB = 1000;
        private const int MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES = 180;
        private const int MAX_AGILENT_TO_CDF_RUNTIME_MINUTES = 10;

        #endregion

        #region "Class-wide variables"

        private clsToolReturnData mRetData = new clsToolReturnData();

        private DateTime mProcessingStartTime;

        private string mConsoleOutputFilePath;

        private DateTime mLastProgressUpdate;

        private DateTime mLastStatusUpdate;

        private int mStatusUpdateIntervalMinutes;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the dataset integrity step tool
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            // Note that Debug messages are logged if mDebugLevel == 5

            var msg = "Starting DatasetIntegrityPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            var instrumentName = mTaskParams.GetParam("Instrument_Name");
            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);

            var agilentToUimfConverterPath = string.Empty;
            var openChromProgPath = string.Empty;

            // Check whether we need to convert from a .D directory to a .UIMF file
            var instrumentNamesToConvert = new List<string> {
                "IMS08",
                "IMS09"
            };

            var convertAgilentDotDToUIMF = instrumentNamesToConvert.Any(
                instrument => instrumentName.StartsWith(instrument, StringComparison.OrdinalIgnoreCase));

            if (convertAgilentDotDToUIMF)
            {
                agilentToUimfConverterPath = GetAgilentToUIMFProgPath();

                if (!File.Exists(agilentToUimfConverterPath))
                {
                    mRetData.CloseoutMsg = "AgilentToUIMFConverter not found at " + agilentToUimfConverterPath;
                    LogError(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return mRetData;
                }
            }

            // Check whether we need to convert from a .D directory to a .CDF file
            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap)
            {
                openChromProgPath = GetOpenChromProgPath();

                if (!File.Exists(openChromProgPath))
                {
                    mRetData.CloseoutMsg = "OpenChrom not found at " + openChromProgPath;
                    LogError(mRetData.CloseoutMsg);
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return mRetData;
                }
            }

            // Store the version info in the database
            if (!StoreToolVersionInfo(agilentToUimfConverterPath, openChromProgPath))
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mRetData.CloseoutMsg = "Error determining tool version info";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            msg = "Performing integrity test, dataset '" + mDataset + "'";
            LogMessage(msg);

            // Set up the file paths
            var storageVolExt = mTaskParams.GetParam("Storage_Vol_External");
            var storagePath = mTaskParams.GetParam("Storage_Path");
            var datasetDirectory = Path.Combine(storageVolExt, Path.Combine(storagePath, mDataset));
            string dataFileNamePath;

            // Select which tests will be performed based on instrument class

            msg = "Instrument class: " + instClassName;
            LogDebug(msg);

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
            {
                msg = "Instrument class not recognized: " + instClassName;
                LogError(msg);
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return mRetData;
            }

            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestFinniganIonTrapFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.GC_QExactive:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestGQExactiveFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    if (instrumentName.StartsWith("21T", StringComparison.OrdinalIgnoreCase))
                        mRetData.CloseoutType = Test21TRawFile(dataFileNamePath);
                    else
                        mRetData.CloseoutType = TestLTQ_FTFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerFTMS:
                    mRetData.CloseoutType = TestBrukerDirectory(datasetDirectory);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestThermoExactiveFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
                    mRetData.CloseoutType = TestTripleQuadFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
                    if (convertAgilentDotDToUIMF)
                    {
                        // Need to first convert the .d directory to a .UIMF file
                        if (!ConvertAgilentDotDDirectoryToUIMF(datasetDirectory, agilentToUimfConverterPath))
                        {
                            if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                            {
                                mRetData.CloseoutMsg = "Unknown error converting the Agilent .D directory to a .UIMF file";
                                LogError(mRetData.CloseoutMsg);
                            }

                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }

                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION);
                    mRetData.CloseoutType = TestIMSAgilentTOF(dataFileNamePath, instrumentName);
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
                    mRetData.CloseoutType = TestBrukerFT_Directory(datasetDirectory, requireBafOrSerFile: true, requireMCFFile: false, requireSerFile: false, instrumentClass: instrumentClass, instrumentName: instrumentName);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                    // Note: Datasets from this instrument were last used in 2012
                    mRetData.CloseoutType = TestBrukerMaldiImagingDirectory(datasetDirectory);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                    mRetData.CloseoutType = TestBrukerFT_Directory(datasetDirectory, requireBafOrSerFile: false, requireMCFFile: false, requireSerFile: true, instrumentClass: instrumentClass, instrumentName: instrumentName);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:
                    mRetData.CloseoutType = TestBrukerMaldiSpotDirectory(datasetDirectory);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerTOF_BAF:
                    mRetData.CloseoutType = TestBrukerTof_BafDirectory(datasetDirectory, instrumentName);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerTOF_TDF:
                    mRetData.CloseoutType = TestBrukerTof_TdfDirectory(datasetDirectory, instrumentName);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Sciex_QTrap:
                    mRetData.CloseoutType = TestSciexQTrapFile(datasetDirectory, mDataset);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap:
                    // .D directory with a DATA.MS file
                    mRetData.CloseoutType = TestAgilentIonTrapDirectory(datasetDirectory);

                    if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {

                        // Convert the .D directory to a .CDF file
                        if (!ConvertAgilentDotDDirectoryToCDF(datasetDirectory, openChromProgPath))
                        {
                            if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                            {
                                mRetData.CloseoutMsg = "Unknown error converting the Agilent .D directory to a .CDF file";
                                LogError(mRetData.CloseoutMsg);
                            }

                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Agilent_TOF_V2:
                    mRetData.CloseoutType = TestAgilentTOFv2Directory(datasetDirectory);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Illumina_Sequencer:
                    mRetData.CloseoutType = TestIlluminaSequencerDirectory(datasetDirectory);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Shimadzu_GC:
                    dataFileNamePath = Path.Combine(datasetDirectory, mDataset + clsInstrumentClassInfo.DOT_QGD_EXTENSION);
                    mRetData.CloseoutType = TestShimadzuQGDFile(dataFileNamePath);
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Waters_IMS:
                    mRetData.CloseoutType = TestWatersDotRawDirectory(datasetDirectory);
                    break;

                default:
                    msg = "No integrity test available for instrument class " + instClassName;
                    LogWarning(msg);
                    mRetData.EvalMsg = msg;
                    mRetData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    break;
            }

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return mRetData;
        }

        private bool ConvertAgilentDotDDirectoryToCDF(string datasetDirectoryPath, string exePath)
        {
            try
            {

                // Make sure the CDF plugin is installed and that the SerialKey is defined
                if (!ValidateCDFPlugin())
                {
                    return false;
                }

                var mgrName = mMgrParams.GetParam("MgrName", "CTM");
                var dotDDirectoryName = mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                var dotDDirectoryPathLocal = Path.Combine(mWorkDir, dotDDirectoryName);

                var success = CopyDotDDirectoryToLocal(mFileTools, datasetDirectoryPath, dotDDirectoryName, dotDDirectoryPathLocal, false);
                if (!success)
                    return false;

                // Create the BatchJob.obj file
                // This is an XML file with the information required by OpenChrom to create CDF file from the .D directory

                var batchJobFilePath = CreateOpenChromCDFJobFile(dotDDirectoryPathLocal);
                if (string.IsNullOrEmpty(batchJobFilePath))
                {
                    return false;
                }

                // ReSharper disable CommentTypo

                // Construct the command line arguments to run the OpenChrom

                // Syntax:
                // OpenChrom.exe -cli -batchfile E:\CTM_WorkDir\BatchJob.obj
                //
                // Optional: -nosplash

                // ReSharper restore CommentTypo

                // ReSharper disable once StringLiteralTypo
                var arguments = "-cli -batchfile " + batchJobFilePath;
                var consoleOutputFilePath = Path.Combine(mWorkDir, "OpenChrom_ConsoleOutput_" + mgrName + ".txt");

                var cmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = false,
                    EchoOutputToConsole = false,
                    CacheStandardOutput = false,
                    WriteConsoleOutputToFile = false,    // OpenChrom does not produce any console output; so no point in creating it
                    ConsoleOutputFilePath = consoleOutputFilePath
                };

                // This will also call RegisterEvents
                AttachCmdRunnerEvents(cmdRunner);

                mProcessingStartTime = DateTime.UtcNow;
                mLastProgressUpdate = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusUpdateIntervalMinutes = 5;

                var msg = "Converting .D directory to .CDF: " + exePath + " " + arguments;
                LogMessage(msg);

                const int maxRuntimeSeconds = MAX_AGILENT_TO_CDF_RUNTIME_MINUTES * 60;
                success = cmdRunner.RunProgram(exePath, arguments, "OpenChrom", true, maxRuntimeSeconds);

                // Delete the locally cached .D directory
                try
                {
                    ProgRunner.GarbageCollectNow();
                    mFileTools.DeleteDirectory(dotDDirectoryPathLocal, ignoreErrors: true);
                }
                catch (Exception ex)
                {
                    // Do not treat this as a fatal error
                    LogWarning("Exception deleting locally cached .D directory (" + dotDDirectoryPathLocal + "): " + ex.Message);
                }

                if (!success)
                {
                    mRetData.CloseoutMsg = "Error running OpenChrom";
                    LogError(mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("OpenChrom returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to OpenChrom failed (but exit code is 0)");
                    }

                    return false;
                }

                Thread.Sleep(100);

                // Copy the .CDF file to the dataset directory
                success = CopyCDFToDatasetDirectory(mFileTools, datasetDirectoryPath);
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
                mRetData.CloseoutMsg = "Exception converting .D directory to a CDF file";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ConvertAgilentDotDDirectoryToUIMF(string datasetDirectoryPath, string exePath)
        {
            try
            {

                var mgrName = mMgrParams.GetParam("MgrName", "CTM");
                var dotDDirectoryName = mDataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
                var dotDDirectoryPathLocal = Path.Combine(mWorkDir, dotDDirectoryName);

                var success = CopyDotDDirectoryToLocal(mFileTools, datasetDirectoryPath, dotDDirectoryName, dotDDirectoryPathLocal, true);
                if (!success)
                    return false;

                // Examine the .d directory to look for an AcqData subdirectory
                // If it does not have one, it might have a .d subdirectory that itself has an AcqData directory
                // For example, 001_14Sep18_RapidFire.d\sequence1.d\AcqData

                var dotDDirectory = new DirectoryInfo(dotDDirectoryPathLocal);
                var acqDataDir = dotDDirectory.GetDirectories("AcqData");
                var altDirFound = false;

                if (acqDataDir.Length == 0)
                {
                    // Use *.d to look for .d subdirectories
                    var dotDDirectoryAlt = dotDDirectory.GetDirectories("*" + clsInstrumentClassInfo.DOT_D_EXTENSION);

                    if (dotDDirectoryAlt.Length > 0)
                    {
                        foreach (var altDir in dotDDirectoryAlt)
                        {
                            var acqDataDirAlt = altDir.GetDirectories("AcqData");
                            if (acqDataDirAlt.Length > 0)
                            {
                                dotDDirectoryPathLocal = altDir.FullName;
                                altDirFound = true;
                                LogMessage("Using the .d directory below the primary .d subdirectory: " + altDir.FullName);
                                break;
                            }
                        }
                    }

                    if (!altDirFound)
                    {
                        mRetData.CloseoutMsg = ".D directory does not have an AcqData subdirectory";
                        LogError(mRetData.CloseoutMsg);
                        return false;
                    }
                }

                // Construct the command line arguments to run the AgilentToUIMFConverter

                // Syntax:
                // AgilentToUIMFConverter.exe [Agilent .d directory] [Directory to insert file (optional)]

                var arguments = clsConversion.PossiblyQuotePath(dotDDirectoryPathLocal) + " " + clsConversion.PossiblyQuotePath(mWorkDir);
                mConsoleOutputFilePath = Path.Combine(mWorkDir, "AgilentToUIMF_ConsoleOutput_" + mgrName + ".txt");

                var cmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = false,
                    EchoOutputToConsole = false,
                    CacheStandardOutput = false,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = mConsoleOutputFilePath
                };

                // This will also call RegisterEvents
                AttachCmdRunnerEvents(cmdRunner);

                mProcessingStartTime = DateTime.UtcNow;
                mLastProgressUpdate = DateTime.UtcNow;
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusUpdateIntervalMinutes = 5;

                var msg = "Converting .D directory to .UIMF: " + exePath + " " + arguments;
                LogMessage(msg);

                const int maxRuntimeSeconds = MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES * 60;
                success = cmdRunner.RunProgram(exePath, arguments, "AgilentToUIMFConverter", true, maxRuntimeSeconds);

                // Parse the console output file one more time to check for errors
                ParseConsoleOutputFile();

                // Delete the locally cached .D directory
                try
                {
                    ProgRunner.GarbageCollectNow();
                    mFileTools.DeleteDirectory(dotDDirectoryPathLocal, ignoreErrors: true);
                }
                catch (Exception ex)
                {
                    // Do not treat this as a fatal error
                    LogWarning("Exception deleting locally cached .D directory (" + dotDDirectoryPathLocal + "): " + ex.Message);
                }

                if (!success)
                {
                    mRetData.CloseoutMsg = "Error running the AgilentToUIMFConverter";
                    LogError(mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("AgilentToUIMFConverter returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to AgilentToUIMFConverter failed (but exit code is 0)");
                    }

                    return false;
                }

                Thread.Sleep(100);

                if (altDirFound)
                {
                    // We need to rename the .uimf file since we processed a .d directory inside a .d directory
                    var sourceUimfName = Path.ChangeExtension(Path.GetFileName(dotDDirectoryPathLocal), clsInstrumentClassInfo.DOT_UIMF_EXTENSION);
                    var sourceUimf = new FileInfo(Path.Combine(mWorkDir, sourceUimfName));
                    var targetUimfPath = Path.Combine(mWorkDir, mDataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION);

                    if (!string.Equals(sourceUimf.FullName, targetUimfPath, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!sourceUimf.Exists)
                        {
                            mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file named " + sourceUimf.Name;
                            LogError(mRetData.CloseoutMsg + ": " + sourceUimf.FullName);
                            return false;
                        }

                        LogDebug(string.Format("Renaming {0} to {1}", sourceUimf.FullName, Path.GetFileName(targetUimfPath)));
                        sourceUimf.MoveTo(targetUimfPath);
                    }

                }

                // Copy the .UIMF file to the dataset directory
                success = CopyUIMFToDatasetDirectory(mFileTools, datasetDirectoryPath);
                if (!success)
                {
                    return false;
                }

                // Delete the console output file
                DeleteFileIgnoreErrors(mConsoleOutputFilePath);

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception converting .D directory to a UIMF file";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool CopyDotDDirectoryToLocal(
            FileTools fileTools,
            string datasetDirectoryPath,
            string dotDDirectoryName,
            string dotDDirectoryPathLocal,
            bool requireIMSFiles)
        {
            var dotDDirectoryPathRemote = new DirectoryInfo(Path.Combine(datasetDirectoryPath, dotDDirectoryName));

            if (requireIMSFiles)
            {
                // Make sure the .D directory has the required files
                // Older datasets may have had their larger files purged, which will cause the AgilentToUIMFConverter to fail

                var binFiles = dotDDirectoryPathRemote.GetFiles("*.bin", SearchOption.AllDirectories).ToList();
                var fileNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var file in binFiles)
                    fileNames.Add(file.Name);

                var requiredFiles = new List<string>
                {
                    "IMSFrame.bin",
                    "MSPeak.bin",
                    // ReSharper disable once StringLiteralTypo
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
                    LogError(mRetData.CloseoutMsg);
                    return false;
                }
            }

            // Copy the dataset directory locally using Prism.DLL
            // Note that lock files will be used when copying large files (over 20 MB)

            ResetTimestampForQueueWaitTimeLogging();
            fileTools.CopyDirectory(dotDDirectoryPathRemote.FullName, dotDDirectoryPathLocal, true);

            return true;
        }

        private bool CopyCDFToDatasetDirectory(FileTools fileTools, string datasetDirectoryPath)
        {
            var cdfFile = new FileInfo(Path.Combine(mWorkDir, mDataset + ".cdf"));

            if (!cdfFile.Exists)
            {
                mRetData.CloseoutMsg = "OpenChrom did not create a .CDF file";
                LogError(mRetData.CloseoutMsg + ": " + cdfFile.FullName);
                return false;
            }

            var success = CopyFileToDatasetDirectory(fileTools, cdfFile, datasetDirectoryPath);
            return success;
        }

        private bool CopyUIMFToDatasetDirectory(FileTools fileTools, string datasetDirectoryPath)
        {

            var uimfFile = new FileInfo(Path.Combine(mWorkDir, mDataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION));

            if (!uimfFile.Exists)
            {
                mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file named " + uimfFile.Name;
                LogError(mRetData.CloseoutMsg + ": " + uimfFile.FullName);
                return false;
            }

            var success = CopyFileToDatasetDirectory(fileTools, uimfFile, datasetDirectoryPath);
            return success;
        }

        private bool CopyFileToDatasetDirectory(FileTools fileTools, FileSystemInfo dataFile, string datasetDirectoryPath)
        {
            if (mDebugLevel >= 4)
            {
                LogDebug("Copying " + dataFile.Extension + " file to the dataset directory");
            }

            ResetTimestampForQueueWaitTimeLogging();

            var targetFilePath = Path.Combine(datasetDirectoryPath, dataFile.Name);
            fileTools.CopyFileUsingLocks(dataFile.FullName, targetFilePath, overWrite: true);

            if (mDebugLevel >= 4)
            {
                LogDebug("Copy complete");
            }

            try
            {
                // Delete the local copy
                dataFile.Delete();
            }
            catch (Exception ex)
            {
                // Do not treat this as a fatal error
                LogWarning("Exception deleting local copy of the new .UIMF file " + dataFile.FullName + ": " + ex.Message);
            }

            return true;
        }

        private string CreateOpenChromCDFJobFile(string dotDDirectoryPathLocal)
        {
            try
            {
                var sbXML = new StringBuilder();

                sbXML.Append(@"<?xml version=""1.0"" encoding=""utf-8""?>");
                sbXML.Append(@"<BatchProcessJob>");
                sbXML.Append(@"<Header></Header>");

                sbXML.Append(@"<!--Load the following chromatograms.-->");
                sbXML.Append(@"<InputEntries><InputEntry>");
                sbXML.Append(@"<![CDATA[" + dotDDirectoryPathLocal + "]]>");
                sbXML.Append(@"</InputEntry></InputEntries>");

                sbXML.Append(@"<!--Process each chromatogram with the listed methods.-->");
                sbXML.Append(@"<ProcessEntries></ProcessEntries>");

                sbXML.Append(@"<!--Write each processed chromatogram to the given output formats.-->");
                sbXML.Append(@"<OutputEntries>");

                // ReSharper disable once StringLiteralTypo
                sbXML.Append(@"<OutputEntry converterId=""net.openchrom.msd.converter.supplier.cdf"">");
                sbXML.Append(@"<![CDATA[" + mWorkDir + "]]>");
                sbXML.Append(@"</OutputEntry>");
                sbXML.Append(@"</OutputEntries>");

                sbXML.Append(@"<!--Process each chromatogram with the listed report suppliers.-->");
                sbXML.Append(@"<ReportEntries></ReportEntries>");
                sbXML.Append(@"</BatchProcessJob>");

                var jobFilePath = Path.Combine(mWorkDir, "CDFBatchJob.obj");

                using (var writer = new StreamWriter(new FileStream(jobFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(sbXML);
                }

                return jobFilePath;

            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = ".D to CDF conversion failed; error in CreateOpenChromCDFJobFile";
                LogError("Error in CreateOpenChromCDFJobFile: " + ex.Message);
                return string.Empty;
            }
        }

        /// <summary>
        /// Processes directories in directoryList to compare the x_ directory to the non x_ directory
        /// If the x_ directory is empty or if every file in the x_ directory is also in the non x_ directory,
        /// will return True and optionally deletes the x_ directory
        /// </summary>
        /// <param name="directoryList">List of directories; must contain exactly 2 entries</param>
        /// <param name="deleteIfSuperseded"></param>
        /// <returns>True if this is a superseded directory and it is safe to delete</returns>
        private bool DetectSupersededDirectory(IReadOnlyList<DirectoryInfo> directoryList, bool deleteIfSuperseded)
        {
            string msg;

            try
            {
                if (directoryList.Count != 2)
                {
                    msg = "directoryList passed into DetectSupersededDirectory does not contain 2 directories; cannot check for a superseded directory";
                    LogDebug(msg);
                    return false;
                }

                DirectoryInfo newDirectory;
                DirectoryInfo oldDirectory;

                if (directoryList[0].Name.StartsWith("x_", StringComparison.OrdinalIgnoreCase))
                {
                    newDirectory = directoryList[1];
                    oldDirectory = directoryList[0];
                }
                else
                {
                    newDirectory = directoryList[0];
                    oldDirectory = directoryList[1];
                }

                if (oldDirectory.Name == "x_" + newDirectory.Name)
                {
                    // Yes, we have a case of a likely superseded directory
                    // Examine oldDirectory

                    msg = "Comparing files in superseded directory (" + oldDirectory.FullName + ") to newer directory (" + newDirectory.FullName + ")";
                    LogMessage(msg);

                    var directoryIsSuperseded = true;

                    var supersededFiles = oldDirectory.GetFiles("*", SearchOption.AllDirectories);

                    foreach (var supersededFile in supersededFiles)
                    {
                        var newFilePath = supersededFile.FullName.Replace(oldDirectory.FullName, newDirectory.FullName);
                        var newFile = new FileInfo(newFilePath);

                        if (!newFile.Exists)
                        {
                            msg = "File not found in newer directory: " + newFile.FullName;
                            LogMessage(msg);

                            directoryIsSuperseded = false;
                            break;
                        }

                        if (newFile.Length < supersededFile.Length)
                        {
                            msg = "Newer file is smaller than version in superseded directory: " + newFile.FullName;
                            LogMessage(msg);

                            directoryIsSuperseded = false;
                            break;
                        }
                    }

                    if (directoryIsSuperseded && deleteIfSuperseded)
                    {
                        // Delete oldDirectory
                        msg = "Deleting superseded directory: " + oldDirectory.FullName;
                        LogMessage(msg);

                        oldDirectory.Delete(true);
                    }

                    return directoryIsSuperseded;

                }

                msg = "Directory " + oldDirectory.FullName + " is not a superseded directory for " + newDirectory.FullName;
                LogMessage(msg);
                return false;
            }
            catch (Exception ex)
            {
                msg = "Error in DetectSupersededDirectory: " + ex.Message;
                LogError(msg);
                return false;
            }

        }

        /// <summary>
        /// Convert a file size in kilobytes to a string form with units KB, MB or GB
        /// </summary>
        /// <param name="fileSizeKB"></param>
        /// <returns></returns>
        private string FileSizeToString(float fileSizeKB)
        {
            var fileSizeGB = fileSizeKB / 1024.0 / 1024.0;
            var fileSizeMB = fileSizeKB / 1024.0;

            if (fileSizeGB > 1)
                return fileSizeGB.ToString("#0.0") + " GB";

            if (fileSizeMB > 1)
                return fileSizeMB.ToString("#0.0") + " MB";

            return fileSizeKB.ToString("#0") + " KB";
        }

        private string GetAgilentToUIMFProgPath()
        {
            var exeName = mMgrParams.GetParam("AgilentToUIMFProgLoc");
            var exePath = Path.Combine(exeName, "AgilentToUimfConverter.exe");
            return exePath;
        }

        private float GetLargestFileSizeKB(IEnumerable<FileInfo> filesToCheck)
        {
            var largestSizeKB = 0.0f;
            foreach (var file in filesToCheck)
            {
                var fileSizeKB = GetFileSize(file);
                if (fileSizeKB > largestSizeKB)
                    largestSizeKB = fileSizeKB;
            }

            return largestSizeKB;
        }

        private string GetOpenChromProgPath()
        {
            var exeName = mMgrParams.GetParam("OpenChromProgLoc");
            var exePath = Path.Combine(exeName, "openchrom.exe");
            return exePath;
        }

        /// <summary>
        /// Looks files matching fileSpec
        /// For matching files, copies the or moves them up one directory
        /// If matchDatasetName is true then requires that the file start with the name of the dataset
        /// </summary>
        /// <param name="datasetDirectory"></param>
        /// <param name="fileSpec">Files to match, for example *.mis</param>
        /// <param name="matchDatasetName">True if file names must start with mDataset</param>
        /// <param name="copyFile">True to copy the file, false to move it</param>
        private void MoveOrCopyUpOneLevel(DirectoryInfo datasetDirectory, string fileSpec, bool matchDatasetName, bool copyFile)
        {
            foreach (var instrumentFile in datasetDirectory.GetFiles(fileSpec))
            {
                if (matchDatasetName &&
                    !Path.GetFileNameWithoutExtension(instrumentFile.Name).StartsWith(mDataset, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (instrumentFile.Directory?.Parent == null)
                {
                    continue;
                }

                var newPath = Path.Combine(instrumentFile.Directory.Parent.FullName, instrumentFile.Name);
                if (File.Exists(newPath))
                {
                    continue;
                }

                if (copyFile)
                    instrumentFile.CopyTo(newPath, true);
                else
                    instrumentFile.MoveTo(newPath);
            }
        }

        private void ParseConsoleOutputFile()
        {
            var unhandledException = false;
            var exceptionText = string.Empty;
            float percentComplete = 0;

            var reProgressMatcher = new Regex(@"Converting frame (?<FramesProcessed>\d+) / (?<TotalFrames>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (!File.Exists(mConsoleOutputFilePath))
                {
                    return;
                }

                using (var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            continue;
                        }

                        var match = reProgressMatcher.Match(dataLine);
                        if (match.Success)
                        {
                            var framesProcessed = int.Parse(match.Groups["FramesProcessed"].Value);
                            var totalFrames = int.Parse(match.Groups["TotalFrames"].Value);

                            percentComplete = framesProcessed / (float)totalFrames * 100;
                        }
                        else if (unhandledException)
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
                        else if (dataLine.StartsWith("Error:", StringComparison.OrdinalIgnoreCase))
                        {
                            LogError("AgilentToUIMFConverter error: " + dataLine);
                        }
                        else if (dataLine.StartsWith("Exception in", StringComparison.OrdinalIgnoreCase))
                        {
                            LogError("AgilentToUIMFConverter error: " + dataLine);
                        }
                        else if (dataLine.StartsWith("Unhandled Exception", StringComparison.OrdinalIgnoreCase))
                        {
                            LogError("AgilentToUIMFConverter error: " + dataLine);
                            unhandledException = true;
                        }
                    }
                }

                if (!string.IsNullOrEmpty(exceptionText))
                {
                    LogError(exceptionText);
                }

                mStatusTools.UpdateAndWrite(EnumTaskStatusDetail.Running_Tool, percentComplete);

            }
            catch (Exception ex)
            {
                LogError("Exception in ParseConsoleOutputFile: " + ex.Message);
            }

        }

        /// <summary>
        /// If directoryList contains exactly two directories, calls DetectSupersededDirectory and deletes the extra x_ directory (if appropriate)
        /// Returns True if directoryList contains just one directory, or if able to successfully delete the extra x_ directory
        /// </summary>
        /// <param name="directoryList"></param>
        /// <param name="directoryDescription"></param>
        /// <returns>True if success; false if a problem</returns>
        private bool PossiblyRenameSupersededDirectory(IReadOnlyList<DirectoryInfo> directoryList, string directoryDescription)
        {
            var invalidDataset = true;

            if (directoryList.Count == 1)
                return true;

            if (directoryList.Count == 2)
            {
                // If two directories are present and one starts with x_ and all of the files
                // inside the one that start with x_ are also in the directory without x_,
                // delete the x_ directory
                const bool deleteIfSuperseded = true;

                if (DetectSupersededDirectory(directoryList, deleteIfSuperseded))
                {
                    invalidDataset = false;
                }
            }

            if (invalidDataset)
            {
                mRetData.EvalMsg = "Invalid dataset: Multiple " + directoryDescription + " directories found";
                LogError(mRetData.EvalMsg);
                return false;
            }

            return true;
        }

        private bool PositiveNegativeMethodDirectories(IReadOnlyList<DirectoryInfo> methodDirectories)
        {
            if (methodDirectories.Count != 2)
            {
                return false;
            }

            if (methodDirectories[0].Name.IndexOf("_neg", 0, StringComparison.OrdinalIgnoreCase) >= 0 &&
                methodDirectories[1].Name.IndexOf("_pos", 0, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            if (methodDirectories[1].Name.IndexOf("_neg", 0, StringComparison.OrdinalIgnoreCase) >= 0 &&
                methodDirectories[0].Name.IndexOf("_pos", 0, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            return false;
        }

        private void ReportFileSizeTooLarge(string dataFileDescription, string filePath, float actualSizeKB, float maxSizeKB)
        {
            var maxSizeText = FileSizeToString(maxSizeKB);

            // Data file may be corrupt
            var msg = dataFileDescription + " file may be corrupt. Actual file size is " +
                      FileSizeToString(actualSizeKB) + "; " +
                      "max allowable size is " + maxSizeText + "; see " + filePath;

            // Data file size is more than
            mRetData.EvalMsg = dataFileDescription + " file size is more than " + maxSizeText;

            LogError(msg);
        }

        private void ReportFileSizeTooSmall(string dataFileDescription, string filePath, float actualSizeKB, float minSizeKB)
        {
            // Example messages for mRetData.EvalMsg
            // Data file size is 75 KB; minimum allowed size 100 KB
            // ser file size is 8 KB; minimum allowed size 16 KB
            // ser file is 0 bytes

            var minSizeText = FileSizeToString(minSizeKB);

            // Data file may be corrupt
            var msg = dataFileDescription + " file may be corrupt. Actual file size is " +
                      FileSizeToString(actualSizeKB) + "; " +
                      "min allowable size is " + minSizeText + "; see " + filePath;

            if (Math.Abs(actualSizeKB) < 0.0001)
                mRetData.EvalMsg = dataFileDescription + " file is 0 bytes";
            else
                mRetData.EvalMsg = dataFileDescription + " file size is " +
                                   FileSizeToString(actualSizeKB) + "; " +
                                   "minimum allowed size " + minSizeText;

            LogError(msg);
        }

        /// <summary>
        /// Tests an Agilent_Ion_Trap directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestAgilentIonTrapDirectory(string datasetDirectoryPath)
        {

            // Verify only one .D directory in dataset
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);
            var dotDDirectories = datasetDirectory.GetDirectories("*.D").ToList();

            if (dotDDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .D directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dotDDirectories.Count > 1)
            {
                if (!PossiblyRenameSupersededDirectory(dotDDirectories, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Look for Data.MS file in the .D directory
            var instrumentData = dotDDirectories[0].GetFiles("DATA.MS");
            if (dotDDirectories[0].GetFiles("DATA.MS").Length == 0)
            {
                mRetData.EvalMsg = "Invalid dataset: DATA.MS file not found in the .D directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the DATA.MS file
            var dataFileSizeKB = GetFileSize(instrumentData.First());
            if (dataFileSizeKB < AGILENT_DATA_MS_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("DATA.MS", instrumentData.First().FullName, dataFileSizeKB, AGILENT_DATA_MS_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Tests a Agilent_TOF_V2 directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestAgilentTOFv2Directory(string datasetDirectoryPath)
        {
            // Verify only one .D directory in dataset
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);
            var dotDDirectories = datasetDirectory.GetDirectories("*.D").ToList();

            if (dotDDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .D directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dotDDirectories.Count > 1)
            {
                if (!PossiblyRenameSupersededDirectory(dotDDirectories, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Look for the AcqData directory below the .D directory
            var acqDataDirectories = dotDDirectories[0].GetDirectories("AcqData").ToList();
            if (acqDataDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: .D directory does not contain an AcqData subdirectory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (acqDataDirectories.Count > 1)
            {
                mRetData.EvalMsg = "Invalid dataset: multiple AcqData directories found in .D directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // The AcqData directory should contain one or more .Bin files, for example MSScan.bin, MSPeak.bin, and MSProfile.bin
            // Verify that the MSScan.bin file exists
            var msScanFile = acqDataDirectories[0].GetFiles("MSScan.bin").ToList();
            if (msScanFile.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset: MSScan.bin file not found in the AcqData directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            FileInfo msDataFile;

            // Make sure the MSPeak.bin exists
            var msPeakFile = acqDataDirectories[0].GetFiles("MSPeak.bin").ToList();
            if (msPeakFile.Count == 0)
            {
                // Some Agilent_QQQ_04 datasets have MSProfile.bin but do not have MSPeak.bin
                // Check for this
                var msProfileFile = acqDataDirectories[0].GetFiles("MSProfile.bin").ToList();

                if (msProfileFile.Count == 0)
                {
                    mRetData.EvalMsg = "Invalid dataset: MSPeak.bin or MSProfile.bin file not found in the AcqData directory";
                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }

                msDataFile = msProfileFile.First();
            }
            else
            {
                msDataFile = msPeakFile.First();
            }

            // Verify size of the MSScan.bin file
            var msScanBinFileSizeKB = GetFileSize(msScanFile.First());
            if (msScanBinFileSizeKB < AGILENT_MS_SCAN_BIN_FILE_SMALL_SIZE_KB)
            {
                // Allow a small MSScan.bin file if the MSPeak.bin file is also small
                var msPeakBinFileSizeKB = GetFileSize(msDataFile);
                if (msPeakBinFileSizeKB < AGILENT_MS_PEAK_BIN_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall(msDataFile.Name, msDataFile.FullName, msPeakBinFileSizeKB, AGILENT_MS_PEAK_BIN_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }

                if (msScanBinFileSizeKB < AGILENT_MS_SCAN_BIN_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall("MSScan.bin", msScanFile.First().FullName, msScanBinFileSizeKB, AGILENT_MS_SCAN_BIN_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                // The MSScan.bin file is over 50 KB
                // The MSPeak.bin file should be over 500 KB
                var msPeakBinFileSizeKB = GetFileSize(msDataFile);
                if (msPeakBinFileSizeKB < AGILENT_MS_PEAK_BIN_FILE_SMALL_SIZE_KB)
                {
                    if (msPeakBinFileSizeKB < AGILENT_MS_PEAK_BIN_FILE_MIN_SIZE_KB)
                    {
                        ReportFileSizeTooSmall(msDataFile.Name, msDataFile.FullName, msPeakBinFileSizeKB, AGILENT_MS_PEAK_BIN_FILE_MIN_SIZE_KB);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    WarnFileSizeTooSmall(msDataFile.Name, msDataFile.FullName, msPeakBinFileSizeKB, AGILENT_MS_PEAK_BIN_FILE_SMALL_SIZE_KB);
                }
            }

            // The AcqData directory should contain file MSTS.xml
            var mstsFiles = acqDataDirectories[0].GetFiles("MSTS.xml").ToList();
            if (mstsFiles.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset: MSTS.xml file not found in the AcqData directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Check to see if a .M directory exists
            var methodDirectories = acqDataDirectories[0].GetDirectories("*.m").ToList();
            if (methodDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .m directories found found in the AcqData directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (methodDirectories.Count > 1)
            {
                mRetData.EvalMsg = "Invalid dataset: Multiple .m directories found in the AcqData directory";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests the integrity of a Sciex QTrap dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <param name="datasetName"></param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestSciexQTrapFile(string dataFileNamePath, string datasetName)
        {
            // Verify .wiff file exists in storage directory
            var tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + clsInstrumentClassInfo.DOT_WIFF_EXTENSION);
            if (!File.Exists(tempFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + tempFileNamePath + " not found";
                LogError(mRetData.EvalMsg);
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

            // Verify .wiff.scan file exists in storage directory
            tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + ".wiff.scan");
            if (!File.Exists(tempFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + tempFileNamePath + " not found";
                LogError(mRetData.EvalMsg);
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
        }

        /// <summary>
        /// Tests the integrity of a Thermo Ion Trap Raw File
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestFinniganIonTrapFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_LTQ, true);
        }

        /// <summary>
        /// Tests the integrity of a 21T dataset .raw file
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType Test21TRawFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB_21T, RAW_FILE_MAX_SIZE_MB_ORBITRAP, true);
        }

        /// <summary>
        /// Tests the integrity of a GC_QExactive dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestGQExactiveFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, false);
        }

        /// <summary>
        /// Tests the integrity of an Orbitrap (LTQ_FT) dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestLTQ_FTFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, false);
        }

        /// <summary>
        /// Tests the integrity of a Thermo_Exactive dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestThermoExactiveFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, false);
        }

        /// <summary>
        /// Tests the integrity of a Triple Quad (TSQ) dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestTripleQuadFile(string dataFileNamePath)
        {
            return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB_ORBITRAP, true);
        }

        /// <summary>
        /// Test the integrity of a Thermo .Raw file
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
            // Verify file exists in storage directory
            if (!File.Exists(dataFileNamePath))
            {
                // File not found; look for alternate extensions
                var alternateExtensions = new List<string>();
                var alternateFound = false;

                alternateExtensions.Add("mgf");
                alternateExtensions.Add("mzXML");
                alternateExtensions.Add("mzML");

                foreach (var altExtension in alternateExtensions)
                {
                    var dataFileNamePathAlt = Path.ChangeExtension(dataFileNamePath, altExtension);
                    if (File.Exists(dataFileNamePathAlt))
                    {
                        mRetData.EvalMsg = "Raw file not found, but ." + altExtension + " file exists";
                        LogMessage(mRetData.EvalMsg);
                        minFileSizeKB = 25;
                        maxFileSizeMB = RAW_FILE_MAX_SIZE_MB_ORBITRAP;
                        dataFileNamePath = dataFileNamePathAlt;
                        alternateFound = true;
                        openRawFileIfTooSmall = false;
                        break;
                    }
                }

                if (!alternateFound)
                {
                    mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                    LogError(mRetData.EvalMsg);
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
                        using (var reader = new XRawFileIO(dataFileNamePath))
                        {
                            RegisterEvents(reader);

                            var scanCount = reader.GetNumScans();

                            if (scanCount > 0)
                            {

                                var dataCount = reader.GetScanData2D(1, out _);

                                if (dataCount > 0)
                                {
                                    validFile = true;
                                }

                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        LogError("Exception opening .Raw file: " + ex.Message);
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
                ReportFileSizeTooLarge("Data", dataFileNamePath, dataFileSizeKB, maxFileSizeMB * 1024);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests a Bruker directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <returns></returns>
        private EnumCloseOutType TestBrukerDirectory(string datasetDirectoryPath)
        {
            // Verify that directory 0.ser exists
            if (!Directory.Exists(Path.Combine(datasetDirectoryPath, "0.ser")))
            {
                mRetData.EvalMsg = "Invalid dataset: 0.ser directory not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that file acqus exists
            var serDirectoryPath = Path.Combine(datasetDirectoryPath, "0.ser");
            if (!File.Exists(Path.Combine(serDirectoryPath, "acqus")))
            {
                mRetData.EvalMsg = "Invalid dataset: acqus file not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify size of the acqus file
            var dataFileSizeKB = GetFileSize(Path.Combine(serDirectoryPath, "acqus"));
            if (dataFileSizeKB <= 0F)
            {
                mRetData.EvalMsg = "Invalid dataset: acqus file contains no data";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that the ser file is present
            if (!File.Exists(Path.Combine(serDirectoryPath, "ser")))
            {
                mRetData.EvalMsg = "Invalid dataset: ser file not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify the size of the ser file
            dataFileSizeKB = GetFileSize(Path.Combine(serDirectoryPath, "ser"));
            if (dataFileSizeKB < 100)
            {
                mRetData.EvalMsg = "Invalid dataset: ser file too small";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests a BrukerTOF_BAF directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <param name="instrumentName"></param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerTof_BafDirectory(string datasetDirectoryPath, string instrumentName)
        {
            var requiredFiles = new Dictionary<string, float> {
                { "analysis.baf", BAF_FILE_MIN_SIZE_KB}
            };

            return TestBrukerTof_Directory(datasetDirectoryPath, instrumentName, requiredFiles);
        }

        /// <summary>
        /// Tests a BrukerTOF_TDF directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <param name="instrumentName"></param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerTof_TdfDirectory(string datasetDirectoryPath, string instrumentName)
        {
            var requiredFiles = new Dictionary<string, float> {
                { "analysis.tdf", TDF_FILE_MIN_SIZE_KB},
                { "analysis.tdf_bin", TDF_BIN_FILE_MIN_SIZE_KB}
            };

            return TestBrukerTof_Directory(datasetDirectoryPath, instrumentName, requiredFiles, false);
        }

        /// <summary>
        /// Tests a BrukerTOF_TDF directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <param name="instrumentName"></param>
        /// <param name="requiredInstrumentFiles">Dictionary listing the required file(s); keys are filename and values are minimum size in KB</param>
        /// <param name="requireMethodDirectory">When true, a subdirectory ending in .m must exist</param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerTof_Directory(
            string datasetDirectoryPath,
            string instrumentName,
            Dictionary<string, float> requiredInstrumentFiles,
            bool requireMethodDirectory = true)
        {
            // Verify only one .D directory in the dataset
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);
            var dotDDirectories = datasetDirectory.GetDirectories("*.D").ToList();

            if (dotDDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .D directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dotDDirectories.Count > 1)
            {
                if (!PossiblyRenameSupersededDirectory(dotDDirectories, clsInstrumentClassInfo.DOT_D_EXTENSION))
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that the files in requiredInstrumentFiles exist
            foreach (var requiredFile in requiredInstrumentFiles)
            {
                var requiredFileName = requiredFile.Key;
                var minimumFileSizeKB = requiredFile.Value;

                var foundFiles = dotDDirectories[0].GetFiles(requiredFileName).ToList();
                if (foundFiles.Count == 0)
                {
                    mRetData.EvalMsg = string.Format("Invalid dataset: {0} file not found", requiredFileName);
                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }

                // Verify size of the instrument file
                var dataFileSizeKB = GetFileSize(foundFiles.First());
                if (dataFileSizeKB < minimumFileSizeKB)
                {
                    ReportFileSizeTooSmall(requiredFileName, foundFiles.First().FullName, dataFileSizeKB, minimumFileSizeKB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Check to see if at least one .M directory exists
            var methodDirectories = dotDDirectories[0].GetDirectories("*.m").ToList();
            if (methodDirectories.Count < 1)
            {
                if (!requireMethodDirectory)
                {
                    // .m directory does not exist; no further checks need to be performed
                    return EnumCloseOutType.CLOSEOUT_SUCCESS;
                }

                mRetData.EvalMsg = "Invalid dataset: No .m directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (methodDirectories.Count > 1)
            {
                // Multiple .M directories
                // This is OK for the Bruker Imaging instruments and for Maxis_01
                var instrumentNameLCase = instrumentName.ToLower();
                if (!instrumentNameLCase.Contains("imaging") && !instrumentNameLCase.Contains("maxis"))
                {
                    // It's also OK if there are two directories, and one contains _neg and one contains _pos
                    if (!PositiveNegativeMethodDirectories(methodDirectories))
                    {
                        mRetData.EvalMsg = "Invalid dataset: Multiple .M directories found";
                        LogError(mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            // Determine if at least one .method file exists
            var methodFiles = methodDirectories.First().GetFiles("*.method").ToList();
            if (methodFiles.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset: No .method files found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests a BrukerFT directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <param name="requireBafOrSerFile">Set to True to require that the analysis.baf or ser file be present</param>
        /// <param name="requireMCFFile">Set to True to require that the .mcf file be present</param>
        /// <param name="requireSerFile">Set to True to require that the ser file be present</param>
        /// <param name="instrumentClass"></param>
        /// <param name="instrumentName"></param>
        /// <returns>Enum indicating test result</returns>
        private EnumCloseOutType TestBrukerFT_Directory(
            string datasetDirectoryPath,
            bool requireBafOrSerFile,
            bool requireMCFFile,
            bool requireSerFile,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            string instrumentName)
        {
            float dataFileSizeKB;
            float bafFileSizeKB = 0;

            // If there is only one .D directory in the dataset, the directory name should match the dataset name
            // Otherwise, if multiple directories, make sure each has a ser file
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);
            var dotDDirectories = datasetDirectory.GetDirectories("*.D").ToList();

            if (dotDDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .D directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dotDDirectories.Count == 1)
            {
                var baseName = Path.GetFileNameWithoutExtension(dotDDirectories[0].Name);
                if (!string.Equals(mDataset, baseName, StringComparison.OrdinalIgnoreCase))
                {
                    mRetData.EvalMsg = string.Format(
                        "Invalid dataset: directory name {0} does not match the dataset name",
                        dotDDirectories[0].Name);

                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                var allowMultipleDirectories = false;

                if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2)
                {
                    var serFound = false;
                    var fidFound = false;

                    // Each .D directory should have a ser file, fid file, or .baf file
                    foreach (var dotDDirectory in dotDDirectories)
                    {
                        var serFiles = dotDDirectory.GetFiles("ser", SearchOption.TopDirectoryOnly).Length;
                        var fidFiles = dotDDirectory.GetFiles("fid", SearchOption.TopDirectoryOnly).Length;
                        var bafFiles = dotDDirectory.GetFiles("*.baf", SearchOption.TopDirectoryOnly).Length;

                        if (serFiles > 0)
                            serFound = true;

                        if (fidFiles > 0)
                            fidFound = true;

                        if (serFiles == 0 && fidFiles == 0 && bafFiles == 0)
                        {
                            mRetData.EvalMsg = "Invalid dataset: ser, fid, or .baf file not found in " + dotDDirectory.Name;
                            LogError(mRetData.EvalMsg);
                            return EnumCloseOutType.CLOSEOUT_FAILED;
                        }
                    }

                    // Require at least one directory to have a ser or fid file
                    if (!(serFound || fidFound))
                    {
                        // If we get here, none of the directories had a ser or fid file
                        // Based on logic in the above for loop, then must have had a .baf file

                        mRetData.EvalMsg = "Invalid dataset: none of the .D directories had a ser or fid file, but instead had .baf files; instrument class should likely be BrukerFT_BAF or BrukerTOF_BAF";
                        LogError(mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    allowMultipleDirectories = true;
                }
                else if (dotDDirectories.Count == 2)
                {
                    // If one directory contains a ser file and the other directory contains an analysis.baf, we'll allow this
                    // This is sometimes the case for 15T_FTICR_Imaging
                    var serCount = 0;
                    var bafCount = 0;
                    foreach (var dotDDirectory in dotDDirectories)
                    {
                        if (dotDDirectory.GetFiles("ser", SearchOption.TopDirectoryOnly).Length == 1)
                            serCount += 1;

                        if (dotDDirectory.GetFiles("analysis.baf", SearchOption.TopDirectoryOnly).Length == 1)
                            bafCount += 1;
                    }

                    if (bafCount == 1 && serCount == 1)
                        allowMultipleDirectories = true;
                }

                if (!allowMultipleDirectories)
                {
                    if (!PossiblyRenameSupersededDirectory(dotDDirectories, clsInstrumentClassInfo.DOT_D_EXTENSION))
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                }

            }

            // Possibly verify that the analysis.baf file exists
            var bafFile = dotDDirectories[0].GetFiles("analysis.baf").ToList();
            var bafFileExists = bafFile.Count > 0;

            if (bafFileExists)
            {
                // Verify size of the analysis.baf file
                dataFileSizeKB = GetFileSize(bafFile.First());
                if (dataFileSizeKB < BAF_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall("Analysis.baf", bafFile.First().FullName, dataFileSizeKB, BAF_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
                bafFileSizeKB = dataFileSizeKB;
            }

            // Check whether any .mcf files exist
            // Note that "*.mcf" will match files with extension .mcf and files with extension .mcf_idx

            var mctFileName = string.Empty;
            dataFileSizeKB = 0;
            var mcfFileExists = false;
            long mcfFileSizeMax = 0;

            foreach (var mcfFile in dotDDirectories[0].GetFiles("*.mcf"))
            {
                if (mcfFile.Length > dataFileSizeKB * 1024)
                {
                    dataFileSizeKB = mcfFile.Length / (1024F);
                    mctFileName = mcfFile.Name;
                    mcfFileExists = true;
                }

                if (mcfFile.Length > mcfFileSizeMax)
                    mcfFileSizeMax = mcfFile.Length;
            }

            if (!mcfFileExists && requireMCFFile)
            {
                mRetData.EvalMsg = "Invalid dataset: .mcf file not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (mcfFileExists)
            {
                // Verify size of the largest .mcf file
                float minSizeKB;
                if (string.Equals(mctFileName, "Storage.mcf_idx", StringComparison.OrdinalIgnoreCase))
                    minSizeKB = 4;
                else
                    minSizeKB = MCF_FILE_MIN_SIZE_KB;

                if (dataFileSizeKB < minSizeKB)
                {
                    ReportFileSizeTooSmall(".MCF", mctFileName, dataFileSizeKB, minSizeKB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Verify ser file (if it exists)
            // For 15T imaging directories, only checking the ser file in the first .D directory
            var serFile = dotDDirectories[0].GetFiles("ser").ToList();
            var serFileExists = serFile.Count > 0;

            if (serFileExists)
            {
                // ser file found; verify its size
                dataFileSizeKB = GetFileSize(serFile.First());
                if (dataFileSizeKB < SER_FILE_MIN_SIZE_KB)
                {
                    // If on the 15T and the ser file is small but the .mcf file is not empty, then this is OK
                    if (!(string.Equals(instrumentName, "15T_FTICR", StringComparison.OrdinalIgnoreCase) && mcfFileSizeMax > 0))
                    {
                        ReportFileSizeTooSmall("ser", serFile.First().FullName, dataFileSizeKB, SER_FILE_MIN_SIZE_KB);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            else
            {   // ser file not found in the first .D directory
                if (requireSerFile)
                {
                    mRetData.EvalMsg = "Invalid dataset: ser file not found in " + dotDDirectories[0].Name;
                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }

                // Check to see if a fid file exists instead of a ser file
                var fidFile = dotDDirectories[0].GetFiles("fid").ToList();
                if (fidFile.Count > 0)
                {
                    // fid file found; verify size
                    dataFileSizeKB = GetFileSize(fidFile.First());
                    if (dataFileSizeKB < FID_FILE_MIN_SIZE_KB)
                    {
                        ReportFileSizeTooSmall("fid", fidFile.First().FullName, dataFileSizeKB, FID_FILE_MIN_SIZE_KB);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    // No ser or fid file found
                    // Ignore this error if on the 15T
                    if (!string.Equals(instrumentName, "15T_FTICR", StringComparison.OrdinalIgnoreCase))
                    {
                        mRetData.EvalMsg = "Invalid dataset: No ser or fid file found";
                        if (bafFileSizeKB > 0 && bafFileSizeKB < 100)
                        {
                            mRetData.EvalMsg += "; additionally, the analysis.baf file is quite small";
                        }
                        LogError(mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (requireBafOrSerFile && !bafFileExists && !serFileExists)
            {
                mRetData.EvalMsg = "Invalid dataset: dataset does not have an analysis.baf file or ser file";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2)
            {
                // Look for any files in the first .D directory that match Dataset.mis or Dataset.jpg
                // If found, copy them up one directory

                MoveOrCopyUpOneLevel(dotDDirectories[0], "*.mis", matchDatasetName: true, copyFile: true);
                MoveOrCopyUpOneLevel(dotDDirectories[0], "*.bak", matchDatasetName: true, copyFile: true);
                MoveOrCopyUpOneLevel(dotDDirectories[0], "*.jpg", matchDatasetName: false, copyFile: true);
            }

            // Check to see if a .M directory exists
            var methodDirectories = dotDDirectories[0].GetDirectories("*.m").ToList();
            if (methodDirectories.Count < 1)
            {
                if (string.Equals(instrumentName, "15T_FTICR", StringComparison.OrdinalIgnoreCase))
                {
                    // 15T datasets acquired in March 2019 have an analysis.baf file but no .m directories
                    LogWarning("Dataset does not have a method directory below the .d directory; this is allowed on the 15T");
                    return EnumCloseOutType.CLOSEOUT_SUCCESS;
                }

                mRetData.EvalMsg = "Invalid dataset: No .m directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (methodDirectories.Count > 1)
            {
                // Multiple .M directories
                // Allow this if there are two directories, and one contains _neg and one contains _pos
                // Also allow this if on the 12T or on the 15T
                var instrumentNameLCase = instrumentName.ToLower();

                if (!PositiveNegativeMethodDirectories(methodDirectories) &&
                    instrumentNameLCase.Contains("15t_fticr") &&
                    instrumentNameLCase.Contains("12t_fticr") &&
                    instrumentNameLCase.Contains("imaging"))
                {
                    mRetData.EvalMsg = "Invalid dataset: Multiple .M directories found";
                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Determine if apexAcquisition.method file exists and meets minimum size requirements
            var apexAcqMethod = methodDirectories.First().GetFiles("apexAcquisition.method").ToList();
            if (apexAcqMethod.Count == 0)
            {
                mRetData.EvalMsg = "Invalid dataset: apexAcquisition.method file not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            dataFileSizeKB = GetFileSize(apexAcqMethod.First());
            if (dataFileSizeKB < ACQ_METHOD_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("apexAcquisition.method", apexAcqMethod.First().FullName, dataFileSizeKB, ACQ_METHOD_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Tests a BrukerMALDI_Imaging directory for integrity
        /// This function does not apply to instrument class BrukerMALDI_Imaging_V2
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestBrukerMaldiImagingDirectory(string datasetDirectoryPath)
        {
            // Verify at least one zip file exists in dataset directory
            var fileList = Directory.GetFiles(datasetDirectoryPath, "*.zip");
            if (fileList.Length < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No zip files found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests a BrukerMALDI_Spot directory for integrity
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to the dataset directory</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestBrukerMaldiSpotDirectory(string datasetDirectoryPath)
        {

            // Verify the dataset directory doesn't contain any .zip files
            var zipFiles = Directory.GetFiles(datasetDirectoryPath, "*.zip");
            if (zipFiles.Length > 0)
            {
                mRetData.EvalMsg = "Zip files found in dataset directory " + datasetDirectoryPath;
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether the dataset directory contains just one data directory or multiple data directories
            var subDirectories = Directory.GetDirectories(datasetDirectoryPath);

            if (subDirectories.Length < 1)
            {
                mRetData.EvalMsg = "No subdirectories were found in the dataset directory " + datasetDirectoryPath;
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (subDirectories.Length > 1)
            {
                // Make sure the subdirectories match the naming convention for MALDI spot directories
                // Example directory names:
                //  0_D4
                //  0_E10
                //  0_N4

                var maldiSpotDirMatcher = new Regex(@"^\d_[A-Z]\d+$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var subdirectory in subDirectories)
                {
                    LogDebug("Test directory " + subdirectory + " against RegEx " + maldiSpotDirMatcher);

                    var subdirectoryName = Path.GetFileName(subdirectory);
                    if (subdirectoryName != null && !maldiSpotDirMatcher.IsMatch(subdirectoryName, 0))
                    {
                        mRetData.EvalMsg = string.Format("Dataset directory contains multiple subdirectories, " +
                                                         "but directory {0} does not match the expected pattern ({1}); see {2}",
                                                         subdirectoryName, maldiSpotDirMatcher, datasetDirectoryPath);
                        LogError(mRetData.EvalMsg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            // If we got to here, everything is OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Verifies the integrity of a .gz file
        /// </summary>
        /// <param name="gzipFilePath">File to verify</param>
        /// <param name="errorMessage">Error message (output)</param>
        /// <returns>True if valid, false if an error</returns>
        private bool TestGzipFile(string gzipFilePath, out string errorMessage)
        {
            const int BYTES_PER_READ = 81920;
            long bytesRead = 0;

            errorMessage = string.Empty;

            try
            {

                var sourceFile = new FileInfo(gzipFilePath);
                if (!sourceFile.Exists)
                {
                    errorMessage = "File not found: " + gzipFilePath;
                    return false;
                }

                using (var inFile = new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var reader = new GZipStream(inFile, CompressionMode.Decompress))
                    {
                        if (!reader.CanRead)
                        {
                            errorMessage = "File is not readable";
                            return false;
                        }

                        var buffer = new byte[BYTES_PER_READ];

                        // Copy the decompression stream into the output file.
                        while (reader.CanRead)
                        {
                            var newBytes = reader.Read(buffer, 0, BYTES_PER_READ);
                            if (newBytes <= 0)
                                break;

                            bytesRead += newBytes;
                        }

                    }

                    LogMessage(string.Format("Read {0:N1} KB from {1}", bytesRead / 1024.0, sourceFile.Name));
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error testing .gz file " + gzipFilePath, ex);
                errorMessage = ex.Message;
                return false;
            }

        }

        private EnumCloseOutType TestIlluminaSequencerDirectory(string datasetDirectoryPath)
        {
            var dataFileNamePath = Path.Combine(datasetDirectoryPath, mDataset + clsInstrumentClassInfo.DOT_TXT_GZ_EXTENSION);

            // Verify file exists in storage directory
            if (!File.Exists(dataFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(dataFileNamePath);

            // Check min size
            if (dataFileSizeKB < ILLUMINA_TXT_GZ_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, ILLUMINA_TXT_GZ_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dataFileSizeKB < ILLUMINA_TXT_GZ_FILE_SMALL_SIZE_KB)
            {
                // File is less than 1 MB in size
                mRetData.EvalMsg = string.Format("Data file size is less than {0:F0} KB", ILLUMINA_TXT_GZ_FILE_SMALL_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify the integrity of the .gz file
            var validGzFile = TestGzipFile(dataFileNamePath, out var errorMessage);

            if (!validGzFile)
            {
                mRetData.EvalMsg = string.Format(errorMessage);
                return EnumCloseOutType.CLOSEOUT_FAILED;

            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        private EnumCloseOutType TestIMSAgilentTOF(string dataFileNamePath, string instrumentName)
        {
            // Verify file exists in storage directory
            if (!File.Exists(dataFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(dataFileNamePath);

            // Check min size
            if (instrumentName.StartsWith("TIMS_Maxis", StringComparison.OrdinalIgnoreCase))
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

                var validFile = UimfFileHasData(dataFileNamePath, out var uimfStatusMessage);

                if (dataFileSizeKB < UIMF_FILE_SMALL_SIZE_KB)
                {
                    // File is between 5 and 50 KB
                    // Make sure that one of the frame scans has data

                    if (!validFile)
                    {
                        mRetData.EvalMsg = string.Format("Data file size is less than {0:F0} KB; it {1}", UIMF_FILE_SMALL_SIZE_KB, uimfStatusMessage);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                }
                else
                {
                    if (!validFile)
                    {
                        mRetData.EvalMsg = string.Format("Data file is {0}; it {1}", FileSizeToString(dataFileSizeKB), uimfStatusMessage);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            // Verify that the pressure columns are in the correct order
            if (!ValidatePressureInfo(dataFileNamePath, instrumentName))
            {
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests the integrity of a Shimadzu GC-MS dataset
        /// </summary>
        /// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
        /// <returns>Enum indicating success or failure</returns>
        private EnumCloseOutType TestShimadzuQGDFile(string dataFileNamePath)
        {
            // Verify file exists in storage directory
            if (!File.Exists(dataFileNamePath))
            {
                mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(dataFileNamePath);

            // Check min size
            if (dataFileSizeKB < SHIMADZU_QGD_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, SHIMADZU_QGD_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Tests the integrity of a Waters .raw directory
        /// </summary>
        /// <param name="datasetDirectoryPath"></param>
        /// <returns></returns>
        private EnumCloseOutType TestWatersDotRawDirectory(string datasetDirectoryPath)
        {
            // There should be one .raw directory in the dataset
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);
            var dotRawDirectories = datasetDirectory.GetDirectories("*.raw").ToList();

            if (dotRawDirectories.Count < 1)
            {
                mRetData.EvalMsg = "Invalid dataset: No .raw directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (dotRawDirectories.Count == 1)
            {
                var baseName = Path.GetFileNameWithoutExtension(dotRawDirectories[0].Name);
                if (!string.Equals(mDataset, baseName, StringComparison.OrdinalIgnoreCase))
                {
                    mRetData.EvalMsg = string.Format(
                        "Invalid dataset: directory name {0} does not match the dataset name",
                        dotRawDirectories[0].Name);

                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (dotRawDirectories.Count > 1)
            {
                mRetData.EvalMsg = "Invalid dataset: Multiple .raw directories found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that at least one _FUNC000.DAT or _FUNC001.DAT file exists
            var datFiles = dotRawDirectories[0].GetFiles("_FUNC*.DAT").ToList();
            var indFiles = dotRawDirectories[0].GetFiles("_FUNC*.ind").ToList();

            var fileExists = datFiles.Count > 0;

            if (!fileExists)
            {
                mRetData.EvalMsg = "Invalid dataset: _FUNC001.DAT file not found";
                LogError(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Verify the size of the .dat file(s)
            var largestDatFileKB = GetLargestFileSizeKB(datFiles);

            if (largestDatFileKB < WATERS_FUNC_DAT_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall(datFiles.First().Name, datFiles.First().FullName, largestDatFileKB, WATERS_FUNC_DAT_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (indFiles.Count == 0)
            {
                mRetData.EvalMsg = "Possibly corrupt dataset; no _FUNC*.ind files";
                LogWarning(mRetData.EvalMsg);
            }
            else
            {
                // Verify the size of the .ind file(s)
                var largestIndFileKB = GetLargestFileSizeKB(indFiles);

                if (largestIndFileKB < WATERS_FUNC_IND_FILE_MIN_SIZE_KB)
                {
                    ReportFileSizeTooSmall(indFiles.First().Name, indFiles.First().FullName, largestDatFileKB, WATERS_FUNC_IND_FILE_MIN_SIZE_KB);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Verify that each .dat file has a .idx file
            foreach (var datFile in datFiles)
            {
                var indexFile = new FileInfo(Path.ChangeExtension(datFile.FullName, "IDX"));

                if (!indexFile.Exists)
                {
                    mRetData.EvalMsg = string.Format("Invalid dataset: {0} file not found", indexFile.Name);
                    LogError(mRetData.EvalMsg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // ReSharper disable CommentTypo
            // Verify that the _FUNCTNS.INF exists
            // ReSharper restore CommentTypo

            var infFile = new FileInfo(Path.Combine(dotRawDirectories[0].FullName, "_FUNCTNS.INF"));

            if (!infFile.Exists)
            {
                // ReSharper disable once StringLiteralTypo
                var errorMessage = "Invalid dataset: _FUNCTNS.INF file not found";
                LogError(errorMessage);
                mRetData.EvalMsg = AppendToComment(mRetData.EvalMsg, errorMessage);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything was OK
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Opens the .UIMF file to obtain the list of frames
        /// Retrieves the scan data for each frame until a scan is encountered that has at least one data point
        /// </summary>
        /// <param name="uimfFilePath">UIMF File to open</param>
        /// <param name="uimfStatusMessage">Output: status message</param>
        /// <returns>True if the file can be opened and has valid spectra, otherwise false</returns>
        private bool UimfFileHasData(string uimfFilePath, out string uimfStatusMessage)
        {
            try
            {
                LogDebug("Opening UIMF file to look for valid data");

                // Open the .UIMF file and read the first scan of the first frame
                using (var uimfReader = new DataReader(uimfFilePath))
                {

                    var frameList = uimfReader.GetMasterFrameList();

                    if (frameList.Count <= 0)
                    {
                        uimfStatusMessage = "appears corrupt (no frame info)";
                        return false;
                    }

                    foreach (var frameEntry in frameList)
                    {
                        var frameNumber = frameEntry.Key;
                        var frameType = frameEntry.Value;
                        var frameScans = uimfReader.GetFrameScans(frameEntry.Key);

                        foreach (var scanNum in frameScans)
                        {

                            var dataCount = uimfReader.GetSpectrum(
                                frameNumber, frameType, scanNum.Scan,
                                out _, out _);

                            if (dataCount > 0)
                            {
                                // Valid data has been found
                                uimfStatusMessage = string.Empty;
                                return true;
                            }
                        }
                    }
                }

                uimfStatusMessage = "has frame info but no scan data";
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in UimfFileHasData: " + ex.Message);
                uimfStatusMessage = "appears corrupt (exception reading data)";
                return false;
            }
        }

        /// <summary>
        /// Extracts the pressure data from the Frame_Parameters table
        /// </summary>
        /// <param name="dataFileNamePath"></param>
        /// <param name="instrumentName"></param>
        /// <returns>True if the pressure values are correct; false if the columns have swapped data</returns>
        private bool ValidatePressureInfo(string dataFileNamePath, string instrumentName)
        {

            // Example of correct pressures:
            //   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
            //   8.33844                     3.87086                3.92628                0.23302

            // Example of incorrect pressures:
            //   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
            //   4.06285                     9.02253                0.41679                4.13393

            LogDebug("Opening UIMF file to read pressure data");

            var ignorePressureErrors = mTaskParams.GetParam("IgnorePressureInfoErrors", false);
            var loggedPressureErrorWarning = false;

            // Open the file with the UIMFReader
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

                    if (instrumentName.StartsWith("IMS05", StringComparison.OrdinalIgnoreCase))
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
                            mRetData.EvalMsg = "Invalid pressure info in the Frame_Parameters table for frame " + frameNumber + ", dataset " + mDataset + "; QuadrupolePressure should be less than the RearIonFunnelPressure and the RearIonFunnelPressure should be less than the HighPressureFunnelPressure";

                            if (ignorePressureErrors)
                            {
                                if (!loggedPressureErrorWarning)
                                {
                                    loggedPressureErrorWarning = true;
                                    LogError(mRetData.EvalMsg, true);
                                }
                            }
                            else
                            {

                                LogError(mRetData.EvalMsg, true);

                                uimfReader.Dispose();
                                return false;
                            }
                        }
                    }

                    if (frameNumber % 100 == 0)
                        LogDebug("Validated frame " + frameNumber);

                }

            }

            return true;
        }

        private void WarnFileSizeTooSmall(string dataFileDescription, string filePath, float actualSizeKB, float smallSizeThresholdKB)
        {
            // Example messages for mRetData.EvalMsg
            // MSPeak.bin file size is 259 KB; typically it is at least 500 KB

            var thresholdText = FileSizeToString(smallSizeThresholdKB);

            // Data file may be corrupt
            var msg = dataFileDescription + " file may be corrupt. Actual file size is " +
                      FileSizeToString(actualSizeKB) + "; " +
                      "typically the file is at least " + thresholdText + "; see " + filePath;

            if (Math.Abs(actualSizeKB) < 0.0001)
                mRetData.EvalMsg = dataFileDescription + " file is 0 bytes";
            else
                mRetData.EvalMsg = dataFileDescription + " file size is " +
                                   FileSizeToString(actualSizeKB) + "; " +
                                   "typically the file is at least " + thresholdText;

            LogWarning(msg);
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
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed clsPluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Gets the length of a single file in KB
        /// </summary>
        /// <param name="fileNamePath">Fully qualified path to input file</param>
        /// <returns>File size in KB</returns>
        private float GetFileSize(string fileNamePath)
        {
            var dataFile = new FileInfo(fileNamePath);
            return GetFileSize(dataFile);
        }

        /// <summary>
        /// Gets the length of a single file in KB
        /// </summary>
        /// <param name="dataFile">File info object</param>
        /// <returns>File size in KB</returns>
        private float GetFileSize(FileInfo dataFile)
        {
            var fileLengthKB = dataFile.Length / 1024F;
            return fileLengthKB;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string agilentToUimfConverterPath, string openChromProgPath)
        {

            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectoryPath = clsUtilities.GetAppDirectoryPath();

            if (string.IsNullOrEmpty(appDirectoryPath))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Integrity plugin");
                return false;
            }

            // Lookup the version of the Capture tool plugin
            var pluginPath = Path.Combine(appDirectoryPath, "DatasetIntegrityPlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
                return false;

            // Lookup the version of SQLite
            var sqLitePath = Path.Combine(appDirectoryPath, "System.Data.SQLite.dll");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, sqLitePath);
            if (!success)
                return false;

            // Lookup the version of the UIMFLibrary
            var uimfLibraryPath = Path.Combine(appDirectoryPath, "UIMFLibrary.dll");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, uimfLibraryPath);
            if (!success)
                return false;

            if (!string.IsNullOrWhiteSpace(agilentToUimfConverterPath))
            {
                success = StoreToolVersionInfoOneFile(ref toolVersionInfo, agilentToUimfConverterPath);
                if (!success)
                    return false;
            }

            // Store path to CaptureToolPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(pluginPath)
            };

            if (!string.IsNullOrWhiteSpace(openChromProgPath))
            {
                toolFiles.Add(new FileInfo(openChromProgPath));
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

        private bool ValidateCDFPlugin()
        {
            try
            {
                var openChromSettingsDirectory = new DirectoryInfo(
                    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                                 @"OpenChromCE\1.0.x\.metadata\.plugins\org.eclipse.core.runtime\.settings"));

                if (!openChromSettingsDirectory.Exists)
                {
                    LogMessage("Creating OpenChrom settings file directory at " + openChromSettingsDirectory.FullName);

                    openChromSettingsDirectory.Create();
                    return false;
                }

                var settingsFile = new FileInfo(
                    Path.Combine(openChromSettingsDirectory.FullName,
                                 "net.openchrom.msd.converter.supplier.agilent.hp.prefs"));

                if (!settingsFile.Exists)
                {
                    // Create the file

                    LogMessage("Creating OpenChrom settings file at " + settingsFile.FullName);

                    using (var writer = settingsFile.CreateText())
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

                using (var reader = new StreamReader(new FileStream(settingsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (string.Equals(dataLine.Trim(), "productTrialKey=true", StringComparison.OrdinalIgnoreCase))
                        {
                            settingsData.Add("productSerialKey=wlkXZsvC-miP6A2KH-DgAuTix2");
                            settingsData.Add("productTrialKey=false");
                            settingsFileUpdateRequired = true;
                        }
                        else
                        {
                            if (dataLine.StartsWith("trace", StringComparison.OrdinalIgnoreCase))
                                traceFound = true;

                            settingsData.Add(dataLine);
                        }

                    }
                }

                if (!settingsFileUpdateRequired)
                {
                    if (mDebugLevel >= 2)
                        LogDebug("OpenChrom settings file is up to date at " + settingsFile.FullName);

                    return true;
                }

                // Need to update the settings file with the SerialKey entry
                Thread.Sleep(50);

                // Possibly add the trace= line
                if (!traceFound)
                    settingsData.Add("trace=1");

                LogMessage("Adding productSerialKey entry to OpenChrom settings file at " + settingsFile.FullName);

                // Actually update the file
                using (var writer = new StreamWriter(new FileStream(settingsFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var dataLine in settingsData)
                        writer.WriteLine(dataLine);
                }

                return true;
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception validating the OpenChrom CDF plugin";
                LogError(mRetData.CloseoutMsg + ": " + ex.Message);
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

        private void CmdRunner_Timeout()
        {
            LogError("cmdRunner timeout reported");
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
                LogMessage(string.Format("AgilentToUIMFConverter running; {0:F1} minutes elapsed",
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
