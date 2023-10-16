using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using CaptureTaskManager;
using PRISM;
using UIMFLibrary;

namespace ImsDemuxPlugin
{
    public class AgilentToUimfConversion : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Acq, Agilentcmd, demultiplexed, demux, IMS, uimf

        // ReSharper restore CommentTypo

        private const float UIMF_FILE_MIN_SIZE_KB = 5;
        private const float UIMF_FILE_SMALL_SIZE_KB = 50;

        private const int MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES = 180;

        private ToolReturnData mRetData = new();

        private DateTime mProcessingStartTime;

        private string mConsoleOutputFilePath;

        private DateTime mLastProgressUpdate;

        private DateTime mLastStatusUpdate;

        private int mStatusUpdateIntervalMinutes;

        private readonly IMgrParams mMgrParams;
        private readonly ITaskParams mTaskParams;

        private string mDataset;
        private string mWorkDir;
        private FileTools mFileTools;
        private short mDebugLevel = 4;
        private readonly string mAgilentToUimfConverterPath;
        private string mDatasetDirectoryPathRemote;
        private readonly Action mlockQueueResetTimestamp;

        public string ErrorMessage { get; }
        public bool InFailureState { get; }

        public AgilentToUimfConversion(IMgrParams mgrParams, ITaskParams taskParams, Action lockQueueResetTimestamp)
        {
            mMgrParams = mgrParams;
            mTaskParams = taskParams;

            UpdateDatasetInfo(mgrParams, taskParams);

            mlockQueueResetTimestamp = lockQueueResetTimestamp;

            // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
            if (mlockQueueResetTimestamp == null)
            {
                // safe to call
                mlockQueueResetTimestamp = () => { };
            }

            mAgilentToUimfConverterPath = GetAgilentToUIMFProgPath();

            if (!File.Exists(mAgilentToUimfConverterPath))
            {
                ErrorMessage = "AgilentToUIMFConverter not found at " + mAgilentToUimfConverterPath;
                InFailureState = true;
            }
        }

        /// <summary>
        /// Convert an Agilent .D directory to a .uimf file
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool RunConvert(ToolReturnData returnData, FileTools fileTools)
        {
            mRetData = returnData;
            mFileTools = fileTools;

            var instClassName = mTaskParams.GetParam("Instrument_Class");
            var instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);

            if (instrumentClass != InstrumentClass.IMS_Agilent_TOF_DotD)
            {
                mRetData.CloseoutMsg = "AgilentToUimfConversion can only convert Agilent IMS .D directories to .UIMF";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            OnStatusEvent("Performing Agilent .D to .UIMF conversion, dataset " + mDataset);

            // Need to first convert the .d directory to a .UIMF file
            if (!ConvertAgilentDotDDirectoryToUIMF(mDatasetDirectoryPathRemote, mAgilentToUimfConverterPath))
            {
                if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
                {
                    mRetData.CloseoutMsg = "Unknown error converting the Agilent .d directory to a .UIMF file";
                    OnErrorEvent(mRetData.CloseoutMsg);
                }

                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                var dataFileNamePath = Path.Combine(mDatasetDirectoryPathRemote, mDataset + InstrumentClassInfo.DOT_UIMF_EXTENSION);
                mRetData.CloseoutType = TestIMSAgilentTOF(dataFileNamePath);
            }

            return mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        private void UpdateDatasetInfo(IMgrParams mgrParams, ITaskParams taskParams)
        {
            mDataset = taskParams.GetParam("Dataset");
            mWorkDir = mgrParams.GetParam("WorkDir");

            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 4);

            // Set up the file paths
            var svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
            var datasetDirectory = taskParams.GetParam(taskParams.HasParam("Directory") ? "Directory" : "Folder");

            mDatasetDirectoryPathRemote = Path.Combine(svrPath, datasetDirectory);
        }

        private bool ConvertAgilentDotDDirectoryToUIMF(string datasetDirectoryPath, string exePath)
        {
            try
            {
                var mgrName = mMgrParams.GetParam("MgrName", "CTM");

                var dotDDirectoryLocal = PluginMain.GetDotDDirectory(mWorkDir, mDataset);

                // Check if it already exists locally first; if demultiplexed then the file should already be in the working directory
                if (!dotDDirectoryLocal.Exists)
                {
                    var directoryCopiedFromRemote = CopyDotDDirectoryToLocal(mFileTools, datasetDirectoryPath, dotDDirectoryLocal.Name, dotDDirectoryLocal.FullName, true, mRetData);

                    if (!directoryCopiedFromRemote)
                    {
                        return false;
                    }
                }

                var dotDDirectoryPathLocal = dotDDirectoryLocal.FullName;

                // Examine the .d directory to look for an AcqData subdirectory
                // If it does not have one, it might have a .d subdirectory that itself has an AcqData directory
                // For example, 001_14Sep18_RapidFire.d\sequence1.d\AcqData

                var dotDDirectory = new DirectoryInfo(dotDDirectoryPathLocal);
                var acqDataDir = dotDDirectory.GetDirectories("AcqData");
                var altDirFound = false;

                if (acqDataDir.Length == 0)
                {
                    // Use *.d to look for .d subdirectories
                    var dotDDirectoryAlt = dotDDirectory.GetDirectories("*" + InstrumentClassInfo.DOT_D_EXTENSION);

                    if (dotDDirectoryAlt.Length > 0)
                    {
                        foreach (var altDir in dotDDirectoryAlt)
                        {
                            var acqDataDirAlt = altDir.GetDirectories("AcqData");

                            if (acqDataDirAlt.Length > 0)
                            {
                                dotDDirectoryPathLocal = altDir.FullName;
                                altDirFound = true;
                                OnStatusEvent("Using the .d directory below the primary .d subdirectory: " + altDir.FullName);
                                break;
                            }
                        }
                    }

                    if (!altDirFound)
                    {
                        mRetData.CloseoutMsg = ".D directory does not have an AcqData subdirectory";
                        OnErrorEvent(mRetData.CloseoutMsg);
                        return false;
                    }
                }

                // Construct the command line arguments to run the AgilentToUIMFConverter

                // Syntax:
                // AgilentToUIMFConverter.exe [Agilent .d directory] [Directory to insert file (optional)]

                var arguments = Conversion.PossiblyQuotePath(dotDDirectoryPathLocal) + " " + Conversion.PossiblyQuotePath(mWorkDir);
                mConsoleOutputFilePath = Path.Combine(mWorkDir, "AgilentToUIMF_ConsoleOutput_" + mgrName + ".txt");

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
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

                OnStatusEvent("Converting .d directory to .UIMF: {0} {1}", exePath, arguments);

                const int maxRuntimeSeconds = MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES * 60;
                var success = cmdRunner.RunProgram(exePath, arguments, "AgilentToUIMFConverter", true, maxRuntimeSeconds);

                // Parse the console output file one more time to check for errors
                ParseConsoleOutputFile();

                // Delete the locally cached .d directory
                try
                {
                    AppUtils.GarbageCollectNow();
                    mFileTools.DeleteDirectory(dotDDirectoryPathLocal, ignoreErrors: true);
                }
                catch (Exception ex)
                {
                    // Do not treat this as a fatal error
                    OnWarningEvent("Exception deleting locally cached .d directory (" + dotDDirectoryPathLocal + "): " + ex.Message);
                }

                if (!success)
                {
                    mRetData.CloseoutMsg = "Error running the AgilentToUIMFConverter";
                    OnErrorEvent(mRetData.CloseoutMsg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        OnWarningEvent("AgilentToUIMFConverter returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        OnWarningEvent("Call to AgilentToUIMFConverter failed (but exit code is 0)");
                    }

                    return false;
                }

                Thread.Sleep(100);

                if (altDirFound)
                {
                    // We need to rename the .uimf file since we processed a .d directory inside a .d directory
                    var sourceUimfName = Path.ChangeExtension(Path.GetFileName(dotDDirectoryPathLocal), InstrumentClassInfo.DOT_UIMF_EXTENSION);
                    var sourceUimf = new FileInfo(Path.Combine(mWorkDir, sourceUimfName));
                    var targetUimfPath = Path.Combine(mWorkDir, mDataset + InstrumentClassInfo.DOT_UIMF_EXTENSION);

                    if (!string.Equals(sourceUimf.FullName, targetUimfPath, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!sourceUimf.Exists)
                        {
                            mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file named " + sourceUimf.Name;
                            OnErrorEvent(mRetData.CloseoutMsg + ": " + sourceUimf.FullName);
                            return false;
                        }

                        OnDebugEvent("Renaming {0} to {1}", sourceUimf.FullName, Path.GetFileName(targetUimfPath));
                        sourceUimf.MoveTo(targetUimfPath);
                    }
                }

                // Copy the .UIMF file to the dataset directory
                var directoryCopiedToRemote = CopyUIMFToDatasetDirectory(mFileTools, datasetDirectoryPath);

                if (!directoryCopiedToRemote)
                {
                    return false;
                }

                // Delete the console output file
                try
                {
                    File.Delete(mConsoleOutputFilePath);
                }
                catch
                {
                    // Ignore errors
                }
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception converting .d directory to a UIMF file";
                OnErrorEvent(mRetData.CloseoutMsg + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy the dataset's .d directory to the local computer
        /// </summary>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <param name="datasetDirectoryPath">Source directory parent</param>
        /// <param name="dotDDirectoryName">Source directory name (name only, not full path)</param>
        /// <param name="dotDDirectoryPathLocal">Target directory (full path of the .D directory)</param>
        /// <param name="requireIMSFiles">If true, require that IMS files be present</param>
        /// <param name="returnData">Instance of ToolReturnData</param>
        /// <returns>True if the .d directory was copied, otherwise false</returns>
        public bool CopyDotDDirectoryToLocal(
            FileTools fileTools,
            string datasetDirectoryPath,
            string dotDDirectoryName,
            string dotDDirectoryPathLocal,
            bool requireIMSFiles,
            ToolReturnData returnData)
        {
            var dotDDirectoryPathRemote = new DirectoryInfo(Path.Combine(datasetDirectoryPath, dotDDirectoryName));

            if (requireIMSFiles)
            {
                // Make sure the .d directory has the required files
                // Older datasets may have had their larger files purged, which will cause the AgilentToUIMFConverter to fail

                var binFiles = PathUtils.FindFilesWildcard(dotDDirectoryPathRemote, "*.bin", true).ToList();

                var fileNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var file in binFiles)
                {
                    fileNames.Add(file.Name);
                }

                var requiredFiles = new List<string>
                {
                    // Not required: "MSPeak.bin",
                    // ReSharper disable once StringLiteralTypo
                    "IMSFrame.bin",
                    "MSPeriodicActuals.bin",
                    "MSProfile.bin",
                    "MSScan.bin"
                };

                // Construct a list of the missing files
                var missingFiles = requiredFiles.Where(requiredFile => !fileNames.Contains(requiredFile)).ToList();

                var errorMessage = string.Empty;

                if (missingFiles.Count == 1)
                {
                    errorMessage = "Cannot convert .d to .UIMF; missing file " + missingFiles.First();
                }

                if (missingFiles.Count > 1)
                {
                    errorMessage = "Cannot convert .d to .UIMF; missing files " + string.Join(", ", missingFiles);
                }

                if (errorMessage.Length > 0)
                {
                    returnData.CloseoutMsg = errorMessage;
                    OnErrorEvent(returnData.CloseoutMsg);
                    return false;
                }
            }

            // Copy the dataset directory locally using Prism.DLL
            // Note that lock files will be used when copying large files (over 20 MB)

            mlockQueueResetTimestamp();
            fileTools.CopyDirectory(dotDDirectoryPathRemote.FullName, dotDDirectoryPathLocal, true);

            return true;
        }

        private bool CopyUIMFToDatasetDirectory(FileTools fileTools, string datasetDirectoryPath)
        {
            var uimfFile = new FileInfo(Path.Combine(mWorkDir, mDataset + InstrumentClassInfo.DOT_UIMF_EXTENSION));

            if (!uimfFile.Exists)
            {
                mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file named " + uimfFile.Name;
                OnErrorEvent(mRetData.CloseoutMsg + ": " + uimfFile.FullName);
                return false;
            }

            return CopyFileToDatasetDirectory(fileTools, uimfFile, datasetDirectoryPath);
        }

        private bool CopyFileToDatasetDirectory(FileTools fileTools, FileSystemInfo dataFile, string datasetDirectoryPath)
        {
            if (mDebugLevel >= 4)
            {
                OnDebugEvent("Copying " + dataFile.Extension + " file to the dataset directory");
            }

            mlockQueueResetTimestamp();

            var targetFilePath = Path.Combine(datasetDirectoryPath, dataFile.Name);
            fileTools.CopyFileUsingLocks(dataFile.FullName, targetFilePath, overWrite: true);

            if (mDebugLevel >= 4)
            {
                OnDebugEvent("Copy complete");
            }

            try
            {
                // Delete the local copy
                dataFile.Delete();
            }
            catch (Exception ex)
            {
                // Do not treat this as a fatal error
                OnWarningEvent("Exception deleting local copy of the new .UIMF file " + dataFile.FullName + ": " + ex.Message);
            }

            return true;
        }

        /// <summary>
        /// Convert a file size in kilobytes to a string form with units KB, MB or GB
        /// </summary>
        /// <param name="fileSizeKB"></param>
        private string FileSizeToString(float fileSizeKB)
        {
            var fileSizeGB = fileSizeKB / 1024.0 / 1024.0;
            var fileSizeMB = fileSizeKB / 1024.0;

            if (fileSizeGB > 1)
            {
                return fileSizeGB.ToString("#0.0") + " GB";
            }

            if (fileSizeMB > 1)
            {
                return fileSizeMB.ToString("#0.0") + " MB";
            }

            return fileSizeKB.ToString("#0") + " KB";
        }

        private string GetAgilentToUIMFProgPath()
        {
            var exeName = mMgrParams.GetParam("AgilentToUIMFProgLoc");
            return Path.Combine(exeName, "AgilentToUimfConverter.exe");
        }

        private void ParseConsoleOutputFile()
        {
            var unhandledException = false;
            var exceptionText = string.Empty;
            float percentComplete = 0;

            var progressMatcher = new Regex(@"Converting frame (?<FramesProcessed>\d+) / (?<TotalFrames>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                    if (string.IsNullOrEmpty(dataLine))
                    {
                        continue;
                    }

                    var match = progressMatcher.Match(dataLine);

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
                            exceptionText = dataLine;
                        }
                        else
                        {
                            exceptionText = "; " + dataLine;
                        }
                    }
                    else if (dataLine.StartsWith("Error:", StringComparison.OrdinalIgnoreCase))
                    {
                        OnErrorEvent("AgilentToUIMFConverter error: " + dataLine);
                    }
                    else if (dataLine.StartsWith("Exception in", StringComparison.OrdinalIgnoreCase))
                    {
                        OnErrorEvent("AgilentToUIMFConverter error: " + dataLine);
                    }
                    else if (dataLine.StartsWith("Unhandled Exception", StringComparison.OrdinalIgnoreCase))
                    {
                        OnErrorEvent("AgilentToUIMFConverter error: " + dataLine);
                        unhandledException = true;
                    }
                }

                if (!string.IsNullOrEmpty(exceptionText))
                {
                    OnErrorEvent(exceptionText);
                }

                OnProgressUpdate("Converting to UIMF", percentComplete);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ParseConsoleOutputFile: " + ex.Message);
            }
        }

        private void ReportFileSizeTooSmall(string dataFileDescription, string filePath, float actualSizeKB, float minSizeKB)
        {
            var minSizeText = FileSizeToString(minSizeKB);

            // File too small, data file may be corrupt

            // Example messages for mRetData.EvalMsg
            //   Data file size is 75 KB; minimum allowed size 100 KB
            //   Data file size is 8 KB; minimum allowed size 16 KB
            //   Data file is 0 bytes

            if (Math.Abs(actualSizeKB) < 0.0001)
            {
                mRetData.EvalMsg = string.Format("{0} file is 0 bytes", dataFileDescription);
            }
            else
            {
                mRetData.EvalMsg = string.Format(
                    "{0} file size is {1}; minimum allowed size is {2}",
                    dataFileDescription, FileSizeToString(actualSizeKB), minSizeText);
            }

            OnErrorEvent("{0} file may be corrupt. Actual file size is {1}; min allowable size is {2}; see {3}",
                dataFileDescription,
                FileSizeToString(actualSizeKB),
                minSizeText,
                filePath);
        }

        private EnumCloseOutType TestIMSAgilentTOF(string uimfFilePath)
        {
            // Verify file exists in storage directory
            if (!File.Exists(uimfFilePath))
            {
                mRetData.EvalMsg = "Data file " + uimfFilePath + " not found";
                OnErrorEvent(mRetData.EvalMsg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Get size of data file
            var dataFileSizeKB = GetFileSize(uimfFilePath);

            // Check min size
            if (dataFileSizeKB < UIMF_FILE_MIN_SIZE_KB)
            {
                ReportFileSizeTooSmall("Data", uimfFilePath, dataFileSizeKB, UIMF_FILE_MIN_SIZE_KB);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            var validFile = UimfFileHasData(uimfFilePath, out var uimfStatusMessage);

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

            // If we get here, everything is OK
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
                OnDebugEvent("Opening UIMF file to look for valid data");

                // Open the .UIMF file and read the first scan of the first frame
                using var uimfReader = new DataReader(uimfFilePath);

                var frameList = uimfReader.GetMasterFrameList();

                if (frameList.Count == 0)
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

                uimfStatusMessage = "has frame info but no scan data";
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in UimfFileHasData: " + ex.Message);
                uimfStatusMessage = "appears corrupt (exception reading data)";
                return false;
            }
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
            return dataFile.Length / 1024F;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        public List<string> GetToolDllPaths()
        {
            var toolPaths = new List<string>();

            if (!string.IsNullOrWhiteSpace(mAgilentToUimfConverterPath))
            {
                toolPaths.Add(mAgilentToUimfConverterPath);
            }

            return toolPaths;
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

        private void CmdRunner_Timeout()
        {
            OnErrorEvent("cmdRunner timeout reported (the AgilentToUimfConverter has been running for over {0} minutes", MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES);
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdate).TotalSeconds >= 30)
            {
                mLastProgressUpdate = DateTime.UtcNow;
                ParseConsoleOutputFile();
                return;
            }

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalMinutes < mStatusUpdateIntervalMinutes)
                return;

            mLastStatusUpdate = DateTime.UtcNow;
            OnStatusEvent("AgilentToUIMFConverter running; {0:F1} minutes elapsed",
                DateTime.UtcNow.Subtract(mProcessingStartTime).TotalMinutes);

            // Increment mStatusUpdateIntervalMinutes by 1 minute every time the status is logged, up to a maximum of 30 minutes
            if (mStatusUpdateIntervalMinutes < 30)
            {
                mStatusUpdateIntervalMinutes++;
            }
        }
    }
}
