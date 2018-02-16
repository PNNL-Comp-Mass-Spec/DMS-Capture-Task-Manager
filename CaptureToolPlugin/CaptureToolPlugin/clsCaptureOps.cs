//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using CaptureTaskManager;
using PRISM;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Dataest capture plugin
    /// </summary>
    public class clsCaptureOps : clsLoggerBase
    {

        #region "Enums"
        public enum RawDSTypes
        {
            None,
            File,
            FolderNoExt,
            FolderExt,
            BrukerImaging,
            BrukerSpot,
            MultiFile
        }

        private enum DatasetFolderState
        {
            Empty,
            NotEmpty,
            Error
        }

        private enum ConnectionType
        {
            NotConnected,
            Prism,
            DotNET
        }
        #endregion

        #region "Classwide variables"

        private readonly IMgrParams mMgrParams;

        private readonly int mSleepInterval;

        // True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
        // False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
        //
        // The capture task managers running on the Proto-x servers have "perspective" = "server"
        // Capture tasks that occur on the Proto-x servers should be limited to certain instruments via table T_Processor_Instrument in the DMS_Capture DB
        // If a capture task manager running on a Proto-x server has the DatasetCapture tool enabled, yet does not have an entry in T_Processor_Instrument,
        //  then no capture tasks are allowed to be assigned to avoid drive path problems
        private bool mClientServer;

        private readonly bool mUseBioNet;
        private readonly bool mTraceMode;

        private readonly string mUserName = "";
        private readonly string mPassword = "";
        private ShareConnector mShareConnectorPRISM;
        private NetworkConnection m_ShareConnectorDotNET;
        private ConnectionType m_ConnectionType = ConnectionType.NotConnected;
        private bool mNeedToAbortProcessing;

        private readonly clsFileTools mFileTools;

        DateTime mLastProgressUpdate = DateTime.Now;

        string mLastProgressFileName = string.Empty;
        float mLastProgressPercent = -1;
        private bool mFileCopyEventsWired;

        string mErrorMessage = string.Empty;

        /// <summary>
        /// List of characters that should be automatically replaced if doing so makes the filename match the dataset name
        /// </summary>
        private readonly Dictionary<char, string> mFilenameAutoFixes;

        #endregion

        #region "Properties"

        public bool NeedToAbortProcessing => mNeedToAbortProcessing;

        #endregion

        #region "Constructor"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
        /// <param name="traceMode">When true, show debug messages at the console</param>
        public clsCaptureOps(IMgrParams mgrParams, bool useBioNet, bool traceMode)
        {
            mMgrParams = mgrParams;
            mTraceMode = traceMode;

            // Get client/server perspective
            //   True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
            //   False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
            var tmpParam = mMgrParams.GetParam("perspective");
            mClientServer = tmpParam.ToLower() == "client";

            // Setup for BioNet use, if applicable
            mUseBioNet = useBioNet;
            if (mUseBioNet)
            {
                mUserName = mMgrParams.GetParam("bionetuser");
                mPassword = mMgrParams.GetParam("bionetpwd");

                if (!mUserName.Contains(@"\"))
                {
                    // Prepend this computer's name to the username
                    mUserName = Environment.MachineName + @"\" + mUserName;
                }
            }

            // Sleep interval for "is dataset complete" testing
            mSleepInterval = mMgrParams.GetParam("sleepinterval", 30);

            // Instantiate mFileTools
            mFileTools = new clsFileTools(mMgrParams.GetParam("MgrName", "CaptureTaskManager"), 1);

            // Note that all of the events and methods in clsFileTools are static
            if (!mFileCopyEventsWired)
            {
                mFileCopyEventsWired = true;
                mFileTools.CopyingFile += OnCopyingFile;
                mFileTools.FileCopyProgress += OnFileCopyProgress;
                mFileTools.ResumingFileCopy += OnResumingFileCopy;
            }

            mFilenameAutoFixes = new Dictionary<char, string> {
                { ' ', "_"},
                { '%', "pct"},
                { '.', "pt"}};
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Look for files in the dataset folder with spaces in the name
        /// If the filename otherwise matches the dataset, rename it
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetFolder">Dataset folder to search</param>
        private void AutoFixFilesWithInvalidChars(string datasetName, DirectoryInfo datasetFolder)
        {
            var candidateFiles = new List<FileSystemInfo>();

            // Find items matching "* *" and "*%*" and "*.*"
            foreach (var item in mFilenameAutoFixes)
            {
                if (item.Key == '.')
                {
                    foreach (var candidateFile in datasetFolder.GetFileSystemInfos("*.*", SearchOption.AllDirectories))
                    {
                        if (Path.GetFileNameWithoutExtension(candidateFile.Name).IndexOf('.') >= 0)
                        {
                            candidateFiles.Add(candidateFile);
                        }
                    }
                }
                else
                {
                    candidateFiles.AddRange(datasetFolder.GetFileSystemInfos("*" + item.Key + "*", SearchOption.AllDirectories));
                }
            }

            var processedFiles = new SortedSet<string>();

            foreach (var datasetFile in candidateFiles)
            {
                if (processedFiles.Contains(datasetFile.FullName))
                    continue;

                processedFiles.Add(datasetFile.FullName);

                var updatedFileName = AutoFixFilename(datasetName, datasetFile.Name, mFilenameAutoFixes);

                if (string.Equals(datasetFile.Name, updatedFileName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                LogMessage("Renaming '" + datasetFile.Name + "' to '" + updatedFileName + "' to remove invalid characters");

                File.Move(datasetFile.FullName, Path.Combine(datasetFolder.FullName, updatedFileName));
            }
        }

        /// <summary>
        /// If the filename contains any of the characters in charsToFind, replace the character with the given replacement string
        /// Next compare to datasetName.  If a match, return the updated filename, otherwise return the original filename
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileName">File name</param>
        /// <param name="charsToFind">Keys are characters to find; values are the replacement text</param>
        /// <returns>Optimal filename to use</returns>
        /// <remarks>When searching for a period, only the base filename is examined</remarks>
        private string AutoFixFilename(string datasetName, string fileName, Dictionary<char, string> charsToFind)
        {
            var matchFound = charsToFind.Keys.Any(item => fileName.IndexOf(item) >= 0);
            if (!matchFound)
                return fileName;

            var fileExtension = Path.GetExtension(fileName);
            var updatedFileName = string.Copy(fileName);

            foreach (var item in charsToFind)
            {
                var baseName = Path.GetFileNameWithoutExtension(updatedFileName);

                if (baseName.IndexOf(item.Key) < 0)
                    continue;

                updatedFileName = baseName.Replace(item.Key.ToString(), item.Value) + fileExtension;

            }

            if (string.Equals(Path.GetFileNameWithoutExtension(updatedFileName), datasetName, StringComparison.OrdinalIgnoreCase))
            {
                return updatedFileName;
            }

            return fileName;
        }

        public void DetachEvents()
        {
            // Un-wire the events
            if (mFileCopyEventsWired && mFileTools != null)
            {
                mFileCopyEventsWired = false;
                mFileTools.CopyingFile -= OnCopyingFile;
                mFileTools.FileCopyProgress -= OnFileCopyProgress;
                mFileTools.ResumingFileCopy -= OnResumingFileCopy;
            }
        }

        /// <summary>
        /// Creates specified folder; if the folder already exists, returns true
        /// </summary>
        /// <param name="inpPath">Fully qualified path for folder to be created</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private void MakeFolderPath(string inpPath)
        {
            // Create specified directory
            try
            {
                var diFolder = new DirectoryInfo(inpPath);

                if (!diFolder.Exists)
                    diFolder.Create();

            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception creating directory " + inpPath;
                LogError(mErrorMessage, ex);
            }

        }

        /// <summary>
        /// Renames each file and subfolder at folderPath to start with x_
        /// </summary>
        /// <param name="folderPath"></param>
        /// <returns></returns>
        /// <remarks>Does not rename LCMethod*.xml files</remarks>
        private bool MarkSupersededFiles(string folderPath)
        {

            try
            {
                var diFolder = new DirectoryInfo(folderPath);

                if (!diFolder.Exists)
                    return true;

                string targetPath;
                var itemCountRenamed = 0;

                var foundFiles = diFolder.GetFiles();
                var filesToSkip = diFolder.GetFiles("LCMethod*.xml");

                // Rename superseded files (but skip LCMethod files)
                foreach (var fiFile in foundFiles)
                {
                    // Rename the file, but only if it is not in filesToSkip
                    var skipFile = filesToSkip.Any(fiFileToSkip => fiFileToSkip.FullName == fiFile.FullName);

                    if (fiFile.Name.StartsWith("x_") && foundFiles.Length == 1)
                    {
                        // File was previously renamed and it is the only file in this folder; don't rename it again
                        continue;
                    }

                    if (skipFile)
                    {
                        continue;
                    }

                    targetPath = Path.Combine(diFolder.FullName, "x_" + fiFile.Name);

                    if (File.Exists(targetPath))
                    {
                        // Target exists; delete it
                        File.Delete(targetPath);
                    }

                    fiFile.MoveTo(targetPath);
                    itemCountRenamed++;
                }

                if (itemCountRenamed > 0)
                {
                    LogMessage("Renamed superseded file(s) at " + diFolder.FullName + " to start with x_");
                }

                // Rename superseded folders
                var diSubFolders = diFolder.GetDirectories();
                itemCountRenamed = 0;
                foreach (var diSubFolder in diSubFolders)
                {
                    if (diSubFolder.Name.StartsWith("x_") && diSubFolders.Length == 1)
                    {
                        // Subfolder was previously renamed and it is the only subfolder in this folder; don't rename it again
                        continue;
                    }

                    targetPath = Path.Combine(diFolder.FullName, "x_" + diSubFolder.Name);

                    if (Directory.Exists(targetPath))
                    {
                        // Target exists; delete it
                        Directory.Delete(targetPath, true);
                    }

                    diSubFolder.MoveTo(targetPath);
                    itemCountRenamed++;
                }

                if (itemCountRenamed > 0)
                {
                    LogMessage("Renamed superseded folder(s) at " + diFolder.FullName + " to start with x_");
                }

                return true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception renaming files/folders to start with x_";
                var msg = mErrorMessage + " at " + folderPath;
                LogError(msg, true);
                LogError("Stack trace", ex);
                return false;
            }

        }

        /// <summary>
        /// Checks to determine if specified folder is empty
        /// </summary>
        /// <param name="dsFolder">Full path specifying folder to be checked</param>
        /// <param name="fileCount">Output parameter: number of files</param>
        /// <param name="instrumentDataFolderCount">Output parameter: number of instrument folders (typically .D folders)</param>
        /// <param name="nonInstrumentDataFolderCount">Output parameter: number of folders (excluding folders included in instrumentDataFolderCount)</param>
        /// <returns>Empty=0, NotEmpty=1, or Error=2</returns>
        private DatasetFolderState IsDSFolderEmpty(string dsFolder, out int fileCount, out int instrumentDataFolderCount, out int nonInstrumentDataFolderCount)
        {
            // Returns count of files or folders if folder is not empty
            // Returns 0 if folder is empty
            // returns -1 on error

            fileCount = 0;
            instrumentDataFolderCount = 0;
            nonInstrumentDataFolderCount = 0;

            try
            {
                var datasetFolder = new DirectoryInfo(dsFolder);

                // Check for files
                fileCount = datasetFolder.GetFiles().Length;

                // Check for .D folders
                // (Future: check for other folder extensions)
                instrumentDataFolderCount = datasetFolder.GetDirectories("*.d").Length;

                // Check for non-instrument folders
                nonInstrumentDataFolderCount = datasetFolder.GetDirectories().Length - instrumentDataFolderCount;

                if (fileCount > 0)
                    return DatasetFolderState.NotEmpty;

                if (nonInstrumentDataFolderCount + instrumentDataFolderCount > 0)
                    return DatasetFolderState.NotEmpty;
            }
            catch (Exception ex)
            {
                // Something really bad happened
                mErrorMessage = "Error checking for empty dataset folder";

                var msg = mErrorMessage + ": " + dsFolder;
                LogError(msg, true);
                LogError("Stack trace", ex);
                return DatasetFolderState.Error;
            }

            // If we got to here, the directory is empty
            return DatasetFolderState.Empty;

        }

        /// <summary>
        /// Performs action specified by DSFolderExistsAction mgr param if a dataset folder already exists
        /// </summary>
        /// <param name="dsFolder">Full path to dataset folder</param>
        /// <param name="copyWithResume">True when we will be using Copy with Resume to capture this instrument's data</param>
        /// <param name="maxFileCountToAllowResume">Maximum number of files that can existing in the dataset folder if we are going to allow CopyWithResume to be used</param>
        /// <param name="maxInstrumentFolderCountToAllowResume">Maximum number of instrument subfolders (at present, .D folders) that can existing in the dataset folder if we are going to allow CopyWithResume to be used</param>
        /// <param name="maxNonInstrumentFolderCountToAllowResume">Maximum number of non-instrument subfolders that can existing in the dataset folder if we are going to allow CopyWithResume to be used</param>
        /// <param name="retData">Return data</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks>If both maxFileCountToAllowResume and maxFolderCountToAllowResume are zero, then requires a minimum number of subfolders or files be present to allow for CopyToResume to be used</remarks>
        private bool PerformDSExistsActions(
            string dsFolder,
            bool copyWithResume,
            int maxFileCountToAllowResume,
            int maxInstrumentFolderCountToAllowResume,
            int maxNonInstrumentFolderCountToAllowResume,
            ref clsToolReturnData retData)
        {
            var switchResult = false;

            switch (IsDSFolderEmpty(dsFolder, out var fileCount, out var instrumentDataFolderCount, out var nonInstrumentDataFolderCount))
            {
                case DatasetFolderState.Empty:
                    // Directory is empty; all is good
                    switchResult = true;
                    break;
                case DatasetFolderState.Error:
                    // There was an error attempting to determine the dataset directory contents
                    // (Error reporting was handled by call to IsDSFolderEmpty above)
                    break;
                case DatasetFolderState.NotEmpty:
                    var DSAction = mMgrParams.GetParam("DSFolderExistsAction");

                    switch (DSAction.ToLower())
                    {
                        case "overwrite_single_item":
                            // If the folder only contains one or two files or only one subfolder
                            // then we're likely retrying capture; rename the one file to start with x_

                            var tooManyFilesOrFolders = false;
                            var folderCount = maxInstrumentFolderCountToAllowResume + maxNonInstrumentFolderCountToAllowResume;

                            if (maxFileCountToAllowResume > 0 || maxInstrumentFolderCountToAllowResume + maxNonInstrumentFolderCountToAllowResume > 0)
                            {
                                if (fileCount > maxFileCountToAllowResume ||
                                    instrumentDataFolderCount > maxInstrumentFolderCountToAllowResume ||
                                    nonInstrumentDataFolderCount > maxNonInstrumentFolderCountToAllowResume)
                                    tooManyFilesOrFolders = true;
                            }
                            else
                            {
                                if (folderCount == 0 && fileCount > 2 || fileCount == 0 && folderCount > 1)
                                    tooManyFilesOrFolders = true;
                            }

                            if (!tooManyFilesOrFolders)
                            {
                                if (copyWithResume)
                                    // Do not rename the folder or file; leave as-is and we'll resume the copy
                                    switchResult = true;
                                else
                                    switchResult = MarkSupersededFiles(dsFolder);
                            }
                            else
                            {
                                if (folderCount == 0 && copyWithResume)
                                    // Do not rename the files; leave as-is and we'll resume the copy
                                    switchResult = true;
                                else
                                {
                                    // Fail the capture task
                                    retData.CloseoutMsg = "Dataset folder already exists and has multiple files or subfolders";
                                    var msg = retData.CloseoutMsg + ": " + dsFolder;
                                    LogError(msg, true);
                                }
                            }

                            break;

                        case "delete":
                            // Attempt to delete dataset folder
                            try
                            {
                                Directory.Delete(dsFolder, true);
                                switchResult = true;
                            }
                            catch (Exception ex)
                            {
                                retData.CloseoutMsg = "Dataset folder already exists and cannot be deleted";
                                var msg = retData.CloseoutMsg + ": " + dsFolder;
                                LogError(msg, true);
                                LogError("Stack trace", ex);

                                switchResult = false;
                            }
                            break;
                        case "rename":
                            // Attempt to rename dataset folder
                            if (RenameDatasetFolder(dsFolder))
                            {
                                switchResult = true;
                            }
                            else
                            {
                                // (Error reporting was handled by previous call to RenameDatasetFolder)
                            }
                            break;
                        case "fail":
                            // Fail the capture task
                            retData.CloseoutMsg = "Dataset folder already exists";
                            var folderExists = retData.CloseoutMsg + ": " + dsFolder;

                            LogError(folderExists, true);
                            break;
                        default:
                            // An invalid value for DSFolderExistsAction was specified

                            retData.CloseoutMsg = "Dataset folder already exists; Invalid action " + DSAction + " specified";
                            var invalidAction = retData.CloseoutMsg + " (" + dsFolder + ")";

                            LogError(invalidAction, true);
                            break;
                    }   // DSAction selection
                    break;
                default:
                    // Shouldn't ever get to here
                    break;
            }

            return switchResult;

        }

        /// <summary>
        /// Prefixes specified folder name with "x_"
        /// </summary>
        /// <param name="DSPath">Full path specifying folder to be renamed</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameDatasetFolder(string DSPath)
        {
            // Rename dataset folder on instrument
            try
            {
                var di = new DirectoryInfo(DSPath);
                if (di.Parent == null)
                    return true;

                var n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
                di.MoveTo(n);

                var msg = "Renamed directory " + DSPath;
                LogMessage(msg);

                return true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error renaming directory " + DSPath;
                LogError(mErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Checks to see if folder size is changing -- possible sign acquisition hasn't finished
        /// </summary>
        /// <param name="folderPath">Full path specifying folder to check</param>
        /// <param name="retData">Output: return data</param>
        /// <returns>TRUE if folder size hasn't changed during SleepInt; FALSE otherwise</returns>
        private bool VerifyConstantFolderSize(string folderPath, ref clsToolReturnData retData)
        {

            try
            {

                var sleepIntervalSeconds = GetSleepIntervalForFolder(folderPath);

                // Sleep interval should be between 1 second and 15 minutes (900 seconds)
                if (sleepIntervalSeconds > 900)
                    sleepIntervalSeconds = 900;

                if (sleepIntervalSeconds < 1)
                    sleepIntervalSeconds = 1;

                var targetFolder = new DirectoryInfo(folderPath);

                // Get the initial size of the folder
                var initialFolderSize = mFileTools.GetDirectorySize(targetFolder.FullName);

                // Wait for specified sleep interval
                VerifyConstantSizeSleep(sleepIntervalSeconds, "folder " + targetFolder.Name);

                // Get the final size of the folder and compare
                var finalFolderSize = mFileTools.GetDirectorySize(folderPath);

                if (finalFolderSize == initialFolderSize)
                    return true;

                LogMessage("Folder size changed from " + initialFolderSize + " bytes to " + finalFolderSize + " bytes: " + targetFolder.FullName);

                return false;

            }
            catch (Exception ex)
            {
                if (ex is IOException && (ex.Message.Contains("user name") || ex.Message.Contains("password")))
                {
                    // Note that this will call LogError and update retData.CloseoutMsg
                    HandleCopyException(ref retData, ex);

                    LogWarning("Source folder path: " + folderPath);
                    return false;
                }

                retData.CloseoutMsg = "Exception validating constant folder size";
                var msg = retData.CloseoutMsg + ": " + folderPath;

                LogError(msg, ex);

                HandleCopyException(ref retData, ex);
                return false;
            }

        }

        /// <summary>
        /// Checks to see if file size is changing -- possible sign acquisition hasn't finished
        /// </summary>
        /// <param name="filePath">Full path specifying file to check</param>
        /// <param name="sleepIntervalSeconds">Interval for checking (seconds)</param>
        /// <param name="retData">Output: return data</param>
        /// <returns>TRUE if file size hasn't changed during SleepInt; FALSE otherwise</returns>
        private bool VerifyConstantFileSize(string filePath, int sleepIntervalSeconds, ref clsToolReturnData retData)
        {
            try
            {

                // Sleep interval should be between 1 second and 15 minutes (900 seconds)
                if (sleepIntervalSeconds > 900)
                    sleepIntervalSeconds = 900;

                if (sleepIntervalSeconds < 1)
                    sleepIntervalSeconds = 1;

                // Get the initial size of the file
                var fiSourceFile = new FileInfo(filePath);
                var initialFileSize = fiSourceFile.Length;

                VerifyConstantSizeSleep(sleepIntervalSeconds, "file " + fiSourceFile.Name);

                // Get the final size of the file and compare
                fiSourceFile.Refresh();
                var finalFileSize = fiSourceFile.Length;

                if (finalFileSize == initialFileSize)
                {
                    if (mTraceMode)
                        clsToolRunnerBase.ShowTraceMessage("File size did not change");
                    return true;
                }

                LogMessage("File size changed from " + initialFileSize + " bytes to " + finalFileSize + " bytes: " + filePath);

                return false;

            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception validating constant file size";
                var msg = retData.CloseoutMsg + ": " + filePath;
                LogError(msg, ex);

                HandleCopyException(ref retData, ex);
                return false;
            }

        }

        /// <summary>
        /// Wait the specified number of seconds, showing a status message every 5 seconds
        /// </summary>
        /// <param name="sleepIntervalSeconds"></param>
        /// <param name="fileOrFolderName"></param>
        private void VerifyConstantSizeSleep(int sleepIntervalSeconds, string fileOrFolderName)
        {
            const int STATUS_MESSAGE_INTERVAL = 5;

            if (mTraceMode)
            {
                clsToolRunnerBase.ShowTraceMessage(
                    string.Format("Monitoring {0} for {1} seconds", fileOrFolderName, sleepIntervalSeconds));
            }

            // Wait for specified sleep interval
            var verificationEndTime = DateTime.UtcNow.AddSeconds(sleepIntervalSeconds);
            var nextStatusTime = DateTime.UtcNow.AddSeconds(STATUS_MESSAGE_INTERVAL);

            while (DateTime.UtcNow < verificationEndTime)
            {
                Thread.Sleep(500);

                if (DateTime.UtcNow <= nextStatusTime)
                    continue;

                nextStatusTime = nextStatusTime.AddSeconds(STATUS_MESSAGE_INTERVAL);
                if (mTraceMode)
                {
                    clsToolRunnerBase.ShowTraceMessage(
                        string.Format("{0:0} seconds remaining", verificationEndTime.Subtract(DateTime.UtcNow).TotalSeconds));
                }
            }

        }

        /// <summary>
        /// Returns a string that describes the username and connection method currently active
        /// </summary>
        /// <returns></returns>
        private string GetConnectionDescription()
        {
            string connectionMode;

            switch (m_ConnectionType)
            {
                case ConnectionType.NotConnected:
                    connectionMode = " as user " + Environment.UserName + " using fso";
                    break;
                case ConnectionType.DotNET:
                    connectionMode = " as user " + mUserName + " using CaptureTaskManager.NetworkConnection";
                    break;
                case ConnectionType.Prism:
                    connectionMode = " as user " + mUserName + " using PRISM.ShareConnector";
                    break;
                default:
                    connectionMode = " via unknown connection mode";
                    break;
            }

            return connectionMode;
        }
        /// <summary>
        /// Determines if raw dataset exists as a file or folder
        /// </summary>
        /// <param name="sourceFolderPath">Full path to instrument transfer folder</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="instrumentClass">Instrument class for dataet to be located</param>
        /// <returns>clsDatasetInfo object containing info on found dataset</returns>
        private clsDatasetInfo GetRawDSType(string sourceFolderPath, string datasetName, clsInstrumentClassInfo.eInstrumentClass instrumentClass)
        {
            // Determines if raw dataset exists as a single file, folder with same name as dataset, or
            // folder with dataset name + extension. Returns object containing info on dataset found

            bool lookForDatasetFile;

            var datasetInfo = new clsDatasetInfo(datasetName);

            var diSourceFolder = new DirectoryInfo(sourceFolderPath);

            // Verify that the instrument transfer folder exists
            if (!diSourceFolder.Exists)
            {
                LogError("Source folder not found: [" + diSourceFolder.FullName + "]");

                datasetInfo.DatasetType = RawDSTypes.None;
                return datasetInfo;
            }

            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
                    // Preferentially capture dataset folders
                    // If a folder is not found, will instead look for a dataset file
                    lookForDatasetFile = false;
                    break;
                default:
                    // First look for a file with name DSName, if not found, look for a folder
                    lookForDatasetFile = true;
                    break;
            }

            for (var iteration = 1; iteration <= 2; iteration++)
            {
                if (lookForDatasetFile)
                {
                    // Get all files with a specified name
                    var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath));
                    var foundFiles = diSourceDir.GetFiles(datasetName + ".*");
                    if (foundFiles.Length > 0)
                    {
                        datasetInfo.FileOrFolderName = datasetName;
                        datasetInfo.FileList = foundFiles;

                        if (datasetInfo.FileCount == 1)
                        {
                            datasetInfo.FileOrFolderName = datasetInfo.FileList[0].Name;
                            datasetInfo.DatasetType = RawDSTypes.File;
                        }
                        else
                        {
                            datasetInfo.DatasetType = RawDSTypes.MultiFile;
                            var fileNames = foundFiles.Select(file => file.Name).ToList();
                            LogWarning(string.Format(
                                "Dataset name matched multiple files for iteration {0} in folder {1}: {2}",
                                iteration,
                                diSourceDir.FullName,
                                string.Join(", ", fileNames.Take(5))));
                        }

                        return datasetInfo;
                    }
                }
                else
                {
                    // Check for a folder with specified name
                    var subFolders = Directory.GetDirectories(sourceFolderPath);
                    foreach (var testFolder in subFolders)
                    {
                        // Using Path.GetFileNameWithoutExtension on folders is cheezy, but it works. This is done
                        // because the Path class methods that deal with directories ignore the possibilty there
                        // might be an extension. Apparently when sending in a string, Path can't tell a file from
                        // a directory
                        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(testFolder);
                        if (fileNameWithoutExtension == null ||
                            !string.Equals(fileNameWithoutExtension, datasetName, StringComparison.CurrentCultureIgnoreCase))
                        {
                            continue;
                        }

                        if (string.IsNullOrEmpty(Path.GetExtension(testFolder)))
                        {
                            // Found a directory that has no extension
                            datasetInfo.FileOrFolderName = Path.GetFileName(testFolder);

                            // Check the instrument class to determine the appropriate return type
                            switch (instrumentClass)
                            {
                                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                                    datasetInfo.DatasetType = RawDSTypes.BrukerImaging;
                                    break;
                                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:
                                    datasetInfo.DatasetType = RawDSTypes.BrukerSpot;
                                    break;
                                default:
                                    datasetInfo.DatasetType = RawDSTypes.FolderNoExt;
                                    break;
                            }
                        }
                        else
                        {
                            // Directory name has an extension
                            datasetInfo.FileOrFolderName = Path.GetFileName(testFolder);
                            datasetInfo.DatasetType = RawDSTypes.FolderExt;
                        }

                        if (iteration > 1)
                        {
                            LogMessage(string.Format(
                                           "Dataset name did not match a file, but it did match folder {0}, dataset type is {1}",
                                           datasetInfo.FileOrFolderName,
                                           datasetInfo.DatasetType));
                        }
                        return datasetInfo;
                    }
                }

                lookForDatasetFile = !lookForDatasetFile;
            }

            // If we got to here, the raw dataset wasn't found (either as a file or a folder), so there was a problem
            datasetInfo.DatasetType = RawDSTypes.None;
            return datasetInfo;

        }

        /// <summary>
        /// Connect to a BioNet share using either mShareConnectorPRISM or m_ShareConnectorDotNET
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="pwd">Password</param>
        /// <param name="shareFolderPath">Share path</param>
        /// <param name="connectionType">Connection type enum (ConnectionType.DotNET or ConnectionType.Prism)</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConnectToShare(
            string userName,
            string pwd,
            string shareFolderPath,
            ConnectionType connectionType,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            bool success;

            if (connectionType == ConnectionType.DotNET)
            {
                success = ConnectToShare(userName, pwd, shareFolderPath, out m_ShareConnectorDotNET, out closeoutType, out evalCode);
            }
            else
            {
                // Assume Prism Connector
                success = ConnectToShare(userName, pwd, shareFolderPath, out mShareConnectorPRISM, out closeoutType, out evalCode);
            }

            return success;

        }

        /// <summary>
        /// Connect to a remote share using a specific username and password
        /// Uses class PRISM.ShareConnector
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="pwd">Password</param>
        /// <param name="shareFolderPath">Share path</param>
        /// <param name="myConn">Connection object (output)</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConnectToShare(
            string userName,
            string pwd,
            string shareFolderPath,
            out ShareConnector myConn,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            closeoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            evalCode = EnumEvalCode.EVAL_CODE_SUCCESS;

            myConn = new ShareConnector(userName, pwd)
            {
                Share = shareFolderPath
            };

            if (myConn.Connect())
            {
                LogDebug("Connected to Bionet (" + shareFolderPath + ") as user " + userName + " using PRISM.ShareConnector");
                m_ConnectionType = ConnectionType.Prism;
                return true;
            }

            mErrorMessage = "Error " + myConn.ErrorMessage + " connecting to " + shareFolderPath + " as user " + userName + " using 'secfso'";

            var msg = string.Copy(mErrorMessage);

            if (myConn.ErrorMessage == "1326")
                msg += "; you likely need to change the Capture_Method from secfso to fso";
            if (myConn.ErrorMessage == "53")
                msg += "; the password may need to be reset";

            LogError(msg);

            if (myConn.ErrorMessage == "1219" || myConn.ErrorMessage == "1203" || myConn.ErrorMessage == "53" || myConn.ErrorMessage == "64")
            {
                // Likely had error "An unexpected network error occurred" while copying a file for a previous dataset
                // Need to completely exit the capture task manager
                mNeedToAbortProcessing = true;
                closeoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                evalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else
            {
                closeoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            m_ConnectionType = ConnectionType.NotConnected;
            return false;
        }

        /// <summary>
        /// Connect to a remote share using a specific username and password
        /// Uses class CaptureTaskManager.NetworkConnection
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="pwd">Password</param>
        /// <param name="shareFolderPath">Share path</param>
        /// <param name="myConn">Connection object (output)</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConnectToShare(
            string userName,
            string pwd,
            string shareFolderPath,
            out NetworkConnection myConn,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            evalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            myConn = null;

            try
            {
                // Make sure shareFolderPath does not end in a back slash
                if (shareFolderPath.EndsWith(@"\"))
                    shareFolderPath = shareFolderPath.Substring(0, shareFolderPath.Length - 1);

                var accessCredentials = new System.Net.NetworkCredential(userName, pwd, "");

                myConn = new NetworkConnection(shareFolderPath, accessCredentials);

                LogDebug("Connected to Bionet (" + shareFolderPath + ") as user " + userName + " using CaptureTaskManager.NetworkConnection");
                m_ConnectionType = ConnectionType.DotNET;

                closeoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                return true;

            }
            catch (Exception ex)
            {
                mErrorMessage = "Error connecting to " + shareFolderPath + " as user " + userName + " (using NetworkConnection class)";
                LogError(mErrorMessage, ex);

                var retData = new clsToolReturnData();
                HandleCopyException(ref retData, ex);

                closeoutType = retData.CloseoutType;
                evalCode = retData.EvalCode;

                m_ConnectionType = ConnectionType.NotConnected;
                return false;

            }

        }

        /// <summary>
        /// Disconnect from a bionet share if required
        /// </summary>
        private void DisconnectShareIfRequired()
        {
            if (m_ConnectionType == ConnectionType.Prism)
                DisconnectShare(ref mShareConnectorPRISM);
            else if (m_ConnectionType == ConnectionType.DotNET)
                DisconnectShare(ref m_ShareConnectorDotNET);
        }

        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="myConn">Connection object (class PRISM.ShareConnector) for shared drive</param>
        private void DisconnectShare(ref ShareConnector myConn)
        {
            myConn.Disconnect();
            clsProgRunner.GarbageCollectNow();

            LogDebug("Bionet disconnected");
            m_ConnectionType = ConnectionType.NotConnected;

        }

        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="myConn">Connection object (class CaptureTaskManager.NetworkConnection) for shared drive</param>
        private void DisconnectShare(ref NetworkConnection myConn)
        {
            myConn.Dispose();
            myConn = null;
            clsProgRunner.GarbageCollectNow();

            LogDebug("Bionet disconnected");
            m_ConnectionType = ConnectionType.NotConnected;

        }

        /// <summary>
        /// Perform a single capture operation
        /// </summary>
        /// <param name="taskParams">Enum indicating status of task</param>
        /// <param name="retData">Return data class; update CloseoutMsg or EvalMsg with text to store in T_Job_Step_Params</param>
        /// <returns>True if success or false if an error.  retData includes addition details on errors</returns>
        public bool DoOperation(ITaskParams taskParams, ref clsToolReturnData retData)
        {
            var datasetName = taskParams.GetParam("Dataset");
            var jobNum = taskParams.GetParam("Job", 0);
            var sourceVol = taskParams.GetParam("Source_Vol");                      // Example: \\exact04.bionet\
            var sourcePath = taskParams.GetParam("Source_Path");                    // Example: ProteomicsData\
            var captureSubfolder = taskParams.GetParam("Capture_Subfolder");        // Typically an empty string, but could be a partial path like: "CapDev" or "Smith\2014"
            var storageVol = taskParams.GetParam("Storage_Vol");                    // Example: E:\
            var storagePath = taskParams.GetParam("Storage_Path");                  // Example: Exact04\2012_1\
            var storageVolExternal = taskParams.GetParam("Storage_Vol_External");   // Example: \\proto-5\

            var instClassName = taskParams.GetParam("Instrument_Class");            // Examples: Finnigan_Ion_Trap, LTQ_FT, Triple_Quad, IMS_Agilent_TOF, Agilent_Ion_Trap
            var instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);     // Enum of instrument class type
            var instName = taskParams.GetParam("Instrument_Name");                  // Instrument name

            var shareConnectorType = mMgrParams.GetParam("ShareConnectorType");        // Can be PRISM or DotNET (but has been PRISM since 2012)
            var computerName = Environment.MachineName;

            ConnectionType connectionType;

            var maxFileCountToAllowResume = 0;
            var maxInstrumentFolderCountToAllowResume = 0;
            var maxNonInstrumentFolderCountToAllowResume = 0;

            // Confirm that the dataset name has no spaces
            if (datasetName.IndexOf(' ') >= 0)
            {
                retData.CloseoutMsg = "Dataset name contains a space";
                LogError(retData.CloseoutMsg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            // Confirm that the dataset name has no invalid characters
            if (NameHasInvalidCharacter(datasetName, "Dataset name", true, ref retData))
                return false;

            // Determine whether the connector class should be used to connect to Bionet
            // This is defined by manager parameter ShareConnectorType
            // Default in October 2014 is PRISM
            if (shareConnectorType.ToLower() == "dotnet")
                connectionType = ConnectionType.DotNET;
            else
                connectionType = ConnectionType.Prism;

            // Determine whether or not we will use Copy with Resume
            // This determines whether or not we add x_ to an existing file or folder,
            // and determines whether we use CopyDirectory or CopyFolderWithResume/CopyFileWithResume
            var copyWithResume = false;
            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
                    copyWithResume = true;
                    break;
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                    copyWithResume = true;
                    maxFileCountToAllowResume = 20;
                    maxInstrumentFolderCountToAllowResume = 20;
                    maxNonInstrumentFolderCountToAllowResume = 1;
                    break;
            }

            var pwd = Pacifica.Core.Utilities.DecodePassword(mPassword);

            string tempVol;

            LogDebug("Started clsCaptureOps.DoOperation()");

            // Setup destination folder based on client/server switch, mClientServer
            // True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
            // False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1

            if (!mClientServer)
            {
                // Look for job parameter Storage_Server_Name in storageVolExternal
                // If mClientServer=false but storageVolExternal does not contain Storage_Server_Name then auto-switch mClientServer to true

                if (!storageVolExternal.ToLower().Contains(computerName.ToLower()))
                {
                    var autoEnableFlag = "AutoEnableClientServer_for_" + computerName;
                    var autoEnabledParamValue = mMgrParams.GetParam(autoEnableFlag, string.Empty);
                    if (string.IsNullOrEmpty(autoEnabledParamValue))
                    {
                        // Using a Manager Parameter to assure that the following log message is only logged once per session
                        // (in case this manager captures multiple datasets in a row)
                        mMgrParams.SetParam(autoEnableFlag, "True");
                        LogMessage("Auto-changing mClientServer to True (perspective=client) " +
                                   "because " + storageVolExternal.ToLower() + " does not contain " + computerName.ToLower());
                    }

                    mClientServer = true;
                }
            }

            if (mClientServer)
            {
                // Example: \\proto-5\
                tempVol = storageVolExternal;
            }
            else
            {
                // Example: E:\
                tempVol = storageVol;
            }

            // Set up paths

            // Directory on storage server where dataset folder goes
            var storageFolderPath = Path.Combine(tempVol, storagePath);

            // Confirm that the storage folder has no invalid characters
            if (NameHasInvalidCharacter(storageFolderPath, "Storage folder path", false, ref retData))
                return false;

            string datasetFolderPath;

            // If Storage_Folder_Name <> "", use it in target folder path. Otherwise use dataset name
            if (!string.IsNullOrWhiteSpace(taskParams.GetParam("Storage_Folder_Name")))
            {
                // HPLC run folder storage path
                datasetFolderPath = Path.Combine(storageFolderPath, taskParams.GetParam("Storage_Folder_Name"));
            }
            else
            {
                // Dataset folder complete path
                datasetFolderPath = Path.Combine(storageFolderPath, datasetName);
            }

            // Confirm that the target dataset folder path has no invalid characters
            if (NameHasInvalidCharacter(datasetFolderPath, "Dataset folder path", false, ref retData))
                return false;

            // Verify that the storage folder on the storage server exists; e.g. \\proto-9\VOrbiETD02\2011_2
            if (!ValidateFolderPath(storageFolderPath))
            {
                LogMessage("Storage folder '" + storageFolderPath + "' does not exist; will auto-create");

                try
                {
                    Directory.CreateDirectory(storageFolderPath);
                    LogDebug("Successfully created " + storageFolderPath);
                }
                catch
                {
                    retData.CloseoutMsg = "Error creating missing storage folder";
                    LogError(retData.CloseoutMsg + ": " + storageFolderPath, true);

                    if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    return false;
                }
            }

            // Verify that dataset folder path doesn't already exist or is empty
            // Example: \\proto-9\VOrbiETD02\2011_2\PTO_Na_iTRAQ_2_17May11_Owl_11-05-09
            if (ValidateFolderPath(datasetFolderPath))
            {
                // Dataset folder exists, so take action specified in configuration
                if (!PerformDSExistsActions(datasetFolderPath, copyWithResume, maxFileCountToAllowResume, maxInstrumentFolderCountToAllowResume, maxNonInstrumentFolderCountToAllowResume, ref retData))
                {
                    PossiblyStoreErrorMessage(ref retData);
                    if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    if (string.IsNullOrEmpty(retData.CloseoutMsg))
                    {
                        retData.CloseoutMsg = "PerformDSExistsActions returned false";
                    }
                    return false;
                }
            }

            // Construct the path to the dataset on the instrument
            // Determine if source dataset exists, and if it is a file or a folder
            var sourceFolderPath = Path.Combine(sourceVol, sourcePath);

            // Confirm that the source folder has no invalid characters
            if (NameHasInvalidCharacter(sourceFolderPath, "Source folder path", false, ref retData))
                return false;

            // Connect to Bionet if necessary
            if (mUseBioNet)
            {
                LogDebug("Bionet connection required for " + sourceVol);

                if (!ConnectToShare(mUserName, pwd, sourceFolderPath, connectionType, out var closeoutType, out var evalCode))
                {
                    retData.CloseoutType = closeoutType;
                    retData.EvalCode = evalCode;

                    PossiblyStoreErrorMessage(ref retData);
                    if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    if (string.IsNullOrEmpty(retData.CloseoutMsg))
                    {
                        retData.CloseoutMsg = "Error connecting to Bionet share";
                    }
                    return false;
                }
            }
            else
            {
                LogDebug("Bionet connection not required for " + sourceVol);
            }

            // If Source_Folder_Name is non-blank, use it. Otherwise use dataset name
            var sourceFolderName = taskParams.GetParam("Source_Folder_Name");

            if (string.IsNullOrWhiteSpace(sourceFolderName))
            {
                sourceFolderName = datasetName;
            }
            else
            {
                // Confirm that the source folder name has no invalid characters
                if (NameHasInvalidCharacter(sourceFolderName, "Job param Source_Folder_Name", true, ref retData))
                    return false;
            }

            // Now that we've had a chance to connect to the share, possibly append a subfolder to the source path
            if (!string.IsNullOrWhiteSpace(captureSubfolder))
            {
                // However, if the subfolder name matches the dataset name, this was probably an error on the operator's part and we likely do not want to use the subfolder name
                if (captureSubfolder.EndsWith(Path.DirectorySeparatorChar + sourceFolderName, StringComparison.OrdinalIgnoreCase) ||
                    captureSubfolder.Equals(sourceFolderName, StringComparison.OrdinalIgnoreCase))
                {
                    var candidateFolderPath = Path.Combine(sourceFolderPath, captureSubfolder);

                    if (!Directory.Exists(candidateFolderPath))
                    {
                        // Leave sourceFolderPath unchanged
                        // Dataset Capture_Subfolder ends with the dataset name. Gracefully ignoring because this appears to be a data entry error; folder not found:
                        LogWarning("Dataset Capture_Subfolder ends with the dataset name. Gracefully ignoring " +
                                   "because this appears to be a data entry error; folder not found: " + candidateFolderPath, true);
                    }
                    else
                    {
                        if (captureSubfolder.Equals(sourceFolderName, StringComparison.OrdinalIgnoreCase))
                        {
                            LogWarning(string.Format(
                                "Dataset Capture_Subfolder is the dataset name; leaving the capture path as {0} " +
                                "so that the entire dataset folder will be copied", sourceFolderPath));
                        }
                        else
                        {
                            if (candidateFolderPath.EndsWith(Path.DirectorySeparatorChar + sourceFolderName, StringComparison.OrdinalIgnoreCase))
                            {
                                var candidateFolderPathTrimmed = candidateFolderPath.Substring(0, candidateFolderPath.Length - sourceFolderName.Length - 1);
                                LogMessage(string.Format(
                                    "Appending captureSubFolder to sourceFolderPath, but removing SourceFolderName, giving: {0} (removed {1})",
                                    candidateFolderPathTrimmed,
                                               sourceFolderName));

                                sourceFolderPath = candidateFolderPathTrimmed;
                            }
                            else
                            {
                                LogMessage("Appending captureSubFolder to sourceFolderPath, giving: " + candidateFolderPath);
                                sourceFolderPath = candidateFolderPath;
                            }

                        }
                    }

                }
                else
                {
                    sourceFolderPath = Path.Combine(sourceFolderPath, captureSubfolder);
                }

                // Confirm that the source folder has no invalid characters
                if (NameHasInvalidCharacter(sourceFolderPath, "Source folder path with captureSubfolder", false, ref retData))
                    return false;

            }

            var datasetInfo = GetRawDSType(sourceFolderPath, datasetName, instrumentClass);
            var sourceType = datasetInfo.DatasetType;


            if (!string.Equals(datasetInfo.DatasetName, datasetName))
            {
                LogWarning(string.Format(
                    "DatasetName in the datasetInfo object is {0}; changing to {1}",
                    datasetInfo.DatasetName,
                    datasetName));

                datasetInfo.DatasetName = datasetName;
            }

            // Set the closeout type to Failed for now
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            bool sourceIsValid;

            if (sourceType == RawDSTypes.None)
            {
                // No dataset file or folder found

                if (mUseBioNet)
                {
                    retData.CloseoutMsg = "Dataset data file not found on Bionet at " + sourceFolderPath;
                }
                else
                {
                    retData.CloseoutMsg = "Dataset data file not found at " + sourceFolderPath;
                }

                string folderStatsMsg;

                if (string.IsNullOrWhiteSpace(sourceFolderName))
                {
                    folderStatsMsg = ReportFolderStats(sourceFolderPath);
                    retData.CloseoutMsg += "; empty SourceFolderName";
                }
                else
                {
                    folderStatsMsg = ReportFolderStats(Path.Combine(sourceFolderPath, sourceFolderName));
                    retData.CloseoutMsg += "; SourceFolderName: " + sourceFolderName;
                }

                LogError(retData.CloseoutMsg + " (" + datasetName + ", job " + jobNum + "); " + folderStatsMsg);
                sourceIsValid = false;
            }
            else
            {
                sourceIsValid = ValidateWithInstrumentClass(datasetName, sourceFolderPath, sourceType, instrumentClass, datasetInfo, ref retData);
            }

            string msg;

            if (!sourceIsValid)
            {
                msg = "Dataset type (" + sourceType + ") is not valid for the instrument class (" + instrumentClass + ")";
            }
            else
            {
                // Perform copy based on source type
                switch (sourceType)
                {
                    case RawDSTypes.File:
                        CaptureFile(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath, copyWithResume);
                        break;

                    case RawDSTypes.MultiFile:
                        CaptureMultiFile(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath, copyWithResume);
                        break;

                    case RawDSTypes.FolderExt:
                        CaptureFolderExt(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath, copyWithResume, instrumentClass, instName);
                        break;

                    case RawDSTypes.FolderNoExt:
                        CaptureFolderNoExt(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath,
                                           copyWithResume, instrumentClass);
                        break;

                    case RawDSTypes.BrukerImaging:
                        CaptureBrukerImaging(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath,
                                             copyWithResume);
                        break;

                    case RawDSTypes.BrukerSpot:
                        CaptureBrukerSpot(out msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath);
                        break;

                    default:
                        msg = "Invalid dataset type found: " + sourceType;
                        retData.CloseoutMsg = msg;
                        LogError(retData.CloseoutMsg, true);
                        DisconnectShareIfRequired();
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        break;
                }
            }

            PossiblyStoreErrorMessage(ref retData);

            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                return true;

            if (string.IsNullOrWhiteSpace(retData.CloseoutMsg))
            {
                if (string.IsNullOrWhiteSpace(msg))
                    retData.CloseoutMsg = "Unknown error performing capture";
                else
                    retData.CloseoutMsg = msg;
            }

            return false;
        }

        /// <summary>
        /// Capture a single file
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument)</param>
        /// <param name="datasetFolderPath">Destination folder</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        private void CaptureFile(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume)
        {
            // Dataset found, and it's a single file

            var fileNames = new List<string>
            {
                datasetInfo.FileOrFolderName
            };

            CaptureOneOrMoreFiles(out msg, ref retData, datasetInfo.DatasetName,
                fileNames, sourceFolderPath, datasetFolderPath, copyWithResume);

        }

        /// <summary>
        /// Capture multiple files, each with the same name but a different extension
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument)</param>
        /// <param name="datasetFolderPath">Destination folder</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        private void CaptureMultiFile(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume)
        {
            // Dataset found, and it's multiple files
            // Each has the same name but a different extension

            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath));
            var foundFiles = diSourceDir.GetFiles(datasetInfo.FileOrFolderName + ".*").ToList();

            var fileNames = foundFiles.Select(file => file.Name).ToList();

            CaptureOneOrMoreFiles(out msg, ref retData, datasetInfo.DatasetName,
                fileNames, sourceFolderPath, datasetFolderPath, copyWithResume);

        }

        /// <summary>
        /// Capture the file (or files) specified by fileNames
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileNames">List of filenames</param>
        /// <param name="sourceFolderPath">Source folder (on instrument)</param>
        /// <param name="datasetFolderPath">Destination folder</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        private void CaptureOneOrMoreFiles(
            out string msg,
            ref clsToolReturnData retData,
            string datasetName,
            ICollection<string> fileNames,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume)
        {
            // Dataset found, and it's either a single file or multiple files with the same name but different extensions

            msg = string.Empty;
            var validFiles = new List<string>();
            var errorMsgs = new List<string>();

            Parallel.ForEach(fileNames, fileName =>
            {
                // First, verify constant file size (indicates acquisition is actually finished)
                var sourceFilePath = Path.Combine(sourceFolderPath, fileName);

                var retDataValidateConstant = new clsToolReturnData();

                var sleepIntervalSeconds = GetSleepIntervalForFile(sourceFilePath);

                if (VerifyConstantFileSize(sourceFilePath, sleepIntervalSeconds, ref retDataValidateConstant))
                {
                    validFiles.Add(fileName);
                }
                else
                {
                    errorMsgs.Add(retDataValidateConstant.CloseoutMsg);
                }

            });

            if (validFiles.Count < fileNames.Count)
            {
                LogWarning("Dataset '" + datasetName + "' not ready; source file's size changed (or authentication error)");
                DisconnectShareIfRequired();
                if (errorMsgs.Count > 0)
                {
                    retData.CloseoutMsg = errorMsgs[0];
                    LogMessage(retData.CloseoutMsg);
                }
                else
                {
                    retData.CloseoutMsg = "File size changed";
                }

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset folder (it's OK if it already exists)
            try
            {
                MakeFolderPath(datasetFolderPath);
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception creating dataset folder";
                msg = retData.CloseoutMsg + " at " + datasetFolderPath;

                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
            }

            var success = false;

            // Copy the data file (or files) to the dataset folder
            // If any of the source files have an invalid character (space, % or period),
            // replace with the default replacement string if doing so will match the dataset name
            try
            {

                foreach (var fileName in fileNames)
                {
                    var sourceFilePath = Path.Combine(sourceFolderPath, fileName);
                    var sourceFileName = Path.GetFileName(sourceFilePath);

                    var targetFileName = AutoFixFilename(datasetName, fileName, mFilenameAutoFixes);
                    var targetFilePath = Path.Combine(datasetFolderPath, targetFileName);

                    if (!string.Equals(sourceFileName, targetFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        LogMessage("Renaming '" + sourceFileName + "' to '" + targetFileName + "' to remove spaces");
                    }

                    if (copyWithResume)
                    {
                        var fiSourceFile = new FileInfo(sourceFilePath);

                        success = mFileTools.CopyFileWithResume(fiSourceFile, targetFilePath, out _);
                    }
                    else
                    {
                        File.Copy(sourceFilePath, targetFilePath);
                        success = true;
                    }

                    if (success)
                    {
                        LogMessage("  copied file " + sourceFilePath + " to " + targetFilePath + GetConnectionDescription());
                    }
                    else
                    {
                        LogError("  file copy failed for " + sourceFilePath + " to " + targetFilePath + GetConnectionDescription());
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Copy exception for dataset " + datasetName + GetConnectionDescription(), ex);

                HandleCopyException(ref retData, ex);
                return;
            }
            finally
            {
                DisconnectShareIfRequired();
            }

            if (success)
            {
                success = CaptureLCMethodFile(datasetName, datasetFolderPath);
            }

            if (success)
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            else
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Looks for the LCMethod file for this dataset
        /// Copies this file to the dataset folder
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="datasetFolderPath"></param>
        /// <returns>True if file found and copied; false if an error</returns>
        /// <remarks>Returns true if the .lcmethod file is not found</remarks>
        private bool CaptureLCMethodFile(string datasetName, string datasetFolderPath)
        {
            const string DEFAULT_METHOD_FOLDER_BASE_PATH = @"\\proto-5\BionetXfer\Run_Complete_Trigger\MethodFiles";

            var success = true;
            var methodFolderBasePath = string.Empty;

            // Look for an LCMethod file associated with this raw spectra file
            // Note that this file is often created 45 minutes to 60 minutes after the run completes
            // and thus when capturing a dataset with an auto-created trigger file, we most likely will not find the .lcmethod file

            // The file will either be located in a folder with the dataset name, or will be in a subfolder based on the year and quarter that the data was acquired

            try
            {
                methodFolderBasePath = mMgrParams.GetParam("LCMethodFilesDir", DEFAULT_METHOD_FOLDER_BASE_PATH);

                if (string.IsNullOrEmpty(methodFolderBasePath) ||
                    string.Equals(methodFolderBasePath, "na", StringComparison.CurrentCultureIgnoreCase))
                {
                    // LCMethodFilesDir is not defined; exit the function
                    return true;
                }

                var diSourceFolder = new DirectoryInfo(methodFolderBasePath);
                if (!diSourceFolder.Exists)
                {
                    LogWarning("LCMethods folder not found: " + methodFolderBasePath, true);

                    // Return true despite not having found the folder since this is not a fatal error for capture
                    return true;
                }

                // Construct a list of folders to search
                var lstFoldersToSearch = new List<string>
                {
                    datasetName
                };

                var year = DateTime.Now.Year;
                var quarter = GetQuarter(DateTime.Now);

                while (year >= 2011)
                {
                    lstFoldersToSearch.Add(year + "_" + quarter);

                    if (quarter > 1)
                        --quarter;
                    else
                    {
                        quarter = 4;
                        --year;
                    }

                    if (year == 2011 && quarter == 2)
                        break;
                }

                // This regex is used to match files with names like:
                // Cheetah_01.04.2012_08.46.17_Sarc_P28_D01_2629_192_3Jan12_Cheetah_11-09-32.lcmethod
                var reLCMethodFile = new Regex(@".+\d+\.\d+\.\d+_\d+\.\d+\.\d+_.+\.lcmethod");
                var lstMethodFiles = new List<FileInfo>();

                // Define the file match spec
                var lcMethodSearchSpec = "*_" + datasetName + ".lcmethod";

                for (var iteration = 1; iteration <= 2; iteration++)
                {

                    foreach (var folderName in lstFoldersToSearch)
                    {
                        var diSubFolder = new DirectoryInfo(Path.Combine(diSourceFolder.FullName, folderName));
                        if (diSubFolder.Exists)
                        {
                            // Look for files that match lcMethodSearchSpec
                            // There might be multiple files if the dataset was analyzed more than once
                            foreach (var methodFile in diSubFolder.GetFiles(lcMethodSearchSpec))
                            {
                                if (iteration == 1)
                                {
                                    // First iteration
                                    // Check each file against the RegEx
                                    if (reLCMethodFile.IsMatch(methodFile.Name))
                                    {
                                        // Match found
                                        lstMethodFiles.Add(methodFile);
                                    }
                                }
                                else
                                {
                                    // Second iteration; accept any match
                                    lstMethodFiles.Add(methodFile);
                                }
                            }
                        }

                        if (lstMethodFiles.Count > 0)
                            break;

                    }

                }

                if (lstMethodFiles.Count == 0)
                {
                    // LCMethod file not found; exit function
                    return true;
                }

                // LCMethod file found
                // Copy to the dataset folder

                foreach (var fiFile in lstMethodFiles)
                {
                    try
                    {
                        var targetFilePath = Path.Combine(datasetFolderPath, fiFile.Name);
                        fiFile.CopyTo(targetFilePath, true);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Exception copying LCMethod file " + fiFile.FullName + ": " + ex.Message);
                    }

                }

                // If the file was found in a dataset folder, rename the source folder to start with x_
                var firstFileDirectory = lstMethodFiles[0].Directory;

                if (firstFileDirectory != null && string.Equals(firstFileDirectory.Name, datasetName, StringComparison.CurrentCultureIgnoreCase))
                {
                    try
                    {
                        var strRenamedSourceFolder = Path.Combine(methodFolderBasePath, "x_" + datasetName);

                        if (Directory.Exists(strRenamedSourceFolder))
                        {
                            // x_ folder already exists; move the files
                            foreach (var fiFile in lstMethodFiles)
                            {
                                var targetFilePath = Path.Combine(strRenamedSourceFolder, fiFile.Name);

                                fiFile.CopyTo(targetFilePath, true);
                                fiFile.Delete();
                            }
                            diSourceFolder.Delete(false);
                        }
                        else
                        {
                            // Rename the folder
                            diSourceFolder.MoveTo(strRenamedSourceFolder);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Exception renaming the folder; log this as a warning
                        LogWarning("Exception renaming source LCMethods folder for " + datasetName + ": " + ex.Message);
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Exception copying LCMethod file for " + datasetName, ex);
                success = false;
            }

            if (string.IsNullOrWhiteSpace(methodFolderBasePath))
                return success;

            var dtCurrentTime = DateTime.Now;
            if (dtCurrentTime.Hour == 18 || dtCurrentTime.Hour == 19 || Environment.MachineName.ToLower().StartsWith("monroe"))
            {
                // Time is between 6 pm and 7:59 pm
                // Check for folders at METHOD_FOLDER_BASE_PATH that start with x_ and have .lcmethod files that are all at least 14 days old
                // These folders are safe to delete
                DeleteOldLCMethodFolders(methodFolderBasePath);
            }

            return success;
        }

        /// <summary>
        /// Capture a dataset folder that has an extension like .D or .Raw
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument); datasetInfo.FileOrFolderName will be appended to this</param>
        /// <param name="datasetFolderPath">Destination folder (on storage server); datasetInfo.FileOrFolderName will be appended to this</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        /// <param name="instrumentClass">Instrument class</param>
        /// <param name="instName">Instrument name</param>
        private void CaptureFolderExt(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            string instName)
        {

            SortedSet<string> filesToSkip = null;

            bool success;

            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
            var diTargetDir = new DirectoryInfo(Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName));

            // Look for a zero-byte .UIMF file or a .UIMF journal file
            // Abort the capture if either is present
            if (IsIncompleteUimfFound(diSourceDir.FullName, out msg, ref retData))
                return;

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap)
            {
                // Confirm that a DATA.MS file exists
                if (IsIncompleteAgilentIonTrap(diSourceDir.FullName, out msg, ref retData))
                    return;
            }

            var brukerDotDFolder = false;

            if (datasetInfo.FileOrFolderName.ToLower().EndsWith(".d"))
            {
                // Bruker .D folder (common for the 12T and 15T)
                // Look for journal files, which we can never copy because they are always locked

                brukerDotDFolder = true;

                var searchSpecList = new Dictionary<string, string>()
                {
                    {"*.mcf_idx-journal", "journal file"}
                };

                if (string.Equals(instName, "12T_FTICR_B", StringComparison.OrdinalIgnoreCase))
                {
                    // Add various mcf and mcf_idx files
                    // Specifically list those that have _1 or _2 etc. because we _do_ want to copy Storage.mcf_idx files
                    searchSpecList.Add("*_1.mcf", "mcf files");
                    searchSpecList.Add("*_2.mcf", "mcf files");
                    searchSpecList.Add("*_3.mcf", "mcf files");
                    searchSpecList.Add("*_4.mcf", "mcf files");
                    searchSpecList.Add("*_1.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_2.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_3.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_4.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("LockInfo", "lock files");
                    searchSpecList.Add("SyncHelper", "sync helper");
                    searchSpecList.Add("ProjectCreationHelper", "project creation helper");
                }

                success = FindFilesToSkip(diSourceDir, datasetInfo, searchSpecList, ref retData, out filesToSkip);
                if (!success)
                {
                    msg = "Error looking for journal files to skip";
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    // Note: error has already been logged and DisconnectShareIfRequired() has already been called
                    return;
                }

            }

            retData.CloseoutMsg = string.Empty;

            if (!VerifyConstantFolderSize(diSourceDir.FullName, ref retData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(retData.CloseoutMsg))
                {
                    retData.CloseoutMsg = "Folder size changed";
                }

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset folder
            try
            {
                MakeFolderPath(datasetFolderPath);
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception creating dataset folder";
                msg = retData.CloseoutMsg + " at " + datasetFolderPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
            }

            // Copy the source folder to the dataset folder
            try
            {
                // Copy the dataset folder
                // Resume copying files that are already present in the target

                if (copyWithResume)
                {
                    const bool recurse = true;
                    success = CopyFolderWithResume(diSourceDir.FullName, diTargetDir.FullName, recurse, ref retData, filesToSkip);
                }
                else
                {
                    if (filesToSkip == null)
                        mFileTools.CopyDirectory(diSourceDir.FullName, diTargetDir.FullName);
                    else
                        mFileTools.CopyDirectory(diSourceDir.FullName, diTargetDir.FullName, filesToSkip.ToList());
                    success = true;
                }

                if (success)
                {
                    msg = "Copied folder " + diSourceDir.FullName + " to " + diTargetDir.FullName + GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, diTargetDir);
                }
                else
                {
                    msg = "Unknown error copying the dataset folder";
                }
            }
            catch (Exception ex)
            {
                msg = "Copy exception for dataset " + datasetInfo.DatasetName + GetConnectionDescription();
                LogError(msg, ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
            }

            DisconnectShareIfRequired();

            if (success)
            {
                success = CaptureLCMethodFile(datasetInfo.DatasetName, datasetFolderPath);

                if (brukerDotDFolder)
                {
                    // Look for and delete certain zero-byte files
                    DeleteZeroByteBrukerFiles(diTargetDir);
                }
            }

            if (success)
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            else
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Look for an incomplete Agilent Ion Trap .D folder
        /// </summary>
        /// <returns>True if incomplete</returns>
        private bool IsIncompleteAgilentIonTrap(
            string sourceFolderPath,
            out string msg,
            ref clsToolReturnData retData)
        {
            msg = string.Empty;

            try
            {
                var diSourceFolder = new DirectoryInfo(sourceFolderPath);

                var dataMSFile = diSourceFolder.GetFiles("DATA.MS");
                string sourceFolderErrorMessage = null;

                if (dataMSFile.Length == 0)
                {
                    sourceFolderErrorMessage = "DATA.MS file not found; incomplete dataset";
                }
                else
                {
                    if (dataMSFile[0].Length == 0)
                    {
                        sourceFolderErrorMessage = "Source folder has a zero-byte DATA.MS file";
                    }
                }

                if (!string.IsNullOrEmpty(sourceFolderErrorMessage))
                {
                    retData.CloseoutMsg = sourceFolderErrorMessage;
                    msg = retData.CloseoutMsg + " at " + sourceFolderPath;
                    LogError(msg);
                    DisconnectShareIfRequired();
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return true;
                }
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception checking for a DATA.MS file";
                msg = retData.CloseoutMsg + " at " + sourceFolderPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Look for an incomplete .UIMF file, which is either 0 bytes in size or has a corresponding .uimf-journal file
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <returns>True if an incomplete .uimf file is found</returns>
        private bool IsIncompleteUimfFound(
            string sourceFolderPath,
            out string msg,
            ref clsToolReturnData retData)
        {
            msg = string.Empty;

            try
            {
                var diSourceFolder = new DirectoryInfo(sourceFolderPath);

                var uimfJournalFiles = diSourceFolder.GetFiles("*.uimf-journal");
                string sourceFolderErrorMessage = null;

                if (uimfJournalFiles.Length > 0)
                {
                    sourceFolderErrorMessage =
                        "Source folder has SQLite journal files, indicating data acquisition is in progress";
                }
                else
                {
                    var uimfFiles = diSourceFolder.GetFiles("*.uimf");
                    if (uimfFiles.Any(uimfFile => uimfFile.Length == 0))
                    {
                        sourceFolderErrorMessage = "Source folder has a zero-byte UIMF file";
                    }
                }

                if (!string.IsNullOrEmpty(sourceFolderErrorMessage))
                {
                    retData.CloseoutMsg = sourceFolderErrorMessage;
                    msg = retData.CloseoutMsg + " at " + sourceFolderPath;
                    LogError(msg);

                    DisconnectShareIfRequired();
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return true;
                }
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception checking for zero-byte dataset files";
                msg = retData.CloseoutMsg + " at " + sourceFolderPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return true;
            }

            return false;
        }

        private void DeleteZeroByteBrukerFiles(DirectoryInfo diTargetDir)
        {
            try
            {
                var fileNamesToDelete = new List<string>
                {
                    "ProjectCreationHelper",
                    "SyncHelper",
                    "lock.file"
                };

                var fileCountDeleted = 0;
                var deletedFileList = string.Empty;

                if (!diTargetDir.Exists)
                    return;

                var candidateFiles = diTargetDir.GetFiles("*", SearchOption.AllDirectories).ToList();

                foreach (var candidateFile in candidateFiles)
                {
                    if (candidateFile.Length == 0)
                    {
                        if (fileNamesToDelete.Contains(candidateFile.Name))
                        {
                            // Delete this zero-byte file
                            candidateFile.Delete();
                            fileCountDeleted++;
                            if (string.IsNullOrEmpty(deletedFileList))
                                deletedFileList = candidateFile.Name;
                            else
                                deletedFileList += ", " + candidateFile.Name;
                        }
                    }
                }

                if (fileCountDeleted > 0)
                {
                    LogError("Deleted " + fileCountDeleted + " zero byte files in the dataset folder: " + deletedFileList);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in DeleteZeroByteBrukerFiles", ex);
            }

        }

        /// <summary>
        /// Find files to skip based on filename match specs in searchSpec
        /// </summary>
        /// <param name="diSourceFolder"></param>
        /// <param name="datasetInfo"></param>
        /// <param name="searchSpecList">Dictionary where keys are filespecs to pass to .GetFiles() and values are the description of each key</param>
        /// <param name="retData"></param>
        /// <param name="filesToSkip">Output: List of file names to skip</param>
        /// <returns></returns>
        private bool FindFilesToSkip(
            DirectoryInfo diSourceFolder,
            clsDatasetInfo datasetInfo,
            Dictionary<string, string> searchSpecList,
            ref clsToolReturnData retData,
            out SortedSet<string> filesToSkip)
        {

            filesToSkip = new SortedSet<string>();

            try
            {
                foreach (var searchItem in searchSpecList)
                {
                    var searchSpec = searchItem.Key;

                    var foundFiles = diSourceFolder.GetFiles(searchSpec, SearchOption.AllDirectories).ToList();

                    foreach (var file in foundFiles)
                    {
                        if (!filesToSkip.Contains(file.Name))
                            filesToSkip.Add(file.Name);
                    }

                    if (foundFiles.Count == 1)
                    {
                        var firstSkippedFile = foundFiles.FirstOrDefault();
                        if (firstSkippedFile != null)
                            LogMessage("Skipping " + searchItem.Value + ": " + firstSkippedFile.Name);
                    }
                    else if (foundFiles.Count > 1)
                    {
                        var firstSkippedFile = foundFiles.FirstOrDefault();
                        var lastSkippedFile = foundFiles.LastOrDefault();

                        if (firstSkippedFile != null && lastSkippedFile != null)
                            LogMessage("Skipping " + foundFiles.Count + " " + searchItem.Value + "s: " +
                                       "(" + firstSkippedFile.Name + " through " + lastSkippedFile.Name + ")");
                    }
                }

                return true;

            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception getting list of files to skip";
                var msg = retData.CloseoutMsg + " for dataset " + datasetInfo.DatasetName;
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return false;
            }

        }

        /// <summary>
        /// Capture a folder with no extension on the name (the folder name is nearly always the dataset name)
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument); datasetInfo.FileOrFolderName will be appended to this</param>
        /// <param name="datasetFolderPath">Destination folder; datasetInfo.FileOrFolderName will not be appended to this (constrast with CaptureFolderExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        /// <param name="instrumentClass">Instrument class</param>
        private void CaptureFolderNoExt(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass)
        {
            var filesToSkip = new SortedSet<string>();

            bool success;

            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
            var diTargetDir = new DirectoryInfo(datasetFolderPath);

            // Look for a zero-byte .UIMF file or a .UIMF journal file
            // Abort the capture if either is present
            if (IsIncompleteUimfFound(diSourceDir.FullName, out msg, ref retData))
                return;

            // Verify the folder doesn't contain a group of ".d" folders
            var lstDotDFolders = diSourceDir.GetDirectories("*.d", SearchOption.TopDirectoryOnly);
            if (lstDotDFolders.Length > 1)
            {
                var allowMultipleFolders = false;

                if (lstDotDFolders.Length == 2)
                {
                    // If one folder contains a ser file and the other folder contains an analysis.baf, we'll allow this
                    // This is sometimes the case for the 15T_FTICR_Imaging
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

                if (!allowMultipleFolders && instrumentClass == clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2)
                {
                    // Effective July 2016, we allow Bruker Imaging datasets to have multiple .D subfolders
                    // They typically each have their own ser file
                    allowMultipleFolders = true;
                }

                if (!allowMultipleFolders)
                {
                    retData.CloseoutMsg = "Multiple .D folders found in dataset folder";
                    msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                    LogError(msg);
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the folder doesn't contain ".IMF" files
            if (diSourceDir.GetFiles("*.imf", SearchOption.TopDirectoryOnly).Length > 0)
            {
                retData.CloseoutMsg = "Dataset folder contains a series of .IMF files -- upload a .UIMF file instead";
                msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                LogError(msg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF)
            {
                // Possibly skip the Fragmentation_Profile.txt file
                var fragProfileFile = new FileInfo(Path.Combine(diSourceDir.FullName, "Fragmentation_Profile.txt"));

                if (fragProfileFile.Exists && FragmentationProfileFileIsDefault(fragProfileFile))
                {
                    filesToSkip.Add(fragProfileFile.Name);
                }

            }

            if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Sciex_QTrap)
            {
                // Make sure that it doesn't have more than 2 subfolders (it typically won't have any, but we'll allow 2)
                if (diSourceDir.GetDirectories("*", SearchOption.TopDirectoryOnly).Length > 2)
                {
                    retData.CloseoutMsg = "Dataset folder has more than 2 subfolders";
                    msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                    LogError(msg);
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }

                // Verify that the folder has a .wiff or a .wiff.scan file
                if (diSourceDir.GetFiles("*.wiff*", SearchOption.TopDirectoryOnly).Length == 0)
                {
                    retData.CloseoutMsg = "Dataset folder does not contain any .wiff files";
                    msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                    LogError(msg);
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the folder size is constant (indicates acquisition is actually finished)
            if (!VerifyConstantFolderSize(diSourceDir.FullName, ref retData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(retData.CloseoutMsg))
                {
                    retData.CloseoutMsg = "Folder size changed";
                }

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Copy the dataset folder to the storage server
            try
            {

                if (copyWithResume)
                {
                    const bool recurse = true;
                    success = CopyFolderWithResume(diSourceDir.FullName, diTargetDir.FullName, recurse, ref retData, filesToSkip);
                }
                else
                {
                    mFileTools.CopyDirectory(diSourceDir.FullName, diTargetDir.FullName, filesToSkip.ToList());
                    success = true;
                }

                if (success)
                {
                    msg = "Copied folder " + diSourceDir.FullName + " to " + diTargetDir.FullName + GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, diTargetDir);
                }
                else
                {
                    msg = "Unknown error copying the dataset folder";
                }
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset folder " + diSourceDir.FullName + GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                HandleCopyException(ref retData, ex);
                return;
            }
            finally
            {
                DisconnectShareIfRequired();
            }

            if (success)
            {
                success = CaptureLCMethodFile(datasetInfo.DatasetName, diTargetDir.FullName);
            }

            if (success)
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            else
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Capture a Bruker imaging folder
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument); datasetInfo.FileOrFolderName will be appended to this</param>
        /// <param name="datasetFolderPath">Destination folder; datasetInfo.FileOrFolderName will not be appended to this (constrast with CaptureFolderExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        private void CaptureBrukerImaging(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath,
            bool copyWithResume)
        {
            // Dataset found; it's a Bruker imaging folder

            bool success;

            // First, verify the folder size is constant (indicates acquisition is actually finished)
            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
            var diTargetDir = new DirectoryInfo(datasetFolderPath);

            // Check to see if the folders have been zipped
            var zipFileList = Directory.GetFiles(diSourceDir.FullName, "*.zip");
            if (zipFileList.Length < 1)
            {
                // Data files haven't been zipped, so throw error
                retData.CloseoutMsg = "No zip files found in dataset folder";
                msg = retData.CloseoutMsg + " at " + diSourceDir.FullName;
                LogError(msg);
                DisconnectShareIfRequired();

                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (!VerifyConstantFolderSize(diSourceDir.FullName, ref retData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(retData.CloseoutMsg))
                {
                    retData.CloseoutMsg = "Folder size changed";
                }

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset folder
            try
            {
                MakeFolderPath(diTargetDir.FullName);
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception creating dataset folder";
                msg = retData.CloseoutMsg + " at " + diTargetDir.FullName;
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
            }

            // Copy only the files in the dataset folder to the storage server. Do not copy folders
            try
            {
                if (copyWithResume)
                {
                    const bool recurse = false;
                    success = CopyFolderWithResume(diSourceDir.FullName, diTargetDir.FullName, recurse, ref retData);
                }
                else
                {

                    var foundFiles = Directory.GetFiles(diSourceDir.FullName);

                    foreach (var fileToCopy in foundFiles)
                    {
                        var fi = new FileInfo(fileToCopy);
                        fi.CopyTo(Path.Combine(diTargetDir.FullName, fi.Name));
                    }
                    success = true;
                }

                if (success)
                {
                    msg = "Copied files in folder " + diSourceDir.FullName + " to " + diTargetDir.FullName + GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, diTargetDir);
                }
                else
                {
                    msg = "Unknown error copying the dataset files";
                }
            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception copying files from dataset folder";
                msg = retData.CloseoutMsg + " " + diSourceDir.FullName + GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
            }
            finally
            {
                DisconnectShareIfRequired();
            }

            if (success)
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            else
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Capture a folder from a Bruker_Spot instrument
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="retData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFolderPath">Source folder (on instrument); datasetInfo.FileOrFolderName will be appended to this</param>
        /// <param name="datasetFolderPath">Destination folder; datasetInfo.FileOrFolderName will not be appended to this (constrast with CaptureFolderExt)</param>
        private void CaptureBrukerSpot(
            out string msg,
            ref clsToolReturnData retData,
            clsDatasetInfo datasetInfo,
            string sourceFolderPath,
            string datasetFolderPath)
        {
            // Dataset found; it's a Bruker_Spot instrument type
            // First, verify the folder size is constant (indicates acquisition is actually finished)
            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
            var diTargetDir = new DirectoryInfo(datasetFolderPath);

            // Verify the dataset folder doesn't contain any .zip files
            var zipFiles = diSourceDir.GetFiles("*.zip");

            if (zipFiles.Length > 0)
            {
                retData.CloseoutMsg = "Zip files found in dataset folder";
                msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                LogError(msg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            // Check whether the dataset folder contains just one data folder or multiple data folders
            var dataFolders = diSourceDir.GetDirectories().ToList();

            if (dataFolders.Count < 1)
            {
                retData.CloseoutMsg = "No subfolders were found in the dataset folder ";
                msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
                LogError(msg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (dataFolders.Count > 1)
            {
                // Make sure the subfolders match the naming convention for MALDI spot folders
                // Example folder names:
                //  0_D4
                //  0_E10
                //  0_N4

                const string MALDI_SPOT_FOLDER_REGEX = @"^\d_[A-Z]\d+$";
                var reMaldiSpotFolder = new Regex(MALDI_SPOT_FOLDER_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var folder in dataFolders)
                {
                    LogDebug("Test folder " + folder + " against RegEx " + reMaldiSpotFolder);

                    if (!reMaldiSpotFolder.IsMatch(folder.Name, 0))
                    {
                        retData.CloseoutMsg = "Dataset folder contains multiple subfolders, but folder " + folder.Name + " does not match the expected pattern";
                        msg = retData.CloseoutMsg + " (" + reMaldiSpotFolder + "); see " + diSourceDir.FullName;
                        LogError(msg);
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return;
                    }

                }
            }

            if (!VerifyConstantFolderSize(diSourceDir.FullName, ref retData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(retData.CloseoutMsg))
                {
                    retData.CloseoutMsg = "Folder size changed";
                }

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Copy the dataset folder (and all subfolders) to the storage server
            try
            {
                mFileTools.CopyDirectory(diSourceDir.FullName, diTargetDir.FullName);
                msg = "Copied folder " + diSourceDir.FullName + " to " + diTargetDir.FullName + GetConnectionDescription();
                LogMessage(msg);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset folder " + diSourceDir.FullName + GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                HandleCopyException(ref retData, ex);
            }
            finally
            {
                DisconnectShareIfRequired();
            }
        }

        private bool CopyFolderWithResume(
            string sourceFolderPath,
            string targetFolderPath,
            bool recurse,
            ref clsToolReturnData retData)
        {
            return CopyFolderWithResume(sourceFolderPath, targetFolderPath, recurse, ref retData, new SortedSet<string>());
        }

        private bool CopyFolderWithResume(
            string sourceFolderPath,
            string targetFolderPath,
            bool recurse,
            ref clsToolReturnData retData,
            SortedSet<string> filesToSkip)
        {
            const clsFileTools.FileOverwriteMode overwriteMode = clsFileTools.FileOverwriteMode.OverWriteIfDateOrLengthDiffer;

            var success = false;
            var doCopy = true;

            while (doCopy)
            {
                var dtCopyStart = DateTime.UtcNow;

                string msg;
                try
                {
                    // Clear any previous errors
                    mErrorMessage = string.Empty;

                    success = mFileTools.CopyDirectoryWithResume(
                        sourceFolderPath, targetFolderPath,
                        recurse, overwriteMode, filesToSkip.ToList(),
                        out var fileCountSkipped, out var fileCountResumed, out var fileCountNewlyCopied);

                    doCopy = false;

                    if (success)
                    {
                        msg = "  directory copy complete; CountCopied = " + fileCountNewlyCopied + "; " +
                              "CountSkipped = " + fileCountSkipped + "; " +
                              "CountResumed = " + fileCountResumed;
                        LogDebug(msg);
                    }
                    else
                    {
                        msg = "  directory copy failed for " + sourceFolderPath + " to " + targetFolderPath + GetConnectionDescription();
                        LogError(msg);
                    }

                }
                catch (Exception ex)
                {
                    if (string.IsNullOrWhiteSpace(mFileTools.CurrentSourceFile))
                        msg = "Error while copying directory: ";
                    else
                        msg = "Error while copying " + mFileTools.CurrentSourceFile + ": ";

                    mErrorMessage = string.Copy(msg);

                    if (ex.Message.Length <= 350)
                        msg += ex.Message;
                    else
                        msg += ex.Message.Substring(0, 350);

                    LogError(msg);

                    doCopy = false;
                    if (mFileTools.CurrentCopyStatus == clsFileTools.CopyStatus.BufferedCopy ||
                        mFileTools.CurrentCopyStatus == clsFileTools.CopyStatus.BufferedCopyResume)
                    {
                        // Exception occurred during the middle of a buffered copy
                        // If at least 10 seconds have elapsed, auto-retry the copy again
                        var elapsedTime = DateTime.UtcNow.Subtract(dtCopyStart).TotalSeconds;
                        if (elapsedTime >= 10)
                        {
                            doCopy = true;
                            msg = "  " + elapsedTime.ToString("0") + " seconds have elapsed; will attempt to resume copy";
                            LogMessage(msg);
                        }
                    }

                    HandleCopyException(ref retData, ex);

                }
            }

            if (success)
            {
                // CloseoutType may have been set to CLOSEOUT_FAILED by HandleCopyException; reset it to CLOSEOUT_SUCCESS
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                retData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            }
            return success;

        }

        /// <summary>
        /// Look for LCMethod folders that start with x_ and have .lcmethod files that are more than 2 weeks old
        /// Matching folders are deleted
        /// Note that in February 2012 we plan to switch to saving .lcmethod files in Year_Quarter folders (e.g. 2012_1 or 2012_2) and thus we won't need to call this function in the future
        /// </summary>
        /// <param name="lcMethodsFolderPath"></param>
        private void DeleteOldLCMethodFolders(string lcMethodsFolderPath)
        {
            try
            {
                var diLCMethodsFolder = new DirectoryInfo(lcMethodsFolderPath);
                if (!diLCMethodsFolder.Exists)
                    return;

                var diSubfolders = diLCMethodsFolder.GetDirectories("x_*");

                foreach (var diFolder in diSubfolders)
                {
                    var safeToDelete = true;

                    // Make sure all of the files in the folder are at least 14 days old
                    foreach (var fileOrFolder in diFolder.GetFileSystemInfos())
                    {
                        if (DateTime.UtcNow.Subtract(fileOrFolder.LastWriteTimeUtc).TotalDays <= 14)
                        {
                            // File was modified within the last 2 weeks; do not delete this folder
                            safeToDelete = false;
                            break;
                        }

                    }

                    if (!safeToDelete)
                        continue;

                    try
                    {
                        diFolder.Delete(true);

                        LogMessage("Deleted old LCMethods folder: " + diFolder.FullName);
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception deleting old LCMethods folder", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception looking for old LC Method folders", true);
                LogError("Stack trace", ex);
            }
        }

        private bool FragmentationProfileFileIsDefault(FileSystemInfo fragProfileFile)
        {
            try
            {
                // Regex to match lines of the form:
                // 0, 0, 0, 0, 0
                var zeroLineMatcher = new Regex("^[0, ]+$", RegexOptions.Compiled);

                using (var reader = new StreamReader(new FileStream(fragProfileFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var dataLineCount = 0;
                    var lineAllZeroes = false;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        dataLineCount++;

                        lineAllZeroes = zeroLineMatcher.IsMatch(dataLine);
                    }

                    if (dataLineCount == 1 && lineAllZeroes)
                    {
                        LogMessage("Skipping capture of default fragmentation profile file, " + fragProfileFile.FullName);
                        return true;
                    }

                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception examining the Fragmentation_Profile.txt file", ex);
            }
            return false;

        }

        /// <summary>
        /// Return the current quarter for a given date (based on the month)
        /// </summary>
        /// <param name="dtDate"></param>
        /// <returns></returns>
        private int GetQuarter(DateTime dtDate)
        {
            switch (dtDate.Month)
            {
                case 1:
                case 2:
                case 3:
                    return 1;
                case 4:
                case 5:
                case 6:
                    return 2;
                case 7:
                case 8:
                case 9:
                    return 3;
                default:
                    return 4;
            }
        }

        /// <summary>
        /// Return the sleep interval for a file or folder that is the given days old
        /// </summary>
        /// <param name="itemAgeDays">Days before now that the file or folder was modified</param>
        /// <param name="minimumTimeSeconds">Minimum sleep time</param>
        /// <returns>
        /// mSleepInterval if less than 10 days old
        /// minimumTimeSeconds if more than 30 days old
        /// Otherwise, a value between minimumTimeSeconds and mSleepInterval
        /// </returns>
        private int GetSleepInterval(double itemAgeDays, int minimumTimeSeconds)
        {
            const int AGED_FILE_DAYS_MINIMUM = 10;
            const int AGED_FILE_DAYS_MAXIMUM = 30;

            if (itemAgeDays < AGED_FILE_DAYS_MINIMUM)
                return mSleepInterval;

            if (itemAgeDays > AGED_FILE_DAYS_MAXIMUM)
                return minimumTimeSeconds;

            var scalingMultiplier = (AGED_FILE_DAYS_MAXIMUM - itemAgeDays) /
                                    (AGED_FILE_DAYS_MAXIMUM - AGED_FILE_DAYS_MINIMUM);

            var maximumTimeSeconds = Math.Max(mSleepInterval, minimumTimeSeconds);

            var sleepTimeSeconds = scalingMultiplier * (maximumTimeSeconds - minimumTimeSeconds) + minimumTimeSeconds;

            return (int)Math.Round(sleepTimeSeconds);

        }

        /// <summary>
        /// Return the appropriate interval to wait while examining that a file's size does not change
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <returns>Sleep time, in seconds</returns>
        private int GetSleepIntervalForFile(string sourceFilePath)
        {
            const int MINIMUM_TIME_SECONDS = 3;

            try
            {
                var fiSourceFile = new FileInfo(sourceFilePath);

                if (!fiSourceFile.Exists)
                    return MINIMUM_TIME_SECONDS;

                var fileAgeDays = DateTime.UtcNow.Subtract(fiSourceFile.LastWriteTimeUtc).TotalDays;

                return GetSleepInterval(fileAgeDays, MINIMUM_TIME_SECONDS);
            }
            catch (Exception ex)
            {
                LogError("Error in GetSleepIntervalForFile", ex);
                return mSleepInterval;
            }
        }

        /// <summary>
        /// Return the appropriate interval to wait while examining that a directory's size does not change
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <returns>Sleep time, in seconds</returns>
        private int GetSleepIntervalForFolder(string sourceFolderPath)
        {
            const int MINIMUM_TIME_SECONDS = 3;

            try
            {
                var diSourceDir = new DirectoryInfo(sourceFolderPath);

                if (!diSourceDir.Exists)
                    return MINIMUM_TIME_SECONDS;

                // Find the newest file in the folder
                var files = diSourceDir.GetFileSystemInfos("*", SearchOption.AllDirectories);

                if (files.Length == 0)
                    return MINIMUM_TIME_SECONDS;

                var mostRecentWriteTime = (from item in files orderby item.LastWriteTimeUtc select item.LastWriteTimeUtc).Max();

                var fileAgeDays = DateTime.UtcNow.Subtract(mostRecentWriteTime).TotalDays;

                return GetSleepInterval(fileAgeDays, MINIMUM_TIME_SECONDS);
            }
            catch (Exception ex)
            {
                LogError("Error in GetSleepIntervalForFolder", ex);
                return mSleepInterval;
            }
        }

        private void HandleCopyException(ref clsToolReturnData retData, Exception ex)
        {
            if (ex.Message.Contains("An unexpected network error occurred") ||
                ex.Message.Contains("Multiple connections") ||
                ex.Message.Contains("specified network name is no longer available"))
            {
                // Need to completely exit the capture task manager
                mNeedToAbortProcessing = true;
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else if (ex.Message.Contains("unknown user name or bad password") || ex.Message.Contains("user name or password"))
            {
                // This error randomly occurs; no need to log a full stack trace
                retData.CloseoutMsg = "Authentication failure: " + ex.Message;
                LogError(retData.CloseoutMsg);

                // Set the EvalCode to 3 so that capture can be retried
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Return true if the file or path has any invalid characters
        /// </summary>
        /// <param name="fileOrPath">Filename or full file/folder path</param>
        /// <param name="itemDescription">Description of fileOrPath; included in CloseoutMsg if there is a problem</param>
        /// <param name="isFile">True for a file; false for a path</param>
        /// <param name="retData">Return data object</param>
        /// <returns>True if an error; false if no problems</returns>
        private static bool NameHasInvalidCharacter(string fileOrPath, string itemDescription, bool isFile, ref clsToolReturnData retData)
        {
            var invalidCharIndex = fileOrPath.IndexOfAny(isFile ? Path.GetInvalidFileNameChars() : Path.GetInvalidPathChars());

            if (invalidCharIndex < 0)
            {
                return false;
            }

            retData.CloseoutMsg = string.IsNullOrWhiteSpace(itemDescription) ? fileOrPath : itemDescription;
            retData.CloseoutMsg += " contains an invalid character at index " + invalidCharIndex + ": " + fileOrPath[invalidCharIndex];
            LogError(retData.CloseoutMsg, true);
            retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return true;
        }

        /// <summary>
        /// Store mErrorMessage in retData.CloseoutMsg if an error exists yet retData.CloseoutMsg is empty
        /// </summary>
        /// <param name="retData"></param>
        private void PossiblyStoreErrorMessage(ref clsToolReturnData retData)
        {

            if (!string.IsNullOrWhiteSpace(mErrorMessage) && string.IsNullOrWhiteSpace(retData.CloseoutMsg))
            {
                retData.CloseoutMsg = mErrorMessage;
                if (mTraceMode)
                    clsToolRunnerBase.ShowTraceMessage(mErrorMessage);
            }
        }

        /// <summary>
        /// Verifies specified folder path exists
        /// </summary>
        /// <param name="InpPath">Folder path to test</param>
        /// <returns>TRUE if folder was found</returns>
        private bool ValidateFolderPath(string InpPath)
        {
            bool retVal;

            if (Directory.Exists(InpPath))
            {
                retVal = true;
            }
            else
            {
                retVal = false;
            }
            return retVal;
        }

        #endregion

        #region "Event handlers"

        private void OnCopyingFile(string filename)
        {
            LogDebug("Copying file " + filename);
        }

        private void OnResumingFileCopy(string filename)
        {
            LogMessage("Resuming copy of file " + filename);
        }

        private void OnFileCopyProgress(string filename, float percentComplete)
        {

            if (DateTime.Now.Subtract(mLastProgressUpdate).TotalSeconds >= 20 || percentComplete >= 100 && filename == mLastProgressFileName)
            {
                if ((mLastProgressFileName == filename) && (Math.Abs(mLastProgressPercent - percentComplete) < float.Epsilon))
                    // Don't re-display this progress
                    return;

                mLastProgressUpdate = DateTime.Now;
                mLastProgressFileName = filename;
                mLastProgressPercent = percentComplete;
                LogMessage("  copying " + Path.GetFileName(filename) + ": " + percentComplete.ToString("0.0") + "% complete");
            }

        }

        /// <summary>
        /// Report some stats on the given folder, including the number of files and the largest file
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <returns>String describing the folder; if a problem, reports Error: ErrorMsg </returns>
        private string ReportFolderStats(string sourceFolderPath)
        {
            try
            {
                var sourceFolder = new DirectoryInfo(sourceFolderPath);
                if (!sourceFolder.Exists)
                    return "Error: folder not found, " + sourceFolderPath;

                var filesInFolder = sourceFolder.GetFiles();
                float totalSizeKB = 0;
                var largestFileInfo = new KeyValuePair<long, string>(0, "");

                foreach (var file in filesInFolder)
                {
                    totalSizeKB += file.Length / 1024.0f;
                    if (file.Length > largestFileInfo.Key)
                    {
                        largestFileInfo = new KeyValuePair<long, string>(file.Length, file.Name);
                    }
                }

                return string.Format("{0} files, {1:F1} KB total, largest file is {2}",
                    filesInFolder.Length, totalSizeKB, largestFileInfo.Value);

            }
            catch (Exception ex)
            {
                LogError("Error in ReportFirstVisibleFile", ex);
                return "Error: " + ex.Message;
            }
        }

        /// <summary>
        /// Make sure that we matched a file for instruments that save data as a file, or a folder for instruments that save data to a folder
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="sourceFolderPath"></param>
        /// <param name="sourceType"></param>
        /// <param name="instrumentClass">Instrument class</param>
        /// <param name="datasetInfo"></param>
        /// <param name="retData"></param>
        /// <returns>True if the file or folder is appropriate for the instrument class</returns>
        private bool ValidateWithInstrumentClass(
            string dataset,
            string sourceFolderPath,
            RawDSTypes sourceType,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass,
            clsDatasetInfo datasetInfo,
            ref clsToolReturnData retData)
        {
            string entityDescription;

            retData.CloseoutMsg = string.Empty;

            switch (sourceType)
            {
                case RawDSTypes.File:
                    entityDescription = "a file";
                    break;
                case RawDSTypes.FolderNoExt:
                    entityDescription = "a folder";
                    break;
                case RawDSTypes.FolderExt:
                    entityDescription = "a folder";
                    break;
                case RawDSTypes.BrukerImaging:
                case RawDSTypes.BrukerSpot:
                    entityDescription = "a folder";
                    break;
                case RawDSTypes.MultiFile:
                    entityDescription = "multiple files";
                    break;
                default:
                    entityDescription = "an unknown entity";
                    break;
            }

            // Make sure we are capturing the correct entity type (file or folder) based on instrumentClass
            // See table T_Instrument_Class for allowed types
            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
                case clsInstrumentClassInfo.eInstrumentClass.GC_QExactive:
                case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
                case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
                case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
                    if (sourceType != RawDSTypes.File)
                    {
                        if (sourceType == RawDSTypes.FolderNoExt)
                        {
                            // Datasets from LAESI-HMS datasets will have a folder named after the dataset, and inside that folder will be a single .raw file
                            // Confirm that this is the case

                            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
                            var foundFiles = diSourceDir.GetFiles("*.raw").ToList();
                            if (foundFiles.Count == 1)
                                break;

                            if (foundFiles.Count > 1)
                            {
                                // Dataset name matched a folder with multiple .raw files; there must be only one .raw file
                                retData.CloseoutMsg = "Dataset name matched " + entityDescription +
                                                      " with multiple .raw files; there must be only one .raw file";

                                var fileNames = foundFiles.Select(file => file.Name).ToList();
                                LogWarning("Multiple .raw files found in folder " + diSourceDir.FullName + ": " + string.Join(", ", fileNames.Take(5)));

                            }
                            else
                                // Dataset name matched a folder but it does not have a .raw file
                                retData.CloseoutMsg = "Dataset name matched " + entityDescription + " but it does not have a .raw file";

                            break;
                        }

                        if (sourceType == RawDSTypes.MultiFile)
                        {
                            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath));
                            var foundFiles = diSourceDir.GetFiles(datasetInfo.FileOrFolderName + ".*").ToList();
                            if (foundFiles.Count == 2)
                            {
                                // On the 21T each .raw file can have a corresponding .tsv file
                                // Allow for this during capture

                                var rawFound = false;
                                var tsvFound = false;

                                foreach (var file in foundFiles)
                                {
                                    if (string.Equals(Path.GetExtension(file.Name), ".raw", StringComparison.OrdinalIgnoreCase))
                                        rawFound = true;

                                    if (string.Equals(Path.GetExtension(file.Name), ".tsv", StringComparison.OrdinalIgnoreCase))
                                        tsvFound = true;
                                }

                                if (rawFound && tsvFound)
                                {
                                    LogMessage("Capturing a .raw file with a corresponding .tsv file");
                                    break;
                                }
                            }

                            var fileNames = foundFiles.Select(file => file.Name).ToList();
                            LogWarning(
                                "Dataset name matched multiple files in folder " + diSourceDir.FullName + ": " +
                                string.Join(", ", fileNames.Take(5)));

                        }

                        // Dataset name matched multiple files; must be a .raw file
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .raw file";

                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                    if (sourceType != RawDSTypes.FolderNoExt)
                    {
                        // Dataset name matched a file; must be a folder with the dataset name, and inside the folder is a .D folder (and typically some jpg files)
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a folder with the dataset name, and inside the folder is a .D folder (and typically some jpg files)";
                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Bruker_Amazon_Ion_Trap:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerTOF_BAF:
                case clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap:
                case clsInstrumentClassInfo.eInstrumentClass.Agilent_TOF_V2:
                case clsInstrumentClassInfo.eInstrumentClass.PrepHPLC:

                    if (sourceType != RawDSTypes.FolderExt)
                    {
                        // Dataset name matched a file; must be a .d folder
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .d folder";
                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:

                    if (sourceType != RawDSTypes.FolderNoExt)
                    {
                        // Dataset name matched a file; must be a folder with the dataset name
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a folder with the dataset name";
                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.Sciex_TripleTOF:
                    if (sourceType != RawDSTypes.File)
                    {
                        // Dataset name matched a folder; must be a file
                        // Dataset name matched multiple files; must be a file
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a file";
                    }
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
                    if (sourceType != RawDSTypes.File)
                    {

                        if (sourceType == RawDSTypes.FolderExt)
                        {
                            // IMS08_AgQTOF05 collects data as .D folders, which the capture pipeline will then convert to a .uimf file
                            // Make sure the matched folder is a .d file
                            if (datasetInfo.FileOrFolderName.ToLower().EndsWith(".d"))
                                break;
                        }

                        if (sourceType == RawDSTypes.FolderNoExt)
                        {
                            // IMS04_AgTOF05 and similar instruments collect data into a folder named after the dataset
                            // The folder contains a .UIMF file plus several related files
                            // Make sure the folder contains just one .UIMF file

                            var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
                            var foundFiles = diSourceDir.GetFiles("*.uimf").ToList();
                            if (foundFiles.Count == 1)
                                break;

                            if (foundFiles.Count > 1)
                            {
                                // Dataset name matched a folder with multiple .uimf files; there must be only one .uimf file
                                retData.CloseoutMsg = "Dataset name matched " + entityDescription +
                                                      " with multiple .uimf files; there must be only one .uimf file";

                                var fileNames = foundFiles.Select(file => file.Name).ToList();
                                LogWarning("Multiple .uimf files found in folder " + diSourceDir.FullName + ": " + string.Join(", ", fileNames).Take(5));
                            }
                            else
                            {
                                // Dataset name matched a folder but it does not have a .uimf file
                                retData.CloseoutMsg = "Dataset name matched " + entityDescription + " but it does not have a .uimf file";
                                LogWarning("Folder  " + diSourceDir.FullName + " does not have any .uimf files");
                            }

                            break;
                        }

                        if (sourceType != RawDSTypes.FolderExt &&
                            sourceType != RawDSTypes.FolderNoExt &&
                            sourceType != RawDSTypes.MultiFile)
                        {
                            LogWarning("sourceType was not FolderExt, FolderNoExt, or MultiFile; this is unexpected: " + sourceType);
                        }

                        // Dataset name matched multiple files; must be a .uimf file, .d folder, or folder with a single .uimf file
                        retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .uimf file, .d folder, or folder with a single .uimf file";
                    }
                    break;
            }

            if (string.IsNullOrEmpty(retData.CloseoutMsg))
            {
                // We are capturing the right item for this dataset's instrument class
                return true;
            }

            LogError(retData.CloseoutMsg + ": " + dataset, true);

            return false;
        }

        #endregion

    }

}
