
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//						02/03/2010 (DAC) - Modified logging to include job number
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;
using System.Security.Cryptography;

namespace DatasetArchivePlugin
{
    class clsArchiveUpdate : clsOpsBase
    {
        //*********************************************************************************************************
        // Tools to perform archive update operations
        //**********************************************************************************************************

        #region "Constants"
        const int FILE_COMPARE_EQUAL = -1;
        const int FILE_COMPARE_NOT_EQUAL = 0;
        const int FILE_COMPARE_ERROR = 1;

        #endregion

        #region "Class variables"

        // Obsolete: "No longer used"
        // string m_ArchiveSharePath = string.Empty;				// The dataset folder path in the archive, for example: \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25
        // string m_ResultsFolderPathArchive = string.Empty;		// The target path to copy the data to, for example:    \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684
        // string m_ResultsFolderPathServer = string.Empty;		// The source path of the dataset folder (or dataset job results folder) to archive, for example: \\proto-7\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="MgrParams">Manager parameters</param>
        /// <param name="TaskParams">Task parameters</param>
        /// <param name="StatusTools"></param>
        public clsArchiveUpdate(IMgrParams MgrParams, ITaskParams TaskParams, IStatusFile StatusTools)
            : base(MgrParams, TaskParams, StatusTools)
        {
        }
        #endregion

        #region "Methods"
        /// <summary>
        /// Performs an archive update task (overrides base)
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public override bool PerformTask()
        {

            // Perform base class operations
            if (!base.PerformTask())
                return false;

            var statusMessage = "Updating dataset " + m_DatasetName + ", job " + m_TaskParams.GetParam("Job");
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);

            statusMessage = "Pushing dataset folder to MyEMSL";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);

            mMostRecentLogTime = DateTime.UtcNow;
            mLastStatusUpdateTime = DateTime.UtcNow;

            const int iMaxMyEMSLUploadAttempts = 2;
            const bool recurse = true;

            // Set this to true to create the .tar file locally and thus not upload the data to MyEMSL
            var debugMode = Pacifica.Core.EasyHttp.eDebugMode.DebugDisabled;

            if (m_TaskParams.GetParam("DebugTestTar", false))
                debugMode = Pacifica.Core.EasyHttp.eDebugMode.CreateTarLocal;
            else
                if (m_TaskParams.GetParam("MyEMSLOffline", false))
                debugMode = Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode;

            if (debugMode != Pacifica.Core.EasyHttp.eDebugMode.DebugDisabled)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Calling UploadToMyEMSLWithRetry with debugMode=" + debugMode);

            const bool PUSH_TO_TEST_SERVER = false;

            var debugTestInstanceOnly = PUSH_TO_TEST_SERVER;

            if (!debugTestInstanceOnly)
            {
                var copySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode,
                                                          useTestInstance: false);

                if (!copySuccess)
                    return false;

                // Finished with this update task
                statusMessage = "Completed push to MyEMSL, dataset " + m_DatasetName + ", Folder " +
                                m_TaskParams.GetParam("OutputFolderName") + ", job " + m_TaskParams.GetParam("Job");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);
            }

            if (!PUSH_TO_TEST_SERVER)
                return true;

            /*
            // Possibly also upload the dataset to the MyEMSL test instance
            const int PERCENT_DATA_TO_SEND_TO_TEST = 20;
            var testDateCuttoff = new DateTime(2015, 11, 1);

            if (DateTime.Now > testDateCuttoff)
            {
                // Testing has finished
                return true;
            }

            var rand = new Random();
            var randomNumber = rand.Next(0, 100);

            if (randomNumber > PERCENT_DATA_TO_SEND_TO_TEST & !debugTestInstanceOnly)
            {
                // Do not send this dataset to the test server
                return true;
            }
            */

            // Also upload a copy of the data to the MyEMSL test server
            var testCopySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode, useTestInstance: true);
            if (!testCopySuccess)
            {
                statusMessage = "MyEMSL test server upload failed";
                AppendToString(m_WarningMsg, statusMessage);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, statusMessage);
            }
            else
            {
                statusMessage = "Completed push to the MyEMSL test server, dataset " + m_DatasetName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);
            }

            return true;

        }

        /// <summary>
        /// Converts the Samba version of an archive path to a Linux version of the path
        /// </summary>
        /// <param name="sambaPath">Samba path to convert</param>
        /// <returns>Linux version of input path</returns>
        private string ConvertSambaPathToLinuxPath(string sambaPath)
        {
            // Find index of string "dmsarch" in Samba path
            var startIndx = sambaPath.IndexOf("dmsarch");
            if (startIndx < 0)
            {
                //TODO: Substring wasn't found - this is an error that has to be handled.
                return string.Empty;
            }
            var tmpStr = sambaPath.Substring(startIndx);

            // Add on the prefix for the archive
            tmpStr = "/archive/" + tmpStr;

            // Replace and DOS path separators with Linux separators
            tmpStr = tmpStr.Replace(@"\", "/");

            return tmpStr;
        }

        /// <summary>
        /// Compares folders on storage server and archive
        /// </summary>
        /// <param name="svrFolderPath">Location of source folder on storage server</param>
        /// <param name="sambaFolderPath">Samba path to compared folder in archive</param>
        /// <param name="compareErrorCount"></param>
        /// <param name="compareWithHash"></param>
        /// <returns>List of files that need to be copied to the archive</returns>
        private List<clsJobData> CompareFolders(string svrFolderPath, string sambaFolderPath, out int compareErrorCount, ref bool compareWithHash)
        {
            List<string> serverFiles;
            compareErrorCount = 0;
            string msg;

            // Verify server folder exists
            if (!Directory.Exists(svrFolderPath))
            {
                msg = "clsArchiveUpdate.CompareFolders: Storage server folder " + svrFolderPath + " not found";
                LogErrorMessage(msg, "Current Task");
                return null;
            }

            // Verify samba folder exists
            if (!Directory.Exists(sambaFolderPath))
            {
                msg = "clsArchiveUpdate.CompareFolders: Archive folder " + sambaFolderPath + " not found";
                LogErrorMessage(msg, "Current Task");
                return null;
            }

            // Get a list of all the folders in the server folder
            try
            {
                var dirsToScan = new List<string> { svrFolderPath };
                var dirScanner = new DirectoryScanner(dirsToScan);
                serverFiles = dirScanner.PerformScan("*");
            }
            catch (Exception ex)
            {
                msg = "clsArchiveUpdate.CompareFolders: Exception getting file listing, folder " + svrFolderPath;
                LogErrorMessage(msg + "; " + ex.Message, "Exception getting file listing for svrFolderPath");
                return null;
            }

            // Loop through results folder file list, checking for archive copies and comparing if archive copy present
            var returnObject = new List<clsJobData>();
            foreach (string svrFileName in serverFiles)
            {
                // Convert the file name on the server to its equivalent in the archive
                var archFileName = ConvertServerPathToArchivePath(svrFolderPath, sambaFolderPath, svrFileName);
                if (archFileName.Length == 0)
                {
                    msg = "File name not returned when converting from server path to archive path for file" + svrFileName;
                    LogErrorMessage(msg, "Current Task");
                    return null;
                }

                if (archFileName == "Error")
                {
                    msg = "Exception converting server path to archive path for file " + svrFileName + ": " + m_ErrMsg;
                    LogErrorMessage(msg, "Current Task");
                    return null;
                }

                // Determine if file exists in archive
                clsJobData tmpJobData;
                if (File.Exists(archFileName))
                {
                    // File exists in archive, so compare the server and archive versions
                    var compareResult = CompareTwoFiles(svrFileName, archFileName, compareWithHash);

                    if (compareWithHash &&
                        compareResult == FILE_COMPARE_ERROR &&
                        m_ErrMsg.ToLower().Contains("Exception generating hash".ToLower()))
                    {

                        // The file most likely could not be retrieved by the tape robot
                        // Disable hash-based comparisons for this job

                        msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
                        LogErrorMessage(msg, "Current Task");

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Disabling hash-based comparisons for this job");

                        // Retry the comparison, but this time don't generate a hash
                        compareWithHash = false;
                        compareResult = CompareTwoFiles(svrFileName, archFileName, compareWithHash);
                    }

                    switch (compareResult)
                    {
                        case FILE_COMPARE_EQUAL:
                            // Do nothing
                            break;
                        case FILE_COMPARE_NOT_EQUAL:
                            // Add the server file to the list of files to be copied
                            tmpJobData = new clsJobData
                            {
                                SvrFileToUpdate = svrFileName,
                                SambaFileToUpdate = archFileName,
                                SvrDsNamePath = svrFolderPath,
                                RenameFlag = true
                            };
                            returnObject.Add(tmpJobData);
                            break;
                        default:        // Includes FILE_COMPARE_ERROR
                            // There was a problem with the file comparison; abort the update
                            msg = "clsArchiveUpdate.CompareFolders: Error comparing files. Error msg = " + m_ErrMsg;
                            LogErrorMessage(msg, "Current Task");
                            compareErrorCount += 1;
                            break;
                    }	// End switch
                }
                else
                {
                    // File doesn't exist in archive, so add it to the list of files to be copied
                    tmpJobData = new clsJobData
                    {
                        SvrFileToUpdate = svrFileName,
                        SambaFileToUpdate = archFileName,
                        SvrDsNamePath = svrFolderPath,
                        RenameFlag = false
                    };
                    returnObject.Add(tmpJobData);
                }
            }	// End foreach

            // All finished, so return
            return returnObject;
        }

        /// <summary>
        /// Converts a file path on the storage server to its Samba equivalent
        /// </summary>
        /// <param name="svrPath">Path on server to folder being compared</param>
        /// <param name="archPath">Path in archive to folder being compared</param>
        /// <param name="inpFileName">File being compared</param>
        /// <returns>Full path in archive to file</returns>
        string ConvertServerPathToArchivePath(string svrPath, string archPath, string inpFileName)
        {
            // Convert by replacing storage server path with archive path (Samba version)
            try
            {
                var tmpPath = inpFileName.Replace(svrPath, archPath);
                return tmpPath;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Exception converting path name " + svrPath + ": " + ex.Message;
                return "Error";
            }
        }

        /// <summary>
        /// Compares two files, optionally using a SHA hash
        /// </summary>
        /// <param name="srcFileName">Fully qualified path to first file (should reside on the Proto storage server)</param>
        /// <param name="archFileName">Fully qualified path to second file (should reside in the EMSL archive)</param>
        /// <param name="generateHash"></param>
        /// <returns>Integer representing files equal, not equal, or error</returns>
        private int CompareTwoFiles(string srcFileName, string archFileName, bool generateHash)
        {
            m_ErrMsg = string.Empty;

            // First compare the file lengths
            var fiSourceFile = new FileInfo(srcFileName);
            var fiArchiveFile = new FileInfo(archFileName);

            if (!fiSourceFile.Exists)
            {
                m_ErrMsg = "File " + fiSourceFile.FullName + " not found (CompareTwoFiles)";
                return FILE_COMPARE_ERROR;
            }

            if (!fiArchiveFile.Exists)
            {
                return FILE_COMPARE_NOT_EQUAL;
            }

            if (fiSourceFile.Length != fiArchiveFile.Length)
                return FILE_COMPARE_NOT_EQUAL;

            // Only generate a hash for the files if the archive file was created within the last 35 days
            // Files older than that may be purged from spinning disk and would thus only reside on tape
            // Since retrieval from tape can be slow, we won't compute a hash if the file is more than 35 days old
            if (generateHash && DateTime.UtcNow.Subtract(fiArchiveFile.LastWriteTimeUtc).TotalDays < 35)
            {
                // Compares two files via SHA hash

                // Compute the has for each file
                var sSourceFileHash = GenerateHashFromFile(fiSourceFile);
                if (string.IsNullOrEmpty(sSourceFileHash))
                {
                    //There was a problem. Description is already in m_ErrMsg
                    return FILE_COMPARE_ERROR;
                }

                var sArchiveFileHash = GenerateHashFromFile(fiArchiveFile);
                if (string.IsNullOrEmpty(sArchiveFileHash))
                {
                    // There was a problem. Description is already in m_ErrMsg
                    return FILE_COMPARE_ERROR;
                }

                if (sSourceFileHash == sArchiveFileHash)
                    return FILE_COMPARE_EQUAL;
                else
                    return FILE_COMPARE_NOT_EQUAL;
            }

            // Simply compare file dates
            // If the source file is newer; then assume we need to copy
            if (fiSourceFile.LastWriteTimeUtc > fiArchiveFile.LastWriteTimeUtc)
                return FILE_COMPARE_NOT_EQUAL;
            else
                return FILE_COMPARE_EQUAL;

        }

        /// <summary>
        /// Generates SHA1 hash for specified file
        /// </summary>
        /// <param name="fiFile">Fileinfo object</param>
        /// <returns>String representation of SHA1 hash</returns>
        private string GenerateHashFromFile(FileInfo fiFile)
        {
            // Generates hash code for specified input file

            //Holds hash value returned from hash generator
            var HashGen = new SHA1CryptoServiceProvider();

            m_ErrMsg = string.Empty;

            FileStream FStream = null;

            try
            {
                //Open the file as a stream for input to the hash class
                FStream = fiFile.OpenRead();
                //Get the file's hash
                var ByteHash = HashGen.ComputeHash(FStream);
                return BitConverter.ToString(ByteHash).Replace("-", string.Empty).ToLower();
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Exception generating hash for file " + fiFile.FullName + ": " + ex.Message;
                return string.Empty;
            }
            finally
            {
                if ((FStream != null))
                {
                    FStream.Close();
                }
            }
        }

        /// <summary>
        /// Write an error message to the log
        /// If msg is blank, then logs the current task description followed by "empty error message"
        /// </summary>
        /// <param name="msg">Error message</param>
        /// <param name="currentTask">Current task</param>
        protected void LogErrorMessage(string msg, string currentTask)
        {
            LogErrorMessage(msg, currentTask, false);
        }

        /// <summary>
        /// Write an error message to the log
        /// If msg is blank, then logs the current task description followed by "empty error message"
        /// </summary>
        /// <param name="msg">Error message</param>
        /// <param name="currentTask">Current task</param>
        /// <param name="logDB">True to log to the database in addition to logging to the local log file</param>
        protected void LogErrorMessage(string msg, string currentTask, bool logDB)
        {
            if (string.IsNullOrWhiteSpace(msg))
                msg = currentTask + ": empty error message";

            if (logDB)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
            else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }

        #endregion


    }	// End class
}	// End namespace
