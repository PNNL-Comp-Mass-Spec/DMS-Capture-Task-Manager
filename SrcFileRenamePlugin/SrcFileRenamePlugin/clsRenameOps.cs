
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using CaptureTaskManager;
using System.IO;
using System.Linq;
using PRISM;

namespace SrcFileRenamePlugin
{
    class clsRenameOps : clsEventNotifier
    {
        //*********************************************************************************************************
        // Class for performing rename operations
        //**********************************************************************************************************

        #region "Enums"
        protected enum RawDSTypes
        {
            None,
            File,
            FolderNoExt,
            FolderExt
        }
        #endregion

        #region "Class variables"
        protected IMgrParams m_MgrParams;
        protected string m_Msg = "";
        protected bool m_UseBioNet;
        protected string m_UserName = "";
        protected string m_Pwd = "";
        protected ShareConnector m_ShareConnector;
        protected bool m_Connected;
        #endregion

        #region "Constructors"
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
        public clsRenameOps(IMgrParams mgrParams, bool useBioNet)
        {
            m_MgrParams = mgrParams;

            // Setup for BioNet use, if applicable
            m_UseBioNet = useBioNet;
            if (m_UseBioNet)
            {
                m_UserName = m_MgrParams.GetParam("bionetuser");
                m_Pwd = m_MgrParams.GetParam("bionetpwd");

                if (!m_UserName.Contains(@"\"))
                {
                    // Prepend this computer's name to the username
                    m_UserName = Environment.MachineName + @"\" + m_UserName;
                }
            }
        }
        #endregion

        #region "Methods"

        /// <summary>
        /// Perform a single rename operation
        /// </summary>
        /// <param name="taskParams">Enum indicating status of task</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns></returns>
        public EnumCloseOutType DoOperation(ITaskParams taskParams, out string errorMessage)
        {
            var datasetName = taskParams.GetParam("Dataset");
            // Example: \\exact04.bionet\
            var sourceVol = taskParams.GetParam("Source_Vol");

            // Example: ProteomicsData\
            var sourcePath = taskParams.GetParam("Source_Path");

            // Typically an empty string, but could be a partial path like: "CapDev" or "Smith\2014"
            var captureSubfolder = taskParams.GetParam("Capture_Subfolder");		

            var pwd = DecodePassword(m_Pwd);

            var msg = "Started clsRenameOps.DoOperation()";
            OnDebugEvent(msg);

            errorMessage = string.Empty;

            // Set up paths

            // Determine if source dataset exists, and if it is a file or a folder
            var sourceFolderPath = Path.Combine(sourceVol, sourcePath);

            // Connect to Bionet if necessary
            if (m_UseBioNet)
            {
                msg = "Bionet connection required for " + sourceVol;
                OnDebugEvent(msg);

                m_ShareConnector = new ShareConnector(m_UserName, pwd)
                {
                    Share = sourceFolderPath
                };

                if (m_ShareConnector.Connect())
                {
                    msg = "Connected to Bionet";
                    OnDebugEvent(msg);
                    m_Connected = true;
                }
                else
                {
                    msg = "Error " + m_ShareConnector.ErrorMessage + " connecting to " + sourceFolderPath + " as user " + m_UserName + " using 'secfso'";

                    if (m_ShareConnector.ErrorMessage == "1326")
                        msg += "; you likely need to change the Capture_Method from secfso to fso";
                    if (m_ShareConnector.ErrorMessage == "53")
                        msg += "; the password may need to be reset";

                    OnErrorEvent(msg);

                    errorMessage = "Error connecting to " + sourceFolderPath + " as user " + m_UserName + " using 'secfso'";
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                msg = "Bionet connection not required for " + sourceVol;
                OnDebugEvent(msg);
            }

            // Now that we've had a chance to connect to the share, possibly append a subfolder to the source path
            if (!string.IsNullOrWhiteSpace(captureSubfolder))
            {

                // However, if the subfolder name matches the dataset name, this was probably an error on the operator's part and we likely do not want to use the subfolder name
                if (captureSubfolder.EndsWith(Path.DirectorySeparatorChar + datasetName, StringComparison.InvariantCultureIgnoreCase))
                {
                    var candidateFolderPath = Path.Combine(sourceFolderPath, captureSubfolder);

                    if (Directory.Exists(candidateFolderPath))
                    {
                        OnWarningEvent("Dataset captureSubfolder ends with the dataset name. Gracefully ignoring because this appears to be a data entry error: " +
                           datasetName);
                        sourceFolderPath = candidateFolderPath.Substring(0, candidateFolderPath.Length - datasetName.Length - 1);
                    }
                    else
                    {
                        sourceFolderPath = candidateFolderPath;
                    }

                }
                else
                {
                    sourceFolderPath = Path.Combine(sourceFolderPath, captureSubfolder);
                }
                                
            }

            var diSourceFolder = new DirectoryInfo(sourceFolderPath);
            int countRenamed;

            if (diSourceFolder.Exists)
            {
                countRenamed = FindFilesToRename(datasetName, diSourceFolder, out errorMessage);
            }
            else
            {
                msg = "Instrument folder not found for dataset " + datasetName + ": " + sourceFolderPath;
                OnErrorEvent(msg);

                errorMessage = "Remote folder not found: " + sourceFolderPath;
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Close connection, if open
            if (m_Connected)
                DisconnectShare(ref m_ShareConnector, out m_Connected);

            if (countRenamed == 0)
            {
                if (string.IsNullOrEmpty(errorMessage))
                    errorMessage = "Data file and/or folder not found on the instrument; cannot rename";

                msg = "Dataset " + datasetName + ":" + errorMessage;
                OnErrorEvent(msg);

                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            return EnumCloseOutType.CLOSEOUT_SUCCESS;
        }

        private int FindFilesToRename(string dataset, DirectoryInfo diSourceFolder, out string errorMessage)
        {
            errorMessage = string.Empty;

            // Construct a list of dataset names to check for
            // The first thing we check for is the official dataset name
            // We next check for various things that operators rename the datasets to
            var lstFileNamesToCheck = new List<string>
            {
                dataset,
                dataset + "-bad",
                dataset + "_bad",
                dataset + "-corrupt",
                dataset + "_corrupt",
                dataset + "-corrupted",
                dataset + "_corrupted",
                dataset + "-chromooff",
                dataset + "-flatline",
                dataset + "-LCFroze",
                dataset + "-mixer",
                dataset + "-NoN2",
                dataset + "-plugged",
                dataset + "-pluggedSPE",
                dataset + "-plunger",
                dataset + "-pumpOFF",
                dataset + "-slow",
                dataset + "-wrongLCmethod",
                dataset + "-air",
                dataset + "-badQC",
                dataset + "-corrupt",
                dataset + "-corrupted",
                dataset + "-plug",
                dataset + "-plugsplit",
                dataset + "-rotor",
                dataset + "-slowsplit",
                "x_" + dataset,
                "x_" + dataset + "-bad"
            };

            var bLoggedDatasetNotFound = false;
            var countRenamed = 0;

            foreach (var sDatasetNameBase in lstFileNamesToCheck)
            {
                if (string.IsNullOrEmpty(sDatasetNameBase))
                {
                    continue;
                }

                bool bAlreadyRenamed;
                if (!dataset.StartsWith("x_") && sDatasetNameBase.StartsWith("x_"))
                    bAlreadyRenamed = true;
                else
                    bAlreadyRenamed = false;

                // Get a list of files containing the dataset name
                var matchedFiles = GetMatchingFileNames(diSourceFolder, sDatasetNameBase);

                // Get a list of folders containing the dataset name
                var matchedFolders = GetMatchingFolderNames(diSourceFolder, sDatasetNameBase);

                // If no files or folders found, return error
                if (matchedFiles.Count + matchedFolders.Count == 0)
                {
                    // No file or folder found
                    // Log a message for the first item checked in lstFileNamesToCheck
                    if (!bLoggedDatasetNotFound)
                    {
                        var msg = "Dataset " + dataset + ": data file and/or folder not found using " + sDatasetNameBase + ".*";
                        OnWarningEvent(msg);
                        bLoggedDatasetNotFound = true;
                    }
                    continue;
                }

                if (bAlreadyRenamed)
                {
                    var msg = "Skipping dataset " + dataset + " since data file and/or folder already renamed to " + sDatasetNameBase;
                    OnStatusEvent(msg);
                    countRenamed++;
                    break;
                }

                // Rename any files found
                foreach (var fileToRename in matchedFiles)
                {
                    if (RenameInstFile(fileToRename, out errorMessage))
                    {
                        countRenamed++;
                        continue;
                    }

                    return 0;
                }

                // Rename any folders found
                foreach (var folderToRename in matchedFolders)
                {
                    if (RenameInstFolder(folderToRename, out errorMessage))
                    {
                        countRenamed++;
                        continue;
                    }
                    return 0;
                }

                // Success; break out of the for loop
                break;
            }

            return countRenamed;
        }

        /// <summary>
        /// Prefixes specified file name with "x_"
        /// </summary>
        /// <param name="fiFile">File to be renamed</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameInstFile(FileInfo fiFile, out string errorMessage)
        {
            // Rename dataset file on instrument
            var newPath = "??";
            errorMessage = string.Empty;

            try
            {
                if (!fiFile.Exists || fiFile.DirectoryName == null)
                    return true;

                newPath = Path.Combine(fiFile.DirectoryName, "x_" + fiFile.Name);
                fiFile.MoveTo(newPath);
                m_Msg = "Renamed file to " + fiFile.FullName;
                OnStatusEvent(m_Msg);
                return true;
            }
            catch (Exception ex)
            {
                m_Msg = "Error renaming file '" + fiFile.FullName + "' to '" + newPath + "'";
                OnErrorEvent(m_Msg, ex);

                if (ex.Message.Contains("Access to the path") && ex.Message.Contains("is denied"))
                    errorMessage = "Error renaming file: access is denied";
                else
                    errorMessage = "Error renaming file: " + ex.GetType();

                return false;
            }
        }

        /// <summary>
        /// Prefixes specified folder name with "x_"
        /// </summary>
        /// <param name="diFolder">Folder to be renamed</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameInstFolder(DirectoryInfo diFolder, out string errorMessage)
        {
            // Rename dataset folder on instrument
            var newPath = "??";
            errorMessage = string.Empty;

            try
            {
                if (!diFolder.Exists || diFolder.Parent == null)
                    return true;

                newPath = Path.Combine(diFolder.Parent.FullName, "x_" + diFolder.Name);
                diFolder.MoveTo(newPath);
                m_Msg = "Renamed directory to " + diFolder.FullName;
                OnStatusEvent(m_Msg);
                return true;
            }
            catch (Exception ex)
            {
                m_Msg = "Error renaming directory '" + diFolder.FullName + "' to '" + newPath + "'";
                OnErrorEvent(m_Msg, ex);

                if (ex.Message.Contains("Access to the path") && ex.Message.Contains("is denied"))
                    errorMessage = "Error renaming directory: access is denied";
                else
                    errorMessage = "Error renaming directory: " + ex.GetType();

                return false;
            }
        }

        /// <summary>
        /// Decrypts password received from ini file
        /// </summary>
        /// <param name="enPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        private string DecodePassword(string enPwd)
        {
            // Decrypts password received from ini file
            // Password was created by alternately subtracting or adding 1 to the ASCII value of each character

            // Convert the password string to a character array
            var pwdChars = enPwd.ToCharArray();
            var pwdBytes = new byte[pwdChars.Length];
            var pwdCharsAdj = new char[pwdChars.Length];

            for (var i = 0; i < pwdChars.Length; i++)
            {
                pwdBytes[i] = (byte)pwdChars[i];
            }

            // Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string
            var retStr = "";
            for (var byteCntr = 0; byteCntr < pwdBytes.Length; byteCntr++)
            {
                if ((byteCntr % 2) == 0)
                {
                    pwdBytes[byteCntr] += 1;
                }
                else
                {
                    pwdBytes[byteCntr] -= 1;
                }
                pwdCharsAdj[byteCntr] = (char)pwdBytes[byteCntr];
                retStr += pwdCharsAdj[byteCntr].ToString();
            }
            return retStr;
        }


        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="MyConn">Connection object for shared drive</param>
        /// <param name="ConnState">Return value specifying connection has been closed</param>
        private void DisconnectShare(ref ShareConnector MyConn, out bool ConnState)
        {
            // Disconnects a shared drive
            MyConn.Disconnect();
            m_Msg = "Bionet disconnected";
            OnDebugEvent(m_Msg);
            ConnState = false;
        }

        /// <summary>
        /// Gets a list of files containing the dataset name
        /// </summary>
        /// <param name="diSourceFolder">Folder to search</param>
        /// <param name="dsName">Dataset name to match</param>
        /// <returns>Array of file paths</returns>
        private List<FileInfo> GetMatchingFileNames(DirectoryInfo diSourceFolder, string dsName)
        {
            return diSourceFolder.GetFiles(dsName + ".*").ToList();
        }

        /// <summary>
        /// Gets a list of folders containing the dataset name
        /// </summary>
        /// <param name="diSourceFolder">Folder to search</param>
        /// <param name="dsName">Dataset name to match</param>
        /// <returns>Array of folder paths</returns>
        private List<DirectoryInfo> GetMatchingFolderNames(DirectoryInfo diSourceFolder, string dsName)
        {
            return diSourceFolder.GetDirectories(dsName + ".*").ToList();
        }

        #endregion
    }
}
