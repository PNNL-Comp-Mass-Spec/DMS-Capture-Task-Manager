
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//
// Last modified 11/17/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;

namespace SrcFileRenamePlugin
{
	class clsRenameOps
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
			protected bool m_UseBioNet = false;
			protected string m_UserName = "";
			protected string m_Pwd = "";
			protected ShareConnector m_ShareConnector;
			protected bool m_Connected = false;
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="MgrParams">Parameters for manager operation</param>
			/// <param name="UseBioNet">Flag to indicate if source instrument is on Bionet</param>
			public clsRenameOps(IMgrParams mgrParams, bool useBioNet)
			{
   			 m_MgrParams = mgrParams;
			    
   			 //Setup for BioNet use, if applicable
   			 m_UseBioNet = useBioNet;
   			 if (m_UseBioNet) 
				 {
       			  m_UserName = m_MgrParams.GetParam("bionetuser");
       			  m_Pwd = m_MgrParams.GetParam("bionetpwd");
   			 }
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Perform a single rename operation
			/// </summary>
			/// <param name="taskParams">Enum indicating status of task</param>
			/// <returns></returns>
			public EnumCloseOutType DoOperation(ITaskParams taskParams)
			{
                const int CHECK_LOOP_COUNT = 4;

				string dataset = taskParams.GetParam("Dataset");
				string sourceVol = taskParams.GetParam("Source_Vol");
				string sourcePath = taskParams.GetParam("Source_Path");
				string pwd = DecodePassword(m_Pwd);
				string msg;

				msg = "Started clsRenameeOps.DoOperation()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Set up paths
				string sourceFolderPath;	// Instrument transfer directory

				// Determine if source dataset exists, and if it is a file or a folder
				sourceFolderPath = Path.Combine(sourceVol, sourcePath);
				// Connect to Bionet if necessary
				if (m_UseBioNet)
				{
					msg = "Bionet connection required";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

					m_ShareConnector = new ShareConnector(m_UserName, pwd);
					m_ShareConnector.Share = sourceFolderPath;
					if (m_ShareConnector.Connect())
					{
						msg = "Connected to Bionet";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						m_Connected = true;
					}
					else
					{
                        msg = "Error " + m_ShareConnector.ErrorMessage + " connecting to " + sourceFolderPath + " as user " + m_UserName + " using 'secfso'";

                        if (m_ShareConnector.ErrorMessage == "1326")
                            msg += "; you likely need to change the Capture_Method from secfso to fso";
                        if (m_ShareConnector.ErrorMessage == "53")
                            msg += "; the password may need to be reset";
                            
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}
				else
				{
					msg = "Bionet connection not required";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}

                bool fileFound = false;
                bool folderFound = false;

                for (int iCheckLoop = 0; iCheckLoop < CHECK_LOOP_COUNT; iCheckLoop++)
                {
                    string sDatasetNameBase;
                    bool bAlreadyRenamed = false;

                    switch (iCheckLoop)
                    {
                        case 0:
                            sDatasetNameBase = String.Copy(dataset);
                            break;
                        case 1:
                            sDatasetNameBase = String.Copy("x_" + dataset);
                            bAlreadyRenamed = true;
                            break;
                        case 2:
                            sDatasetNameBase = String.Copy(dataset + "-bad");
                            break;
                        case 3:
                            sDatasetNameBase = String.Copy("x_" + dataset + "-bad");
                            bAlreadyRenamed = true;
                            break;
                        default:
                            sDatasetNameBase = string.Empty;
                            bAlreadyRenamed = false;
                            break;
                    }

                    if (!String.IsNullOrEmpty(sDatasetNameBase))
                    {
                        // Get a list of files containing the dataset name
                        string[] fileArray = GetMatchingFileNames(sourceFolderPath, sDatasetNameBase);
                        if (fileArray != null && fileArray.Length > 0)
                            fileFound = true;

                        // Get a list of folders containing the dataset name
                        string[] folderArray = GetMatchingFolderNames(sourceFolderPath, sDatasetNameBase);
                        if (folderArray != null && folderArray.Length > 0)
                            folderFound = true;
                  
                        // If no files or folders found, return error
                        if (!(fileFound || folderFound))
                        {
                            // No file or folder found
                            // Log a message, but continue on to the next iteration of the for loop
                            msg = "Dataset " + dataset + ": data file and/or folder not found using " + sDatasetNameBase + ".*";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                        }
                        else
                        {
                            if (bAlreadyRenamed)
                            {
                                msg = "Skipping dataset " + dataset + " since data file and/or folder already renamed to " + sDatasetNameBase;
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                            }
                            else
                            {
                                // Rename any files found
                                if (fileFound)
                                {
                                    foreach (string filePath in fileArray)
                                    {
                                        if (!RenameInstFile(filePath))
                                        {
                                            // Problem was logged by RenameInstFile
                                            if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
                                            return EnumCloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }

                                // Rename any folders found
                                if (folderFound)
                                {
                                    foreach (string folderPath in folderArray)
                                    {
                                        if (!RenameInstFolder(folderPath))
                                        {
                                            // Problem was logged by RenameInstFolder
                                            if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
                                            return EnumCloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }
                            }

                            // Success; break out of the for loop
                            break;
                        }
                    }
                }

                // Close connection, if open
                if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);

                if (!(fileFound || folderFound))
                {
                    msg = "Dataset " + dataset + ": data file and/or folder not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
                else
                    // Report success and exit
                    return EnumCloseOutType.CLOSEOUT_SUCCESS;

			}	// End sub

			/// <summary>
			/// Prefixes specified folder name with "x_"
			/// </summary>
			/// <param name="DSPath">Full path specifying folder to be renamed</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool RenameInstFolder(string InstFolderPath)
			{
				//Rename dataset folder on instrument
				try
				{
					DirectoryInfo di = new DirectoryInfo(InstFolderPath);
					string n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
					di.MoveTo(n);
					m_Msg = "Renamed directory " + InstFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
					return true;
				}
				catch (Exception ex)
				{
					m_Msg = "Error renaming directory " + InstFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Prefixes specified file name with "x_"
			/// </summary>
			/// <param name="DSPath">Full path specifying file to be renamed</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool RenameInstFile(string InstFilePath)
			{
				//Rename dataset folder on instrument
				try
				{
					FileInfo fi = new FileInfo(InstFilePath);
					string n = Path.Combine(fi.DirectoryName, "x_" + fi.Name);
					fi.MoveTo(n);
					m_Msg = "Renamed file " + InstFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
					return true;
				}
				catch (Exception ex)
				{
					m_Msg = "Error renaming file " + InstFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Decrypts password received from ini file
			/// </summary>
			/// <param name="EnPwd">Encoded password</param>
			/// <returns>Clear text password</returns>
			private string DecodePassword(string enPwd)
			{
				// Decrypts password received from ini file
				// Password was created by alternately subtracting or adding 1 to the ASCII value of each character

				// Convert the password string to a character array
				char[] pwdChars = enPwd.ToCharArray();
				byte[] pwdBytes = new byte[pwdChars.Length];
				char[] pwdCharsAdj = new char[pwdChars.Length];

				for (int i = 0; i < pwdChars.Length; i++)
				{
					pwdBytes[i] = (byte)pwdChars[i];
				}

				// Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string
				string retStr = "";
				for (int byteCntr = 0; byteCntr < pwdBytes.Length; byteCntr++)
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
			}	// End sub

			/// <summary>
			/// Determines if raw dataset exists as a file or folder
			/// </summary>
			/// <param name="InstFolder">Full path to instrument transfer folder</param>
			/// <param name="DSName">Dataset name</param>
			/// <param name="MyName">Return value for full name of file or folder found, if any</param>
			/// <returns>Enum specifying type of file/folder found, if any</returns>
			private RawDSTypes GetRawDSType(string InstFolder, string DSName, ref string MyName)
			{
				//Determines if raw dataset exists as a single file, folder with same name as dataset, or 
				//	folder with dataset name + extension. Returns enum specifying what was found and MyName
				// containing full name of file or folder

				string[] MyInfo = null;

				//Verify instrument transfer folder exists
				if (!Directory.Exists(InstFolder))
				{
					MyName = "";
					return RawDSTypes.None;
				}

				//Check for a file with specified name
				MyInfo = Directory.GetFiles(InstFolder);
				foreach (string TestFile in MyInfo)
				{
					if (Path.GetFileNameWithoutExtension(TestFile).ToLower() == DSName.ToLower())
					{
						MyName = Path.GetFileName(TestFile);
						return RawDSTypes.File;
					}
				}

				//Check for a folder with specified name
				MyInfo = Directory.GetDirectories(InstFolder);
				foreach (string TestFolder in MyInfo)
				{
					//Using Path.GetFileNameWithoutExtension on folders is cheezy, but it works. I did this
					//	because the Path class methods that deal with directories ignore the possibilty there
					//	might be an extension. Apparently when sending in a string, Path can't tell a file from
					//	a directory
					if (Path.GetFileNameWithoutExtension(TestFolder).ToLower() == DSName.ToLower())
					{
						if (string.IsNullOrEmpty(Path.GetExtension(TestFolder)))
						{
							//Found a directory that has no extension
							MyName = Path.GetFileName(TestFolder);
							return RawDSTypes.FolderNoExt;
						}
						else
						{
							//Directory name has an extension
							MyName = Path.GetFileName(TestFolder);
							return RawDSTypes.FolderExt;
						}
					}
				}

				//If we got to here, then the raw dataset wasn't found, so there was a problem
				MyName = "";

				return RawDSTypes.None;
			}	// End sub

			/// <summary>
			/// Disconnects a Bionet shared drive
			/// </summary>
			/// <param name="MyConn">Connection object for shared drive</param>
			/// <param name="ConnState">Return value specifying connection has been closed</param>
			private void DisconnectShare(ref ShareConnector MyConn, ref bool ConnState)
			{
				//Disconnects a shared drive
				MyConn.Disconnect();
				m_Msg = "Bionet disconnected";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				ConnState = false;
			}	// End sub

			/// <summary>
			/// Gets a list of files containing the dataset name
			/// </summary>
			/// <param name="instFolder">Folder to search</param>
			/// <param name="dsName">Dataset name to match</param>
			/// <returns>Array of file paths</returns>
			private string[] GetMatchingFileNames(string instFolder, string dsName)
			{
				return Directory.GetFiles(instFolder, dsName + ".*");
			}	// End sub

			/// <summary>
			/// Gets a list of folders containing the dataset name
			/// </summary>
			/// <param name="instFolder">Folder to search</param>
			/// <param name="dsName">Dataset name to match</param>
			/// <returns>Array of folder paths</returns>
			private string[] GetMatchingFolderNames(string instFolder, string dsName)
			{
				return Directory.GetDirectories(instFolder, dsName + ".*");
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
