
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
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
		public clsRenameOps(IMgrParams mgrParams, bool useBioNet)
		{
			m_MgrParams = mgrParams;

			//Setup for BioNet use, if applicable
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
		/// <returns></returns>
		public EnumCloseOutType DoOperation(ITaskParams taskParams)
		{
			string dataset = taskParams.GetParam("Dataset");
			string sourceVol = taskParams.GetParam("Source_Vol");
			string sourcePath = taskParams.GetParam("Source_Path");
			string pwd = DecodePassword(m_Pwd);

		    string msg = "Started clsRenameeOps.DoOperation()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Set up paths

		    // Determine if source dataset exists, and if it is a file or a folder
			string sourceFolderPath = Path.Combine(sourceVol, sourcePath);

			// Connect to Bionet if necessary
			if (m_UseBioNet)
			{
				msg = "Bionet connection required";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				m_ShareConnector = new ShareConnector(m_UserName, pwd)
				{
				    Share = sourceFolderPath
				};

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

            // Construct list of dataset names to check for
            // The first thing we check for is the official dataset name
            // We next check for various things that operators rename the datasets to
			var lstFileNamesToCheck = new List<string>
			{
			    dataset,
			    dataset + "-bad",
			    dataset + "-badQC",
			    dataset + "-plug",
			    dataset + "-plugsplit",
			    dataset + "-mixer",
			    dataset + "-rotor",
			    dataset + "-slowsplit",
			    dataset + "-corrupt",
			    dataset + "-corrupted",
			    "x_" + dataset,
			    "x_" + dataset + "-bad"
			};

			// Construct list of dataset names to check for
			// The first thing we check for is the official dataset name
			// We next check for various things that operators rename the datasets to

		    // Append x_ versions of some of these entries

		    // Could use the following to append x_ for all entries:
			//System.Collections.Generic.List<string> lstFileNamesAlreadyRenamed = new List<string>();
			//foreach (string sDatasetNameBase in lstFileNamesToCheck) 
			//{
			//    if (!sDatasetNameBase.StartsWith("x_"))
			//        lstFileNamesAlreadyRenamed.Add("x_" + sDatasetNameBase);
			//}

			//// Append entries from lstFileNamesAlreadyRenamed to lstFileNamesToCheck
			//lstFileNamesToCheck.AddRange(lstFileNamesAlreadyRenamed);

			bool fileFound = false;
			bool folderFound = false;
			bool bLoggedDatasetNotFound = false;

			foreach (string sDatasetNameBase in lstFileNamesToCheck)
			{
				if (!String.IsNullOrEmpty(sDatasetNameBase))
				{
					bool bAlreadyRenamed;
					if (!dataset.StartsWith("x_") && sDatasetNameBase.StartsWith("x_"))
						bAlreadyRenamed = true;
					else
						bAlreadyRenamed = false;

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
						// Log a message for the first item checked in lstFileNamesToCheck
						if (!bLoggedDatasetNotFound)
						{
							msg = "Dataset " + dataset + ": data file and/or folder not found using " + sDatasetNameBase + ".*";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
							bLoggedDatasetNotFound = true;
						}
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
										if (m_Connected) 
                                            DisconnectShare(ref m_ShareConnector, out m_Connected);

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
										if (m_Connected) 
                                            DisconnectShare(ref m_ShareConnector, out m_Connected);

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
			if (m_Connected) 
                DisconnectShare(ref m_ShareConnector, out m_Connected);

			if (!(fileFound || folderFound))
			{
				msg = "Dataset " + dataset + ": data file and/or folder not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
		
            return EnumCloseOutType.CLOSEOUT_SUCCESS;
		}	// End sub

		/// <summary>
		/// Prefixes specified folder name with "x_"
		/// </summary>
        /// <param name="instFolderPath">Full path specifying folder to be renamed</param>
		/// <returns>TRUE for success, FALSE for failure</returns>
		private bool RenameInstFolder(string instFolderPath)
		{
			//Rename dataset folder on instrument
			try
			{
				var di = new DirectoryInfo(instFolderPath);
				string n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
				di.MoveTo(n);
				m_Msg = "Renamed directory " + instFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
				return true;
			}
			catch (Exception ex)
			{
				m_Msg = "Error renaming directory " + instFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
				return false;
			}
		}

		/// <summary>
		/// Prefixes specified file name with "x_"
		/// </summary>
        /// <param name="instFilePath">Full path specifying file to be renamed</param>
		/// <returns>TRUE for success, FALSE for failure</returns>
		private bool RenameInstFile(string instFilePath)
		{
			//Rename dataset folder on instrument
			try
			{
				var fi = new FileInfo(instFilePath);
				string n = Path.Combine(fi.DirectoryName, "x_" + fi.Name);
				fi.MoveTo(n);
				m_Msg = "Renamed file " + instFilePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
				return true;
			}
			catch (Exception ex)
			{
				m_Msg = "Error renaming file " + instFilePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
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
			char[] pwdChars = enPwd.ToCharArray();
			var pwdBytes = new byte[pwdChars.Length];
			var pwdCharsAdj = new char[pwdChars.Length];

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
		}
		

		/// <summary>
		/// Disconnects a Bionet shared drive
		/// </summary>
		/// <param name="MyConn">Connection object for shared drive</param>
		/// <param name="ConnState">Return value specifying connection has been closed</param>
		private void DisconnectShare(ref ShareConnector MyConn, out bool ConnState)
		{
			//Disconnects a shared drive
			MyConn.Disconnect();
			m_Msg = "Bionet disconnected";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
			ConnState = false;
		}

		/// <summary>
		/// Gets a list of files containing the dataset name
		/// </summary>
		/// <param name="instFolder">Folder to search</param>
		/// <param name="dsName">Dataset name to match</param>
		/// <returns>Array of file paths</returns>
		private string[] GetMatchingFileNames(string instFolder, string dsName)
		{
			return Directory.GetFiles(instFolder, dsName + ".*");
		}

		/// <summary>
		/// Gets a list of folders containing the dataset name
		/// </summary>
		/// <param name="instFolder">Folder to search</param>
		/// <param name="dsName">Dataset name to match</param>
		/// <returns>Array of folder paths</returns>
		private string[] GetMatchingFolderNames(string instFolder, string dsName)
		{
			return Directory.GetDirectories(instFolder, dsName + ".*");
		}

		#endregion
	}	// End class
}	// End namespace
