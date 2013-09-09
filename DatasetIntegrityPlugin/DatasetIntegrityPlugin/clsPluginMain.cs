
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//
// Last modified 10/02/2009
//						07/09/2010 (DAC) - Added new definition for BrukerFT_BAF instrument class
//						11/17/2010 (DAC) - Added new tests for MALDI imaging and spot instrument classes
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CaptureTaskManager;

namespace DatasetIntegrityPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants"
		const float RAW_FILE_MIN_SIZE_KB = 100;
		const float RAW_FILE_MAX_SIZE_MB = 2048;
		const float BAF_FILE_MIN_SIZE_KB = 16;
		const float SER_FILE_MIN_SIZE_KB = 16;
		const float FID_FILE_MIN_SIZE_KB = 16;
		const float ACQ_METHOD_FILE_MIN_SIZE_KB = 5;
		const float SCIEX_WIFF_FILE_MIN_SIZE_KB = 50;
		const float SCIEX_WIFF_SCAN_FILE_MIN_SIZE_KB = 0.03F;
		const float UIMF_FILE_MIN_SIZE_KB = 50;
		const float AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB = 50;
		const float AGILENT_DATA_MS_FILE_MIN_SIZE_KB = 75;
		const float MCF_FILE_MIN_SIZE_KB = 15;		// Malding imaging file

		int MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES = 30;

		#endregion

		#region "Class-wide variables"
		
		protected clsToolReturnData mRetData = new clsToolReturnData();
		protected DateTime mAgilentToUIMFStartTime;
		protected DateTime mLastStatusUpdate;

		#endregion

		#region "Constructors"
		public clsPluginMain()
			: base()
		{
			// Does nothing at present
		}	// End sub
		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the dataset integrity step tool
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;

			msg = "Starting DatasetIntegrityPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				mRetData.CloseoutMsg = "Error determining tool version info";
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return mRetData;
			}

			msg = "Performing integrity test, dataset '" + m_Dataset + "'";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

			// Set up the file paths
			string storageVolExt = m_TaskParams.GetParam("Storage_Vol_External");
			string storagePath = m_TaskParams.GetParam("Storage_Path");
			string datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, m_Dataset));
			string dataFileNamePath;

			// Select which tests will be performed based on instrument class
			string instClassName = m_TaskParams.GetParam("Instrument_Class");
			string instrumentName = m_TaskParams.GetParam("Instrument_Name");

			msg = "Instrument class: " + instClassName;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			clsInstrumentClassInfo.eInstrumentClass instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);
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
					if (instrumentName.StartsWith("IMS08"))
					{
						// Need to first convert the .d folder to a .UIMF file
						if (!ConvertAgilentDFolderToUIMF(datasetFolder))
						{
							if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
							{
								mRetData.CloseoutMsg = "Unknown error converting the Agilent .D to folder to a .UIMF file";
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
							}

							mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						}
					}

					dataFileNamePath = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION);
					mRetData.CloseoutType = TestIMSAgilentTOF(dataFileNamePath);
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
						clsToolReturnData oRetDataAlt = new clsToolReturnData();
						oRetDataAlt.CloseoutType = TestBrukerMaldiImagingFolder(datasetFolder);
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
					mRetData.CloseoutType = TestAgilentIonTrapFolder(datasetFolder);
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

		private bool ConvertAgilentDFolderToUIMF(string datasetFolderPath)
		{
			clsRunDosProgram CmdRunner = null;
			bool success = false;

			try
			{
				// Construct the command line arguments to run the AgilentToUIMFConverter
				string dotDFolderPath = Path.Combine(datasetFolderPath, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION);
				string uimfOutputFilePath = Path.Combine(m_WorkDir, m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION);

				// Syntax:
				// AgilentToUIMFConverter.exe [Agilent .d Folder] [Directory to insert file (optional)]
				//
				string CmdStr = clsConversion.PossiblyQuotePath(dotDFolderPath) + " " + clsConversion.PossiblyQuotePath(m_WorkDir);
				string managerName = m_MgrParams.GetParam("MgrName", "UnknownManager");
				string consoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, "AgilentToUIMF_ConsoleOutput_" + managerName + ".txt");

				CmdRunner = new clsRunDosProgram(m_WorkDir);
				mAgilentToUIMFStartTime = System.DateTime.UtcNow;
				mLastStatusUpdate = System.DateTime.UtcNow;

				AttachCmdrunnerEvents(CmdRunner);

				string exePath = m_MgrParams.GetParam("AgilentToUIMFProgLoc");
				exePath = Path.Combine(exePath, "AgilentToUimfConverter.exe");

				if (!File.Exists(exePath))
				{
					mRetData.CloseoutMsg = "AgilentToUIMFConverter not found at " + exePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					return false;
				}

				CmdRunner.CreateNoWindow = false;
				CmdRunner.EchoOutputToConsole = false;
				CmdRunner.CacheStandardOutput = false;
				CmdRunner.WriteConsoleOutputToFile = true;
				CmdRunner.ConsoleOutputFilePath = consoleOutputFilePath;

				int iMaxRuntimeSeconds = MAX_AGILENT_TO_UIMF_RUNTIME_MINUTES * 60;
				bool bSuccess = CmdRunner.RunProgram(exePath, CmdStr, "AgilentToUIMFConverter", true, iMaxRuntimeSeconds);

				ParseConsoleOutputFileForErrors(consoleOutputFilePath);

				if (!bSuccess)
				{
					mRetData.CloseoutMsg = "Error running the AgilentToUIMFConverter";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);

					if (CmdRunner.ExitCode != 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "AgilentToUIMFConverter returned a non-zero exit code: " + CmdRunner.ExitCode.ToString());
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to AgilentToUIMFConverter failed (but exit code is 0)");
					}

					return false;
				}

				System.Threading.Thread.Sleep(100);
				
				// Copy the .UIMF file to the dataset folder
				var fiUIMF = new FileInfo(uimfOutputFilePath);
				if (!fiUIMF.Exists)
				{
					mRetData.CloseoutMsg = "AgilentToUIMFConverter did not create a .UIMF file";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + uimfOutputFilePath);
					return false;
				}

				if (m_DebugLevel >= 4)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying .UIMF file to the dataset folder");
				}

				string targetFilePath = Path.Combine(datasetFolderPath, fiUIMF.Name);
				fiUIMF.CopyTo(targetFilePath, true);

				if (m_DebugLevel >= 4)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copy complete");
				}

				try
				{
					// Delete the local copy
					fiUIMF.Delete();
				}
				catch (Exception ex)
				{
					// Do not treat this as a fatal error
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception deleting local copy of the new .UIMF file " + fiUIMF.FullName + ": " + ex.Message);
				}

				try
				{
					// Delete the console output file
					File.Delete(consoleOutputFilePath);
				}
				catch
				{
					// Do not treat this as a fatal error
				}

				success = true;
			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Exception converting .D folder to a UIMF file";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
				return false;
			}
			finally
			{
				DetachCmdrunnerEvents(CmdRunner);
			}

			return success;
		}

		/// <summary>
		/// Processes folders in folderList to compare the x_ folder to the non x_ folder
		/// If the x_ folder is empty or if every file in the x_ folder is also in the non x_ folder, then returns True and optionally deletes the x_ folder
		/// </summary>
		/// <param name="folderList">List of folders; must contain exactly 2 entries</param>
		/// <param name="sSupersededFolderPath"></param>
		/// <returns>True if this is a superseded folder and it is safe to delete</returns>
		private bool DetectSupersededFolder(string[] folderList, bool bDeleteIfSuperseded)
		{
			string msg;
			string sNewFolder = string.Empty;
			string sOldFolder = string.Empty;

			System.IO.DirectoryInfo diOldFolder;
			System.IO.DirectoryInfo diNewFolder;

			try
			{
				if (folderList.Length != 2)
				{
					msg = "folderList passed into DetectSupersededFolder does not contain 2 folders; cannot check for a superseded folder";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					return false;
				}

				diNewFolder = new System.IO.DirectoryInfo(folderList[0]);
				diOldFolder = new System.IO.DirectoryInfo(folderList[1]);

				if (diNewFolder.Name.ToLower().StartsWith("x_"))
				{
					// Swap things around
					diNewFolder = new System.IO.DirectoryInfo(folderList[1]);
					diOldFolder = new System.IO.DirectoryInfo(folderList[0]);
				}

				if (diOldFolder.Name == "x_" + diNewFolder.Name)
				{
					// Yes, we have a case of a likely superseded folder
					// Examine diOldFolder

					msg = "Comparing files in superseded folder (" + diOldFolder.FullName + ") to newer folder (" + diNewFolder.FullName + ")";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

					bool bFolderIsSuperseded = true;

					System.IO.FileInfo[] fiSupersededFiles;
					fiSupersededFiles = diOldFolder.GetFiles("*", SearchOption.AllDirectories);

					foreach (System.IO.FileInfo fiFile in fiSupersededFiles)
					{
						string sNewfilePath = fiFile.FullName.Replace(diOldFolder.FullName, diNewFolder.FullName);
						System.IO.FileInfo fiNewFile = new System.IO.FileInfo(sNewfilePath);

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
				else
				{

					msg = "Folder " + diOldFolder.FullName + " is not a superseded folder for " + diNewFolder.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					return false;
				}

			}
			catch (Exception ex)
			{
				msg = "Error in DetectSupersededFolder: " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}


		}

		/// <summary>
		/// Looks file files matching fileSpec
		/// For matching files, copies the or moves them up one folder
		/// If matchDatasetName is true then requires that the file start with the name of the dataset
		/// </summary>
		/// <param name="diDatasetFolder"></param>
		/// <param name="fileSpec">Files to match, for example *.mis</param>
		/// <param name="matchDatasetName">True if filenames must start with m_Dataset</param>
		/// <param name="copyFile">True to copy the file, false to move it</param>
		private void MoveOrCopyUpOneLevel(DirectoryInfo diDatasetFolder, string fileSpec, bool matchDatasetName, bool copyFile)
		{
			foreach (FileInfo fiFile in diDatasetFolder.GetFiles(fileSpec))
			{
				if (!matchDatasetName || Path.GetFileNameWithoutExtension(fiFile.Name).ToLower().StartsWith(m_Dataset.ToLower()))
				{
					string newPath = Path.Combine(fiFile.Directory.Parent.FullName, fiFile.Name);
					if (!File.Exists(newPath))
					{
						if (copyFile)
							fiFile.CopyTo(newPath, true);
						else
							fiFile.MoveTo(newPath);
					}
				}
			}
		}

		protected void ParseConsoleOutputFileForErrors(string sConsoleOutputFilePath)
		{
			string sLineIn = string.Empty;
			bool blnUnhandledException = false;
			string sExceptionText = string.Empty;

			try
			{
				if (System.IO.File.Exists(sConsoleOutputFilePath))
				{
					using (System.IO.StreamReader srInFile = new System.IO.StreamReader(new System.IO.FileStream(sConsoleOutputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite)))
					{

						while (srInFile.Peek() > -1)
						{
							sLineIn = srInFile.ReadLine();

							if (!string.IsNullOrEmpty(sLineIn))
							{
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
					}

					if (!string.IsNullOrEmpty(sExceptionText))
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, sExceptionText);
					}
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
		private bool PossiblyRenameSupersededFolder(string[] folderList, string folderDescription)
		{
			bool bInvalid = true;

			if (folderList.Length == 1)
				return true;

			if (folderList.Length == 2)
			{
				// If two folders are present and one starts with x_ and all of the files inside the one that start with x_ are also in the folder without x_,
				// then delete the x_ folder
				bool bDeleteIfSuperseded = true;

				if (DetectSupersededFolder(folderList, bDeleteIfSuperseded))
				{
					bInvalid = false;
				}
			}

			if (bInvalid)
			{
				mRetData.EvalMsg = "Invalid dataset. Multiple " + folderDescription + " folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return false;
			}
			else
			{
				return true;
			}
		}


		private bool PositiveNegativeMethodFolders(List<string> subFolderList)
		{
			if (subFolderList == null)
				return false;

			if (subFolderList.Count == 2)
			{
				if (subFolderList[0].IndexOf("_neg", 0, StringComparison.CurrentCultureIgnoreCase) >= 0 &&
					subFolderList[1].IndexOf("_pos", 0, StringComparison.CurrentCultureIgnoreCase) >= 0)
				{
					return true;
				}

				if (subFolderList[1].IndexOf("_neg", 0, StringComparison.CurrentCultureIgnoreCase) >= 0 &&
	                subFolderList[0].IndexOf("_pos", 0, StringComparison.CurrentCultureIgnoreCase) >= 0)
				{
					return true;
				}

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

			string msg;
			msg = sDataFileDescription + " file may be corrupt. Actual file size is " +
				  fActualSize.ToString("####0.0") + " KB; " +
				  "max allowable size is " + sMaxSize + "; see " + sFilePath;

			mRetData.EvalMsg = sDataFileDescription + " file size is more than " + sMaxSize;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
		}

		private void ReportFileSizeTooSmall(string sDataFileDescription, string sFilePath, float fActualSize, float fMinSize)
		{
			string sMinSize;
			sMinSize = fMinSize.ToString("#0") + " KB";

			string msg;
			msg = sDataFileDescription + " file may be corrupt. Actual file size is " +
				  fActualSize.ToString("####0.0") + " KB; " +
				  "min allowable size is " + sMinSize + "; see " + sFilePath;

			mRetData.EvalMsg = sDataFileDescription + " file size is less than " + sMinSize;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
		}

		/// <summary>
		/// Tests a Agilent_Ion_Trap folder for integrity
		/// </summary>
		/// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
		/// <returns>Enum indicating test result</returns>
		private EnumCloseOutType TestAgilentIonTrapFolder(string datasetFolderPath)
		{
			float dataFileSizeKB;

			string instName = m_TaskParams.GetParam("Instrument_Name");

			// Verify only one .D folder in dataset
			string[] folderList = Directory.GetDirectories(datasetFolderPath, "*.D");
			if (folderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .D folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (folderList.Length > 1)
			{
				if (!PossiblyRenameSupersededFolder(folderList, clsInstrumentClassInfo.DOT_D_EXTENSION))
					return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Look for Data.MS file in the .D folder
			string tempFileNamePath = Path.Combine(folderList[0], "DATA.MS");
			if (!File.Exists(tempFileNamePath))
			{
				mRetData.EvalMsg = "Invalid dataset. DATA.MS file not found in the .D folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify size of the DATA.MS file
			dataFileSizeKB = GetFileSize(tempFileNamePath);
			if (dataFileSizeKB <= AGILENT_DATA_MS_FILE_MIN_SIZE_KB)
			{
				ReportFileSizeTooSmall("DATA.MS", tempFileNamePath, dataFileSizeKB, AGILENT_DATA_MS_FILE_MIN_SIZE_KB);
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
			float dataFileSizeKB;

			string instName = m_TaskParams.GetParam("Instrument_Name");

			// Verify only one .D folder in dataset
			string[] folderList = Directory.GetDirectories(datasetFolderPath, "*.D");
			if (folderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .D folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (folderList.Length > 1)
			{				
				if (!PossiblyRenameSupersededFolder(folderList, clsInstrumentClassInfo.DOT_D_EXTENSION))
					return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Look for AcqData folder below .D folder
			string[] acqDataFolderList = Directory.GetDirectories(folderList[0], "AcqData");
			if (folderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. .D folder does not contain an AcqData subfolder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (folderList.Length > 1)
			{
				mRetData.EvalMsg = "Invalid dataset. Multiple AcqData folders found in .D folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// The AcqData folder should contain one or more .Bin files, for example MSScan.bin, MSPeak.bin, and MSProfile.bin
			// Verify that the MSScan.bin file exists
			string tempFileNamePath = Path.Combine(acqDataFolderList[0], "MSScan.bin");
			if (!File.Exists(tempFileNamePath))
			{
				mRetData.EvalMsg = "Invalid dataset. MSScan.bin file not found in the AcqData folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify size of the MSScan.bin file
			dataFileSizeKB = GetFileSize(tempFileNamePath);
			if (dataFileSizeKB <= AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB)
			{
				ReportFileSizeTooSmall("MSScan.bin", tempFileNamePath, dataFileSizeKB, AGILENT_MSSCAN_BIN_FILE_MIN_SIZE_KB);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// The AcqData folder should contain file MSTS.xml
			tempFileNamePath = Path.Combine(acqDataFolderList[0], "MSTS.xml");
			if (!File.Exists(tempFileNamePath))
			{
				mRetData.EvalMsg = "Invalid dataset. MSTS.xml file not found in the AcqData folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Check to see if a .M folder exists
			string[] subFolderList = Directory.GetDirectories(acqDataFolderList[0], "*.m");
			if (subFolderList == null || subFolderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .m folders found found in the AcqData folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (subFolderList.Length > 1)
			{
				mRetData.EvalMsg = "Invalid dataset. Multiple .m folders found in the AcqData folder";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// If we got to here, everything is OK
			return EnumCloseOutType.CLOSEOUT_SUCCESS;
		}	// End sub
		
		/// <summary>
		/// Tests a Sciex QTrap dataset's integrity
		/// </summary>
		/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
		/// <returns>Enum indicating success or failure</returns>
		private EnumCloseOutType TestSciexQtrapFile(string dataFileNamePath, string datasetName)
		{
			float dataFileSizeKB;
			string tempFileNamePath;

			// Verify .wiff file exists in storage folder
			tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + clsInstrumentClassInfo.DOT_WIFF_EXTENSION);
			if (!File.Exists(tempFileNamePath))
			{
				mRetData.EvalMsg = "Data file " + tempFileNamePath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Get size of .wiff file
			dataFileSizeKB = GetFileSize(tempFileNamePath);

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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
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
			return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, RAW_FILE_MAX_SIZE_MB);
		}

		/// <summary>
		/// Tests an Orbitrap (LTQ_FT) dataset's integrity
		/// </summary>
		/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
		/// <returns>Enum indicating success or failure</returns>
		private EnumCloseOutType TestLTQFTFile(string dataFileNamePath)
		{
			return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, 100000);
		}

		/// <summary>
		/// Tests an Thermo_Exactive dataset's integrity
		/// </summary>
		/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
		/// <returns>Enum indicating success or failure</returns>
		private EnumCloseOutType TestThermoExactiveFile(string dataFileNamePath)
		{
			return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, 100000);
		}

		/// <summary>
		/// Tests an Triple Quad (TSQ) dataset's integrity
		/// </summary>
		/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
		/// <returns>Enum indicating success or failure</returns>
		private EnumCloseOutType TestTripleQuadFile(string dataFileNamePath)
		{
			return TestThermoRawFile(dataFileNamePath, RAW_FILE_MIN_SIZE_KB, 100000);
		}

		/// <summary>
		/// Test a Thermo .Raw file's integrity
		/// If the .Raw file is not found, then looks for a .mgf file, .mzXML, or .mzML file
		/// </summary>
		/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
		/// <param name="minFileSizeKB">Minimum allowed file size</param>
		/// <param name="maxFileSizeMB">Maximum allowed file size</param>
		/// <returns></returns>
		private EnumCloseOutType TestThermoRawFile(string dataFileNamePath, float minFileSizeKB, float maxFileSizeMB)
		{
			float dataFileSizeKB;

			// Verify file exists in storage folder
			if (!File.Exists(dataFileNamePath))
			{
				// File not found; look for alternate extensions
				System.Collections.Generic.List<string> lstAlternateExtensions = new System.Collections.Generic.List<string>();
				bool bAlternateFound = false;
				lstAlternateExtensions.Add("mgf");
				lstAlternateExtensions.Add("mzXML");
				lstAlternateExtensions.Add("mzML");

				foreach (string altExtension in lstAlternateExtensions)
				{
					string dataFileNamePathAlt = System.IO.Path.ChangeExtension(dataFileNamePath, altExtension);
					if (File.Exists(dataFileNamePathAlt))
					{
						mRetData.EvalMsg = "Raw file not found, but ." + altExtension + " file exists";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, mRetData.EvalMsg);
						minFileSizeKB = 25;
						maxFileSizeMB = 100000;
						dataFileNamePath = dataFileNamePathAlt;
						bAlternateFound=true;
						break;
					}
				}
				
				if (!bAlternateFound)
				{
					mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			// Get size of data file
			dataFileSizeKB = GetFileSize(dataFileNamePath);

			// Check min size
			if (dataFileSizeKB < minFileSizeKB)
			{
				ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, minFileSizeKB);
				return EnumCloseOutType.CLOSEOUT_FAILED;
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
			float dataFileSizeKB;

			// Verify 0.ser folder exists
			if (!Directory.Exists(Path.Combine(datasetFolderPath, "0.ser")))
			{
				mRetData.EvalMsg = "Invalid dataset. 0.ser folder not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify acqus file exists
			string dataFolder = Path.Combine(datasetFolderPath, "0.ser");
			if (!File.Exists(Path.Combine(dataFolder, "acqus")))
			{
				mRetData.EvalMsg = "Invalid dataset. acqus file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify size of the acqus file
			dataFileSizeKB = GetFileSize(Path.Combine(dataFolder, "acqus"));
			if (dataFileSizeKB <= 0F)
			{
				mRetData.EvalMsg = "Invalid dataset. acqus file contains no data";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify ser file present
			if (!File.Exists(Path.Combine(dataFolder, "ser")))
			{
				mRetData.EvalMsg = "Invalid dataset. ser file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify size of the ser file
			dataFileSizeKB = GetFileSize(Path.Combine(dataFolder, "ser"));
			if (dataFileSizeKB <= 100)
			{
				mRetData.EvalMsg = "Invalid dataset. ser file too small";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// If we got to here, everything is OK
			return EnumCloseOutType.CLOSEOUT_SUCCESS;
		}	// End sub

		/// <summary>
		/// Tests a BrukerTOF_BAF folder for integrity
		/// </summary>
		/// <param name="datasetFolderPath">Fully qualified path to the dataset folder</param>
		/// <returns>Enum indicating test result</returns>
		private EnumCloseOutType TestBrukerTof_BafFolder(string datasetFolderPath, string instrumentName)
		{
			float dataFileSizeKB;

			// Verify only one .D folder in dataset
			string[] folderList = Directory.GetDirectories(datasetFolderPath, "*.D");
			if (folderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .D folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (folderList.Length > 1)
			{
				if (!PossiblyRenameSupersededFolder(folderList, clsInstrumentClassInfo.DOT_D_EXTENSION))
					return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify analysis.baf file exists
			if (!File.Exists(Path.Combine(folderList[0], "analysis.baf")))
			{
				mRetData.EvalMsg = "Invalid dataset. analysis.baf file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify size of the analysis.baf file
			string dataFileNamePath = Path.Combine(folderList[0], "analysis.baf");
			dataFileSizeKB = GetFileSize(dataFileNamePath);
			if (dataFileSizeKB <= BAF_FILE_MIN_SIZE_KB)
			{
				ReportFileSizeTooSmall("Analysis.baf", dataFileNamePath, dataFileSizeKB, BAF_FILE_MIN_SIZE_KB);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Check to see if at least one .M folder exists
			List<string> subFolderList = Directory.GetDirectories(folderList[0], "*.M").ToList<string>();

			if (subFolderList == null || subFolderList.Count < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .M folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (subFolderList.Count > 1)
			{
				// Multiple .M folders
				// This is OK for the Buker Imaging instruments
				if (!instrumentName.ToLower().Contains("imaging"))
				{
					// It's also OK if there are two folders, and one contains _neg and one contains _pos
					if (!PositiveNegativeMethodFolders(subFolderList))
					{
						mRetData.EvalMsg = "Invalid dataset. Multiple .M folders found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}
			}

			// Determine if at least one .method file exists
			string[] methodFiles = Directory.GetFiles(subFolderList[0], "*.method");
			if (methodFiles.Length == 0)
			{
				mRetData.EvalMsg = "Invalid dataset. No .method files found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
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
		/// <returns>Enum indicating test result</returns>
		private EnumCloseOutType TestBrukerFT_Folder(string datasetFolderPath, bool requireBAFFile, bool requireMCFFile, clsInstrumentClassInfo.eInstrumentClass instrumentClass, string instrumentName)
		{
			float dataFileSizeKB;

			string tempFileNamePath = string.Empty;
			bool fileExists = false;

			// Verify only one .D folder in dataset
			string[] folderList = Directory.GetDirectories(datasetFolderPath, "*.D");
			if (folderList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .D folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (folderList.Length > 1)
			{
				if (!PossiblyRenameSupersededFolder(folderList, clsInstrumentClassInfo.DOT_D_EXTENSION))
					return EnumCloseOutType.CLOSEOUT_FAILED;				
			}

			// Verify analysis.baf file exists
			tempFileNamePath = Path.Combine(folderList[0], "analysis.baf");
			fileExists = File.Exists(tempFileNamePath);
			if (!fileExists && requireBAFFile)
			{
				mRetData.EvalMsg = "Invalid dataset. analysis.baf file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			if (fileExists)
			{
				// Verify size of the analysis.baf file
				dataFileSizeKB = GetFileSize(tempFileNamePath);
				if (dataFileSizeKB <= BAF_FILE_MIN_SIZE_KB)
				{
					ReportFileSizeTooSmall("Analysis.baf", tempFileNamePath, dataFileSizeKB, BAF_FILE_MIN_SIZE_KB);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			
			// Check whether any .mcf files exist
			// Note that "*.mcf" will match files with extension .mcf and files with extension .mcf_idx
			DirectoryInfo diDatasetFolder = new DirectoryInfo(folderList[0]);
			tempFileNamePath = string.Empty;
			dataFileSizeKB = 0;
			fileExists = false;
			long mcfFileSizeMax = 0;
			foreach (FileInfo fiFile in diDatasetFolder.GetFiles("*.mcf"))
			{
				if (fiFile.Length > dataFileSizeKB * 1024)
				{
					dataFileSizeKB = (float)fiFile.Length / (1024F); 
					tempFileNamePath = fiFile.Name;
					fileExists = true;
				}

				if (fiFile.Length > mcfFileSizeMax)
					mcfFileSizeMax = fiFile.Length;
			}

			if (!fileExists && requireMCFFile)
			{
				mRetData.EvalMsg = "Invalid dataset; .mcf file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			if (fileExists)
			{
				// Verify size of the largest .mcf file
				float minSizeKb;
				if (tempFileNamePath.ToLower() == "Storage.mcf_idx".ToLower())
					minSizeKb = 4;
				else
					minSizeKb = MCF_FILE_MIN_SIZE_KB;

				if (dataFileSizeKB <= minSizeKb)
				{
					ReportFileSizeTooSmall(".MCF", tempFileNamePath, dataFileSizeKB, minSizeKb);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			// Verify ser file (if it exists)
			tempFileNamePath = Path.Combine(folderList[0], "ser");
			if (File.Exists(tempFileNamePath))
			{
				// ser file found; verify its size				
				dataFileSizeKB = GetFileSize(tempFileNamePath);
				if (dataFileSizeKB <= SER_FILE_MIN_SIZE_KB)
				{
					// If on the 15T and the ser file is small but the .mcf file is not empty, then this is OK
					if (!(instrumentName == "15T_FTICR" && mcfFileSizeMax > 0))
					{
						ReportFileSizeTooSmall("ser", tempFileNamePath, dataFileSizeKB, SER_FILE_MIN_SIZE_KB);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}
			}
			else
			{
				// Check to see if a fid file exists instead of a ser file
				tempFileNamePath = Path.Combine(folderList[0], "fid");
				if (File.Exists(tempFileNamePath))
				{
					// fid file found; verify size					
					dataFileSizeKB = GetFileSize(tempFileNamePath);
					if (dataFileSizeKB <= FID_FILE_MIN_SIZE_KB)
					{
						ReportFileSizeTooSmall("fid", tempFileNamePath, dataFileSizeKB, FID_FILE_MIN_SIZE_KB);
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
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
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
			List<string> subFolderList = Directory.GetDirectories(folderList[0], "*.M").ToList<string>();

			if (subFolderList == null || subFolderList.Count < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No .M folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}
			else if (subFolderList.Count > 1)
			{
				// Multiple .M folders
				// Allow this if there are two folders, and one contains _neg and one contains _pos
				// Also allow this if on the 15T
				if (!PositiveNegativeMethodFolders(subFolderList) && instrumentName != "15T_FTICR" && !instrumentName.ToLower().Contains("imaging"))
				{
					mRetData.EvalMsg = "Invalid dataset. Multiple .M folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			// Determine if apexAcquisition.method file exists and meets minimum size requirements
			string methodFolderPath = subFolderList[0];
			if (!File.Exists(Path.Combine(methodFolderPath, "apexAcquisition.method")))
			{
				mRetData.EvalMsg = "Invalid dataset. apexAcquisition.method file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			tempFileNamePath = Path.Combine(methodFolderPath, "apexAcquisition.method");
			dataFileSizeKB = GetFileSize(tempFileNamePath);
			if (dataFileSizeKB <= ACQ_METHOD_FILE_MIN_SIZE_KB)
			{
				ReportFileSizeTooSmall("apexAcquisition.method", tempFileNamePath, dataFileSizeKB, ACQ_METHOD_FILE_MIN_SIZE_KB);
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
			string[] fileList = Directory.GetFiles(datasetFolderPath, "*.zip");
			if (fileList.Length < 1)
			{
				mRetData.EvalMsg = "Invalid dataset. No zip files found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
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
			string[] zipFiles = Directory.GetFiles(datasetFolderPath, "*.zip");
			if (zipFiles.Length > 0)
			{
				mRetData.EvalMsg = "Zip files found in dataset folder " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Check whether the dataset folder contains just one data folder or multiple data folders
			string[] dataFolders = Directory.GetDirectories(datasetFolderPath);

			if (dataFolders.Length < 1)
			{
				mRetData.EvalMsg = "No subfolders were found in the dataset folder " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
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
				System.Text.RegularExpressions.Regex reMaldiSpotFolder;
				reMaldiSpotFolder = new System.Text.RegularExpressions.Regex(MALDI_SPOT_FOLDER_REGEX, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

				for (int i = 0; i < dataFolders.Length; i++)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.DEBUG, "Test folder " + dataFolders[i] + " against RegEx " + reMaldiSpotFolder.ToString());

					string sDirName = System.IO.Path.GetFileName(dataFolders[i]);
					if (!reMaldiSpotFolder.IsMatch(sDirName, 0))
					{
						mRetData.EvalMsg = "Dataset folder contains multiple subfolders, but folder " + sDirName + " does not match the expected pattern (" + reMaldiSpotFolder.ToString() + "); see " + datasetFolderPath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}

				}
			}

			// If we got to here, everything is OK
			return EnumCloseOutType.CLOSEOUT_SUCCESS;
		}	// End sub

		private EnumCloseOutType TestIMSAgilentTOF(string dataFileNamePath)
		{
			float dataFileSizeKB;

			// Verify file exists in storage folder
			if (!File.Exists(dataFileNamePath))
			{
				mRetData.EvalMsg = "Data file " + dataFileNamePath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, mRetData.EvalMsg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Get size of data file
			dataFileSizeKB = GetFileSize(dataFileNamePath);

			// Check min size
			if (dataFileSizeKB < UIMF_FILE_MIN_SIZE_KB)
			{
				ReportFileSizeTooSmall("Data", dataFileNamePath, dataFileSizeKB, UIMF_FILE_MIN_SIZE_KB);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Verify that the pressure columns are in the correct order
			if (!ValidatePressureInfo(dataFileNamePath))
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
		/// <returns>True if the pressure values are correct; false if the columns have swapped data</returns>
		protected bool ValidatePressureInfo(string dataFileNamePath)
		{

			// Example of correct pressures:
			//   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
			//   8.33844                     3.87086                3.92628                0.23302

			// Example of incorrect pressures:
			//   HighPressureFunnelPressure  IonFunnelTrapPressure  RearIonFunnelPressure  QuadrupolePressure
			//   4.06285                     9.02253                0.41679                4.13393

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Opening UIMF file to read pressure data");

			// Open the file with the UIMFRader
			UIMFLibrary.DataReader objUimfReader = new UIMFLibrary.DataReader(dataFileNamePath);
			System.Collections.Generic.Dictionary<int, UIMFLibrary.DataReader.FrameType> dctMasterFrameList;
			dctMasterFrameList = objUimfReader.GetMasterFrameList();

			foreach (int iFrameNumber in dctMasterFrameList.Keys)
			{
				UIMFLibrary.FrameParameters oFrameParams = objUimfReader.GetFrameParameters(iFrameNumber);

				bool bNPressureColumnsArePresent = (oFrameParams.QuadrupolePressure > 0 &&
													oFrameParams.RearIonFunnelPressure > 0 &&
													oFrameParams.HighPressureFunnelPressure > 0 &&
													oFrameParams.IonFunnelTrapPressure > 0);

				if (bNPressureColumnsArePresent)
				{
					bool bPressuresAreInCorrectOrder = (oFrameParams.QuadrupolePressure < oFrameParams.RearIonFunnelPressure &&
														oFrameParams.RearIonFunnelPressure < oFrameParams.HighPressureFunnelPressure);

					if (!bPressuresAreInCorrectOrder)
					{
						mRetData.EvalMsg = "Data file " + dataFileNamePath + " has invalid pressure info in the Frame_Parameters table for frame " + oFrameParams.FrameNum;

						string msg = mRetData.EvalMsg + "; QuadrupolePressure should be less than the RearIonFunnelPressure and the RearIonFunnelPressure should be less than the HighPressureFunnelPressure.";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

						objUimfReader.Dispose();
						return false;
					}
				}

				if (iFrameNumber % 100 == 0)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Validated frame " + oFrameParams.FrameNum);

			}

			objUimfReader.Dispose();
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
			string msg = "Starting clsPluginMain.Setup()";
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
			FileInfo fi = new FileInfo(fileNamePath);
			Single fileLengthKB = (float)fi.Length / (1024F);
			return fileLengthKB;
		}

		/// <summary>
		/// Stores the tool version info in the database
		/// </summary>
		/// <remarks></remarks>
		protected bool StoreToolVersionInfo()
		{

			string strToolVersionInfo = string.Empty;
			System.IO.FileInfo ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
			bool bSuccess;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			// Lookup the version of the Capture tool plugin
			string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "DatasetIntegrityPlugin.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the Capture tool plugin
			string strSQLitePath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "System.Data.SQLite.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strSQLitePath);
			if (!bSuccess)
				return false;

			// Lookup the version of the Capture tool plugin
			string strUIMFLibraryPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "UIMFLibrary.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
			if (!bSuccess)
				return false;

			// Store path to CaptureToolPlugin.dll in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));

			try
			{
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
			}
			catch (System.Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}

		}

		#endregion


		#region "Event handlers"

		private void AttachCmdrunnerEvents(clsRunDosProgram CmdRunner)
		{
			try
			{
				CmdRunner.LoopWaiting += new clsRunDosProgram.LoopWaitingEventHandler(CmdRunner_LoopWaiting);
				CmdRunner.Timeout += new clsRunDosProgram.TimeoutEventHandler(CmdRunner_Timeout);
			}
			catch
			{
				// Ignore errors here
			}
		}

		private void DetachCmdrunnerEvents(clsRunDosProgram CmdRunner)
		{
			try
			{
				if (CmdRunner != null)
				{
					CmdRunner.LoopWaiting -= CmdRunner_LoopWaiting;
					CmdRunner.Timeout -= CmdRunner_Timeout;
				}
			}
			catch
			{
				// Ignore errors here
			}
		}

		void CmdRunner_Timeout()
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CmdRunner timeout reported");
		}

		void CmdRunner_LoopWaiting()
		{

			if (System.DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 300)
			{
				mLastStatusUpdate = System.DateTime.UtcNow;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "AgilentToUIMFConverter running; " + System.DateTime.UtcNow.Subtract(mAgilentToUIMFStartTime).TotalMinutes + " minutes elapsed");
			}
		}

		#endregion
	}	// End class
}	// End namespace
