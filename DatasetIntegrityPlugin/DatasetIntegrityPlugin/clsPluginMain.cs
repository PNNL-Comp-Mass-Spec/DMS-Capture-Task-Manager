
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
using CaptureTaskManager;
using System.IO;

namespace DatasetIntegrityPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants"
			const float RAW_FILE_MIN_SIXE = 0.1F;	//MB
			const float RAW_FILE_MAX_SIZE = 2048F;	//MB
			const float BAF_FILE_MIN_SIZE = 0.1F;	//MB
			const float SER_FILE_MIN_SIZE = 0.016F;	//MB
			const float FID_FILE_MIN_SIZE = 0.016F;	//MB
			const float ACQ_METHOD_FILE_MIN_SIZE = 0.005F;	//MB
			const float SCIEX_WIFF_FILE_MIN_SIZE = 0.1F; //MB
			const float SCIEX_WIFF_SCAN_FILE_MIN_SIZE = 0.001F; //MB
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
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				msg = "Performing integrity test, dataset '" + dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

				// Set up the file paths
				string storageVolExt = m_TaskParams.GetParam("Storage_Vol_External");
				string storagePath = m_TaskParams.GetParam("Storage_Path");
				string datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, dataset));
				string dataFileNamePath;

				// Select which tests will be performed based on instrument class
				string instClass = m_TaskParams.GetParam("Instrument_Class");

				msg = "Instrument class: " + instClass;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
		
				switch (instClass.ToLower())
				{
					case "finnigan_ion_trap":
						dataFileNamePath = Path.Combine(datasetFolder,dataset + ".raw");
						retData.CloseoutType = TestFinniganIonTrapFile(dataFileNamePath);
						break;
					case "ltq_ft":
						dataFileNamePath = Path.Combine(datasetFolder, dataset + ".raw");
						retData.CloseoutType = TestLTQFTFile(dataFileNamePath);
						break;
					case "brukerftms":
						retData.CloseoutType = TestBrukerFolder(datasetFolder);
						break;
					case "thermo_exactive":
						dataFileNamePath = Path.Combine(datasetFolder, dataset + ".raw");
						retData.CloseoutType = TestThernoExactiveFile(dataFileNamePath);
						break;
					case "triple_quad":
						dataFileNamePath = Path.Combine(datasetFolder, dataset + ".raw");
						retData.CloseoutType = TestTripleQuadFile(dataFileNamePath);
						break;
					case "brukerft_baf":
						retData.CloseoutType = TestBrukerFT_BafFolder(datasetFolder);
						break;
					case "brukermaldi_imaging":
						retData.CloseoutType = TestBrukerMaldiImagingFolder(datasetFolder);
						break;
					case "brukermaldi_spot":
						retData.CloseoutType = TestBrukerMaldiSpotFolder(datasetFolder);
						break;
					case "brukertof_baf":
						retData.CloseoutType = TestBrukerTof_BafFolder(datasetFolder);
						break;
					case "sciex_qtrap":
						retData.CloseoutType = TestSciexQtrapFile(datasetFolder, dataset);
						break;
					default:
						msg = "No integrity test avallable for instrument class " + instClass;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
						retData.EvalMsg = msg;
						retData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
						break;
				}	// End switch

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;
			}	// End sub

			/// <summary>
			/// Tests a Sciex QTrap dataset's integrity
			/// </summary>
			/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestSciexQtrapFile(string dataFileNamePath, string datasetName)
			{
				string msg;
				float dataFileSize;
				string tempFileNamePath;

				// Verify .wiff file exists in storage folder
				tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + ".wiff");
				if (!File.Exists(tempFileNamePath))
				{
					msg = "Data file " + tempFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of .wiff file
				dataFileSize = GetFileSize(tempFileNamePath);

				// Check .wiff file min size
				if (dataFileSize < SCIEX_WIFF_FILE_MIN_SIZE)
				{
					msg = "Data file " + tempFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + SCIEX_WIFF_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify .wiff.scan file exists in storage folder
				tempFileNamePath = Path.Combine(dataFileNamePath, datasetName + ".wiff.scan");
				if (!File.Exists(tempFileNamePath))
				{
					msg = "Data file " + tempFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of .wiff.scan file
				dataFileSize = GetFileSize(tempFileNamePath);

				// Check .wiff.scan file min size
				if (dataFileSize < SCIEX_WIFF_SCAN_FILE_MIN_SIZE)
				{
					msg = "Data file " + tempFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + SCIEX_WIFF_SCAN_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
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
				string msg;
				float dataFileSize;

				// Verify file exists in storage folder
				if (!File.Exists(dataFileNamePath))
				{
					msg = "Data file " + dataFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of data file
				dataFileSize = GetFileSize(dataFileNamePath);

				// Check min size
				if (dataFileSize < RAW_FILE_MIN_SIXE)
				{
					msg = "Data file " + dataFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + RAW_FILE_MIN_SIXE.ToString("#0.00")+ "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Check max size
				if (dataFileSize > RAW_FILE_MAX_SIZE)
				{
					msg = "Data file " + dataFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.0") +
								"MB. Max allowable size is " + RAW_FILE_MAX_SIZE.ToString("#####0.0");
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything was OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests an Orbitrap (LTQ_FT) dataset's integrity
			/// </summary>
			/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestLTQFTFile(string dataFileNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify file exists in storage folder
				if (!File.Exists(dataFileNamePath))
				{
					msg = "Data file " + dataFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of data file
				dataFileSize = GetFileSize(dataFileNamePath);

				// Check min size
				if (dataFileSize < RAW_FILE_MIN_SIXE)
				{
					msg = "Data file " + dataFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + RAW_FILE_MIN_SIXE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything was OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests an Thermo_Exactive dataset's integrity
			/// </summary>
			/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestThernoExactiveFile(string dataFileNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify file exists in storage folder
				if (!File.Exists(dataFileNamePath))
				{
					msg = "Data file " + dataFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of data file
				dataFileSize = GetFileSize(dataFileNamePath);

				// Check min size
				if (dataFileSize < RAW_FILE_MIN_SIXE)
				{
					msg = "Data file " + dataFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + RAW_FILE_MIN_SIXE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything was OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests an Triple Quad (TSQ) dataset's integrity
			/// </summary>
			/// <param name="dataFileNamePath">Fully qualified path to dataset file</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestTripleQuadFile(string dataFileNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify file exists in storage folder
				if (!File.Exists(dataFileNamePath))
				{
					msg = "Data file " + dataFileNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Get size of data file
				dataFileSize = GetFileSize(dataFileNamePath);

				// Check min size
				if (dataFileSize < RAW_FILE_MIN_SIXE)
				{
					msg = "Data file " + dataFileNamePath + " may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + RAW_FILE_MIN_SIXE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything was OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests a bruker folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns></returns>
			private EnumCloseOutType TestBrukerFolder(string datasetNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify 0.ser folder exists
				if (!Directory.Exists(Path.Combine(datasetNamePath,"0.ser")))
				{
					msg = "Invalid dataset. 0.ser folder not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify acqus file exists
				string dataFolder = Path.Combine(datasetNamePath, "0.ser");
				if (!File.Exists(Path.Combine(dataFolder,"acqus")))
				{
					msg = "Invalid dataset. acqus file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify size of acqus file
				dataFileSize = GetFileSize(Path.Combine(dataFolder, "acqus"));
				if (dataFileSize <= 0F)
				{
					msg = "Invalid dataset. acqus file contains no data";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify ser file present
				if (!File.Exists(Path.Combine(dataFolder, "ser")))
				{
					msg = "Invalid dataset. ser file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify size of ser file
				dataFileSize = GetFileSize(Path.Combine(dataFolder, "ser"));
				if (dataFileSize <= 0.1F)
				{
					msg = "Invalid dataset. ser file too small";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything is OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests a BrukerTOF_BAF folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns>Enum indicating test result</returns>
			private EnumCloseOutType TestBrukerTof_BafFolder(string datasetNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify only one .D folder in dataset
				string[] folderList = Directory.GetDirectories(datasetNamePath, "*.D");
				if (folderList.Length < 1)
				{
					msg = "Invalid dataset. No .D folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else if (folderList.Length > 1)
				{
					msg = "Invalid dataset. Multiple .D folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify analysis.baf file exists
				if (!File.Exists(Path.Combine(folderList[0], "analysis.baf")))
				{
					msg = "Invalid dataset. analysis.baf file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify size of analysis.baf file
				dataFileSize = GetFileSize(Path.Combine(folderList[0], "analysis.baf"));
				if (dataFileSize <= BAF_FILE_MIN_SIZE)
				{
					msg = "Analysis.baf file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + BAF_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Check to see if at least one .M folder exists
				string[] subFolderList = Directory.GetDirectories(folderList[0], "*.M");
				if (subFolderList.Length < 0)
				{
					msg = "Invalid dataset. No .M folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else if (subFolderList.Length > 1)
				{
					msg = "Invalid dataset. Multiple .M folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Determine if at least one .method file exists
				string[] methodFiles = Directory.GetFiles(subFolderList[0],"*.method");
				if (methodFiles.Length == 0)
				{
					msg = "Invalid dataset. No .method files found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything is OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests a BrukerFT_BAF folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns>Enum indicating test result</returns>
			private EnumCloseOutType TestBrukerFT_BafFolder(string datasetNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify only one .D folder in dataset
				string[] folderList = Directory.GetDirectories(datasetNamePath, "*.D");
				if (folderList.Length < 1)
				{
					msg = "Invalid dataset. No .D folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else if (folderList.Length > 1)
				{
					msg = "Invalid dataset. Multiple .D folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify analysis.baf file exists
				if (!File.Exists(Path.Combine(folderList[0], "analysis.baf")))
				{
					msg = "Invalid dataset. analysis.baf file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify size of analysis.baf file
				dataFileSize = GetFileSize(Path.Combine(folderList[0], "analysis.baf"));
				if (dataFileSize <= BAF_FILE_MIN_SIZE)
				{
					msg = "Analysis.baf file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + BAF_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify ser file exists
				if (File.Exists(Path.Combine(folderList[0], "ser")))
				{
					// ser file found; verify size
					dataFileSize = GetFileSize(Path.Combine(folderList[0], "ser"));
					if (dataFileSize <= SER_FILE_MIN_SIZE)
					{
						msg = "ser file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
									"MB. Min allowable size is " + SER_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}
				else
				{
					// Check to see if a fid file exists instead of a ser file
					if (File.Exists(Path.Combine(folderList[0], "fid")))
					{
						// fid file found; verify size
						dataFileSize = GetFileSize(Path.Combine(folderList[0], "fid"));
						if (dataFileSize <= FID_FILE_MIN_SIZE)
						{
							msg = "fid file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
										"MB. Min allowable size is " + FID_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
					}
					else
					{
						// No ser or fid file found
						msg = "Invalid dataset. No ser or fid file found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}

				// Check to see if a .M folder exists
				string[] subFolderList = Directory.GetDirectories(folderList[0], "*.M");
				if (subFolderList.Length < 0)
				{
					msg = "Invalid dataset. No .M folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else if (subFolderList.Length > 1)
				{
					msg = "Invalid dataset. Multiple .M folders found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Determine if apexAcquisition.method file exists and meets minimum size requirements
				string methodFolderPath = subFolderList[0];
				if (!File.Exists(Path.Combine(methodFolderPath,"apexAcquisition.method")))
				{
					msg = "Invalid dataset. apexAcquisition.method file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				dataFileSize = GetFileSize(Path.Combine(methodFolderPath, "apexAcquisition.method"));
				if (dataFileSize <= ACQ_METHOD_FILE_MIN_SIZE)
				{
					msg = "apexAcquisition.method file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + ACQ_METHOD_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything is OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests a BrukerMALDI_Imaging folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestBrukerMaldiImagingFolder(string datasetNamePath)
			{
				string msg;

				// Verify at least one zip file exists in dataset folder
				string[] fileList = Directory.GetFiles(datasetNamePath, "*.zip");
				if (fileList.Length < 1)
				{
					msg = "Invalid dataset. No zip files found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything is OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Tests a BrukerMALDI_Spot folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns>Enum indicating success or failure</returns>
			private EnumCloseOutType TestBrukerMaldiSpotFolder(string datasetNamePath)
			{
				string msg;

				//Verify the dataset folder contains just one data folder, unzipped
				string[] zipFiles = Directory.GetFiles(datasetNamePath, "*.zip");
				string[] dataFolders = Directory.GetDirectories(datasetNamePath);

				if (zipFiles.Length > 0)
				{
					msg = "Zip files found in dataset folder " + datasetNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else if (dataFolders.Length != 1)
				{
					msg = "Multiple data files found in dataset folder " + datasetNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// If we got to here, everything is OK
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

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
			/// Gets the length of a single file in MB
			/// </summary>
			/// <param name="fileNamePath">Fully qualified path to input file</param>
			/// <returns>File size in MB</returns>
			private float GetFileSize(string fileNamePath)
			{
				FileInfo fi = new FileInfo(fileNamePath);
				Single fileLengthMB = (float)fi.Length / (1024F * 1024F);
				return fileLengthMB;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
