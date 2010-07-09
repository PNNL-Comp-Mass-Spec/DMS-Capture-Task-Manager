
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//
// Last modified 10/02/2009
//						07/09/2010 (DAC) - Added new definition for BrukerFT_BAF instrument class
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
			const float BAF_FILE_MIN_SIZE = 1.0F;	//MB
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
			/// Tests a BrukerFT_BAF folder for integrity
			/// </summary>
			/// <param name="datasetNamePath">Fully qualified path to the dataset folder</param>
			/// <returns></returns>
			private EnumCloseOutType TestBrukerFT_BafFolder(string datasetNamePath)
			{
				string msg;
				float dataFileSize;

				// Verify analysis.baf file exists
				if (!File.Exists(Path.Combine(datasetNamePath, "analysis.baf")))
				{
					msg = "Invalid dataset. analysis.baf file now found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify size of analysis.baf file
				dataFileSize = GetFileSize(Path.Combine(datasetNamePath, "analysis.baf"));
				if (dataFileSize <= BAF_FILE_MIN_SIZE)
				{
					msg = "Data file may be corrupt. Actual file size: " + dataFileSize.ToString("####0.00") +
								"MB. Min allowable size is " + BAF_FILE_MIN_SIZE.ToString("#0.00") + "MB.";
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
