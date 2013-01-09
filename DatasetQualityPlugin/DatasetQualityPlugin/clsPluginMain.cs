
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/02/2009
//
// Last modified 10/02/2009
//               09/17/2012 mem - Moved from the DatasetInfo plugin to the DatasetQuality plugin
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using CaptureTaskManager;

namespace DatasetQualityPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants and Enums"
		int MAX_QUAMETER_RUNTIME_MINUTES = 150;
		
		string STORE_QUAMETER_RESULTS_SP_NAME = "StoreQuameterResults";
		string QUAMETER_IDFREE_METRICS_FILE = "Quameter_IDFree.tsv";
		string QUAMETER_CONSOLE_OUTPUT_FILE = "Quameter_Console_Output.txt";
		#endregion

		#region "Class-wide variables"
		clsToolReturnData mRetData = new clsToolReturnData();
		string m_WorkDir;
		string m_Dataset;
		int m_DatasetID;
		int m_DebugLevel;

		clsRunDosProgram CmdRunner;
		PRISM.DataBase.clsExecuteDatabaseSP mExecuteSP;

		System.DateTime mLastStatusUpdate = System.DateTime.UtcNow;
		System.DateTime mQuameterStartTime = System.DateTime.UtcNow;

		#endregion

		#region "Constructors"
		public clsPluginMain()
			: base()
		{
			
		}

		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the dataset info step tool
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;

			msg = "Starting DatasetQualityPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;

			m_WorkDir = m_MgrParams.GetParam("workdir");

			m_Dataset = m_TaskParams.GetParam("Dataset");
			if (!int.TryParse(m_TaskParams.GetParam("Dataset_ID"), out m_DatasetID))
			{
				m_DatasetID = 0;
			}
			m_DebugLevel = clsConversion.CIntSafe(m_MgrParams.GetParam("debuglevel"), 4);

			if (m_DebugLevel >= 5)
			{
				msg = "Creating dataset info for dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			if (clsMetaDataFile.CreateMetadataFile(m_MgrParams, m_TaskParams))
			{
				// Everything was good
				if (m_DebugLevel >= 4)
				{
					msg = "Metadata file created for dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}
			else
			{
				// There was a problem
				msg = "Problem creating metadata file for dataset " + m_Dataset + ". See local log for details";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
				mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
				mRetData.EvalMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}

			// Determine whether or not we will run Quameter
			// At present we only process Thermo .Raw files

			// Set up the file paths
			string storageVolExt = m_TaskParams.GetParam("Storage_Vol_External");
			string storagePath = m_TaskParams.GetParam("Storage_Path");
			string datasetFolder = Path.Combine(storageVolExt, Path.Combine(storagePath, m_Dataset));
			string dataFilePathRemote = string.Empty;
			bool bRunQuameter = false;

			string instClassName = m_TaskParams.GetParam("Instrument_Class");

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
				case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
				case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
					dataFilePathRemote = Path.Combine(datasetFolder, m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION);
					break;
				case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
					// Quameter crashes on TSQ files; skip them
					dataFilePathRemote = string.Empty;
					break;
				default:
					dataFilePathRemote = string.Empty;
					break;
			}

			if (!string.IsNullOrEmpty(dataFilePathRemote))
				bRunQuameter = true;

			// Store the version info in the database
			// Store the Quameter version if dataFileNamePath is not empty
			if (!StoreToolVersionInfo(bRunQuameter))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				mRetData.CloseoutMsg = "Error determining tool version info";
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return mRetData;
			}

			if (bRunQuameter)
			{
				string quameterExePath = GetQuameterPath();
				System.IO.FileInfo fiQuameter = new System.IO.FileInfo(quameterExePath);

				if (!fiQuameter.Exists)
				{
					mRetData.CloseoutMsg = "Quameter not found at " + quameterExePath;
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return mRetData;
				}

				bool bSuccess;
				bSuccess = ProcessThermoRawFile(dataFilePathRemote, instrumentClass, fiQuameter);

				if (bSuccess)
				{
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				}
				else
				{
					// Quameter failed
					// Copy the Quameter log file to the Dataset's QC folder
					// We only save the log file if an error occurs since it typically doesn't contain any useful information
					bSuccess = CopyFilesToDatasetFolder(datasetFolder);

					if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
						mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

				}
				
			}
			else
			{
				msg = "Skipping Quameter since instrument class " + instClassName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			ClearWorkDir();

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub

		protected void ClearWorkDir()
		{
			
			try
			{
				System.IO.DirectoryInfo diWorkDir = new System.IO.DirectoryInfo(m_WorkDir);

				// Delete any files that start with the dataset name
				foreach (System.IO.FileInfo fiFile in diWorkDir.GetFiles(m_Dataset + "*.*"))
				{
					DeleteFileIgnoreErrors(fiFile.FullName);
				}

				// Delete any files that contain Quameter
				foreach (System.IO.FileInfo fiFile in diWorkDir.GetFiles("*Quameter*.*"))
				{
					DeleteFileIgnoreErrors(fiFile.FullName);
				}

			}
			catch
			{
				// Ignore errors here
			}

		}

		/// <summary>
		/// Convert the Quameter results to XML
		/// </summary>
		/// <param name="lstResults"></param>
		/// <param name="sXMLResults"></param>
		/// <returns></returns>
		protected bool ConvertResultsToXML(List<KeyValuePair<String, String>> lstResults, out string sXMLResults)
		{

			// XML will look like:

			// <?xml version="1.0" encoding="utf-8" standalone="yes"?>
			// <Quameter_Results>
			//   <Dataset>Shew119-01_17july02_earth_0402-10_4-20</Dataset>
			//   <Job>780000</Job>
			//   <Measurements>
			//     <Measurement Name="XIC-WideFrac">0.35347</Measurement>
			//     <Measurement Name="XIC-FWHM-Q1">20.7009</Measurement>
			//     <Measurement Name="XIC-FWHM-Q2">22.3192</Measurement>
			//     <Measurement Name="XIC-FWHM-Q3">24.794</Measurement>
			//     <Measurement Name="XIC-Height-Q2">1.08473</Measurement>
			//     etc.
			//   </Measurements>
			// </Quameter_Results>

			System.Text.StringBuilder sbXML = new System.Text.StringBuilder();
			sXMLResults = string.Empty;

			string sJobNum = m_TaskParams.GetParam("Job");

			try
			{
				sbXML.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
				sbXML.Append("<Quameter_Results>");

				sbXML.Append("<Dataset>" + m_Dataset + "</Dataset>");
				sbXML.Append("<Job>" + sJobNum + "</Job>");

				sbXML.Append("<Measurements>");

				foreach (KeyValuePair<String, String> kvResult in lstResults)
				{
					sbXML.Append("<Measurement Name=\"" + kvResult.Key + "\">" + kvResult.Value + "</Measurement>");
				}

				sbXML.Append("</Measurements>");
				sbXML.Append("</Quameter_Results>");

				sXMLResults = sbXML.ToString();

			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Error converting Quameter results to XML";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);				
				return false;
			}

			return true;

		}

		protected bool CopyFilesToDatasetFolder(string datasetFolder)
		{
			

			try 
			{
				System.IO.DirectoryInfo diDatasetQCFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(datasetFolder, "QC"));

				if (!diDatasetQCFolder.Exists)
				{
					diDatasetQCFolder.Create();
				}


				if (!CopyFileToServer(QUAMETER_CONSOLE_OUTPUT_FILE, m_WorkDir, diDatasetQCFolder.FullName))
					return false;

				// Uncomment the following to copy the Metrics file to the server
				//if (!CopyFileToServer(QUAMETER_IDFREE_METRICS_FILE, m_WorkDir, diDatasetQCFolder.FullName)) return false;


			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Error creating the Dataest QC folder";
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);				
				return false;
			}

			return true;			
		}

		protected bool CopyFileToServer(string sFileName, string sSourceFolder, string sTargetFolder)
		{
			string sSourceFilePath;
			string sTargetFilePath;

			try 
			{
				sSourceFilePath = System.IO.Path.Combine(sSourceFolder, sFileName);

				if (System.IO.File.Exists(sSourceFilePath))
				{
					sTargetFilePath = System.IO.Path.Combine(sTargetFolder, sFileName);
					m_FileTools.CopyFile(sSourceFilePath, sTargetFilePath, true);
				}
			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Error copying file " + sFileName + " to Dataset folder";
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);				
				return false;
			}

			return true;			
		}

		/// <summary>
		/// Construct the full path to the MSFileInfoScanner.DLL
		/// </summary>
		/// <returns></returns>
		protected string GetQuameterPath()
		{
			// Look for parameter 64bitQuameter
			// To add this to a job, use the following command in SSMS:
			// exec AddUpdateJobParameter 976722, 'JobParameters', '64bitQuameter', 'True'
			bool bUse64Bit = m_TaskParams.GetParam("64bitQuameter", false);

			string sQuameterFolder = m_MgrParams.GetParam("QuameterProgLoc", string.Empty);

			if (string.IsNullOrEmpty(sQuameterFolder))
				return string.Empty;
			else
			{
				if (bUse64Bit)
					return Path.Combine(sQuameterFolder, "64bit\\Quameter.exe");
				else
					return Path.Combine(sQuameterFolder, "Quameter.exe");
			}
		}


		/// <summary>
		/// Extract the results from the Quameter results file
		/// </summary>
		/// <param name="ResultsFilePath"></param>
		/// <returns></returns>
		/// <remarks></remarks>
		protected List<KeyValuePair<String, String>> LoadQuameterResults(string ResultsFilePath)
		{

			// The Quameter results file has two rows, a header row and a data row
			// Filename	StartTimeStamp   XIC-WideFrac   XIC-FWHM-Q1   XIC-FWHM-Q2   XIC-FWHM-Q3   XIC-Height-Q2   etc.
			// QC_Shew_12_02_Run-06_4Sep12_Roc_12-03-30.RAW   2012-09-04T20:33:29Z   0.35347   20.7009   22.3192   24.794   etc.

			// The measurments are returned via this list
			List<KeyValuePair<String, String>> lstResults = new List<KeyValuePair<String, String>>();

			if (!System.IO.File.Exists(ResultsFilePath))
			{
				mRetData.CloseoutMsg = "Quameter Results file not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mRetData.CloseoutMsg + ": " + ResultsFilePath);
				return lstResults;
			}

			if (m_DebugLevel >= 5)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing Quameter Results file " + ResultsFilePath);
			}

			string sLineIn = string.Empty;
			string[] strHeaders = null;
			string[] strData = null;

			using (System.IO.StreamReader srInFile = new System.IO.StreamReader(new System.IO.FileStream(ResultsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite)))
			{
				if (srInFile.Peek() > -1)
				{
					// Read the header line
					sLineIn = srInFile.ReadLine();
				}
				else
				{
					sLineIn = string.Empty;
				}

				if (string.IsNullOrWhiteSpace(sLineIn))
				{
					mRetData.CloseoutMsg = "Quameter Results file is empty (no header line)";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mRetData.CloseoutMsg);
					return lstResults;
				}

				// Parse the headers
				strHeaders = sLineIn.Split('\t');

				if (srInFile.Peek() > -1)
				{
					// Read the data line
					// Read the header line
					sLineIn = srInFile.ReadLine();
				}
				else
				{
					sLineIn = string.Empty;
				}

				if (string.IsNullOrWhiteSpace(sLineIn))
				{
					mRetData.CloseoutMsg = "Quameter Results file is empty (headers, but no data)";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mRetData.CloseoutMsg);
					return lstResults;
				}

				// Parse the data
				strData = sLineIn.Split('\t');

				if (strHeaders.Length > strData.Length)
				{
					// More headers than data values
					mRetData.CloseoutMsg = "Quameter Results file data line (" + strData.Length + " items) does not match the header line (" + strHeaders.Length + " items)";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mRetData.CloseoutMsg);
					return lstResults;
				}

				// Store the results by stepping through the arrays
				// Skip the first two items provided they are "filename" and "StartTimeStamp")
				int indexStart = 0;
				if (strHeaders[indexStart].ToLower() == "filename")
				{
					indexStart++;

					if (strHeaders[indexStart].ToLower() == "starttimestamp")
					{
						indexStart++;
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The second column in the Quameter metrics file is not StartTimeStamp; this is unexpected");
					}
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The first column in the Quameter metrics file is not Filename; this is unexpected");
				}

				for (int index = indexStart; index < strHeaders.Length; index++)
				{
					if (string.IsNullOrWhiteSpace(strHeaders[index]))
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Column " + (index + 1).ToString() + " in the Quameter metrics file is empty; this is unexpected");
					}
					else
					{
						// Replace dashes with underscores in the metric names
						string sHeaderName = strHeaders[index].Trim().Replace("-", "_");

						string sDataItem;
						if (string.IsNullOrWhiteSpace(strData[index]))
							sDataItem = string.Empty;
						else
							sDataItem = string.Copy(strData[index]).Trim();

						lstResults.Add(new KeyValuePair<String, String>(sHeaderName, sDataItem));
					}

				}

			}

			return lstResults;

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
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Quameter error: " + sLineIn);

								}
								else if (sLineIn.StartsWith("Unhandled Exception"))
								{
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Quameter error: " + sLineIn);
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

		protected bool PostProcessMetricsFile(string metricsOutputFileName)
		{
			string sLineIn;
			bool bReplaceOrginal = false;

			try
			{
				string sCorrectedFilePath = metricsOutputFileName + ".new";

				using (System.IO.StreamWriter swCorrectedFile = new System.IO.StreamWriter(new System.IO.FileStream(sCorrectedFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read)))
				{
					using (System.IO.StreamReader srMetricsFile = new System.IO.StreamReader(new System.IO.FileStream(metricsOutputFileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite)))
					{
						while (srMetricsFile.Peek() > -1)
						{
							sLineIn = srMetricsFile.ReadLine();

							if (!string.IsNullOrEmpty(sLineIn))
							{
								if (sLineIn.IndexOf("-1.#IND") > 0)
								{
									sLineIn = sLineIn.Replace("-1.#IND", "");
									bReplaceOrginal = true;
								}
								swCorrectedFile.WriteLine(sLineIn);
							}
							else
							{
								swCorrectedFile.WriteLine();
							}
						}
					}
				}

				if (bReplaceOrginal)
				{
					System.Threading.Thread.Sleep(100);

					// Corrections were made; replace the original file
					System.IO.File.Copy(sCorrectedFilePath, metricsOutputFileName, true);
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error in PostProcessMetricsFile: " + ex.Message, ex);
			}


			return true;

		}
		
		protected bool PostQuameterResultsToDB(string sXMLResults)
		{
			// This Connection String points to the DMS_Capture database
			string sConnectionString = null;
			sConnectionString = m_MgrParams.GetParam("connectionstring");

			// Note that m_DatasetID gets populated by runTool

			return PostQuameterResultsToDB(m_DatasetID, sXMLResults, sConnectionString, STORE_QUAMETER_RESULTS_SP_NAME);

		}

		protected bool PostQuameterResultsToDB(int intDatasetID, string sXMLResults)
		{
			// This Connection String points to the DMS_Capture database
			string sConnectionString = null;
			sConnectionString = m_MgrParams.GetParam("connectionstring");

			return PostQuameterResultsToDB(intDatasetID, sXMLResults, sConnectionString, STORE_QUAMETER_RESULTS_SP_NAME);

		}

		protected bool PostQuameterResultsToDB(int intDatasetID, string sXMLResults, string sConnectionString)
		{

			return PostQuameterResultsToDB(intDatasetID, sXMLResults, sConnectionString, STORE_QUAMETER_RESULTS_SP_NAME);

		}

		public bool PostQuameterResultsToDB(int intDatasetID, string sXMLResults, string sConnectionString, string sStoredProcedure)
		{

			const int MAX_RETRY_COUNT = 3;
			const int SEC_BETWEEN_RETRIES = 20;

			int intStartIndex = 0;
			int intResult = 0;

			string sXMLResultsClean = null;

			System.Data.SqlClient.SqlCommand objCommand;

			bool blnSuccess = false;

			try
			{
				if (m_DebugLevel >= 5)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Posting Quameter Results to the database (using Dataset ID " + intDatasetID.ToString() + ")");
				}

				// We need to remove the encoding line from sXMLResults before posting to the DB
				// This line will look like this:
				//   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

				intStartIndex = sXMLResults.IndexOf("?>");
				if (intStartIndex > 0)
				{
					sXMLResultsClean = sXMLResults.Substring(intStartIndex + 2).Trim();
				}
				else
				{
					sXMLResultsClean = sXMLResults;
				}

				// Call stored procedure sStoredProcedure using connection string sConnectionString


				if (string.IsNullOrWhiteSpace(sConnectionString))
				{
					mRetData.CloseoutMsg = "Connection string empty in PostQuameterResultsToDB";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Connection string not defined; unable to post the Quameter results to the database");
					return false;
				}

				if (string.IsNullOrWhiteSpace(sStoredProcedure))
				{
					sStoredProcedure = STORE_QUAMETER_RESULTS_SP_NAME;
				}

				objCommand = new System.Data.SqlClient.SqlCommand();

				{
					objCommand.CommandType = System.Data.CommandType.StoredProcedure;
					objCommand.CommandText = sStoredProcedure;

					objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
					objCommand.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

					objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@DatasetID", System.Data.SqlDbType.Int));
					objCommand.Parameters["@DatasetID"].Direction = System.Data.ParameterDirection.Input;
					objCommand.Parameters["@DatasetID"].Value = intDatasetID;

					objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ResultsXML", System.Data.SqlDbType.Xml));
					objCommand.Parameters["@ResultsXML"].Direction = System.Data.ParameterDirection.Input;
					objCommand.Parameters["@ResultsXML"].Value = sXMLResultsClean;
				}

				mExecuteSP = new PRISM.DataBase.clsExecuteDatabaseSP(sConnectionString);
				AttachExecuteSpEvents();

				intResult = mExecuteSP.ExecuteSP(objCommand, MAX_RETRY_COUNT, SEC_BETWEEN_RETRIES);

				if (intResult == PRISM.DataBase.clsExecuteDatabaseSP.RET_VAL_OK)
				{
					// No errors
					blnSuccess = true;
				}
				else
				{
					mRetData.CloseoutMsg = "Error storing Quameter Results in database, " + sStoredProcedure + " returned " + intResult.ToString();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					blnSuccess = false;
				}

			}
			catch (System.Exception ex)
			{
				mRetData.CloseoutMsg = "Exception storing Quameter Results in database";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg, ex);
				blnSuccess = false;
			}
			finally
			{
				DetachExecuteSpEvents();
				mExecuteSP = null;
			}

			return blnSuccess;
		}


		protected bool ProcessThermoRawFile(string dataFilePathRemote, clsInstrumentClassInfo.eInstrumentClass instrumentClass, System.IO.FileInfo fiQuameter)
		{

			try
			{

				// Copy the appropriate config file to the working directory
				string configFileNameSource;

				string configFilePathSource;
				string configFilePathTarget;

				switch (instrumentClass)
				{
					case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
						// Assume low-res precursor spectra
						configFileNameSource = "quameter_ltq.cfg";
						break;
					case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
					case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
					case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
						// Assume high-res precursor spectra
						configFileNameSource = "quameter_orbitrap.cfg";
						break;
					default:
						// Assume high-res precursor spectra
						configFileNameSource = "quameter_orbitrap.cfg";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unexpected Thermo instrumentClass; will assume high-res precursor spectra");
						break;
				}

				configFilePathSource = Path.Combine(fiQuameter.Directory.FullName, configFileNameSource);
				configFilePathTarget = Path.Combine(m_WorkDir, "quameter.cfg");

				if (!System.IO.File.Exists(configFilePathSource) && fiQuameter.Directory.FullName.ToLower().EndsWith("64bit"))
				{
					// Using the 64-bit version of quameter
					// Look for the .cfg file up one directory
					configFilePathSource = Path.Combine(fiQuameter.Directory.Parent.FullName, configFileNameSource);
				}

				if (!System.IO.File.Exists(configFilePathSource))
				{
					mRetData.CloseoutMsg = "Quameter parameter file not found " + configFilePathSource;
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}

				System.IO.File.Copy(configFilePathSource, configFilePathTarget, true);

				// Copy the .Raw file to the working directory
				if (m_DebugLevel >= 4)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying the .Raw file from " + dataFilePathRemote);
				}

				string dataFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(dataFilePathRemote));

				try
				{
					System.IO.File.Copy(dataFilePathRemote, dataFilePathLocal, true);
				}
				catch (Exception ex)
				{
					mRetData.CloseoutMsg = "Exception copying the .Raw file locally";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}

				// Run Quameter
				mRetData.CloseoutMsg = string.Empty;
				bool bSuccess = RunQuameter(fiQuameter, System.IO.Path.GetFileName(dataFilePathLocal), QUAMETER_IDFREE_METRICS_FILE);

				if (!bSuccess)
				{
					if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
					{
						mRetData.CloseoutMsg = "Unknown error running Quameter";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					}

					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}

			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Exception in ProcessThermoRawFile";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return false;
			}

			return true;
		}


		/// <summary>
		/// Read the Quameter results files, convert to XML, and post to DMS
		/// </summary>
		/// <param name="ResultsFilePath">Path to the Quameter results file</param>
		/// <returns></returns>
		/// <remarks></remarks>
		protected bool ReadAndStoreQuameterResults(string ResultsFilePath)
		{

			bool blnSuccess = false;
			List<KeyValuePair<String, String>> lstResults = default(List<KeyValuePair<String, String>>);

			try
			{
				lstResults = LoadQuameterResults(ResultsFilePath);

				if (lstResults.Count == 0)
				{
					if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
					{
						mRetData.CloseoutMsg = "No Quameter results were found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": lstResults.Count == 0");
					}

				}
				else
				{
					// Convert the results to XML format
					string sXMLResults;

					blnSuccess = ConvertResultsToXML(lstResults, out sXMLResults);

					if (blnSuccess)
					{
						// Store the results in the database
						blnSuccess = PostQuameterResultsToDB(sXMLResults);

						if (!blnSuccess)
						{
							if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
							{
								mRetData.CloseoutMsg = "Unknown error posting quameter results to the database";
							}
						}
					}

				}

			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Exception parsing Quameter results";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception parsing Quameter results and posting to the database", ex);
				blnSuccess = false;
			}

			return blnSuccess;

		}

		protected bool RunQuameter(System.IO.FileInfo fiQuameter, string dataFileName, string metricsOutputFileName)
		{

			try
			{
				// Construct the command line arguments
				// Always use "cpus 1" since it guarantees that the metrics will always be written out in the same order
				string CmdStrQuameter = clsConversion.PossiblyQuotePath(dataFileName) + " -MetricsType idfree -OutputFilepath " + clsConversion.PossiblyQuotePath(metricsOutputFileName) + " -cpus 1";

				CmdRunner = new clsRunDosProgram(m_WorkDir);
				mQuameterStartTime = System.DateTime.UtcNow;
				mLastStatusUpdate = System.DateTime.UtcNow;

				AttachCmdrunnerEvents();

				// Create a batch file to run the command
				// Capture the console output (including output to the error stream) via redirection symbols: 
				//    strExePath CmdStr > ConsoleOutputFile.txt 2>&1

				string sBatchFileName = "Run_Quameter.bat";

				// Update the Exe path to point to the RunProgram batch file; update CmdStr to be empty
				string sExePath = System.IO.Path.Combine(m_WorkDir, sBatchFileName);
				string CmdStr = string.Empty;
				string sConsoleOutputFileName = QUAMETER_CONSOLE_OUTPUT_FILE;

				// Create the batch file
				using (System.IO.StreamWriter swBatchFile = new System.IO.StreamWriter(new System.IO.FileStream(sExePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read)))
				{
					swBatchFile.WriteLine(fiQuameter.FullName + " " + CmdStrQuameter + " > " + sConsoleOutputFileName + " 2>&1");
				}

				System.Threading.Thread.Sleep(100);

				CmdRunner.CreateNoWindow = false;
				CmdRunner.EchoOutputToConsole = false;
				CmdRunner.CacheStandardOutput = false;
				CmdRunner.WriteConsoleOutputToFile = false;

				int iMaxRuntimeSeconds = MAX_QUAMETER_RUNTIME_MINUTES * 60;
				bool bSuccess = CmdRunner.RunProgram(sExePath, CmdStr, "Quameter", true, iMaxRuntimeSeconds);

				ParseConsoleOutputFileForErrors(System.IO.Path.Combine(m_WorkDir, sConsoleOutputFileName));

				if (!bSuccess)
				{
					mRetData.CloseoutMsg = "Error running Quameter";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);

					if (CmdRunner.ExitCode != 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Quameter returned a non-zero exit code: " + CmdRunner.ExitCode.ToString());
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to Quameter failed (but exit code is 0)");
					}

					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}
				else
				{
					if (m_DebugLevel >= 4)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Quameter Complete");
					}
				}

				System.Threading.Thread.Sleep(100);

				string metricsOutputFilePath = System.IO.Path.Combine(m_WorkDir, metricsOutputFileName);

				if (!System.IO.File.Exists(metricsOutputFilePath))
				{
					mRetData.CloseoutMsg = "Metrics file was not created";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg);
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}

				// Post-process the metrics output file to replace -1.#IND with empty strings
				PostProcessMetricsFile(metricsOutputFilePath);

				// Parse the metrics file and post to the database
				if (!ReadAndStoreQuameterResults(metricsOutputFilePath))
				{
					if (string.IsNullOrEmpty(mRetData.CloseoutMsg))
					{
						mRetData.CloseoutMsg = "Error parsing Quameter results";
					}
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return false;
				}				
			}
			catch (Exception ex)
			{
				mRetData.CloseoutMsg = "Exception in RunQuameter";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mRetData.CloseoutMsg + ": " + ex.Message);
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return false;
			}
			finally
			{
				DetachCmdrunnerEvents();
			}

			return true;

		}

		/// <summary>
		/// Initializes the dataset info tool
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
		/// Stores the tool version info in the database
		/// </summary>
		/// <remarks></remarks>
		protected bool StoreToolVersionInfo(bool storeQuameterVersion)
		{

			string sToolVersionInfo = string.Empty;
			System.IO.FileInfo ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
			bool bSuccess;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			// Lookup the version of the Capture tool plugin
			string sPluginPath = Path.Combine(ioAppFileInfo.DirectoryName, "DatasetQualityPlugin.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref sToolVersionInfo, sPluginPath);
			if (!bSuccess)
				return false;

			// Store path to CaptureToolPlugin.dll in ioToolFiles
			List<System.IO.FileInfo> ioToolFiles = new List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(sPluginPath));

			if (storeQuameterVersion)
			{
				// Quameter is a C++ program, so we can only store the date
				ioToolFiles.Add(new System.IO.FileInfo(GetQuameterPath()));
			}

			try
			{
				return base.SetStepTaskToolVersion(sToolVersionInfo, ioToolFiles, false);
			}
			catch (System.Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}

		}

		#endregion


		#region "Event handlers"

		private void AttachCmdrunnerEvents()
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

		private void AttachExecuteSpEvents()
		{
			try
			{
				mExecuteSP.DBErrorEvent += new PRISM.DataBase.clsExecuteDatabaseSP.DBErrorEventEventHandler(mExecuteSP_DBErrorEvent);
			}
			catch
			{
				// Ignore errors here
			}
		}

		private void DetachCmdrunnerEvents()
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

		private void DetachExecuteSpEvents()
		{
			try
			{
				if (mExecuteSP != null)
				{
					mExecuteSP.DBErrorEvent -= mExecuteSP_DBErrorEvent;
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Quameter running; " + System.DateTime.UtcNow.Subtract(mQuameterStartTime).TotalMinutes + " minutes elapsed");
			}
		}

		void mExecuteSP_DBErrorEvent(string Message)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Stored procedure execution error: " + Message);
		}

		#endregion

	}	// End class

}	// End namespace
