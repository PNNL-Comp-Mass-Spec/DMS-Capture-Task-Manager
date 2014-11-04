
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;
using System.Runtime.InteropServices;		// Required for call to GetDiskFreeSpaceEx

namespace CaptureTaskManager
{

	public class clsMainProgram
	{

		//*********************************************************************************************************
		// Main program execution loop for application
		//**********************************************************************************************************

		#region "Enums"
		private enum BroadcastCmdType
		{
			Shutdown,
			ReadConfig,
			Invalid
		}

		private enum LoopExitCode
		{
			NoTaskFound,
			ConfigChanged,
			ExceededMaxTaskCount,
			DisabledMC,
			DisabledLocally,
			ExcessiveRequestErrors,
			InvalidWorkDir,
			ShutdownCmdReceived,
			UpdateRequired,
			FlagFile,
			NeedToAbortProcessing
		}
		#endregion

		#region "Constants"
		private const int MAX_ERROR_COUNT = 4;

		private const string CUSTOM_LOG_SOURCE_NAME = "Capture Task Manager";
		public const string CUSTOM_LOG_NAME = "DMSCapTaskMgr";

		#endregion

		#region "Class variables"
		private clsMgrSettings m_MgrSettings;
		private clsCaptureTask m_Task;
		private FileSystemWatcher m_FileWatcher;
		private IToolRunner m_CapTool;
		private bool m_ConfigChanged;
		private int m_TaskRequestErrorCount;
		private IStatusFile m_StatusFile;

		private clsMessageHandler m_MsgHandler;
		private bool m_MsgQueueInitSuccess;
	
		private LoopExitCode m_LoopExitCode;

		private string m_MgrName = "Unknown";
		private string m_StepTool = "Unknown";
		private string m_Job = "Unknown";
		private string m_Dataset = "Unknown";
		private int m_DebugLevel = 4;

		private bool m_Running;
		private System.Timers.Timer m_StatusTimer;
		private DateTime m_DurationStart;
		private bool m_ManagerDeactivatedLocally;
		
		#endregion

		#region "Delegates"
		#endregion

		#region "Events"
		#endregion

		#region "Properties"

		public bool ManagerDeactivatedLocally
		{
			get { return m_ManagerDeactivatedLocally; }
		}

		#endregion

		#region "Constructors"
		/// <summary>
		/// Constructor
		/// </summary>
		public clsMainProgram()
		{
			// Does nothing at present
		}
		#endregion

		#region "Methods"

		/// <summary>
		/// Evaluates the LoopExitCode to determine whether or not manager can request another task
		/// </summary>
		/// <param name="eLoopExitCode"></param>
		/// <returns>True if OK to request another task</returns>
		private bool EvaluateLoopExitCode(LoopExitCode eLoopExitCode)
		{
			string msg;
			bool restartOK = true;

			// Determine cause of loop exit and respond accordingly
			switch (eLoopExitCode)
			{
				case LoopExitCode.ConfigChanged:
					// Reload the manager config
					msg = "Reloading configuration and restarting manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					// Unsubscribe message handler events and close msssage handler
					if (m_MsgQueueInitSuccess)
					{
						m_MsgHandler.BroadcastReceived -= OnBroadcastReceived;
						m_MsgHandler.CommandReceived -= OnCommandReceived;
						m_MsgHandler.Dispose();
					}
					restartOK = true;
					break;

				case LoopExitCode.DisabledMC:
					// Manager is disabled via manager control db
					msg = "Manager disabled in manager control DB";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_StatusFile.UpdateDisabled(false);
					restartOK = false;
					break;

				case LoopExitCode.DisabledLocally:
					// Manager disabled locally
					msg = "Manager disabled locally";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_StatusFile.UpdateDisabled(true);
					restartOK = false;
					break;

				case LoopExitCode.ExcessiveRequestErrors:
					// Too many errors
					msg = "Excessive errors requesting task; closing manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					
					// Do not create a flag file; intermittent network connectivity is likely resulting in failure to request a task
					// This will likely clear up eventually

					m_StatusFile.UpdateStopped(true);

					restartOK = false;
					break;

				case LoopExitCode.InvalidWorkDir:
					// Working directory not valid
					msg = "Working directory problem, disabling manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

					// Note: We previously called DisableManagerLocally() to update CaptureTaskManager.config.exe
					// We now create a flag file instead
					// This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

					/*
						if (!m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False"))
						{
							msg = "Error while disabling manager: " + m_MgrSettings.ErrMsg;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						}
						m_StatusFile.UpdateDisabled(true);
					*/

					m_StatusFile.CreateStatusFlagFile();
					m_StatusFile.UpdateStopped(true);

					restartOK = false;
					break;

				case LoopExitCode.NoTaskFound:
					// No capture task found
					msg = "No capture tasks found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					m_StatusFile.UpdateStopped(false);
					restartOK = false;
					break;

				case LoopExitCode.ShutdownCmdReceived:
					// Shutdown command received
					msg = "Shutdown command received, closing manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_StatusFile.UpdateStopped(false);
					restartOK = false;
					break;

				case LoopExitCode.ExceededMaxTaskCount:
					// Max number of consecutive jobs reached
					msg = "Exceeded maximum job count, closing manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_StatusFile.UpdateStopped(false);
					restartOK = false;
					break;

				case LoopExitCode.UpdateRequired:
					// Manager update required
					msg = "Manager update is required, closing manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_MgrSettings.AckManagerUpdateRequired();
					m_StatusFile.UpdateStopped(false);
					restartOK = false;
					break;

				case LoopExitCode.FlagFile:
					// Flag file is present
					msg = "Flag file exists - unable to continue analysis";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					m_StatusFile.UpdateStopped(true);
					restartOK = false;
					break;

				case LoopExitCode.NeedToAbortProcessing:
					// Step tool set flag NeedToAbortProcessing to true
					msg = "NeedToAbortProcessing = true, closing manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_StatusFile.UpdateStopped(false);
					restartOK = false;
					break;

				default:
					// Should never get here
					break;
			}	// End switch

			return restartOK;
		}

		/// <summary>
		/// Initializes the manager
		/// </summary>
		/// <returns>TRUE for success; FALSE otherwise</returns>
		public bool InitMgr()
		{
			// Get the manager settings
			// If you get an exception here while debugging in Visual Studio, then be sure 
			//  that "UsingDefaults" is set to False in CaptureTaskManager.exe.config               
			try
			{
				m_MgrSettings = new clsMgrSettings();
			}
			catch (Exception ex)
			{
				if (String.Equals(ex.Message, clsMgrSettings.DEACTIVATED_LOCALLY))
				{
					m_ManagerDeactivatedLocally = true;
				}
				else
				{
					// Failures are logged by clsMgrSettings to application event logs;
					//  this includes MgrActive_Local = False
					// 
					// If the DMSCapTaskMgr application log does not exist yet, the Log4Net SysLogger will create it (see file Logging.config)
					// However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
					// Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

					Console.WriteLine();
					Console.WriteLine(@"===============================================================");
					Console.WriteLine(@"Exception instantiating clsMgrSettings: " + ex.Message);
					Console.WriteLine(@"===============================================================");
					Console.WriteLine();
					Console.WriteLine(@"You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " + CUSTOM_LOG_NAME + @" application log");
					Console.WriteLine();
					System.Threading.Thread.Sleep(500);
				}

				return false;
			}

			// Update the cached values for this manager and job
			m_MgrName = m_MgrSettings.GetParam("MgrName");
			m_StepTool = "Unknown";
			m_Job = "Unknown";
			m_Dataset = "Unknown";
		
			// Confirm that the application event log exists
			{
				if (!EventLog.SourceExists(CUSTOM_LOG_SOURCE_NAME))
				{
					var SourceData = new EventSourceCreationData(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);
					EventLog.CreateEventSource(SourceData);
				}
			}

			// Set up the loggers
			string logFileName = m_MgrSettings.GetParam("logfilename");
			m_DebugLevel = clsConversion.CIntSafe(m_MgrSettings.GetParam("debuglevel"), 4);
			clsLogTools.CreateFileLogger(logFileName, m_DebugLevel);

			if (clsConversion.CBoolSafe(m_MgrSettings.GetParam("ftplogging"))) clsLogTools.CreateFtpLogFileLogger("Dummy.txt");
			string logCnStr = m_MgrSettings.GetParam("connectionstring");
			
			clsLogTools.CreateDbLogger(logCnStr, "CaptureTaskMan: " + m_MgrName);

			// Make initial log entry
			string msg = "=== Started Capture Task Manager V" + Application.ProductVersion + " ===== ";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			// Setup the message queue
			m_MsgQueueInitSuccess = false;
			m_MsgHandler = new clsMessageHandler();
			m_MsgHandler.BrokerUri = m_MsgHandler.BrokerUri = m_MgrSettings.GetParam("MessageQueueURI");
			m_MsgHandler.CommandQueueName = m_MgrSettings.GetParam("ControlQueueName");
			m_MsgHandler.BroadcastTopicName = m_MgrSettings.GetParam("BroadcastQueueTopic");
			m_MsgHandler.StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");
			m_MsgHandler.MgrSettings = m_MgrSettings;

			// Initialize the message queue
			// Start this in a separate thread so that we can abort the initialization if necessary
			InitializeMessageQueue();

			if (m_MsgQueueInitSuccess)
			{
				//Connect message handler events
				m_MsgHandler.CommandReceived += OnCommandReceived;
				m_MsgHandler.BroadcastReceived += OnBroadcastReceived;
			}

			// Setup a file watcher for the config file
			var fInfo = new FileInfo(Application.ExecutablePath);
			m_FileWatcher = new FileSystemWatcher();
			m_FileWatcher.BeginInit();
			m_FileWatcher.Path = fInfo.DirectoryName;
			m_FileWatcher.IncludeSubdirectories = false;
			m_FileWatcher.Filter = m_MgrSettings.GetParam("configfilename");
			m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
			m_FileWatcher.EndInit();
			m_FileWatcher.EnableRaisingEvents = true;

			// Subscribe to the file watcher Changed event
			m_FileWatcher.Changed += FileWatcherChanged;

			// Set up the tool for getting tasks
			m_Task = new clsCaptureTask(m_MgrSettings);

			// Set up the status file class
			string statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
			m_StatusFile = new clsStatusFile(statusFileNameLoc);
			m_StatusFile.MonitorUpdateRequired += OnStatusMonitorUpdateReceived;
			m_StatusFile.LogToMsgQueue = clsConversion.CBoolSafe(m_MgrSettings.GetParam("LogStatusToMessageQueue"));
			m_StatusFile.MgrName = m_MgrName;
			m_StatusFile.MgrStatus = EnumMgrStatus.Running;
			m_StatusFile.WriteStatusFile();

			// Set up the status reporting time
			m_StatusTimer = new System.Timers.Timer();
			m_StatusTimer.BeginInit();
			m_StatusTimer.Enabled = false;
			m_StatusTimer.Interval = 60 * 1000;	// 1 minute
			m_StatusTimer.EndInit();
			m_StatusTimer.Elapsed += m_StatusTimer_Elapsed;

			// Get the most recent job history
			string historyFile = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");
			if (File.Exists(historyFile))
			{
				try
				{
					// Create an instance of StreamReader to read from a file.
					// The using statement also closes the StreamReader.
					using (var sr = new StreamReader(historyFile))
					{
						String line;
						// Read and display lines from the file until the end of 
						// the file is reached.
						while ((line = sr.ReadLine()) != null)
						{
							if (line.Contains("RecentJob: "))
							{
								string tmpStr = line.Replace("RecentJob: ", "");
								m_StatusFile.MostRecentJobInfo = tmpStr;
								break;
							}
						}
					}
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
													"Exception readining status history file", ex);
				}
			}
			// Everything worked!
			return true;
		}

		private bool InitializeMessageQueue()
		{

			var worker = new System.Threading.Thread(InitializeMessageQueueWork);
			worker.Start();

			// Wait a maximum of 15 seconds
			if (!worker.Join(15000))
			{
				worker.Abort();
				m_MsgQueueInitSuccess = false;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to initialize the message queue (timeout after 15 seconds)");
			}

			return m_MsgQueueInitSuccess;
		}

		private void InitializeMessageQueueWork()
		{

			if (!m_MsgHandler.Init())
			{
				// Most error messages provided by .Init method, but debug message is here for program tracking
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler init error");
				m_MsgQueueInitSuccess = false;
			}
			else
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler initialized");
				m_MsgQueueInitSuccess = true;
			}

		}

		/// <summary>
		/// Main loop for task performance
		/// </summary>
		/// <returns>TRUE if loop exits and manager restart is OK, FALSE otherwise</returns>
		public bool PerformMainLoop()
		{
			int taskCount = 1;

			var dtLastConfigDBUpdate = DateTime.UtcNow;

			m_Running = true;

			// Begin main execution loop
			while (m_Running)
			{

				try
				{

					//Verify that an error hasn't left the the system in an odd state
					if (StatusFlagFileError())
					{
						m_LoopExitCode = LoopExitCode.FlagFile;
						break;
					}

					// Check for configuration change
					// This variable will be true if the CaptureTaskManager.exe.config file has been updated
					if (m_ConfigChanged)
					{
						// Local config file has changed
						m_LoopExitCode = LoopExitCode.ConfigChanged;
						break;
					}

					// Reload the manager control DB settings in case they have changed
					// However, only reload every 2 minutes
					if (!UpdateMgrSettings(ref dtLastConfigDBUpdate, 2))
					{
						// Error updating manager settings
						m_LoopExitCode = LoopExitCode.UpdateRequired;
						break;
					}

					// Check to see if manager is still active
					if (!clsConversion.CBoolSafe(m_MgrSettings.GetParam("mgractive")))
					{
						// Disabled via manager control db
						m_LoopExitCode = LoopExitCode.DisabledMC;
						break;
					}

					if (!clsConversion.CBoolSafe(m_MgrSettings.GetParam("mgractive_local")))
					{
						m_LoopExitCode = LoopExitCode.DisabledLocally;
						break;
					}

					if (clsConversion.CBoolSafe(m_MgrSettings.GetParam("ManagerUpdateRequired")))
					{
						m_LoopExitCode = LoopExitCode.UpdateRequired;
						break;
					}

					// Check for excessive number of errors
					if (m_TaskRequestErrorCount > MAX_ERROR_COUNT)
					{
						m_LoopExitCode = LoopExitCode.ExcessiveRequestErrors;
						break;
					}

					// Check working directory
					if (!ValidateWorkingDir())
					{
						m_LoopExitCode = LoopExitCode.InvalidWorkDir;
						break;
					}

					// Check whether the computer is likely to install the monthly Windows Updates within the next few hours
					string pendingWindowsUpdateMessage;
					if (clsWindowsUpdateStatus.UpdatesArePending(DateTime.Now, out pendingWindowsUpdateMessage))
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, pendingWindowsUpdateMessage);
						m_LoopExitCode = LoopExitCode.NoTaskFound;
						break;
					}					
				

					// Delete temp files between 1:00 am and 1:30 am, or after every 50 tasks
					if (taskCount == 1 && DateTime.Now.Hour == 1 && DateTime.Now.Minute < 30 || taskCount % 50 == 0)
					{
						RemoveOldTempFiles();
						RemoveOldFTPLogFiles();
					}

					// Attempt to get a capture task
					EnumRequestTaskResult taskReturn = m_Task.RequestTask();
					switch (taskReturn)
					{
						case EnumRequestTaskResult.NoTaskFound:
							m_Running = false;
							m_LoopExitCode = LoopExitCode.NoTaskFound;
							break;

						case EnumRequestTaskResult.ResultError:
							// Problem with task request; Errors are logged by request method
							m_TaskRequestErrorCount++;
							break;

						case EnumRequestTaskResult.TaskFound:

							EnumCloseOutType eTaskCloseout;
							PerformTask(out eTaskCloseout);

							// Increment and test the task counter
							taskCount++;
							if (taskCount > int.Parse(m_MgrSettings.GetParam("maxrepetitions", "1")))
							{
								m_Running = false;
								m_LoopExitCode = LoopExitCode.ExceededMaxTaskCount;
							}

							if (eTaskCloseout == EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
							{
								m_Running = false;
								m_LoopExitCode = LoopExitCode.NeedToAbortProcessing;
							}

							break;

						default:
							//Shouldn't ever get here!
							break;
					}	// End switch (taskReturn)

				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in PerformMainLoop", ex);
				}


			}	// End while

			m_Running = false;

			// Write the recent job history file				
			try
			{
				string historyFile = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");

				using (var sw = new StreamWriter(historyFile, false))
				{
					sw.WriteLine("RecentJob: " + m_StatusFile.MostRecentJobInfo);
				}
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
												"Exception writing job history file", ex);
			}

			// Evaluate the loop exit code
			bool restartOK = EvaluateLoopExitCode(m_LoopExitCode);

			if (!restartOK)
			{
				const string msg = "===== Closing Capture Task Manager =====";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}

			return restartOK;
		}


		private void PerformTask(out EnumCloseOutType eTaskCloseout)
		{
			string msg;
			eTaskCloseout = EnumCloseOutType.CLOSEOUT_NOT_READY;

			try
			{
				// Cache the job parameters
				m_StepTool = m_Task.GetParam("StepTool");
				m_Job = m_Task.GetParam("Job");
				m_Dataset = m_Task.GetParam("Dataset");
				string stepNumber = m_Task.GetParam("Step");

				msg = "Job " + m_Job + ", step " + stepNumber + " assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Update the status
				m_StatusFile.JobNumber = int.Parse(m_Job);
				m_StatusFile.Dataset = m_Dataset;
				m_StatusFile.MgrStatus = EnumMgrStatus.Running;
				m_StatusFile.Tool = m_StepTool;
				m_StatusFile.TaskStatus = EnumTaskStatus.Running;
				m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.Running_Tool;
				m_StatusFile.MostRecentJobInfo = DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss tt") +
															", Job " + m_Job + ", Step " + stepNumber +
															", Tool " + m_StepTool;

				m_StatusFile.WriteStatusFile();

				// Create the tool runner object
				if (!SetToolRunnerObject(m_StepTool))
				{
					msg = m_MgrName + ": Unable to SetToolRunnerObject, job " + m_Job
								+ ", Dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

					msg = "Unable to SetToolRunnerObject";
					m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg);
					m_StatusFile.UpdateIdle();
					return;
				}


				// Make sure we have enough free space on the drive with the dataset folder
				if (!ValidateFreeDiskSpace(out msg))
				{
					if (string.IsNullOrEmpty(msg))
						msg = "Insufficient free space (location undefined)";

					m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg);
					m_StatusFile.UpdateIdle();
					return;
				}

				// Run the tool plugin
				m_DurationStart = DateTime.UtcNow;
				m_StatusTimer.Enabled = true;
				clsToolReturnData toolResult = m_CapTool.RunTool();
				m_StatusTimer.Enabled = false;

				eTaskCloseout = toolResult.CloseoutType;
				string sCloseoutMessage;

				switch (eTaskCloseout)
				{
					case EnumCloseOutType.CLOSEOUT_FAILED:
						msg = m_MgrName + ": Failure running tool " + m_StepTool
									+ ", job " + m_Job + ", Dataset " + m_Dataset;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						if (!String.IsNullOrEmpty(toolResult.CloseoutMsg))
							sCloseoutMessage = toolResult.CloseoutMsg;
						else
							sCloseoutMessage = "Failure running tool " + m_StepTool;

						m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
						break;

					case EnumCloseOutType.CLOSEOUT_NOT_READY:
						msg = m_MgrName + ": Dataset not ready, tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

						sCloseoutMessage = "Dataset not ready";

						if (!String.IsNullOrEmpty(toolResult.CloseoutMsg))
							sCloseoutMessage += ": " + toolResult.CloseoutMsg;

						m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
						break;

					case EnumCloseOutType.CLOSEOUT_SUCCESS:
						msg = m_MgrName + ": Step complete, tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						m_Task.CloseTask(eTaskCloseout, toolResult.CloseoutMsg, toolResult.EvalCode, toolResult.EvalMsg);
						break;

					case EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING:
						msg = m_MgrName + ": Failure running tool " + m_StepTool
									+ ", job " + m_Job + ", Dataset " + m_Dataset
									+ "; CloseOut = NeedToAbortProcessing";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						sCloseoutMessage = "Error: NeedToAbortProcessing";
						m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
						break;

					default:
						// Should never get here
						break;
				}	// End switch (toolResult)

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running task", ex);

				msg = m_MgrName + ": Failure running tool " + m_StepTool
								   + ", job " + m_Job + ", Dataset " + m_Dataset
								   + "; CloseOut = Exception";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

				msg = "Exception: " + ex.Message;
				m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg, EnumEvalCode.EVAL_CODE_FAILED, "Exception running tool");
			}


			// Update the status
			m_StatusFile.ClearCachedInfo();

			m_StatusFile.MgrStatus = EnumMgrStatus.Running;
			m_StatusFile.TaskStatus = EnumTaskStatus.No_Task;
			m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.No_Task;
			m_StatusFile.WriteStatusFile();
			
		}

		public void PostTestLogMessage()
		{
			try
			{
				string sMessage = "Test log message: " + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
				Console.WriteLine(@"Posting test log message to the " + CUSTOM_LOG_NAME + @" Windows event log");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.INFO, sMessage);
				Console.WriteLine(@" ... Success!");
			}
			catch (Exception ex)
			{
				Console.WriteLine(@"Error writing to event log: " + ex.Message);
			}

		}

		/// <summary>
		/// Look for and remove FTPLog_ files that were created over 64 days ago in the application folder
		/// </summary>
		protected void RemoveOldFTPLogFiles()
		{
			const int iAgedLogFileDays = 64;
			RemoveOldFTPLogFiles(iAgedLogFileDays);
		}

		/// <summary>
		/// Look for and remove FTPLog_ files that were created over iAgedLogFileDays days ago in the application folder
		/// </summary>
		/// <remarks>Also removes zero-byte FTPLog_ files</remarks>
		protected void RemoveOldFTPLogFiles(int iAgedLogFileDays)
		{

			if (iAgedLogFileDays < 7)
				iAgedLogFileDays = 7;

			try
			{
				var fiApplication = new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);

				foreach (FileInfo fiFile in fiApplication.Directory.GetFiles("FTPlog_*"))
				{
					try
					{
						if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalDays > iAgedLogFileDays || fiFile.Length == 0)
						{
							fiFile.Delete();
						}
					}
					// ReSharper disable once EmptyGeneralCatchClause
					catch
					{
						// Ignore exceptions
					}
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception removing old FTP log files: " + ex.Message);
			}
		}

		/// <summary>
		/// Look for and remove old .tmp and .zip files
		/// </summary>
		protected void RemoveOldTempFiles()
		{
			// Remove .tmp and .zip files over 12 hours old in the Windows Temp folder
			const int iAgedTempFilesHours = 12;
			string sTempFolderPath = Path.GetTempPath();
			RemoveOldTempFiles(iAgedTempFilesHours, sTempFolderPath);

		}

		protected void RemoveOldTempFiles(int iAgedTempFilesHours, string sTempFolderPath)
		{
			// This list tracks the file specs to search for in folder sTempFolderPath
			var lstSearchSpecs = new List<string>
			{
				"*.tmp",
				"*.zip"
			};

			RemoveOldTempFiles(iAgedTempFilesHours, sTempFolderPath, lstSearchSpecs);
		}

		/// <summary>
		/// Look for and remove files
		/// </summary>
		/// <param name="iAgedTempFilesHours">Files more than this many hours old will be deleted</param>
		/// <param name="sTempFolderPath">Path to the folder to look for and delete old files</param>
		/// <param name="lstSearchSpecs">File specs to search for in folder sTempFolderPath, e.g. "*.txt"</param>
		protected void RemoveOldTempFiles(int iAgedTempFilesHours, string sTempFolderPath, List<string> lstSearchSpecs)
		{
			try
			{
				int iTotalDeleted = 0;

				if (iAgedTempFilesHours < 2)
					iAgedTempFilesHours = 2;

				var diFolder = new DirectoryInfo(sTempFolderPath);
				string msg;
				if (!diFolder.Exists)
				{
					msg = "Folder not found: " + sTempFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					return;
				}

				// Process each entry in lstSearchSpecs
				foreach (string sSpec in lstSearchSpecs)
				{
					int iDeleteCount = 0;
					foreach (FileInfo fiFile in diFolder.GetFiles(sSpec))
					{
						try
						{
							if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > iAgedTempFilesHours)
							{
								fiFile.Delete();
								iDeleteCount += 1;
							}
						}
						// ReSharper disable once EmptyGeneralCatchClause
						catch
						{
							// Ignore exceptions
						}
					}

					iTotalDeleted += iDeleteCount;
				}

				if (iTotalDeleted > 0)
				{
					msg = "Deleted " + iTotalDeleted + " temp file";
					if (iTotalDeleted > 1)
						msg += "s";

					msg += " over " + iAgedTempFilesHours + " hours old in folder " + sTempFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception removing old temp files: " + ex.Message);
			}
		}


		/// <summary>
		/// Sets the tool runner object for this job
		/// </summary>
		/// <returns></returns>
		private bool SetToolRunnerObject(string stepToolName)
		{
			string msg;

			// Load the tool runner
			m_CapTool = clsPluginLoader.GetToolRunner(stepToolName);
			if (m_CapTool == null)
			{
				msg = "Unable to load tool runner for StepTool " + stepToolName + ": " + clsPluginLoader.ErrMsg;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
			msg = "Loaded tool runner for Step Tool " + stepToolName;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			try
			{
                
#if MyEMSL_OFFLINE
	// When this Conditional Compilation Constant is defined, then the DatasetArchive plugin will set debugMode 
	// to Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode when calling UploadToMyEMSLWithRetry()
	// This in turn results in writeToDisk becoming True in SendFileListToDavAsTar
    m_Task.AddAdditionalParameter("MyEMSLOffline", "true");
	clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding job parameter MyEMSLOffline=true");
#endif

#if MyEMSL_TEST_TAR
    m_Task.AddAdditionalParameter("DebugTestTar", "true");
	clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding job parameter DebugTestTar=true");
#endif

				// Setup the new tool runner
				m_CapTool.Setup(m_MgrSettings, m_Task, m_StatusFile);
			}
			catch (Exception ex)
			{
				msg = "Exception calling CapTool.Setup(): " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			return true;
		}

		/// <summary>
		/// Looks for flag file; auto cleans if ManagerErrorCleanupMode is >= 1
		/// </summary>
		/// <returns>True if a flag file exists and it was not auto-cleaned; false if no problems</returns>
		private bool StatusFlagFileError()
		{
			if (m_StatusFile.DetectStatusFlagFile())
			{
				bool blnMgrCleanupSuccess;
				try
				{
					var objCleanupMgrErrors = new clsCleanupMgrErrors(m_MgrSettings.GetParam("MgrCnfgDbConnectStr"),
																					  m_MgrName,
																					  m_MgrSettings.GetParam("WorkDir"),
																					  m_StatusFile);

					int CleanupModeVal = clsConversion.CIntSafe(m_MgrSettings.GetParam("ManagerErrorCleanupMode"), 0);
					blnMgrCleanupSuccess = objCleanupMgrErrors.AutoCleanupManagerErrors(CleanupModeVal);

				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error calling AutoCleanupManagerErrors from StatusFlagFileError: " + ex.Message);
					blnMgrCleanupSuccess = false;
				}


				if (blnMgrCleanupSuccess)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

					// No error; return false
					return false;
				}
				// Error removing flag file; return true
				return true;

			}
			// No error; return false
			return false;

		}

		/// <summary>
		/// Reloads the manager settings from the manager control database 
		/// if at least MinutesBetweenUpdates minutes have elapsed since the last update
		/// </summary>
		/// <param name="dtLastConfigDBUpdate"></param>
		/// <param name="MinutesBetweenUpdates"></param>
		/// <returns></returns>
		private bool UpdateMgrSettings(ref DateTime dtLastConfigDBUpdate, double MinutesBetweenUpdates)
		{
			bool bSuccess = true;

			if (DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates)
			{
				dtLastConfigDBUpdate = DateTime.UtcNow;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings using Manager Control database");

				if (!m_MgrSettings.LoadMgrSettingsFromDB())
				{
					// Error retrieving settings from the manager control DB
					string msg;

					if (string.IsNullOrEmpty(m_MgrSettings.ErrMsg))
						msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";
					else
						msg = m_MgrSettings.ErrMsg;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

					bSuccess = false;
				}
				else
				{
					// Update the log level
					m_DebugLevel = clsConversion.CIntSafe(m_MgrSettings.GetParam("debuglevel"), 4);
					clsLogTools.SetFileLogLevel(m_DebugLevel);
				}
			}

			return bSuccess;
		}

		[DllImport("kernel32", CharSet = CharSet.Auto)]
		static extern int GetDiskFreeSpaceEx(
		 string lpDirectoryName,
		 out ulong lpFreeBytesAvailable,
		 out ulong lpTotalNumberOfBytes,
		 out ulong lpTotalNumberOfFreeBytes);

		protected bool GetDiskFreeSpace(string directoryPath, out long freeBytesAvailableToUser, out long totalDriveCapacityBytes, out long totalNumberOfFreeBytes)
		{

			ulong freeAvailableUser;
			ulong totalDriveCapacity;
			ulong totalFree;

			int iResult = GetDiskFreeSpaceEx(directoryPath, out freeAvailableUser, out totalDriveCapacity, out totalFree);

			if (iResult == 0)
			{
				freeBytesAvailableToUser = 0;
				totalDriveCapacityBytes = 0;
				totalNumberOfFreeBytes = 0;

				return false;
			}
			
			freeBytesAvailableToUser = (long)freeAvailableUser;
			totalDriveCapacityBytes = (long)totalDriveCapacity;
			totalNumberOfFreeBytes = (long)totalFree;

			return true;
		}

		protected string GetStoragePathBase()
		{
			string storagePath = m_Task.GetParam("Storage_Path");

			// Make sure storagePath only contains the root folder, not several folders
			// In other words, if storagePath = "VOrbiETD03\2011_4" change it to just "VOrbiETD03"
			int slashLoc = storagePath.IndexOf(Path.DirectorySeparatorChar);
			if (slashLoc > 0)
				storagePath = storagePath.Substring(0, slashLoc);

			// Always use the UNC path defined by Storage_Vol_External when checking drive free space
			// Example path is: \\Proto-7\
			string datasetStoragePathBase = m_Task.GetParam("Storage_Vol_External");

			datasetStoragePathBase = Path.Combine(datasetStoragePathBase, storagePath);

			return datasetStoragePathBase;

		}

		/// <summary>
		/// Validates that the dataset storage drive has sufficient free space
		/// </summary>
		/// <param name="ErrorMessage"></param>
		/// <returns>True if OK; false if not enough free space</returns>
		protected bool ValidateFreeDiskSpace(out string ErrorMessage)
		{
			const int DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 30;

			string datasetStoragePath = string.Empty;
			ErrorMessage = string.Empty;

			try
			{
				string stepToolLCase = m_StepTool.ToLower();

				if (stepToolLCase.Contains("archiveupdate") ||
					stepToolLCase.Contains("datasetarchive") ||
					stepToolLCase.Contains("sourcefilerename"))
				{

					// We don't need to validate free space with these step tools
					return true;
				}

				datasetStoragePath = GetStoragePathBase();

				long freeBytesAvailableToUser;
				long totalDriveCapacityBytes;
				long totalNumberOfFreeBytes;
				if (GetDiskFreeSpace(datasetStoragePath, out freeBytesAvailableToUser, out totalDriveCapacityBytes, out totalNumberOfFreeBytes))
				{
					double freeSpaceGB = totalNumberOfFreeBytes / 1024.0 / 1024.0 / 1024.0;

					if (freeSpaceGB < DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB)
					{
						ErrorMessage = "Dataset directory drive has less than " + DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB.ToString("0") + "GB free: " + freeSpaceGB.ToString("0.00") + " GB available";

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage + ": " + datasetStoragePath);
						return false;
					}

				}
				else
				{					
					ErrorMessage = "Error validating dataset storage free drive space (GetDiskFreeSpaceEx returned false): " + datasetStoragePath;
					if (Environment.MachineName.ToUpper().StartsWith("MONROE"))
					{
						Console.WriteLine("Warning: " + ErrorMessage);
						return true;
					}

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage);
					return false;
				}

			}
			catch (Exception ex)
			{
				ErrorMessage = "Exception validating dataset storage free drive space: " + datasetStoragePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage + "; " + ex.Message);
				return false;
			}

			return true;
		}

		/// <summary>
		/// Verifies working directory is properly specified
		/// </summary>
		/// <returns>TRUE for success, FALSE otherwise</returns>
		private bool ValidateWorkingDir()
		{
			string workingDir = m_MgrSettings.GetParam("WorkDir");

			if (!Directory.Exists(workingDir))
			{
				const string alternateWorkDir = @"E:\CapMan_WorkDir";

				if (Directory.Exists(alternateWorkDir))
				{
					// Auto-update the working directory
					m_MgrSettings.SetParam("WorkDir", alternateWorkDir);

					string msg = "Invalid working directory: " + workingDir + "; automatically switched to " + alternateWorkDir;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				else
				{
					string msg = "Invalid working directory: " + workingDir;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

					return false;
				}
			}

			// No problem found
			return true;
		}
		
		#endregion

		#region "Event handlers"
		private void FileWatcherChanged(object sender, FileSystemEventArgs e)
		{
			const string msg = "clsMainProgram.FileWatcherChanged event received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			m_ConfigChanged = true;
			m_FileWatcher.EnableRaisingEvents = false;
		}

		private void OnBroadcastReceived(string cmdText)
		{
			string msg = "clsMainProgram.OnBroadcasetReceived event; message = " + cmdText;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			clsBroadcastCmd recvCmd;

			// Parse the received message
			try
			{
				recvCmd = clsXMLTools.ParseBroadcastXML(cmdText);
			}
			catch (Exception Ex)
			{
				msg = "Exception while parsing broadcast data";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
				return;
			}

			// Determine if the message applies to this machine
			if (!recvCmd.MachineList.Contains(m_MgrName))
			{
				// Received command doesn't apply to this manager
				msg = "Received command not applicable to this manager instance";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				return;
			}

			// Get the command and take appropriate action
			switch (recvCmd.MachCmd.ToLower())
			{
				case "shutdown":
					m_LoopExitCode = LoopExitCode.ShutdownCmdReceived;
					m_Running = false;
					break;
				case "readconfig":
					msg = "Reload config message received";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_ConfigChanged = true;
					m_Running = false;
					break;
				default:
					// Invalid command received; do nothing except log it
					msg = "Invalid broadcast command received: " + cmdText;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					break;
			}
		}

		private void OnCommandReceived(string cmdText)
		{
			//TODO: (Future)
		}

		void OnStatusMonitorUpdateReceived(string msg)
		{
			if (m_MsgQueueInitSuccess)
				m_MsgHandler.SendMessage(msg);
		}

		/// <summary>
		/// Updates the status at m_StatusTimer interval
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		void m_StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
		{
			TimeSpan duration = DateTime.UtcNow - m_DurationStart;
			m_StatusFile.Duration = (Single)duration.TotalHours;
			m_StatusFile.WriteStatusFile();
		}
		#endregion
	}	// End class
}	// End namespace
