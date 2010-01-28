
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
using System.Linq;
using System.Text;
using System.IO;
using System.Windows.Forms;

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
				ExcessiveErrors,
				InvalidWorkDir,
				ShutdownCmdReceived
			}
		#endregion

		#region "Constants"
			private const int MAX_ERROR_COUNT = 4;
		#endregion

		#region "Class variables"
			private clsMgrSettings m_MgrSettings;
			private clsCaptureTask m_Task;
			private FileSystemWatcher m_FileWatcher;
			private IToolRunner m_CapTool;
			private bool m_ConfigChanged = false;
			private int m_ErrorCount = 0;
			private IStatusFile m_StatusFile;
			private clsMessageHandler m_MsgHandler;
			private LoopExitCode m_LoopExitCode;
			private bool m_Running;
			private System.Timers.Timer m_StatusTimer;
			private DateTime m_DurationStart;
		#endregion

		#region "Delegates"
		#endregion

		#region "Events"
		#endregion

		#region "Properties"
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			public clsMainProgram()
			{
				// Does nothing at present
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Initializes the manager
			/// </summary>
			/// <returns>TRUE for success; FALSE otherwise</returns>
			public bool InitMgr()
			{
				string msg;

				// Get the manager settings
				try
				{
					m_MgrSettings = new clsMgrSettings();
				}
				catch (Exception ex)
				{
					// Failures are logged by clsMgrSettings
					return false;
				}

				// Set up the loggers
				string logFileName = m_MgrSettings.GetParam("logfilename");
				int debugLevel = int.Parse(m_MgrSettings.GetParam("debuglevel"));
				clsLogTools.CreateFileLogger(logFileName, debugLevel);
				string logCnStr = m_MgrSettings.GetParam("connectionstring");
				string moduleName = m_MgrSettings.GetParam("modulename");
				clsLogTools.CreateDbLogger(logCnStr, moduleName);

				// Make initial log entry
				msg = "=== Started Capture Task Manager V" + Application.ProductVersion + " ===== ";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

				// Setup the message queue
				m_MsgHandler = new clsMessageHandler();
				m_MsgHandler.BrokerUri = m_MsgHandler.BrokerUri = m_MgrSettings.GetParam("MessageQueueURI");
				m_MsgHandler.CommandQueueName = m_MgrSettings.GetParam("ControlQueueName");
				m_MsgHandler.BroadcastTopicName = m_MgrSettings.GetParam("BroadcastQueueTopic");
				m_MsgHandler.StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");
				m_MsgHandler.MgrSettings = m_MgrSettings;
				if (!m_MsgHandler.Init())
				{
					// Most error messages provided by .Init method, but debug message is here for program tracking
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler init error");
					return false;
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler initialized");
				}

				//Connect message handler events
				m_MsgHandler.CommandReceived += new MessageProcessorDelegate(OnCommandReceived);
				m_MsgHandler.BroadcastReceived += new MessageProcessorDelegate(OnBroadcastReceived);

				// Setup a file watcher for the config file
				FileInfo fInfo = new FileInfo(Application.ExecutablePath);
				m_FileWatcher = new FileSystemWatcher();
				m_FileWatcher.BeginInit();
				m_FileWatcher.Path = fInfo.DirectoryName;
				m_FileWatcher.IncludeSubdirectories = false;
				m_FileWatcher.Filter = m_MgrSettings.GetParam("configfilename");
				m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
				m_FileWatcher.EndInit();
				m_FileWatcher.EnableRaisingEvents = true;

				// Subscribe to the file watcher Changed event
				m_FileWatcher.Changed += new FileSystemEventHandler(FileWatcherChanged);

				// Set up the tool for getting tasks
				m_Task = new clsCaptureTask(m_MgrSettings);

				// Set up the status file class
				string statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
				m_StatusFile = new clsStatusFile(statusFileNameLoc);
				m_StatusFile.MonitorUpdateRequired += new StatusMonitorUpdateReceived(OnStatusMonitorUpdateReceived);
				m_StatusFile.LogToMsgQueue=bool.Parse(m_MgrSettings.GetParam("LogStatusToMessageQueue"));
				m_StatusFile.MgrName=m_MgrSettings.GetParam("MgrName");
				m_StatusFile.MgrStatus=EnumMgrStatus.Running;
				m_StatusFile.WriteStatusFile();

				// Set up the status reporting time
				m_StatusTimer = new System.Timers.Timer();
				m_StatusTimer.BeginInit();
				m_StatusTimer.Enabled = false;
				m_StatusTimer.Interval = 60000;	// 1 minute
				m_StatusTimer.EndInit();
				m_StatusTimer.Elapsed += new System.Timers.ElapsedEventHandler(m_StatusTimer_Elapsed);
				// Everything worked!
				return true;
			}

			/// <summary>
			/// Main loop for task performance
			/// </summary>
			/// <returns>TRUE if loop exits and manager restart is OK, FALSE otherwise</returns>
			public bool PerformMainLoop()
			{
				bool restartOK = true;
				int taskCount = 1;
				int maxTaskCount = int.Parse(m_MgrSettings.GetParam("maxrepetitions"));
				m_Running=true;
				string msg;

				// Begin main execution loop
				while (m_Running)
				{
					// Check for configuration change
					if (m_ConfigChanged)
					{
						m_LoopExitCode = LoopExitCode.ConfigChanged;
						m_Running = false;
						break;
					}

					// Check if manager is still active
					if (!bool.Parse(m_MgrSettings.GetParam("mgractive")))
					{
						// Disabled via manager control db
						m_Running = false;
						m_LoopExitCode = LoopExitCode.DisabledMC;
						break;
					}
					if (!bool.Parse(m_MgrSettings.GetParam("mgractive_local")))
					{
						m_Running = false;
						m_LoopExitCode = LoopExitCode.DisabledLocally;
						break;
					}

					// Check for excessive number of errors
					if (m_ErrorCount > MAX_ERROR_COUNT)
					{
						m_Running = false;
						m_LoopExitCode = LoopExitCode.ExcessiveErrors;
						break;
					}

					// Check working directory
					if (!ValidateWorkingDir())
					{
						m_Running = false;
						m_LoopExitCode = LoopExitCode.InvalidWorkDir;
						break;
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
							m_ErrorCount++;
							break;
						case EnumRequestTaskResult.TaskFound:
							msg = "Job " + m_Task.GetParam("Job") + ", step " + m_Task.GetParam("Step") + " assigned";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
							// Update the status
							m_StatusFile.JobNumber = int.Parse(m_Task.GetParam("Job"));
							m_StatusFile.Dataset = m_Task.GetParam("Dataset");
							m_StatusFile.MgrStatus = EnumMgrStatus.Running;
							m_StatusFile.Tool = m_Task.GetParam("StepTool");
							m_StatusFile.TaskStatus = EnumTaskStatus.Running;
							m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.Running_Tool;
							m_StatusFile.WriteStatusFile();

							// Create the tool runner object
							if (!SetToolRunnerObject())
							{
								msg = m_MgrSettings.GetParam("MgrName") + ": Unable to SetToolRunnerObject, job " + m_Task.GetParam("Job")
											+ ", Dataset " + m_Task.GetParam("Dataset");
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
								m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg);
								m_StatusFile.UpdateIdle();
								break;
							}

							// Run the tool plugin
							m_DurationStart = DateTime.Now;
							m_StatusTimer.Enabled = true;
							clsToolReturnData toolResult = m_CapTool.RunTool();
							m_StatusTimer.Enabled = false;

							switch (toolResult.CloseoutType)
							{
								case EnumCloseOutType.CLOSEOUT_FAILED:
									msg = m_MgrSettings.GetParam("MgrName") + ": Failure running tool " + m_Task.GetParam("StepTool")
												+ ", job " + m_Task.GetParam("Job") + ", Dataset " + m_Task.GetParam("Dataset");
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
									m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED,msg);
//									m_StatusFile.UpdateIdle();
									break;
								case EnumCloseOutType.CLOSEOUT_NOT_READY:
									msg = m_MgrSettings.GetParam("MgrName") + ": Dataset not ready, tool " + m_Task.GetParam("StepTool")
												+ ", job " + m_Task.GetParam("Job") + ", Dataset " + m_Task.GetParam("Dataset");
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
									m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_NOT_READY, "Dataset " + m_Task.GetParam("Dataset") + " not ready");
//									m_StatusFile.UpdateIdle();
									break;
								case EnumCloseOutType.CLOSEOUT_SUCCESS:
									msg = m_MgrSettings.GetParam("MgrName") + ": Step complete, tool " + m_Task.GetParam("StepTool")
												+ ", job " + m_Task.GetParam("Job") + ", Dataset " + m_Task.GetParam("Dataset");
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
									m_Task.CloseTask(toolResult.CloseoutType, toolResult.CloseoutMsg,toolResult.EvalCode,toolResult.EvalMsg);
									break;
								default:
									// Should never get here
									break;
							}	// End switch (toolResult)

							// Update the status
							m_StatusFile.JobNumber = 0;
							m_StatusFile.Duration = 0;
							m_StatusFile.Dataset = "";
							m_StatusFile.MgrStatus = EnumMgrStatus.Running;
							m_StatusFile.Progress = 0;
							m_StatusFile.Tool = "";
							m_StatusFile.TaskStatus = EnumTaskStatus.No_Task;
							m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.No_Task;
							m_StatusFile.WriteStatusFile();

							// Increment and test the task counter
							taskCount++;
							if (taskCount > int.Parse(m_MgrSettings.GetParam("maxrepetitions")))
							{
								m_Running = false;
								m_LoopExitCode = LoopExitCode.ExceededMaxTaskCount;
							}
							break;
						default:
							//Shouldn't ever get here!
							break;
					}	// End switch (taskReturn)
				}	// End while

				// Determine cause of loop exit and respond accordingly
				switch (m_LoopExitCode)
				{
					case LoopExitCode.ConfigChanged:
						// Reload the manager config
						msg = "Reloading configuration and restarting manager";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						// Unsubscribe message handler events and close msssage handler
						m_MsgHandler.BroadcastReceived -= OnBroadcastReceived;
						m_MsgHandler.CommandReceived -= OnCommandReceived;
						m_MsgHandler.Dispose();
						restartOK = true;
						break;
					case LoopExitCode.DisabledMC:
						// Manager is disabled via manager control db
						msg = "Manager disabled in manager control DB";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						m_StatusFile.UpdateDisabled(false);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.DisabledLocally:
						// Manager disabled locally
						msg = "Manager disabled locally";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						m_StatusFile.UpdateDisabled(true);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.ExcessiveErrors:
						// Too many errors
						msg = "Excessive errors; Manager is disabling itself";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,msg);
						if (!m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False"))
						{
							msg = "Error while disabling manager: " + m_MgrSettings.ErrMsg;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,msg);
						}
						m_StatusFile.UpdateDisabled(true);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.InvalidWorkDir:
						// Working directory not valid
						msg = "Working directory problem, disabling manager";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						if (!m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False"))
						{
							msg = "Error while disabling manager: " + m_MgrSettings.ErrMsg;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						}
						m_StatusFile.UpdateDisabled(true);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.NoTaskFound:
						// No capture task found
						msg = "No capture tasks found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						m_StatusFile.UpdateStopped(false);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.ShutdownCmdReceived:
						// Shutdown command received
						msg = "Shutdown command received, closing manager";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						m_StatusFile.UpdateStopped(false);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					case LoopExitCode.ExceededMaxTaskCount:
						// Max number of consecutive jobs reached
						msg = "Exceeded maximum job count, closing manager";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						m_StatusFile.UpdateStopped(false);
						msg = "===== Closing Capture Task Manager =====";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						restartOK = false;
						break;
					default:
						// Should never get here
						break;
				}	// End switch
				return restartOK;
			}	// End sub

			/// <summary>
			/// Verifies working directory is properly specified
			/// </summary>
			/// <returns>TRUE for success, FALSE otherwise</returns>
			private bool ValidateWorkingDir()
			{
				string workingDir = m_MgrSettings.GetParam("WorkDir");

				if (!Directory.Exists(workingDir))
				{
					string msg = "Invalid working directory: " + workingDir;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}

				// No problem found
				return true;
			}	// End sub

			/// <summary>
			/// Sets the tool runner object for this job
			/// </summary>
			/// <returns></returns>
			private bool SetToolRunnerObject()
			{
				string msg;
				string stepToolName = m_Task.GetParam("StepTool");

				// Load the tool runner
				m_CapTool = clsPluginLoader.GetToolRunner(stepToolName);
				if (m_CapTool == null)
				{
					msg = "Unable to load tool runner for StepTool " + stepToolName + ": " + clsPluginLoader.ErrMsg;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,msg);
					return false;
				}
				msg = "Loaded tool runner for Step Tool " + stepToolName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Setup the new tool runner
				m_CapTool.Setup(m_MgrSettings, m_Task, m_StatusFile);
				return true;
			}	// End sub
		#endregion

		#region "Event handlers"
			private void FileWatcherChanged(object sender, FileSystemEventArgs e)
			{
				string msg = "clsMainProgram.FileWatcherChanged event received";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				m_ConfigChanged = true;
				m_FileWatcher.EnableRaisingEvents = false;
			}	// End sub

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
				if (!recvCmd.MachineList.Contains(m_MgrSettings.GetParam("MgrName")))
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
			}	// End sub

			private void OnCommandReceived(string cmdText)
			{
				//TODO: (Future)
			}	// End sub

			void OnStatusMonitorUpdateReceived(string msg)
			{
				m_MsgHandler.SendMessage(msg);
			}	// End sub

			/// <summary>
			/// Updates the status at m_StatusTimer interval
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
			void m_StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
			{
				TimeSpan duration = DateTime.Now - m_DurationStart;
				int durationMinutes = duration.Minutes;
				m_StatusFile.Duration = durationMinutes / 60f;
				m_StatusFile.WriteStatusFile();
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
