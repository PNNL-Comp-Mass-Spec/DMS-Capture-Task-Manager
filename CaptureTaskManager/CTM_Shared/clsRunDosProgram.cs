using System;
using System.Collections.Generic;

namespace CaptureTaskManager
{
	/// <summary>
	/// Provides a looping wrapper around a ProgRunner object for running command-line programs
	/// Ported from the Analysis Tool Manager
	/// </summary>
	public class clsRunDosProgram
	{

		#region "Module variables"
		private bool m_CreateNoWindow = true;
		private int m_MonitorInterval = 2000;		// Msec

		private int m_MaxRuntimeSeconds = 0;
		private string m_WorkDir;
		private int m_DebugLevel = 0;

		private int m_ExitCode = 0;
		private bool m_CacheStandardOutput = false;

		private bool m_EchoOutputToConsole = true;
		private bool m_WriteConsoleOutputToFile = false;

		private string m_ConsoleOutputFilePath = string.Empty;
		private bool m_AbortProgramNow;

		private bool m_AbortProgramPostLogEntry;
		
		//Runs specified program
		private PRISM.Processes.clsProgRunner m_ProgRunner;

		#endregion

		#region "Events"
		/// <summary>
		/// Class is waiting until next time it's due to check status of called program (good time for external processing)
		/// </summary>
		/// <remarks></remarks>
		public event LoopWaitingEventHandler LoopWaiting;
		public delegate void LoopWaitingEventHandler();

		/// <summary>
		/// Text was written to the console
		/// </summary>
		/// <param name="NewText"></param>
		/// <remarks></remarks>
		public event ConsoleOutputEventEventHandler ConsoleOutputEvent;
		public delegate void ConsoleOutputEventEventHandler(string NewText);

		/// <summary>
		/// Error message was written to the console
		/// </summary>
		/// <param name="NewText"></param>
		/// <remarks></remarks>
		public event ConsoleErrorEventEventHandler ConsoleErrorEvent;
		public delegate void ConsoleErrorEventEventHandler(string NewText);

		/// <summary>
		/// Program execution exceeded MaxRuntimeSeconds
		/// </summary>
		/// <remarks></remarks>
		public event TimeoutEventHandler Timeout;
		public delegate void TimeoutEventHandler();

		#endregion

		#region "Properties"

		/// <summary>
		/// Text written to the Console by the external program (including carriage returns)
		/// </summary>
		public string CachedConsoleOutput
		{
			get
			{
				if (m_ProgRunner == null)
				{
					return string.Empty;
				}
				else
				{
					return m_ProgRunner.CachedConsoleOutput;
				}
			}
		}

		/// <summary>
		/// Any text written to the Error buffer by the external program
		/// </summary>
		public string CachedConsoleError
		{
			get
			{
				if (m_ProgRunner == null)
				{
					return string.Empty;
				}
				else
				{
					return m_ProgRunner.CachedConsoleError;
				}
			}
		}

		/// <summary>
		/// When true then will cache the text the external program writes to the console
		/// Can retrieve using the CachedConsoleOutput readonly property
		/// Will also fire event ConsoleOutputEvent as new text is written to the console
		/// </summary>
		/// <remarks>If this is true, then no window will be shown, even if CreateNoWindow=False</remarks>
		public bool CacheStandardOutput
		{
			get { return m_CacheStandardOutput; }
			set { m_CacheStandardOutput = value; }
		}

		/// <summary>
		/// File path to which the console output will be written if WriteConsoleOutputToFile is true
		/// If blank, then file path will be auto-defined in the WorkDir  when program execution starts
		/// </summary>
		/// <value></value>
		/// <returns></returns>
		/// <remarks></remarks>
		public string ConsoleOutputFilePath
		{
			get { return m_ConsoleOutputFilePath; }
			set
			{
				if (value == null)
					value = string.Empty;
				m_ConsoleOutputFilePath = value;
			}
		}

		/// <summary>
		/// Determine if window should be displayed.
		/// Will be forced to True if CacheStandardOutput = True
		/// </summary>
		public bool CreateNoWindow
		{
			get { return m_CreateNoWindow; }
			set { m_CreateNoWindow = value; }
		}

		/// <summary>
		/// Debug level for logging
		/// </summary>
		public int DebugLevel
		{
			get { return m_DebugLevel; }
			set { m_DebugLevel = value; }
		}

		/// <summary>
		/// When true, then echoes, in real time, text written to the Console by the external program 
		/// Ignored if CreateNoWindow = False
		/// </summary>
		public bool EchoOutputToConsole
		{
			get { return m_EchoOutputToConsole; }
			set { m_EchoOutputToConsole = value; }
		}

		/// <summary>
		/// Exit code when process completes.
		/// </summary>
		public int ExitCode
		{
			get { return m_ExitCode; }
		}

		/// <summary>
		/// Maximum amount of time (seconds) that the program will be allowed to run; 0 if allowed to run indefinitely
		/// </summary>
		/// <value></value>
		public int MaxRuntimeSeconds
		{
			get { return m_MaxRuntimeSeconds; }
		}

		/// <summary>
		/// How often (milliseconds) internal monitoring thread checks status of external program
		/// Minimum allowed value is 250 milliseconds
		/// </summary>
		public int MonitorInterval
		{
			get { return m_MonitorInterval; }
			set
			{
				if (value < 250)
					value = 250;
				m_MonitorInterval = value;
			}
		}

		/// <summary>
		/// Returns true if program was aborted via call to AbortProgramNow()
		/// </summary>
		public bool ProgramAborted
		{
			get { return m_AbortProgramNow; }
		}

		/// <summary>
		/// Current monitoring state
		/// </summary>
		public PRISM.Processes.clsProgRunner.States State
		{
			get
			{
				if (m_ProgRunner == null)
				{
					return PRISM.Processes.clsProgRunner.States.NotMonitoring;
				}
				else
				{
					return m_ProgRunner.State;
				}
			}
		}

		/// <summary>
		/// Working directory for process execution.
		/// </summary>
		public string WorkDir
		{
			get { return m_WorkDir; }
			set { m_WorkDir = value; }
		}

		/// <summary>
		/// When true then will write the standard output to a file in real-time
		/// Will also fire event ConsoleOutputEvent as new text is written to the console
		/// Define the path to the file using property ConsoleOutputFilePath; if not defined, the file
		/// will be created in the WorkDir (though, if WorkDir is blank, then will be created in the folder with the Program we're running)
		/// </summary>
		/// <remarks>If this is true, then no window will be shown, even if CreateNoWindow=False</remarks>
		public bool WriteConsoleOutputToFile
		{
			get { return m_WriteConsoleOutputToFile; }
			set { m_WriteConsoleOutputToFile = value; }
		}
		#endregion

		#region "Methods"
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="WorkDir">Workdirectory for input/output files, if any</param>
		/// <remarks></remarks>

		public clsRunDosProgram(string WorkDir)
		{
			m_WorkDir = WorkDir;

		}

		/// <summary>
		/// Call this function to instruct this class to terminate the running program
		/// Will post an entry to the log
		/// </summary>
		public void AbortProgramNow()
		{
			AbortProgramNow(blnPostLogEntry: true);
		}

		/// <summary>
		/// Call this function to instruct this class to terminate the running program
		/// </summary>
		/// <param name="blnPostLogEntry">True if an entry should be posted to the log</param>
		/// <remarks></remarks>
		public void AbortProgramNow(bool blnPostLogEntry)
		{
			m_AbortProgramNow = true;
			m_AbortProgramPostLogEntry = blnPostLogEntry;
		}

		protected void AttachProgRunnerEvents()
		{
			try
			{
				m_ProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
				m_ProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
				m_ProgRunner.ProgChanged += ProgRunner_ProgChanged;
			}
			catch
			{
				// Ignore errors here
			}
		}

		protected void DetachProgRunnerEvents()
		{
			try
			{
				if (m_ProgRunner != null)
				{
					m_ProgRunner.ConsoleErrorEvent -= ProgRunner_ConsoleErrorEvent;
					m_ProgRunner.ConsoleOutputEvent -= ProgRunner_ConsoleOutputEvent;
					m_ProgRunner.ProgChanged -= ProgRunner_ProgChanged;
				}
			}
			catch
			{
				// Ignore errors here
			}
		}

		/// <summary>
		/// Runs a program and waits for it to exit
		/// </summary>
		/// <param name="ProgNameLoc">The path to the program to run</param>
		/// <param name="CmdLine">The arguments to pass to the program, for example /N=35</param>
		/// <param name="ProgName">The name of the program to use for the Window title</param>
		/// <returns>True if success, false if an error</returns>
		/// <remarks>Ignores the result code reported by the program</remarks>
		public bool RunProgram(string ProgNameLoc, string CmdLine, string ProgName)
		{
			bool UseResCode = false;
			return RunProgram(ProgNameLoc, CmdLine, ProgName, UseResCode);
		}

		public bool RunProgram(string ProgNameLoc, string CmdLine, string ProgName, bool UseResCode)
		{
			int MaxRuntimeSeconds = 0;
			return RunProgram(ProgNameLoc, CmdLine, ProgName, UseResCode, MaxRuntimeSeconds);
		}

		/// <summary>
		/// Runs a program and waits for it to exit
		/// </summary>
		/// <param name="ProgNameLoc">The path to the program to run</param>
		/// <param name="CmdLine">The arguments to pass to the program, for example /N=35</param>
		/// <param name="ProgName">The name of the program to use for the Window title</param>
		/// <param name="UseResCode">If true, then returns False if the ProgRunner ExitCode is non-zero</param>
		/// <param name="MaxRuntimeSeconds">If a positive number, then program execution will be aborted if the runtime exceeds MaxRuntimeSeconds</param>
		/// <returns>True if success, false if an error</returns>
		/// <remarks>MaxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
		public bool RunProgram(string ProgNameLoc, string CmdLine, string ProgName, bool UseResCode, int MaxRuntimeSeconds)
		{

			System.DateTime dtStartTime;
			bool blnRuntimeExceeded = false;
			bool blnAbortLogged = false;

			// Require a minimum monitoring interval of 250 mseconds
			if (m_MonitorInterval < 250)
				m_MonitorInterval = 250;

			if (MaxRuntimeSeconds > 0 && MaxRuntimeSeconds < 15)
			{
				MaxRuntimeSeconds = 15;
			}
			m_MaxRuntimeSeconds = MaxRuntimeSeconds;

			// Re-instantiate m_ProgRunner each time RunProgram is called since it is disposed of later in this function
			// Also necessary to avoid problems caching the console output
			m_ProgRunner = new PRISM.Processes.clsProgRunner();
			{
				m_ProgRunner.Arguments = CmdLine;
				m_ProgRunner.CreateNoWindow = m_CreateNoWindow;
				m_ProgRunner.MonitoringInterval = m_MonitorInterval;
				m_ProgRunner.Name = ProgName;
				m_ProgRunner.Program = ProgNameLoc;
				m_ProgRunner.Repeat = false;
				m_ProgRunner.RepeatHoldOffTime = 0;
				m_ProgRunner.WorkDir = m_WorkDir;
				m_ProgRunner.CacheStandardOutput = m_CacheStandardOutput;
				m_ProgRunner.EchoOutputToConsole = m_EchoOutputToConsole;

				m_ProgRunner.WriteConsoleOutputToFile = m_WriteConsoleOutputToFile;
				m_ProgRunner.ConsoleOutputFilePath = m_ConsoleOutputFilePath;
			}

			AttachProgRunnerEvents();

			if (m_DebugLevel >= 4)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Arguments = " + m_ProgRunner.Arguments);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Program = " + m_ProgRunner.Program);
			}

			m_AbortProgramNow = false;
			m_AbortProgramPostLogEntry = true;
			blnRuntimeExceeded = false;
			blnAbortLogged = false;
			dtStartTime = System.DateTime.UtcNow;

			try
			{
				// Start the program executing
				m_ProgRunner.StartAndMonitorProgram();

				// Loop until program is complete, or until m_MaxRuntimeSeconds seconds elapses
				// And (ProgRunner.State <> 10)
				while ((m_ProgRunner.State != PRISM.Processes.clsProgRunner.States.NotMonitoring))
				{
					if (LoopWaiting != null)
					{
						LoopWaiting();
					}
					System.Threading.Thread.Sleep(m_MonitorInterval);

					if (m_MaxRuntimeSeconds > 0)
					{
						if (System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > m_MaxRuntimeSeconds && !m_AbortProgramNow)
						{
							m_AbortProgramNow = true;
							blnRuntimeExceeded = true;
							if (Timeout != null)
							{
								Timeout();
							}
						}
					}

					if (m_AbortProgramNow)
					{
						if (m_AbortProgramPostLogEntry && !blnAbortLogged)
						{
							blnAbortLogged = true;
							if (blnRuntimeExceeded)
							{
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  Aborting ProgRunner since " + m_MaxRuntimeSeconds + " seconds has elapsed");
							}
							else
							{
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  Aborting ProgRunner since AbortProgramNow() was called");
							}
						}
						m_ProgRunner.StopMonitoringProgram(Kill: true);
					}
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception running DOS program " + ProgNameLoc + "; " + clsErrors.GetExceptionStackTrace(ex));
				DetachProgRunnerEvents();
				m_ProgRunner = null;
				return false;
			}

			// Cache the exit code in m_ExitCode
			m_ExitCode = m_ProgRunner.ExitCode;
			DetachProgRunnerEvents();
			m_ProgRunner = null;

			if ((UseResCode & m_ExitCode != 0))
			{
				if ((m_AbortProgramNow && m_AbortProgramPostLogEntry) || !m_AbortProgramNow)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  ProgRunner.ExitCode = " + m_ExitCode.ToString() + " for Program = " + ProgNameLoc);
				}
				return false;
			}

			if (m_AbortProgramNow)
			{
				return false;
			}
			else
			{
				return true;
			}

		}
		#endregion

		private void ProgRunner_ConsoleErrorEvent(string NewText)
		{
			if (ConsoleErrorEvent != null)
			{
				ConsoleErrorEvent(NewText);
			}
			Console.WriteLine("Console error: " + Environment.NewLine + NewText);
		}

		private void ProgRunner_ConsoleOutputEvent(string NewText)
		{
			if (ConsoleOutputEvent != null)
			{
				ConsoleOutputEvent(NewText);
			}
		}

		private void ProgRunner_ProgChanged(PRISM.Processes.clsProgRunner obj)
		{
			// This event is ignored by this class
		}

	}

}
