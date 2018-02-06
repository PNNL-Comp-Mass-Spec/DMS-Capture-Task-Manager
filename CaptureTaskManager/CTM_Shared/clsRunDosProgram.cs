using System;
using System.Threading;
using PRISM;

namespace CaptureTaskManager
{
    /// <summary>
    /// Provides a looping wrapper around a ProgRunner object for running command-line programs
    /// Ported from the Analysis Tool Manager
    /// </summary>
    public class clsRunDosProgram : clsEventNotifier
    {
        #region "Module variables"

        // msec
        private int m_MonitorInterval = 2000;

        private bool m_AbortProgramPostLogEntry;

        // Runs specified program
        private clsProgRunner m_ProgRunner;

        #endregion

        #region "Events"

        /// <summary>
        /// Class is waiting until next time it's due to check status of called program (good time for external processing)
        /// </summary>
        /// <remarks></remarks>
        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        /// <summary>
        /// Text that was written to the console
        /// </summary>
        /// <remarks></remarks>
        public event ConsoleOutputEventEventHandler ConsoleOutputEvent;

        public delegate void ConsoleOutputEventEventHandler(string newText);

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

                return m_ProgRunner.CachedConsoleOutput;
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

                return m_ProgRunner.CachedConsoleError;
            }
        }

        /// <summary>
        /// When true then will cache the text the external program writes to the console
        /// Can retrieve using the CachedConsoleOutput readonly property
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// </summary>
        /// <remarks>If this is true, no window will be shown, even if CreateNoWindow=False</remarks>
        public bool CacheStandardOutput { get; set; } = false;

        /// <summary>
        /// When true, the program name and command line arguments will be added to the top of the console output file
        /// </summary>
        /// <remarks>Defaults to true</remarks>
        public bool ConsoleOutputFileIncludesCommandLine { get; set; } = true;

        /// <summary>
        /// File path to which the console output will be written if WriteConsoleOutputToFile is true
        /// If blank, file path will be auto-defined in the WorkDir  when program execution starts
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string ConsoleOutputFilePath { get; set; } = string.Empty;

        /// <summary>
        /// Determine if window should be displayed.
        /// Will be forced to True if CacheStandardOutput = True
        /// </summary>
        public bool CreateNoWindow { get; set; } = true;

        /// <summary>
        /// When true, echoes, in real time, text written to the Console by the external program
        /// Ignored if CreateNoWindow = False
        /// </summary>
        public bool EchoOutputToConsole { get; set; } = true;

        /// <summary>
        /// Exit code when process completes.
        /// </summary>
        public int ExitCode { get; private set; }

        /// <summary>
        /// Maximum amount of time (seconds) that the program will be allowed to run; 0 if allowed to run indefinitely
        /// </summary>
        /// <value></value>
        public int MaxRuntimeSeconds { get; private set; }

        /// <summary>
        /// How often (milliseconds) internal monitoring thread checks status of external program
        /// Minimum allowed value is 250 milliseconds
        /// </summary>
        public int MonitorInterval
        {
            get => m_MonitorInterval;
            set
            {
                if (value < 250)
                    value = 250;
                m_MonitorInterval = value;
            }
        }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        public int ProcessID
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return 0;
                }

                return m_ProgRunner.PID;
            }
        }

        /// <summary>
        /// Returns true if program was aborted via call to AbortProgramNow()
        /// </summary>
        public bool ProgramAborted { get; private set; }

        /// <summary>
        /// Current monitoring state
        /// </summary>
        public clsProgRunner.States State
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return clsProgRunner.States.NotMonitoring;
                }

                return m_ProgRunner.State;
            }
        }

        /// <summary>
        /// Working directory for process execution.
        /// </summary>
        public string WorkDir { get; set; }

        /// <summary>
        /// When true then will write the standard output to a file in real-time
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// Define the path to the file using property ConsoleOutputFilePath; if not defined, the file
        /// will be created in the WorkDir (though, if WorkDir is blank, will be created in the folder with the Program we're running)
        /// </summary>
        /// <remarks>
        /// Defaults to false
        /// If this is true, no window will be shown, even if CreateNoWindow=False
        /// </remarks>
        public bool WriteConsoleOutputToFile { get; set; } = false;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDir">Workdirectory for input/output files, if any</param>
        /// <param name="debugLevel">Debug level (Higher values mean more log messages)</param>
        /// <remarks></remarks>
        public clsRunDosProgram(string workDir, int debugLevel = 1)
        {
            WorkDir = workDir;
            DebugLevel = debugLevel;
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// Will post an entry to the log
        /// </summary>
        public void AbortProgramNow()
        {
            AbortProgramNow(postLogEntry: true);
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// </summary>
        /// <param name="postLogEntry">True if an entry should be posted to the log</param>
        /// <remarks></remarks>
        public void AbortProgramNow(bool postLogEntry)
        {
            m_AbortProgramPostLogEntry = postLogEntry;
            ProgramAborted = true;
        }

        private void OnLoopWaiting()
        {
            LoopWaiting?.Invoke();
        }

        private void OnTimeout()
        {
            Timeout?.Invoke();
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Ignores the result code reported by the program</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName)
        {
            const bool useResCode = false;
            return RunProgram(progNameLoc, cmdLine, progName, useResCode);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example: /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">Whether or not to use the result code to determine success or failure of program execution</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Ignores the result code reported by the program</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName, bool useResCode)
        {
            const int maxRuntimeSeconds = 0;
            return RunProgram(progNameLoc, cmdLine, progName, useResCode, maxRuntimeSeconds);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">If true, returns False if the ProgRunner ExitCode is non-zero</param>
        /// <param name="maxRuntimeSeconds">If a positive number, program execution will be aborted if the runtime exceeds maxRuntimeSeconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>maxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName, bool useResCode, int maxRuntimeSeconds)
        {
            // Require a minimum monitoring interval of 250 mseconds
            if (m_MonitorInterval < 250)
                m_MonitorInterval = 250;

            if (maxRuntimeSeconds > 0 && maxRuntimeSeconds < 15)
            {
                maxRuntimeSeconds = 15;
            }
            MaxRuntimeSeconds = maxRuntimeSeconds;

            // Re-instantiate m_ProgRunner each time RunProgram is called since it is disposed of later in this function
            // Also necessary to avoid problems caching the console output
            m_ProgRunner = new clsProgRunner
            {
                Arguments = cmdLine,
                CreateNoWindow = CreateNoWindow,
                MonitoringInterval = m_MonitorInterval,
                Name = progName,
                Program = progNameLoc,
                Repeat = false,
                RepeatHoldOffTime = 0,
                WorkDir = WorkDir,
                CacheStandardOutput = CacheStandardOutput,
                EchoOutputToConsole = EchoOutputToConsole,
                WriteConsoleOutputToFile = WriteConsoleOutputToFile,
                ConsoleOutputFilePath = ConsoleOutputFilePath,
                ConsoleOutputFileIncludesCommandLine = ConsoleOutputFileIncludesCommandLine
            };

            m_ProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
            m_ProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
            m_ProgRunner.ProgChanged += ProgRunner_ProgChanged;

            OnStatusEvent("RunProgram " + m_ProgRunner.Program + " " + m_ProgRunner.Arguments);

            m_AbortProgramPostLogEntry = true;
            ProgramAborted = false;

            var runtimeExceeded = false;
            var abortLogged = false;
            var startTime = DateTime.UtcNow;

            try
            {
                // Start the program executing
                m_ProgRunner.StartAndMonitorProgram();

                // Loop until program is complete, or until MaxRuntimeSeconds seconds elapses
                while (m_ProgRunner.State != clsProgRunner.States.NotMonitoring)
                {
                    OnLoopWaiting();
                    Thread.Sleep(m_MonitorInterval);

                    if (MaxRuntimeSeconds > 0)
                    {
                        if (DateTime.UtcNow.Subtract(startTime).TotalSeconds > MaxRuntimeSeconds && !ProgramAborted)
                        {
                            AbortProgramNow(false);
                            runtimeExceeded = true;
                            OnTimeout();
                        }
                    }

                    if (!ProgramAborted)
                    {
                        continue;
                    }

                    if (m_AbortProgramPostLogEntry && !abortLogged)
                    {
                        abortLogged = true;
                        string msg;
                        if (runtimeExceeded)
                        {
                            msg = "  Aborting ProgRunner for " + progName + " since " + MaxRuntimeSeconds + " seconds has elapsed";
                        }
                        else
                        {
                            msg = "  Aborting ProgRunner for " + progName + " since AbortProgramNow() was called";
                        }

                        OnErrorEvent(msg);
                    }
                    m_ProgRunner.StopMonitoringProgram(kill: true);

                }
            }
            catch (Exception ex)
            {
                var msg = "Exception running DOS program " + progNameLoc;
                OnErrorEvent(msg, ex);
                m_ProgRunner = null;
                return false;
            }

            // Cache the exit code in ExitCode
            ExitCode = m_ProgRunner.ExitCode;
            m_ProgRunner = null;

            if (useResCode & ExitCode != 0)
            {
                if (ProgramAborted && m_AbortProgramPostLogEntry || !ProgramAborted)
                {
                    var msg = "  ProgRunner.ExitCode = " + ExitCode + " for Program = " + progNameLoc;
                    OnErrorEvent(msg);
                }
                return false;
            }

            if (ProgramAborted)
            {
                return false;
            }

            return true;
        }

        #endregion

        private void ProgRunner_ConsoleErrorEvent(string newText)
        {
            OnErrorEvent("Console error: " + newText);
        }

        private void ProgRunner_ConsoleOutputEvent(string newText)
        {
            ConsoleOutputEvent?.Invoke(newText);
        }

        private void ProgRunner_ProgChanged(clsProgRunner obj)
        {
            // This event is ignored by this class
        }
    }

}
