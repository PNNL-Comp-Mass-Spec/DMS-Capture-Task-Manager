using System;

namespace CaptureTaskManager
{
    /// <summary>
    /// Provides a looping wrapper around a ProgRunner object for running command-line programs
    /// Ported from the Analysis Tool Manager
    /// </summary>
    public class clsRunDosProgram
    {
        #region "Module variables"

        private bool mCreateNoWindow = true;
        private int mMonitorInterval = 2000; // Msec

        private string mWorkDir;
        private int mDebugLevel;

        private bool mCacheStandardOutput;

        private bool mEchoOutputToConsole = true;
        private bool mWriteConsoleOutputToFile;

        private string mConsoleOutputFilePath = string.Empty;

        private bool mAbortProgramPostLogEntry;

        //Runs specified program
        private PRISM.Processes.clsProgRunner mProgRunner;

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

        public delegate void ConsoleOutputEventEventHandler(string NewText);

        /// <summary>
        /// Error message that was written to the console
        /// </summary>
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
                if (mProgRunner == null)
                {
                    return string.Empty;
                }

                return mProgRunner.CachedConsoleOutput;
            }
        }

        /// <summary>
        /// Any text written to the Error buffer by the external program
        /// </summary>
        public string CachedConsoleError
        {
            get
            {
                if (mProgRunner == null)
                {
                    return string.Empty;
                }

                return mProgRunner.CachedConsoleError;
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
            get { return mCacheStandardOutput; }
            set { mCacheStandardOutput = value; }
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
            get { return mConsoleOutputFilePath; }
            set
            {
                if (value == null)
                    value = string.Empty;
                mConsoleOutputFilePath = value;
            }
        }

        /// <summary>
        /// Determine if window should be displayed.
        /// Will be forced to True if CacheStandardOutput = True
        /// </summary>
        public bool CreateNoWindow
        {
            get { return mCreateNoWindow; }
            set { mCreateNoWindow = value; }
        }

        /// <summary>
        /// Debug level for logging
        /// </summary>
        public int DebugLevel
        {
            get { return mDebugLevel; }
            set { mDebugLevel = value; }
        }

        /// <summary>
        /// When true, then echoes, in real time, text written to the Console by the external program 
        /// Ignored if CreateNoWindow = False
        /// </summary>
        public bool EchoOutputToConsole
        {
            get { return mEchoOutputToConsole; }
            set { mEchoOutputToConsole = value; }
        }

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
            get { return mMonitorInterval; }
            set
            {
                if (value < 250)
                    value = 250;
                mMonitorInterval = value;
            }
        }

        /// <summary>
        /// Returns true if program was aborted via call to AbortProgramNow()
        /// </summary>
        public bool ProgramAborted { get; private set; }

        /// <summary>
        /// Current monitoring state
        /// </summary>
        public PRISM.Processes.clsProgRunner.States State
        {
            get
            {
                if (mProgRunner == null)
                {
                    return PRISM.Processes.clsProgRunner.States.NotMonitoring;
                }

                return mProgRunner.State;
            }
        }

        /// <summary>
        /// Working directory for process execution.
        /// </summary>
        public string WorkDir
        {
            get { return mWorkDir; }
            set { mWorkDir = value; }
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
            get { return mWriteConsoleOutputToFile; }
            set { mWriteConsoleOutputToFile = value; }
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
            mWorkDir = WorkDir;
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
            ProgramAborted = true;
            mAbortProgramPostLogEntry = blnPostLogEntry;
        }

        protected void AttachProgRunnerEvents()
        {
            try
            {
                mProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
                mProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
                mProgRunner.ProgChanged += ProgRunner_ProgChanged;
            }
                // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        protected void DetachProgRunnerEvents()
        {
            try
            {
                if (mProgRunner != null)
                {
                    mProgRunner.ConsoleErrorEvent -= ProgRunner_ConsoleErrorEvent;
                    mProgRunner.ConsoleOutputEvent -= ProgRunner_ConsoleOutputEvent;
                    mProgRunner.ProgChanged -= ProgRunner_ProgChanged;
                }
            }
                // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example: /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Ignores the result code reported by the program</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName)
        {
            const bool UseResCode = false;
            return RunProgram(progNameLoc, cmdLine, progName, UseResCode);
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
        /// <param name="UseResCode">If true, then returns False if the ProgRunner ExitCode is non-zero</param>
        /// <param name="maxRuntimeSeconds">If a positive number, then program execution will be aborted if the runtime exceeds MaxRuntimeSeconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>maxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName, bool UseResCode, int maxRuntimeSeconds)
        {
            // Require a minimum monitoring interval of 250 mseconds
            if (mMonitorInterval < 250)
                mMonitorInterval = 250;

            if (maxRuntimeSeconds > 0 && maxRuntimeSeconds < 15)
            {
                maxRuntimeSeconds = 15;
            }
            MaxRuntimeSeconds = maxRuntimeSeconds;

            // Re-instantiate mProgRunner each time RunProgram is called since it is disposed of later in this function
            // Also necessary to avoid problems caching the console output
            mProgRunner = new PRISM.Processes.clsProgRunner();
            {
                mProgRunner.Arguments = cmdLine;
                mProgRunner.CreateNoWindow = mCreateNoWindow;
                mProgRunner.MonitoringInterval = mMonitorInterval;
                mProgRunner.Name = progName;
                mProgRunner.Program = progNameLoc;
                mProgRunner.Repeat = false;
                mProgRunner.RepeatHoldOffTime = 0;
                mProgRunner.WorkDir = mWorkDir;
                mProgRunner.CacheStandardOutput = mCacheStandardOutput;
                mProgRunner.EchoOutputToConsole = mEchoOutputToConsole;

                mProgRunner.WriteConsoleOutputToFile = mWriteConsoleOutputToFile;
                mProgRunner.ConsoleOutputFilePath = mConsoleOutputFilePath;
            }

            AttachProgRunnerEvents();

            if (mDebugLevel >= 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Arguments = " + mProgRunner.Arguments);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Program = " + mProgRunner.Program);
            }

            ProgramAborted = false;
            mAbortProgramPostLogEntry = true;
            var blnRuntimeExceeded = false;
            var blnAbortLogged = false;
            var dtStartTime = DateTime.UtcNow;

            try
            {
                // Start the program executing
                mProgRunner.StartAndMonitorProgram();

                // Loop until program is complete, or until mMaxRuntimeSeconds seconds elapses
                // And (ProgRunner.State <> 10)
                while ((mProgRunner.State != PRISM.Processes.clsProgRunner.States.NotMonitoring))
                {
                    LoopWaiting?.Invoke();
                    System.Threading.Thread.Sleep(mMonitorInterval);

                    if (MaxRuntimeSeconds > 0)
                    {
                        if (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > MaxRuntimeSeconds && !ProgramAborted)
                        {
                            ProgramAborted = true;
                            blnRuntimeExceeded = true;
                            Timeout?.Invoke();
                        }
                    }

                    if (!ProgramAborted)
                    {
                        continue;
                    }

                    if (mAbortProgramPostLogEntry && !blnAbortLogged)
                    {
                        blnAbortLogged = true;
                        if (blnRuntimeExceeded)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                                 "  Aborting ProgRunner since " + MaxRuntimeSeconds +
                                                 " seconds has elapsed");
                        }
                        else
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                                 "  Aborting ProgRunner since AbortProgramNow() was called");
                        }
                    }
                    mProgRunner.StopMonitoringProgram(Kill: true);
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "Exception running DOS program " + progNameLoc + "; " +
                                     clsErrors.GetExceptionStackTrace(ex));
                DetachProgRunnerEvents();
                mProgRunner = null;
                return false;
            }

            // Cache the exit code in mExitCode
            ExitCode = mProgRunner.ExitCode;
            DetachProgRunnerEvents();
            mProgRunner = null;

            if ((UseResCode & ExitCode != 0))
            {
                if ((ProgramAborted && mAbortProgramPostLogEntry) || !ProgramAborted)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         "  ProgRunner.ExitCode = " + ExitCode + " for Program = " + progNameLoc);
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

        private void ProgRunner_ConsoleErrorEvent(string NewText)
        {
            ConsoleErrorEvent?.Invoke(NewText);
            Console.WriteLine("Console error: " + Environment.NewLine + NewText);
        }

        private void ProgRunner_ConsoleOutputEvent(string NewText)
        {
            ConsoleOutputEvent?.Invoke(NewText);
        }

        private void ProgRunner_ProgChanged(PRISM.Processes.clsProgRunner obj)
        {
            // This event is ignored by this class
        }
    }
}
