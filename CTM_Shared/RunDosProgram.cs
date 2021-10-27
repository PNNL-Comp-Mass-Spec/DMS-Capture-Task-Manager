using PRISM;
using System;
using System.IO;

namespace CaptureTaskManager
{
    /// <summary>
    /// Provides a looping wrapper around a ProgRunner object for running command-line programs
    /// Ported from the Analysis Tool Manager
    /// </summary>
    public class RunDosProgram : EventNotifier
    {
        public const string RUN_PROGRAM_STATUS_LINE = "RunProgram";

        /// <summary>
        /// Monitor interval, in milliseconds
        /// </summary>
        private int mMonitorInterval = 2000;

        private bool mAbortProgramPostLogEntry;

        /// <summary>
        /// Program runner
        /// </summary>
        private ProgRunner mProgRunner;

        private DateTime mStopTime;

        private bool mIsRunning;

        /// <summary>
        /// Class is waiting until next time it's due to check status of called program (good time for external processing)
        /// </summary>
        public event LoopWaitingEventHandler LoopWaiting;

        /// <summary>
        /// Delegate for LoopWaitingEventHandler
        /// </summary>
        public delegate void LoopWaitingEventHandler();

        /// <summary>
        /// Text that was written to the console
        /// </summary>
        public event ConsoleOutputEventEventHandler ConsoleOutputEvent;

        /// <summary>
        /// Delegate for ConsoleOutputEventEventHandler
        /// </summary>
        /// <param name="newText"></param>
        public delegate void ConsoleOutputEventEventHandler(string newText);

        /// <summary>
        /// Program execution exceeded MaxRuntimeSeconds
        /// </summary>
        public event TimeoutEventHandler Timeout;

        /// <summary>
        /// Delegate for TimeoutEventHandler
        /// </summary>
        public delegate void TimeoutEventHandler();

        /// <summary>
        /// Text written to the Console by the external program (including carriage returns)
        /// </summary>
        // ReSharper disable once UnusedMember.Global
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
        // ReSharper disable once UnusedMember.Global
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
        /// Can retrieve using the CachedConsoleOutput read-only property
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
        public string ConsoleOutputFilePath { get; set; } = string.Empty;

        /// <summary>
        /// Determine if window should be displayed.
        /// Will be forced to True if CacheStandardOutput = True
        /// </summary>
        public bool CreateNoWindow { get; set; } = true;

        /// <summary>
        /// Debug level for logging
        /// </summary>
        public int DebugLevel { get; set; }

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
        public int MaxRuntimeSeconds { get; private set; }

        /// <summary>
        /// How often (milliseconds) internal monitoring thread checks status of external program
        /// Minimum allowed value is 250 milliseconds
        /// </summary>
        public int MonitorInterval
        {
            get => mMonitorInterval;
            set
            {
                if (value < 250)
                {
                    value = 250;
                }

                mMonitorInterval = value;
            }
        }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        // ReSharper disable once UnusedMember.Global
        public int ProcessID
        {
            get
            {
                if (mProgRunner == null)
                {
                    return 0;
                }

                return mProgRunner.PID;
            }
        }

        /// <summary>
        /// Returns true if program was aborted via call to AbortProgramNow()
        /// </summary>
        public bool ProgramAborted { get; private set; }

        /// <summary>
        /// Time that the program runner has been running for (or time that it ran if finished)
        /// </summary>
        public TimeSpan RunTime => StopTime.Subtract(StartTime);

        /// <summary>
        /// Time the program runner started (UTC-based)
        /// </summary>
        public DateTime StartTime { get; private set; }

        /// <summary>
        /// Time the program runner finished (UTC-based)
        /// </summary>
        /// <remarks>Will be the current time-of-day if still running</remarks>
        public DateTime StopTime => mIsRunning ? DateTime.UtcNow : mStopTime;

        /// <summary>
        /// Current monitoring state
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public ProgRunner.States State
        {
            get
            {
                if (mProgRunner == null)
                {
                    return ProgRunner.States.NotMonitoring;
                }

                return mProgRunner.State;
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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDir">Work directory for input/output files, if any</param>
        /// <param name="debugLevel">Debug level (Higher values mean more log messages)</param>
        public RunDosProgram(string workDir, int debugLevel = 1)
        {
            WorkDir = workDir;
            DebugLevel = debugLevel;
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// Will post an entry to the log
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public void AbortProgramNow()
        {
            AbortProgramNow(postLogEntry: true);
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// </summary>
        /// <param name="postLogEntry">True if an entry should be posted to the log</param>
        public void AbortProgramNow(bool postLogEntry)
        {
            mAbortProgramPostLogEntry = postLogEntry;
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
        /// <remarks>Ignores the result code reported by the program</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public bool RunProgram(string executablePath, string arguments, string progName)
        {
            const bool useResCode = false;
            return RunProgram(executablePath, arguments, progName, useResCode);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <remarks>Ignores the result code reported by the program</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example: /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">Whether or not to use the result code to determine success or failure of program execution</param>
        /// <returns>True if success, false if an error</returns>
        public bool RunProgram(string executablePath, string arguments, string progName, bool useResCode)
        {
            const int maxRuntimeSeconds = 0;
            return RunProgram(executablePath, arguments, progName, useResCode, maxRuntimeSeconds);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <remarks>maxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">If true, returns False if the ProgRunner ExitCode is non-zero</param>
        /// <param name="maxRuntimeSeconds">If a positive number, program execution will be aborted if the runtime exceeds maxRuntimeSeconds</param>
        /// <returns>True if success, false if an error</returns>
        public bool RunProgram(string executablePath, string arguments, string progName, bool useResCode, int maxRuntimeSeconds)
        {
            // Require a minimum monitoring interval of 250 milliseconds
            if (mMonitorInterval < 250)
            {
                mMonitorInterval = 250;
            }

            if (maxRuntimeSeconds is > 0 and < 15)
            {
                maxRuntimeSeconds = 15;
            }
            MaxRuntimeSeconds = maxRuntimeSeconds;

            if (executablePath.StartsWith("/") && Path.DirectorySeparatorChar == '\\')
            {
                // Log a warning
                OnWarningEvent("Unix-style path on a Windows machine; program execution may fail: " + executablePath);
            }

            // Re-instantiate mProgRunner each time RunProgram is called since it is disposed of later in this function
            // Also necessary to avoid problems caching the console output
            mProgRunner = new ProgRunner
            {
                Arguments = arguments,
                CreateNoWindow = CreateNoWindow,
                MonitoringInterval = mMonitorInterval,
                Name = progName,
                Program = executablePath,
                Repeat = false,
                RepeatHoldOffTime = 0,
                WorkDir = WorkDir,
                CacheStandardOutput = CacheStandardOutput,
                EchoOutputToConsole = EchoOutputToConsole,
                WriteConsoleOutputToFile = WriteConsoleOutputToFile,
                ConsoleOutputFilePath = ConsoleOutputFilePath,
                ConsoleOutputFileIncludesCommandLine = ConsoleOutputFileIncludesCommandLine
            };

            RegisterEvents(mProgRunner);

            mProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
            mProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
            mProgRunner.ProgChanged += ProgRunner_ProgChanged;

            OnStatusEvent(RUN_PROGRAM_STATUS_LINE + " " + mProgRunner.Program + " " + mProgRunner.Arguments);

            mAbortProgramPostLogEntry = true;
            ProgramAborted = false;

            var runtimeExceeded = false;
            var abortLogged = false;

            try
            {
                // Start the program executing
                mProgRunner.StartAndMonitorProgram();

                StartTime = DateTime.UtcNow;
                mStopTime = DateTime.MinValue;
                mIsRunning = true;

                // Loop until program is complete, or until MaxRuntimeSeconds seconds elapses
                while (mProgRunner.State != ProgRunner.States.NotMonitoring)
                {
                    OnLoopWaiting();
                    ProgRunner.SleepMilliseconds(mMonitorInterval);

                    if (MaxRuntimeSeconds > 0)
                    {
                        if (RunTime.TotalSeconds > MaxRuntimeSeconds && !ProgramAborted)
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

                    if (mAbortProgramPostLogEntry && !abortLogged)
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

                    mProgRunner.StopMonitoringProgram(kill: true);
                } // end while

                mStopTime = DateTime.UtcNow;
                mIsRunning = false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception running external program " + executablePath, ex);
                mProgRunner = null;

                mStopTime = DateTime.UtcNow;
                mIsRunning = false;

                return false;
            }

            // Cache the exit code in ExitCode
            ExitCode = mProgRunner.ExitCode;
            mProgRunner = null;

            if (useResCode && ExitCode != 0)
            {
                if (ProgramAborted && mAbortProgramPostLogEntry || !ProgramAborted)
                {
                    OnErrorEvent("  ProgRunner.ExitCode = " + ExitCode + " for Program = " + executablePath);
                }
                return false;
            }

            return !ProgramAborted;
        }

        private void ProgRunner_ConsoleErrorEvent(string newText)
        {
            OnErrorEvent("Console error: " + newText);
        }

        private void ProgRunner_ConsoleOutputEvent(string newText)
        {
            ConsoleOutputEvent?.Invoke(newText);
        }

        private void ProgRunner_ProgChanged(ProgRunner obj)
        {
            // This event is ignored by this class
        }
    }
}
