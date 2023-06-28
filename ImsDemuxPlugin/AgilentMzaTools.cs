using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using CaptureTaskManager;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// This class converts a .D directory to a .mza file
    /// </summary>
    public class AgilentMzaTools : EventNotifier
    {
        // Ignore Spelling: Demux, IMS, MZA

        // Set the max runtime at 12 hours
        private const int MAX_RUNTIME_MINUTES = 12 * 60;

        /// <summary>
        /// Full path to mza.exe
        /// </summary>
        private readonly string mMzaConverterPath;

        /// <summary>
        /// MZA console output file
        /// </summary>
        private string mConsoleOutputFilePath;

        private DateTime mLastProgressUpdateTime;
        private DateTime mLastProgressMessageTime;

        private DateTime mStartTime;
        private readonly List<string> mLoggedConsoleOutputErrors;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mzaConverterPath"></param>
        public AgilentMzaTools(string mzaConverterPath)
        {
            mMzaConverterPath = mzaConverterPath;

            mLoggedConsoleOutputErrors = new List<string>();
        }

        /// <summary>
        /// Converts a .D directory to a .mza file
        /// </summary>
        /// <remarks>The .mza file will be created in the parent directory of the .D directory</remarks>
        /// <param name="workingDirectoryPath">Directory where the console output file should be created</param>
        /// <param name="returnData"></param>
        /// <param name="dotDDirectory">Full path to the .D directory</param>
        public void ConvertDataset(
            string workingDirectoryPath,
            ToolReturnData returnData,
            DirectoryInfo dotDDirectory)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mMzaConverterPath))
                {
                    returnData.CloseoutMsg = "Field mMzaConverterPath is undefined";
                    OnErrorEvent(returnData.CloseoutMsg);
                    return;
                }

                var workingDirectory = new DirectoryInfo(workingDirectoryPath);

                // Construct the command line arguments

                // Input file
                var arguments = string.Format("-file {0} -intensityThreshold 20", Conversion.PossiblyQuotePath(dotDDirectory.FullName));

                mConsoleOutputFilePath = Path.Combine(workingDirectory.FullName, "Mza_ConsoleOutput.txt");

                OnStatusEvent(mMzaConverterPath + " " + arguments);
                var cmdRunner = new RunDosProgram(workingDirectory.FullName);

                mStartTime = DateTime.UtcNow;
                mLastProgressUpdateTime = DateTime.UtcNow;
                mLastProgressMessageTime = DateTime.UtcNow;

                AttachCmdRunnerEvents(cmdRunner);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;

                // Create a console output file
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = mConsoleOutputFilePath;

                var success = cmdRunner.RunProgram(mMzaConverterPath, arguments, "MZA", true, MAX_RUNTIME_MINUTES * 60);

                ParseConsoleOutputFile();

                if (success)
                {
                    return;
                }

                returnData.CloseoutMsg = "Error running the MZA Converter";
                OnErrorEvent(returnData.CloseoutMsg);

                if (cmdRunner.ExitCode != 0)
                {
                    OnWarningEvent("MZA returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    OnWarningEvent("Call to MZA failed (but exit code is 0)");
                }
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception in ConvertDataset";
                OnErrorEvent(returnData.CloseoutMsg, ex);
            }
        }

        private void ParseConsoleOutputFile()
        {
            // ReSharper disable CommentTypo

            // Example Console output:
            //
            // Processing file: PNACIC_EPA_0776_Ac-APPI_POS_Test_24Aug22_Tahiti_Infusion.d
            //      Total time (min): 0.51

            try
            {
                if (string.IsNullOrEmpty(mConsoleOutputFilePath))
                {
                    return;
                }

                using var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.StartsWith("Error in") ||
                        dataLine.StartsWith("Error:") ||
                        dataLine.StartsWith("Exception"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnErrorEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }
                    }
                    else if (dataLine.StartsWith("Warning:"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnWarningEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (!mLoggedConsoleOutputErrors.Contains(ex.Message))
                {
                    OnErrorEvent("Exception in ParseConsoleOutputFile", ex);
                    mLoggedConsoleOutputErrors.Add(ex.Message);
                }
            }
        }

        private void AttachCmdRunnerEvents(RunDosProgram cmdRunner)
        {
            try
            {
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
                cmdRunner.Timeout += CmdRunner_Timeout;
            }
            catch
            {
                // Ignore errors here
            }
        }

        private void CmdRunner_Timeout()
        {
            OnErrorEvent("CmdRunner timeout reported");
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < 30)
            {
                return;
            }

            mLastProgressUpdateTime = DateTime.UtcNow;

            ParseConsoleOutputFile();

            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds >= 90)
            {
                mLastProgressMessageTime = DateTime.UtcNow;
                OnDebugEvent("{0} running; {1:F1} minutes elapsed",
                    "MZA",
                    DateTime.UtcNow.Subtract(mStartTime).TotalMinutes);
            }
        }
    }
}
