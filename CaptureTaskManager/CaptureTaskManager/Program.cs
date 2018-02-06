//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using PRISM;
using PRISM.Logging;

namespace CaptureTaskManager
{
    /// <summary>
    /// Application entry class
    /// </summary>
    static class Program
    {
        private const string PROGRAM_DATE = "February 5, 2018";

        private static bool mCodeTestMode;
        private static bool mTraceMode;

        #region "Methods"

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <returns>0 if no error; error code if an error</returns>
        /// <remarks>The STAThread attribute is required for OxyPlot functionality</remarks>
        [STAThread]
        static int Main()
        {
            mCodeTestMode = false;

            var commandLineParser = new clsParseCommandLine();

            mTraceMode = false;

            var osVersionInfo = new clsOSVersionInfo();

            var osVersion = osVersionInfo.GetOSVersion();
            if (osVersion.IndexOf("windows", StringComparison.OrdinalIgnoreCase) < 0)
            {
                // Running on Linux
                // Auto-enable offline mode
                clsUtilities.EnableOfflineMode(true);
            }

            bool validArgs;

            // Look for /T or /Test on the command line
            // If present, this means "code test mode" is enabled
            if (commandLineParser.ParseCommandLine())
            {
                validArgs = SetOptionsUsingCommandLineParameters(commandLineParser);
            }
            else
            {
                if (commandLineParser.NoParameters)
                {
                    validArgs = true;
                }
                else
                {
                    if (commandLineParser.NeedToShowHelp)
                    {
                        ShowProgramHelp();
                    }
                    else
                    {
                        Console.WriteLine("Error parsing the command line arguments");
                    }
                    return -1;
                }
            }

            if (commandLineParser.NeedToShowHelp || !validArgs)
            {
                ShowProgramHelp();
                return -1;
            }

            ShowTraceMessage("Command line arguments parsed");

            // Note: CodeTestMode is enabled using command line switch /T
            if (mCodeTestMode)
            {
                try
                {
                    ShowTraceMessage("Code test mode enabled");

                    var testHarness = new clsCodeTest();

                    testHarness.TestConnection();

                }
                catch (Exception ex)
                {
                    ShowErrorMessage("Exception calling clsCodeTest", ex);
                    return -1;
                }

                ShowTraceMessage("Exiting application");

                clsParseCommandLine.PauseAtConsole(500);
                return 0;

            }

            // Initiate automated analysis
            var restart = true;
            do
            {
                try
                {
                    // if (mTraceMode)
                    //    clsUtilities.VerifyFolder("Program.Main");

                    ShowTraceMessage("Instantiating clsMainProgram");

                    // Initialize the main execution class
                    var oMainProgram = new clsMainProgram(mTraceMode);
                    var mgrInitSuccess = oMainProgram.InitMgr();
                    if (!mgrInitSuccess)
                    {
                        restart = false;
                    }

                    if (mgrInitSuccess)
                        restart = oMainProgram.PerformMainLoop();

                }
                catch (Exception ex)
                {
                    // Report any exceptions not handled at a lower level to the console
                    LogTools.LogError("Critical exception starting application", ex);
                    clsParseCommandLine.PauseAtConsole(1500);
                    FileLogger.FlushPendingMessages();
                    return 1;
                }
            } while (restart);

            ShowTraceMessage("Exiting application");
            FileLogger.FlushPendingMessages();
            return 0;
        }

        private static void SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false

            var strValidParameters = new[] { "T", "Test", "Trace" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(lstValidParameters))
                {
                    ShowErrorMessage("Invalid commmand line parameters",
                                     (from item in commandLineParser.InvalidParameters(lstValidParameters) select "/" + item).ToList());

                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.IsParameterPresent("T"))
                {
                    mCodeTestMode = true;
                }

                if (commandLineParser.IsParameterPresent("Test"))
                {
                    mCodeTestMode = true;
                }

                if (commandLineParser.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters", ex);
                return false;
            }
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                var exeName = Path.GetFileName(PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath());

                Console.WriteLine("This program processes DMS datasets for PRISM. " +
                                  "Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine +
                                  System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) +
                                  " [/EL] [/T] [/Test] [/Trace]");
                Console.WriteLine();

                Console.WriteLine("Use /EL to post a test message to the Windows Event Log named 'DMSCapTaskMgr' then exit the program. " +
                                  "When setting up the Capture Task Manager on a new computer, you should call this command once from a Windows Command Prompt that you started using 'Run as Administrator'");
                Console.WriteLine();
                Console.WriteLine("Use /T or /Test to start the program in code test mode.");
                Console.WriteLine();
                Console.WriteLine("Use /Trace to enable trace mode");
                Console.WriteLine();

                Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine("This is version " + PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://www.sysbio.org/resources/staff/");
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Error displaying the program syntax: " + ex.Message);
            }
        }

        private static void ShowTraceMessage(string message)
        {
            if (mTraceMode)
                clsMainProgram.ShowTraceMessage(message);
        }

        #endregion
    }
}