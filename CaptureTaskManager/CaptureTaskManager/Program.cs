// -------------------------------------------------------------------------------
// Written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2009
//
// E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
// Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/
// -------------------------------------------------------------------------------
//
// Licensed under the 2-Clause BSD License; you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
// https://opensource.org/licenses/BSD-2-Clause
//
// Copyright 2018 Battelle Memorial Institute

using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CaptureTaskManager
{
    /// <summary>
    /// Application entry class
    /// </summary>
    static class Program
    {
        private const string PROGRAM_DATE = "May 7, 2019";

        private static bool mCodeTestMode;
        private static bool mTraceMode;
        private static bool mShowVersionOnly;

        #region "Methods"

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <returns>0 if no error; error code if an error</returns>
        /// <remarks>The STAThread attribute is required for OxyPlot functionality</remarks>
        [STAThread]
        static int Main()
        {

            var commandLineParser = new clsParseCommandLine();

            mCodeTestMode = false;
            mTraceMode = false;
            mShowVersionOnly = false;

            var osVersionInfo = new OSVersionInfo();

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
                        ConsoleMsgUtils.ShowWarning("Error parsing the command line arguments");
                        clsParseCommandLine.PauseAtConsole(750);
                    }
                    return -1;
                }
            }

            if (commandLineParser.NeedToShowHelp || !validArgs)
            {
                ShowProgramHelp();
                return -1;
            }

            ShowTrace("Command line arguments parsed");

            if (mShowVersionOnly)
            {
                DisplayVersion();
                ProgRunner.SleepMilliseconds(500);
                return 0;
            }

            // Note: CodeTestMode is enabled using command line switch /T
            if (mCodeTestMode)
            {
                try
                {
                    ShowTrace("Code test mode enabled");

                    var testHarness = new clsCodeTest();

                    testHarness.TestConnection();

                }
                catch (Exception ex)
                {
                    ShowErrorMessage("Exception calling clsCodeTest", ex);
                    return -1;
                }

                ShowTrace("Exiting application");

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

                    ShowTrace("Instantiating clsMainProgram");

                    // Initialize the main execution class
                    var mainProcess = new clsMainProgram(mTraceMode);
                    var mgrInitSuccess = mainProcess.InitMgr();
                    if (!mgrInitSuccess)
                    {
                        restart = false;
                    }

                    if (mgrInitSuccess)
                    {
                        restart = mainProcess.PerformMainLoop();
                    }
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

            ShowTrace("Exiting application");
            FileLogger.FlushPendingMessages();
            return 0;
        }

        private static void DisplayVersion()
        {
            Console.WriteLine();
            Console.WriteLine("DMS Capture Task Manager");
            Console.WriteLine("Version " + GetAppVersion(PROGRAM_DATE));
            Console.WriteLine("Host    " + System.Net.Dns.GetHostName());
            Console.WriteLine("User    " + Environment.UserName);
            Console.WriteLine();

            DisplayOSVersion();
        }

        private static void DisplayOSVersion()
        {

            try
            {
                // For this to work properly on Windows 10, you must add a app.manifest file
                // and uncomment the versions of Windows listed below
                // <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1">
                //
                // See https://stackoverflow.com/a/36158739/1179467

                var osVersionInfo = new OSVersionInfo();
                var osDescription = osVersionInfo.GetOSVersion();

                Console.WriteLine("OS Version: " + osDescription);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error displaying the OS version", ex);
            }
        }

        /// <summary>
        /// Returns the .NET assembly version followed by the program date
        /// </summary>
        /// <param name="programDate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private static string GetAppVersion(string programDate)
        {
            return PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppVersion(programDate);
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false

            var validParameters = new List<string> {
                "T",
                "Test",
                "Trace",
                "Verbose",
                "Version"
            };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(validParameters))
                {
                    ShowErrorMessage("Invalid command line parameters",
                                     (from item in commandLineParser.InvalidParameters(validParameters) select "/" + item).ToList());

                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.IsParameterPresent("T"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("Test"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("Trace"))
                    mTraceMode = true;

                if (commandLineParser.IsParameterPresent("Verbose"))
                    mTraceMode = true;

                if (commandLineParser.IsParameterPresent("Version"))
                    mShowVersionOnly = true;

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters", ex);
                return false;
            }
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                var exeName = Path.GetFileName(PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath());

                Console.WriteLine("This program processes DMS datasets for PRISM. " +
                                  "Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine +
                                  exeName + " [/T] [/Test] [/Trace] [/Version]");
                Console.WriteLine();

                Console.WriteLine();
                Console.WriteLine("Use /T or /Test to start the program in code test mode.");
                Console.WriteLine();
                Console.WriteLine("Use /Trace or /Verbose to enable trace mode");
                Console.WriteLine();
                Console.WriteLine("Use /Version to see the program version and OS version");
                Console.WriteLine();
                Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine("Version: " + GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/");
                Console.WriteLine();

                Console.WriteLine("Licensed under the 2-Clause BSD License; you may not use this file except in compliance with the License. " +
                                  "You may obtain a copy of the License at https://opensource.org/licenses/BSD-2-Clause");
                Console.WriteLine();

                ProgRunner.SleepMilliseconds(1500);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Error displaying the program syntax: " + ex.Message);
            }
        }

        private static void ShowTrace(string message)
        {
            if (mTraceMode)
                clsMainProgram.ShowTraceMessage(message);
        }

        #endregion
    }
}