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
using System.Threading;

namespace CaptureTaskManager
{
    /// <summary>
    /// Application entry class
    /// </summary>
    static class Program
    {
        private const string PROGRAM_DATE = "June 15, 2020";

        private static bool mTraceMode;

        #region "Methods"

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <returns>0 if no error; error code if an error</returns>
        /// <remarks>The STAThread attribute is required for OxyPlot functionality</remarks>
        [STAThread]
        static int Main(string[] args)
        {
            mTraceMode = false;

            var osVersionInfo = new OSVersionInfo();

            var osVersion = osVersionInfo.GetOSVersion();
            if (osVersion.IndexOf("windows", StringComparison.OrdinalIgnoreCase) < 0)
            {
                // Running on Linux
                // Auto-enable offline mode
                clsUtilities.EnableOfflineMode(true);
            }

            var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name);

            var cmdLineParser = new CommandLineParser<CommandLineOptions>(exeName, GetAppVersion(PROGRAM_DATE))
            {
                ProgramInfo = "This program processes DMS datasets for PRISM. " +
                              "Normal operation is to run the program without any command line switches.",
                ContactInfo =
                    "Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                    Environment.NewLine +
                    "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                    "Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/" + Environment.NewLine + Environment.NewLine +
                    "Licensed under the 2-Clause BSD License; you may not use this file except in compliance with the License.  " +
                    "You may obtain a copy of the License at https://opensource.org/licenses/BSD-2-Clause"
            };

            var parsed = cmdLineParser.ParseArgs(args, false);
            var options = parsed.ParsedResults;
            if (args.Length > 0 && !parsed.Success)
            {
                // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(1500);
                return -1;
            }

            mTraceMode = options.TraceMode;

            ShowTrace("Command line arguments parsed");

            if (options.ShowVersionOnly)
            {
                DisplayVersion();
                ProgRunner.SleepMilliseconds(500);
                return 0;
            }

            // Note: CodeTestMode is enabled using command line switch /T
            if (options.CodeTestMode)
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

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowTrace(string message)
        {
            if (mTraceMode)
                clsMainProgram.ShowTraceMessage(message);
        }

        #endregion
    }
}