//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using PRISM;

namespace CaptureTaskManager
{
    /// <summary>
    /// Application entry class
    /// </summary>
    static class Program
    {
        private const string PROGRAM_DATE = "October 24, 2017";

        private static bool mCodeTestMode;
        private static bool mCreateEventLog;
        private static bool mTraceMode;

        #region "Methods"

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <remarks>The STAThread attribute is required for OxyPlot functionality</remarks>
        [STAThread]
        static void Main()
        {
            var restart = false;

            var objParseCommandLine = new clsParseCommandLine();

            // Look for /T or /Test on the command line
            // If present, this means "code test mode" is enabled
            if (objParseCommandLine.ParseCommandLine())
            {
                SetOptionsUsingCommandLineParameters(objParseCommandLine);
            }

            if (objParseCommandLine.NeedToShowHelp)
            {
                ShowProgramHelp();
                return;
            }

            // Note: CodeTestMode is enabled using command line switch /T
            if (mCodeTestMode)
            {
                try
                {
                    var oCodeTest = new clsCodeTest();

                    oCodeTest.TestConnection();

                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(@"Exception calling clsCodeTest: " + ex.Message);
                }
                return;
            }
            // Initiate automated analysis

            do
            {
                try
                {
                    if (mTraceMode) clsUtilities.VerifyFolder("Program.Main");

                    // Initialize the main execution class
                    var oMainProgram = new clsMainProgram(mTraceMode);
                    var mgrInitSuccess = oMainProgram.InitMgr();
                    if (!mgrInitSuccess)
                    {
                        restart = false;
                    }

                    if (mCreateEventLog && (mgrInitSuccess || (oMainProgram.ManagerDeactivatedLocally)))
                    {
                        oMainProgram.PostTestLogMessage();
                        restart = false;
                    }
                    else
                    {
                        if (mgrInitSuccess)
                            restart = oMainProgram.PerformMainLoop();
                    }
                }
                catch (System.Security.SecurityException ex)
                {
                    const string errMsg = "Security exception";

                    Console.WriteLine();
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine(errMsg + @": " + ex.Message);
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine();
                    Console.WriteLine(
                        @"You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " +
                        clsMainProgram.CUSTOM_LOG_NAME + @" application log");
                    Console.WriteLine();
                }
                catch (Exception ex)
                {
                    const string errMsg = "Critical exception starting application";

                    Console.WriteLine();
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine(errMsg + @": " + ex.Message);
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine();

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.FATAL, errMsg, ex);
                    System.Threading.Thread.Sleep(500);

                    return;
                }
            } while (restart);

            if (mTraceMode)
                Console.WriteLine("Exiting");
        }

        private static void SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false

            var strValidParameters = new[] {"T", "EL", "Test", "Trace"};

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(strValidParameters))
                {
                    return;
                }

                // Query objParseCommandLine to see if various parameters are present
                if (objParseCommandLine.IsParameterPresent("T"))
                {
                    mCodeTestMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("Test"))
                {
                    mCodeTestMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("EL"))
                {
                    mCreateEventLog = true;
                }

                if (objParseCommandLine.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }
        }

        private static void ShowProgramHelp()
        {
            try
            {
                Console.WriteLine(
                    @"This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine(@"Program syntax:" + Environment.NewLine +
                                  System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) +
                                  @" [/EL] [/T] [/Test] [/Trace]");
                Console.WriteLine();

                Console.WriteLine(@"Use /EL to post a test message to the Windows Event Log named 'DMSCapTaskMgr' then exit the program. " +
                                  "When setting up the Capture Task Manager on a new computer, you should call this command once from a Windows Command Prompt that you started using 'Run as Administrator'");
                Console.WriteLine();
                Console.WriteLine(@"Use /T or /Test to start the program in code test mode.");
                Console.WriteLine();
                Console.WriteLine(@"Use /Trace to enable trace mode");
                Console.WriteLine();

                Console.WriteLine(
                    @"Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine(@"This is version " + System.Windows.Forms.Application.ProductVersion + @" (" +
                                  PROGRAM_DATE + @")");
                Console.WriteLine();

                Console.WriteLine(@"E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine(@"Website: http://panomics.pnnl.gov/ or http://www.sysbio.org/resources/staff/");
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Error displaying the program syntax: " + ex.Message);
            }
        }

        #endregion
    }
}