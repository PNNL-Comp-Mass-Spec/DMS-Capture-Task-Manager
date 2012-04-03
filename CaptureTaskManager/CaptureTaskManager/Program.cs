
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
using System.Windows.Forms;

namespace CaptureTaskManager
{
	static class Program
	{
		//*********************************************************************************************************
		// Application startup program
		//**********************************************************************************************************

		public const string PROGRAM_DATE = "January 30, 2012";

		private static bool mCodeTestMode = false;

		#region "Methods"

			/// <summary>
			/// The main entry point for the application.
			/// </summary>
			[STAThread]
			static void Main()
			{
				bool restart = false;

				FileProcessor.clsParseCommandLine objParseCommandLine = new FileProcessor.clsParseCommandLine();

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
				else
				{
					// Note: CodeTestMode is enabled using command line switch /T
					if (mCodeTestMode)
					{

						try
						{
							clsCodeTest oCodeTest = new clsCodeTest();

							oCodeTest.TestConnection();

							return;

						}
						catch (Exception ex)
						{
							Console.WriteLine("Exception calling clsCodeTest: " + ex.Message);
						}

					}
					else
					{
						// Initiate automated analysis


						clsMainProgram oMainProgram;

						do
						{
							try
							{
								//Initialize the main execution class
								oMainProgram = new clsMainProgram();
								if (!oMainProgram.InitMgr())
								{
									return;
								}

								restart = oMainProgram.PerformMainLoop();

								oMainProgram = null;
							}
							catch (Exception ex)
							{
								string errMsg = "Critical exception starting application";
								Console.WriteLine("===============================================================");
								Console.WriteLine(errMsg);
								Console.WriteLine("===============================================================");								

								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.FATAL, errMsg, ex);
								System.Threading.Thread.Sleep(500);

								return;
							}
						} while (restart);

					}

				}

			}	// End sub

			private static bool SetOptionsUsingCommandLineParameters(FileProcessor.clsParseCommandLine objParseCommandLine)
			{
				// Returns True if no problems; otherwise, returns false

				string strValue = string.Empty;
				string[] strValidParameters = new string[] {"T"};

				try
				{
					// Make sure no invalid parameters are present
					if (objParseCommandLine.InvalidParametersPresent(strValidParameters))
					{
						return false;
					}
					else
					{
						{
							// Query objParseCommandLine to see if various parameters are present
							if (objParseCommandLine.IsParameterPresent("T"))
							{
								mCodeTestMode = true;
							}					
						}

						return true;
					}

				}
				catch (Exception ex)
				{
					Console.WriteLine("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
				}

				return false;
			}


			private static void ShowProgramHelp()
			{

				try
				{
					Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.");
					Console.WriteLine();
					Console.WriteLine("Program syntax:" + Environment.NewLine + System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) + " [/T]");
					Console.WriteLine();

					Console.WriteLine("Use /T to start the program in code test mode.");
					Console.WriteLine();

					Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
					Console.WriteLine();

					Console.WriteLine("This is version " + System.Windows.Forms.Application.ProductVersion + " (" + PROGRAM_DATE + ")");
					Console.WriteLine();

					Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
					Console.WriteLine("Website: http://ncrr.pnnl.gov/ or http://www.sysbio.org/resources/staff/");
					Console.WriteLine();


					// Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
					System.Threading.Thread.Sleep(750);

				}
				catch (Exception ex)
				{
					Console.WriteLine("Error displaying the program syntax: " + ex.Message);
				}

			}


		#endregion
	}	// End class
}	// End namespace
