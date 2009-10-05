
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

		#region "Class variables"
			private static clsMainProgram m_MainProgram;
		#endregion

		#region "Methods"
			/// <summary>
			/// The main entry point for the application.
			/// </summary>
			[STAThread]
			static void Main()
			{
				bool restart = false;
				do
				{
					try
					{
						if (m_MainProgram == null)
						{
							//Initialize the main execution class
							m_MainProgram = new clsMainProgram();
							if (!m_MainProgram.InitMgr())
							{
								return;
							}
						}
						restart = m_MainProgram.PerformMainLoop();
						m_MainProgram = null;
					}
					catch (Exception ex)
					{
						string errMsg = "Critical exception starting application";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.FATAL, errMsg, ex);
						return;
					}
				} while (restart);
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
