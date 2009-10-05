
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 009/10/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	class clsStatusData
	{
		//*********************************************************************************************************
		//Class to hold long-term data for status reporting. This is a hack to avoid adding an instance of the
		//	status file class to the log tools class
		//**********************************************************************************************************

		#region "Class variables"
			private static string m_MostRecentLogMessage;
			private static Queue<string> m_ErrorQueue = new Queue<string>();
		#endregion

		#region "Properties"
			public static string MostRecentLogMessage
			{
				get
				{
					return m_MostRecentLogMessage;
				}
				set
				{
					//Filter out routine startup and shutdown messages
					if (value.Contains("=== Started") || (value.Contains("===== Closing")))
					{
						//Do nothing
					}
					else
					{
						m_MostRecentLogMessage=value;
					}
				}
			}

			public static Queue<string> ErrorQueue
			{
				get
				{
					return m_ErrorQueue;
				}
			}
		#endregion

		#region "Methods"
			public static void AddErrorMessage(string ErrMsg)
			{
				//Add the most recent error message
				m_ErrorQueue.Enqueue(ErrMsg);

				//If there are > 4 entries in the queue, then delete the oldest ones
				string dumStr;
				if (m_ErrorQueue.Count > 4)
				{
					while (m_ErrorQueue.Count > 4)
					{
						dumStr = m_ErrorQueue.Dequeue();
					}
				}
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
