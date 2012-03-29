
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/19/2009
//
// Last modified 10/19/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DatasetArchivePlugin
{
	public interface IArchiveOps
	{
		//*********************************************************************************************************
		// Interface for archive or archive update classes
		//**********************************************************************************************************

		#region "Properties"
			/// <summary>
			/// Error message from archive ops result
			/// </summary>
			string ErrMsg { get; }
		#endregion

		#region "Methods"
			/// <summary>
			/// Performs an archive or update operation
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			bool PerformTask();
		#endregion

			#region "Event Delegates and Classes"

			event MyEMSLUploadEventHandler MyEMSLUploadComplete;

			#endregion

	}	// End interface

}	// End namespace
