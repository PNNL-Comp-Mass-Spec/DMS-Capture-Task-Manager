
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//
// Last modified 09/15/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	public interface ITaskParams
	{
		//*********************************************************************************************************
		// Interface for step task parameters
		//**********************************************************************************************************

		#region "Methods"
			string GetParam(string name);
			bool AddAdditionalParameter(string paramName, string paramValue);
			void SetParam(string keyName, string value);
		#endregion
	}	// End interface
}	// End namespace
