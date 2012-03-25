
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
using System.Collections.Specialized;

namespace CaptureTaskManager
{
	public interface ITaskParams
	{
		//*********************************************************************************************************
		// Interface for step task parameters
		//**********************************************************************************************************

		#region "Properties"
		System.Collections.Generic.Dictionary<string, string> TaskDictionary { get; }
		#endregion

		#region "Methods"
		string GetParam(string name);
		string GetParam(string name, string valueIfMissing);
		bool AddAdditionalParameter(string paramName, string paramValue);
		void SetParam(string keyName, string value);
		#endregion
	}	// End interface
}	// End namespace
