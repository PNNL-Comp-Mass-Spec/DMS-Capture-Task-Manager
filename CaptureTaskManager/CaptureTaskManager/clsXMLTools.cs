
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/01/2009
//
// Last modified 10/01/2009
//*********************************************************************************************************
using System;
using System.Xml;
using System.Collections.Specialized;

namespace CaptureTaskManager
{
	class clsXMLTools
	{
		//*********************************************************************************************************
		// Tools for parsing input XML
		//**********************************************************************************************************

		#region "Methods"
			/// <summary>
			/// Converts command XML string into a dictionary of strings (future)
			/// </summary>
			/// <param name="InputXML">XML string to parse</param>
			/// <returns>String dictionary of command sections</returns>
			public static StringDictionary ParseCommandXML(string InputXML)
			{
				StringDictionary returnDict = new StringDictionary();

				XmlDocument doc = new XmlDocument();
				doc.LoadXml(InputXML);

				try
				{
					returnDict.Add("package", doc.SelectSingleNode("//package").InnerText);
					returnDict.Add("local", doc.SelectSingleNode("//local").InnerText);
					returnDict.Add("share", doc.SelectSingleNode("//share").InnerText);
					returnDict.Add("year", doc.SelectSingleNode("//year").InnerText);
					returnDict.Add("team", doc.SelectSingleNode("//team").InnerText);
					returnDict.Add("folder", doc.SelectSingleNode("//folder").InnerText);
					returnDict.Add("cmd", doc.SelectSingleNode("//cmd").InnerText);

					return returnDict;
				}
				catch (Exception Ex)
				{
					throw new Exception("", Ex);	// Message parameter left blank because it is handled at higher level
				}
			}	// End sub

			/// <summary>
			/// Converts broadcast XML string into a dictionary of strings
			/// </summary>
			/// <param name="InputXML">XML string to parse</param>
			/// <returns>String dictionary of broadcast sections</returns>
			public static clsBroadcastCmd ParseBroadcastXML(string InputXML)
			{
				clsBroadcastCmd returnedData = new clsBroadcastCmd();

				try
				{
					XmlDocument doc = new XmlDocument();
					doc.LoadXml(InputXML);

					// Get list of managers this command applies to
					foreach (XmlNode xn in doc.SelectNodes("//Managers/*"))
					{
						returnedData.MachineList.Add(xn.InnerText);
					}

					// Get command contained in message
					returnedData.MachCmd = doc.SelectSingleNode("//Message").InnerText;

					// Return the parsing results
					return returnedData;
				}
				catch (Exception Ex)
				{
					throw new Exception("Exception while parsing broadcast string", Ex);
				}
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
