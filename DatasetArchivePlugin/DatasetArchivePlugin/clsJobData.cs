
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//*********************************************************************************************************
using System.IO;

namespace DatasetArchivePlugin
{
	class clsJobData
	{
		//*********************************************************************************************************
		//Class to hold data for each archive update task
		//**********************************************************************************************************

		#region "Class variables"
			string m_SvrFileToUpdate = "";	//File, including path on storage server, needing copied
			string m_SambaFileToUpdate = "";	//File, including path, in archive (for rename operation)
			bool m_RenameFlag = false;	//Flag specifying if there's alread a file in the archive that needs renamed
			string m_SvrDSNamePath = null;	//Full path to dataset on storage server
		#endregion

		#region "Properties"
			public string SvrFileToUpdate
			{
				//Full file name and path of file on storage server
				get { return m_SvrFileToUpdate; }
				set { m_SvrFileToUpdate = value; }
			}

			public string SambaFileToUpdate
			{
				//Full name and path of file on Samba share
				get { return m_SambaFileToUpdate; }
				set { m_SambaFileToUpdate = value; }
			}

			public bool RenameFlag
			{
				//Flag to determine if renaming file in archive is necessary
				get { return m_RenameFlag; }
				set { m_RenameFlag = value; }
			}

			public string SvrDsNamePath
			{
				//Dataset name and path on storage server
				get { return m_SvrDSNamePath; }
				set { m_SvrDSNamePath = value; }
			}

			public string RelativeFilePath
			{
				//File path relative to dataset folder
				get { return GetRelativeFilePath(m_SvrFileToUpdate, m_SvrDSNamePath); }
			}

			public string FileName
			{
				get { return Path.GetFileName(m_SvrFileToUpdate); }
			}
		#endregion

		#region "Methods"
			/// <summary>
			/// Converts a full dataset file path to a path relative to the dataset folder
			/// </summary>
			/// <param name="InpFile">Full name and path to file on storage server</param>
			/// <param name="SvrDSNamePath">Full path to dataset on storage server</param>
			/// <returns>Path for file input file relative to dataset folder</returns>
			private string GetRelativeFilePath(string InpFile, string SvrDSNamePath)
			{
				return InpFile.Replace(SvrDSNamePath, "");
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
