
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
		string m_SvrFileToUpdate = string.Empty;		// File, including path on storage server, needing copied
		string m_SambaFileToUpdate = string.Empty;	// File, including path, in archive (for rename operation)
		bool m_RenameFlag = false;			// Flag specifying if there's alread a file in the archive that needs renamed
		string m_SvrDSNamePath = null;		// Full path to dataset on storage server
		bool m_CopySuccess = false;			// Set to true once the file is successfully copied to the archive
		#endregion

		#region "Properties"

		/// <summary>
		/// True if the file is successfully copied to the archive
		/// </summary>
		public bool CopySuccess
		{
			get { return m_CopySuccess; }
			set { m_CopySuccess = value; }
		}

		/// <summary>
		/// Full file name and path of file on storage server
		/// </summary>
		public string SvrFileToUpdate
		{
			get { return m_SvrFileToUpdate; }
			set { m_SvrFileToUpdate = value; }
		}

		/// <summary>
		/// Full name and path of file on Samba share
		/// </summary>
		public string SambaFileToUpdate
		{
			get { return m_SambaFileToUpdate; }
			set { m_SambaFileToUpdate = value; }
		}

		/// <summary>
		/// Flag to determine if renaming file in archive is necessary
		/// </summary>
		public bool RenameFlag
		{
			get { return m_RenameFlag; }
			set { m_RenameFlag = value; }
		}

		/// <summary>
		/// Dataset name and path on storage server
		/// </summary>
		public string SvrDsNamePath
		{
			get { return m_SvrDSNamePath; }
			set { m_SvrDSNamePath = value; }
		}

		/// <summary>
		/// Relative file path (remove parent folder)
		/// </summary>
		public string RelativeFilePath
		{
			get { return GetRelativeFilePath(m_SvrFileToUpdate, m_SvrDSNamePath); }
		}

		/// <summary>
		/// Name of the file (no path info)
		/// </summary>
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
			return InpFile.Replace(SvrDSNamePath, string.Empty);
		}
		#endregion

	}	// End class
}	// End namespace
