using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Jayrock.Json;

namespace Pacifica.Core
{
	public class FileInfoObject : IFileInfoObject
	{
		#region Constructor

		/// <summary>
		/// Instantiate a new FileInfoObject, including computing the Sha-1 hash of the file
		/// </summary>
		/// <param name="absoluteLocalFullPath">Full path to the local file</param>
		/// <param name="baseDSPath">Base dataset folder path</param>
		public FileInfoObject(string absoluteLocalFullPath, string baseDSPath)
		{
			this.AbsoluteLocalPath = absoluteLocalFullPath;
			this.File = new FileInfo(this.AbsoluteLocalPath);
			this._relativeDestinationDirectory = GenerateRelativePath(File.Directory.FullName, baseDSPath);

			this.Sha1HashHex = Utilities.GenerateSha1Hash(AbsoluteLocalPath);

		}

		/// <summary>
		/// Instantiate a new FileInfoObject; auto-computes the Sha-1 hash if sha1Hash is blank or is not exactly 40 characters long
		/// </summary>
		/// <param name="absoluteLocalFullPath">Full path to the local file</param>
		/// <param name="relativeDestinationDirectory">Folder in archive in which to store the file; empty string means to store in the dataset folder</param>
		/// <param name="sha1Hash">Sha-1 hash for the file; if blank then the has will be auto-computed</param>
		public FileInfoObject(string absoluteLocalFullPath, string relativeDestinationDirectory, string sha1Hash = "")
		{
			this.AbsoluteLocalPath = absoluteLocalFullPath;
			this.File = new FileInfo(this.AbsoluteLocalPath);
			this._relativeDestinationDirectory = relativeDestinationDirectory;

			if (!string.IsNullOrWhiteSpace(sha1Hash) && sha1Hash.Length == 40)
				this.Sha1HashHex = sha1Hash;
			else
				this.Sha1HashHex = Utilities.GenerateSha1Hash(AbsoluteLocalPath);
		}

		#endregion

		#region Private Members

		private FileInfo File { get; set; }

		#endregion

		#region IFileInfoObject Members

		public string AbsoluteLocalPath
		{
			get;
			private set;
		}

		private string _relativeDestinationDirectory;

		/// <summary>
		/// Relative destination directory, with Unix-style slashes
		/// </summary>
		public string RelativeDestinationDirectory
		{
			get
			{
				return ConvertWindowsPathToUnix(this._relativeDestinationDirectory);
			}
			private set
			{
				this._relativeDestinationDirectory = value;
			}
		}

		private string _relativeDestinationFullPath;
		public string RelativeDestinationFullPath
		{
			get
			{
				string fileName;
				if (!string.IsNullOrWhiteSpace(DestinationFileName))
				{
					fileName = DestinationFileName;
				}
				else
				{
					fileName = FileName;
				}
				string fullPath = Path.Combine(this.RelativeDestinationDirectory, fileName);
				return ConvertWindowsPathToUnix(fullPath);
			}
			set
			{
				_relativeDestinationFullPath = value;
			}
		}

		public string _destinationFileName;
		public string DestinationFileName
		{
			get
			{
				return _destinationFileName;
			}
			set
			{
				_destinationFileName = value;
			}
		}

		public string FileName
		{
			get
			{
				return File.Name;
			}
		}

		/// <summary>
		/// Sha-1 hash of the file
		/// </summary>
		public string Sha1HashHex
		{
			get;
			private set;
		}

		public long FileSizeInBytes
		{
			get
			{
				return File.Length;
			}
		}

		public DateTime CreationTime
		{
			get
			{
				return File.CreationTime;
			}
		}

		private DateTime _submittedTime = DateTime.Now;
		public DateTime SubmittedTime
		{
			get
			{
				return _submittedTime;
			}
		}

		public string CreationTimeStamp
		{
			get
			{
				return this.File.CreationTime.ToUnixTime().ToString();
			}
		}

		public string SubmittedTimeStamp
		{
			get
			{
				return this.SubmittedTime.ToUnixTime().ToString();
			}
		}

		public Dictionary<string, string> SerializeToDictionaryObject()
		{
			var d = new Dictionary<string, string>();

			d.Add("sha1Hash", this.Sha1HashHex);
			d.Add("destinationDirectory", this.RelativeDestinationDirectory);			// Reported as "subDir" by the MyEMSL Elastic Search
			d.Add("fileName", this.FileName);

			return d;
		}

		#endregion

		#region Methods

		/// <summary>
		/// Converts a windows path of the form \\proto-7\VOrbi05\2013_2\QC_Shew_13_02_500ng_15May13_Lynx_12-12-04\metadata.xml
		/// to the unix form proto-7/VOrbi05/2013_2/QC_Shew_13_02_500ng_15May13_Lynx_12-12-04/metadata.xml
		/// </summary>
		/// <param name="path">Unix-style path</param>
		/// <returns></returns>
		/// <remarks>Removes any leading slashes</remarks>
		protected string ConvertWindowsPathToUnix(string path)
		{
			return path.Replace(@"\", "/").TrimStart(new char[] { '/' });
		}
		#endregion

		#region Static Methods

		public static string GenerateRelativePath(string absoluteLocalPath, string basePath)
		{
			if (absoluteLocalPath.ToLower().StartsWith(basePath.ToLower()))
				return absoluteLocalPath.Substring(basePath.Length);
			else
				throw new InvalidDataException("Cannot generate relative path in GenerateRelativePath since local path (" + absoluteLocalPath + ") does not contain base path (" + basePath + ")");

			// Oldreturn absoluteLocalPath.Replace(basePath, "");
		}

		#endregion
	}
}