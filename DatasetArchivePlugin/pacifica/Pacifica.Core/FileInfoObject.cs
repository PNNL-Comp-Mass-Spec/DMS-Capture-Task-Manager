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

		public FileInfoObject(string absoluteLocalFullPath, string relativeDestinationDirectory, string sha1Hash = "")
		{
			this.AbsoluteLocalPath = absoluteLocalFullPath;
			this._relativeDestinationDirectory = relativeDestinationDirectory;
			this.File = new FileInfo(this.AbsoluteLocalPath);
			if (sha1Hash != string.Empty && sha1Hash.Length == 40)
			{
				this.Sha1HashHex = sha1Hash;
			}
			else
			{
				this.Sha1HashHex = Utilities.GenerateSha1Hash(AbsoluteLocalPath);
			}
		}

		#endregion

		#region Private Members

		private FileInfo File { get; set; }

		#endregion

		#region IFileInfoObject Members

		public string AbsoluteLocalPath { get; private set; }

		private string _relativeDestinationDirectory;
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

		public string _desinationFileName;
		public string DestinationFileName
		{
			get
			{
				return _desinationFileName;
			}
			set
			{
				_desinationFileName = value;
			}
		}

		public string FileName
		{
			get
			{
				return File.Name;
			}
		}

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

		/*
		 * Code deprecated in August 2013
		 * 
			public string SerializeData()
			{
				//JsonObject jso = new JsonObject(this.SerializeToDictionaryObject());
				//return jso.ToString();
				return SerializeToJson();
			}

			public string SerializeToJson()
			{
				JsonObject jso = new JsonObject();
				jso.Put("sha1Hash", this.Sha1HashHex);
				jso.Put("destinationDirectory", this.RelativeDestinationDirectory);
				jso.Put("localFilePath", this.AbsoluteLocalPath);
				jso.Put("fileName", this.FileName);
				jso.Put("sizeInBytes", this.File.Length);
				jso.Put("creationDate", this.CreationTimeStamp);

				return jso.ToString();
			}
		 * 
		*/


		public Dictionary<string, object> SerializeToDictionaryObject()
		{
			Dictionary<string, object> d = new Dictionary<string, object>();

			d.Add("sha1Hash", this.Sha1HashHex);
			d.Add("destinationDirectory", this.RelativeDestinationDirectory);
			d.Add("localFilePath", this.AbsoluteLocalPath);
			d.Add("fileName", this.FileName);
			d.Add("sizeInBytes", this.File.Length);
			d.Add("creationDate", this.CreationTimeStamp);

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