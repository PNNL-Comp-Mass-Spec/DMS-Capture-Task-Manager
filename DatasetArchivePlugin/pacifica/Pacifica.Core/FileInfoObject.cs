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

		public FileInfoObject(string absoluteLocalFullPath, string relativeDestinationDirectory,
				string sha1Hash = "") {
			this.AbsoluteLocalPath = absoluteLocalFullPath;
			this._relativeDestinationDirectory = relativeDestinationDirectory;
			this.File = new FileInfo(this.AbsoluteLocalPath);
			if (sha1Hash != string.Empty && sha1Hash.Length == 40) {
				this.Sha1HashHex = sha1Hash;
			}
			else {
				this.Sha1HashHex = FileInfoObject.GenerateSha1Hash(AbsoluteLocalPath);
			}
		}

		#endregion

		#region Private Members
		
		private FileInfo File { get; set; }

		#endregion

		#region IFileInfoObject Members

		public string AbsoluteLocalPath { get; private set; }

		private string _relativeDestinationDirectory;
		public string RelativeDestinationDirectory {
			get {
				return this._relativeDestinationDirectory.Replace("\\", "/").TrimStart(new char[] { '/' });
			}
			private set {
				this._relativeDestinationDirectory = value;
			}
		}

		private string _relativeDestinationFullPath;
		public string RelativeDestinationFullPath {
			get {
				string fileName;
				if (!string.IsNullOrWhiteSpace(DestinationFileName)) {
					fileName = DestinationFileName;
				}
				else {
					fileName = FileName;
				}
				return Path.Combine(this.RelativeDestinationDirectory,
						fileName).Replace("\\", "/").TrimStart(new char[] { '/' });
			}
			set {
				_relativeDestinationFullPath = value;
			}
		}

		public string _desinationFileName;
		public string DestinationFileName {
			get {
				return _desinationFileName;
			}
			set {
				_desinationFileName = value;
			}
		}

		public string FileName {
			get {
				return File.Name;
			}
		}

		public string Sha1HashHex {
			get;
			private set;
		}

		public long FileSizeInBytes {
			get {
				return File.Length;
			}
		}

		public DateTime CreationTime {
			get {
				return File.CreationTime;
			}
		}

		private DateTime _submittedTime = DateTime.Now;
		public DateTime SubmittedTime {
			get {
				return _submittedTime;
			}
		}

		public string CreationTimeStamp {
			get {
				return this.File.CreationTime.ToUnixTime().ToString();
			}
		}

		public string SubmittedTimeStamp {
			get {
				return this.SubmittedTime.ToUnixTime().ToString();
			}
		}

		public string SerializeData() {
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


		public IDictionary SerializeToDictionaryObject() {
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

		#region Static Methods

		public static string GenerateSha1Hash(string fullFilePath) {
			string hashString = string.Empty;

			FileInfo fi = new FileInfo(fullFilePath);
			if (fi.Exists) {
				SHA1Managed hashProvider = new SHA1Managed();
				byte[] fileHash = hashProvider.ComputeHash(new System.IO.FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
				//byte[] fileHash = hashProvider.ComputeHash(fi.OpenRead());
				hashString = Utilities.ToHexString(fileHash);
			}

			return hashString;
		}

		//public override string ToString() {
		//  var s = this.SerializeToJson();
		//  return s;//this.SerializeToJson();
		//}

		public static string GenerateRelativePath(string absoluteLocalPath, string basePath) {
			return absoluteLocalPath.Replace(basePath, "");
		}

		#endregion
	}
}