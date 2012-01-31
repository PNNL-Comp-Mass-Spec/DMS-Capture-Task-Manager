using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	class clsCodeTest
	{
		public void TestConnection()
		{
			int iIterations = 0;

			Console.WriteLine("Code test mode");

			System.Collections.Generic.Dictionary<string, string> lstCredentials = new System.Collections.Generic.Dictionary<string, string>();

			string sShareFolderPath = @"\\15T_FTICR.bionet\ProteomicsData";

			// Make sure sShareFolderPath does not end in a back slash
			if (sShareFolderPath.EndsWith(@"\"))
				sShareFolderPath = sShareFolderPath.Substring(0, sShareFolderPath.Length - 1);

			lstCredentials.Add("ftms", "PasswordHere");
			lstCredentials.Add("lcmsoperator", "PasswordHere");

			try
			{
				Dictionary<string, string>.Enumerator enumCurrent = lstCredentials.GetEnumerator();

				while (enumCurrent.MoveNext())
				{

					System.Net.NetworkCredential accessCredentials;
					accessCredentials = new System.Net.NetworkCredential(enumCurrent.Current.Key, enumCurrent.Current.Value, "");

					Console.WriteLine("Credentials created for " + enumCurrent.Current.Key);

					NetworkConnection cnBionet = new NetworkConnection(sShareFolderPath, accessCredentials);

					Console.WriteLine("Connected to share");

					System.IO.DirectoryInfo diDirectory = new System.IO.DirectoryInfo(sShareFolderPath);

					Console.WriteLine("Instantiated DirectoryInfo object: " + diDirectory.FullName);

					iIterations = 0;
					foreach (System.IO.FileInfo fiFile in diDirectory.GetFiles())
					{
						Console.WriteLine("File: " + fiFile.Name + " size " + fiFile.Length + " bytes");
						++iIterations;

						if (iIterations > 20)
							break;
					}

					Console.WriteLine("Files Done");

					iIterations = 0;
					foreach (System.IO.DirectoryInfo diFolder in diDirectory.GetDirectories())
					{
						Console.WriteLine("Folder: " + diFolder.Name + " modified " + diFolder.LastWriteTime.ToString());
						++iIterations;

						if (iIterations > 20)
							break;
					}

					Console.WriteLine("Folders Done");

					// Disconnect network connection
					cnBionet.Dispose();
					cnBionet = null;				

				}

			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception: " + ex.Message);
			}

		}

	}
}
